/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.connectors.kafka.configuration

import java.io.Closeable
import java.io.File
import java.net.URI
import java.util.concurrent.TimeUnit
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.connect.errors.ConnectException
import org.neo4j.connectors.driver.auth.AuthConfig
import org.neo4j.connectors.driver.auth.AuthTokenManagerRegistry
import org.neo4j.connectors.kafka.configuration.helpers.ConfigUtils
import org.neo4j.connectors.kafka.configuration.helpers.Validators.validateNonEmptyIfVisible
import org.neo4j.connectors.kafka.configuration.helpers.parseSimpleString
import org.neo4j.connectors.kafka.utils.Telemetry.connectorInformation
import org.neo4j.connectors.kafka.utils.Telemetry.userAgent
import org.neo4j.driver.AccessMode
import org.neo4j.driver.AuthTokenManager
import org.neo4j.driver.Bookmark
import org.neo4j.driver.Config
import org.neo4j.driver.Config.TrustStrategy
import org.neo4j.driver.Config.TrustStrategy.Strategy
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.SessionConfig
import org.neo4j.driver.TransactionConfig
import org.neo4j.driver.net.ServerAddress
import org.slf4j.Logger
import org.slf4j.LoggerFactory

enum class ConnectorType(val description: String) {
  SINK("sink"),
  SOURCE("source"),
}

open class Neo4jConfiguration(configDef: ConfigDef, originals: Map<*, *>, val type: ConnectorType) :
    AbstractConfig(configDef, originals), Closeable {
  private val logger: Logger = LoggerFactory.getLogger(Neo4jConfiguration::class.java)

  val database
    get(): String = getString(DATABASE)

  internal val uris
    get(): List<URI> = getList(URI).map { URI(it) }

  internal val connectionTimeout
    get(): Duration = Duration.parseSimpleString(getString(CONNECTION_TIMEOUT))

  internal val maxRetryTime
    get(): Duration = Duration.parseSimpleString(getString(MAX_TRANSACTION_RETRY_TIMEOUT))

  internal val maxConnectionPoolSize
    get(): Int = getInt(POOL_MAX_CONNECTION_POOL_SIZE)

  internal val connectionAcquisitionTimeout
    get(): Duration = Duration.parseSimpleString(getString(POOL_CONNECTION_ACQUISITION_TIMEOUT))

  internal val idleTimeBeforeTest
    get(): Duration =
        getString(POOL_IDLE_TIME_BEFORE_TEST).orEmpty().run {
          if (this.isEmpty()) {
            (-1).milliseconds
          } else {
            Duration.parseSimpleString(this)
          }
        }

  internal val maxConnectionLifetime
    get(): Duration = Duration.parseSimpleString(getString(POOL_MAX_CONNECTION_LIFETIME))

  internal val encrypted
    get(): Boolean = getString(SECURITY_ENCRYPTED).toBoolean()

  internal val certFiles
    get(): List<File> = getList(SECURITY_CERT_FILES).map { File(it) }

  internal val trustStrategy
    get(): TrustStrategy {
      val strategy: TrustStrategy =
          when (ConfigUtils.getEnum<Strategy>(this, SECURITY_TRUST_STRATEGY)) {
            null -> TrustStrategy.trustSystemCertificates()
            Strategy.TRUST_ALL_CERTIFICATES -> TrustStrategy.trustAllCertificates()
            Strategy.TRUST_SYSTEM_CA_SIGNED_CERTIFICATES -> TrustStrategy.trustSystemCertificates()
            Strategy.TRUST_CUSTOM_CA_SIGNED_CERTIFICATES ->
                TrustStrategy.trustCustomCertificateSignedBy(*certFiles.toTypedArray())
          }

      return if (getString(SECURITY_HOST_NAME_VERIFICATION_ENABLED).toBoolean()) {
        strategy.withHostnameVerification()
      } else {
        strategy.withoutHostnameVerification()
      }
    }

  val driver: Driver by lazy {
    val config = Config.builder()

    val uri = uris
    val mainUri = uri.first()
    if (uri.size > 1) {
      config.withResolver { _ ->
        uri.map { ServerAddress.of(it.host, if (it.port == -1) 7687 else it.port) }.toSet()
      }
    }

    config.withUserAgent(userAgent(type.description, userAgentComment()))
    config.withConnectionAcquisitionTimeout(
        connectionAcquisitionTimeout.inWholeMilliseconds,
        TimeUnit.MILLISECONDS,
    )
    config.withConnectionTimeout(connectionTimeout.inWholeMilliseconds, TimeUnit.MILLISECONDS)
    config.withMaxConnectionPoolSize(maxConnectionPoolSize)
    config.withConnectionLivenessCheckTimeout(
        idleTimeBeforeTest.inWholeMilliseconds,
        TimeUnit.MILLISECONDS,
    )
    config.withMaxConnectionLifetime(
        maxConnectionLifetime.inWholeMilliseconds,
        TimeUnit.MILLISECONDS,
    )
    config.withMaxTransactionRetryTime(maxRetryTime.inWholeMilliseconds, TimeUnit.MILLISECONDS)

    if (uri.none { it.scheme.endsWith("+s", true) || it.scheme.endsWith("+ssc", true) }) {
      if (encrypted) {
        config.withEncryption()
        config.withTrustStrategy(trustStrategy)
      } else {
        config.withoutEncryption()
      }
    }

    GraphDatabase.driver(mainUri, createAuthTokenManager(), config.build())
  }

  internal fun createAuthTokenManager(): AuthTokenManager {
    val type = resolveAuthType(getString(AUTHENTICATION_TYPE))
    return try {
      authRegistry.create(type, authConfig(originals(), type))
    } catch (e: IllegalArgumentException) {
      throw ConnectException("Invalid authentication configuration for '$type': ${e.message}", e)
    }
  }

  open fun sessionConfig(vararg bookmarks: Bookmark): SessionConfig {
    val config = SessionConfig.builder()

    if (database.isNotBlank()) {
      config.withDatabase(database)
    }

    if (bookmarks.isNotEmpty()) {
      config.withBookmarks(*bookmarks)
    }

    config.withDefaultAccessMode(
        when (type) {
          ConnectorType.SOURCE -> AccessMode.READ
          ConnectorType.SINK -> AccessMode.WRITE
        }
    )

    return config.build()
  }

  open fun txConfig(
      applyCustomMetadata: MutableMap<String, Any>.() -> Unit = {}
  ): TransactionConfig =
      TransactionConfig.builder()
          .withMetadata(
              buildMap {
                this.applyCustomMetadata()

                this["app"] = connectorInformation(type.description)

                val metadata = telemetryData()
                if (metadata.isNotEmpty()) {
                  this["metadata"] = metadata
                }
              }
          )
          .build()

  open fun telemetryData(): Map<String, Any> = emptyMap()

  open fun userAgentComment(): String = ""

  override fun close() {
    try {
      driver.close()
    } catch (t: Throwable) {
      logger.warn("unable to close driver", t)
    }
  }

  val connectorName
    get(): String = originals()[CONNECTOR_NAME].toString()

  val taskId
    get(): String = originals()[TASK_ID].toString()

  companion object {
    val DEFAULT_MAX_RETRY_DURATION = 30.seconds

    const val URI = "neo4j.uri"

    const val DATABASE = "neo4j.database"

    const val AUTHENTICATION_PREFIX = "neo4j.authentication"
    const val AUTHENTICATION_TYPE = "$AUTHENTICATION_PREFIX.type"
    const val AUTHENTICATION_BASIC_USERNAME = "$AUTHENTICATION_PREFIX.basic.username"
    const val AUTHENTICATION_BASIC_PASSWORD = "$AUTHENTICATION_PREFIX.basic.password"
    const val AUTHENTICATION_BASIC_REALM = "$AUTHENTICATION_PREFIX.basic.realm"
    const val AUTHENTICATION_KERBEROS_TICKET = "$AUTHENTICATION_PREFIX.kerberos.ticket"
    const val AUTHENTICATION_BEARER_TOKEN = "$AUTHENTICATION_PREFIX.bearer.token"
    const val AUTHENTICATION_CUSTOM_SCHEME = "$AUTHENTICATION_PREFIX.custom.scheme"
    const val AUTHENTICATION_CUSTOM_PRINCIPAL = "$AUTHENTICATION_PREFIX.custom.principal"
    const val AUTHENTICATION_CUSTOM_CREDENTIALS = "$AUTHENTICATION_PREFIX.custom.credentials"
    const val AUTHENTICATION_CUSTOM_REALM = "$AUTHENTICATION_PREFIX.custom.realm"

    const val MAX_TRANSACTION_RETRY_TIMEOUT = "neo4j.max-retry-time"

    const val CONNECTION_TIMEOUT = "neo4j.connection-timeout"
    const val POOL_MAX_CONNECTION_POOL_SIZE = "neo4j.pool.max-connection-pool-size"
    const val POOL_CONNECTION_ACQUISITION_TIMEOUT = "neo4j.pool.connection-acquisition-timeout"
    const val POOL_IDLE_TIME_BEFORE_TEST = "neo4j.pool.idle-time-before-connection-test"
    const val POOL_MAX_CONNECTION_LIFETIME = "neo4j.pool.max-connection-lifetime"

    const val SECURITY_ENCRYPTED = "neo4j.security.encrypted"
    const val SECURITY_HOST_NAME_VERIFICATION_ENABLED =
        "neo4j.security.hostname-verification-enabled"
    const val SECURITY_TRUST_STRATEGY = "neo4j.security.trust-strategy"
    const val SECURITY_CERT_FILES = "neo4j.security.cert-files"

    // internal properties
    const val CONNECTOR_NAME = "name"
    const val TASK_ID = "neo4j.task.id"

    /**
     * Authentication providers visible to the connector plugin. The plugin class loader is used
     * rather than the thread context class loader, so that discovery is predictable under Kafka
     * Connect's plugin isolation.
     */
    internal val authRegistry: AuthTokenManagerRegistry by lazy {
      AuthTokenManagerRegistry.using(Neo4jConfiguration::class.java.classLoader)
    }

    /**
     * Returns the registered provider name matching [type]. An exact match wins, otherwise the
     * match is case-insensitive.
     *
     * @throws ConfigException if no provider, or more than one, matches
     */
    internal fun resolveAuthType(type: String, names: Set<String> = authRegistry.names()): String {
      if (names.contains(type)) {
        return type
      }

      val matches = names.filter { it.equals(type, ignoreCase = true) }
      return when (matches.size) {
        1 -> matches.single()
        0 ->
            throw ConfigException(
                AUTHENTICATION_TYPE,
                type,
                "No authentication provider is registered under this name, available names are ${names.joinToString { "'$it'" }}.",
            )
        else ->
            throw ConfigException(
                AUTHENTICATION_TYPE,
                type,
                "Name matches more than one authentication provider: ${matches.joinToString { "'$it'" }}.",
            )
      }
    }

    /**
     * Builds the provider configuration for [type] from the raw connector configuration. Every key
     * under `neo4j.authentication.<type>.` becomes a parameter with that prefix removed, except
     * `username` and `password`, which are passed separately. Blank values are dropped, as they are
     * what Control Center sends for untouched fields and what unset declared keys default to.
     */
    internal fun authConfig(originals: Map<String, *>, type: String): AuthConfig {
      val prefix = "${AUTHENTICATION_PREFIX}.$type."
      val params =
          originals
              .filterKeys { it.startsWith(prefix) }
              .mapKeys { it.key.removePrefix(prefix) }
              .mapValues {
                when (val value = it.value) {
                  is Password -> value.value()
                  else -> value?.toString()
                }
              }
              .filterValues { !it.isNullOrBlank() }
              .mapValues { it.value!! }
              .toMutableMap()
      return AuthConfig.of(params.remove("username"), params.remove("password"), params)
    }

    /** Perform validation on dependent configuration items */
    fun validate(config: org.apache.kafka.common.config.Config) {
      // authentication configuration
      config.validateNonEmptyIfVisible(AUTHENTICATION_BASIC_USERNAME)
      config.validateNonEmptyIfVisible(AUTHENTICATION_BASIC_PASSWORD)
      config.validateNonEmptyIfVisible(AUTHENTICATION_KERBEROS_TICKET)
      config.validateNonEmptyIfVisible(AUTHENTICATION_BEARER_TOKEN)
      config.validateNonEmptyIfVisible(AUTHENTICATION_CUSTOM_PRINCIPAL)
      config.validateNonEmptyIfVisible(AUTHENTICATION_CUSTOM_CREDENTIALS)
      config.validateNonEmptyIfVisible(AUTHENTICATION_CUSTOM_SCHEME)

      // security configuration
      config.validateNonEmptyIfVisible(SECURITY_ENCRYPTED)
      config.validateNonEmptyIfVisible(SECURITY_HOST_NAME_VERIFICATION_ENABLED)
      config.validateNonEmptyIfVisible(SECURITY_TRUST_STRATEGY)
      config.validateNonEmptyIfVisible(SECURITY_CERT_FILES)
    }

    fun config(): ConfigDef =
        ConfigDef()
            .defineConnectionSettings()
            .defineEncryptionSettings()
            .definePoolSettings()
            .defineRetrySettings()
  }
}
