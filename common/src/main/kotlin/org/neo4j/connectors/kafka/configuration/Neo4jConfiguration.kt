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
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.errors.ConnectException
import org.neo4j.connectors.kafka.configuration.helpers.ConfigUtils
import org.neo4j.connectors.kafka.configuration.helpers.Validators.validateNonEmptyIfVisible
import org.neo4j.connectors.kafka.configuration.helpers.parseSimpleString
import org.neo4j.connectors.kafka.utils.Telemetry.connectorInformation
import org.neo4j.connectors.kafka.utils.Telemetry.userAgent
import org.neo4j.driver.AccessMode
import org.neo4j.driver.AuthToken
import org.neo4j.driver.AuthTokens
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

enum class AuthenticationType {
  NONE,
  BASIC,
  KERBEROS,
  BEARER,
  CUSTOM
}

open class Neo4jConfiguration(configDef: ConfigDef, originals: Map<*, *>, val type: ConnectorType) :
    AbstractConfig(configDef, originals), Closeable {
  private val logger: Logger = LoggerFactory.getLogger(Neo4jConfiguration::class.java)

  val database
    get(): String = getString(DATABASE)

  internal val uris
    get(): List<URI> = getList(URI).map { URI(it) }

  internal val connectionTimeout
    get(): kotlin.time.Duration =
        kotlin.time.Duration.parseSimpleString(getString(CONNECTION_TIMEOUT))

  internal val maxRetryTime
    get(): kotlin.time.Duration =
        kotlin.time.Duration.parseSimpleString(getString(MAX_TRANSACTION_RETRY_TIMEOUT))

  internal val maxConnectionPoolSize
    get(): Int = getInt(POOL_MAX_CONNECTION_POOL_SIZE)

  internal val connectionAcquisitionTimeout
    get(): kotlin.time.Duration =
        kotlin.time.Duration.parseSimpleString(getString(POOL_CONNECTION_ACQUISITION_TIMEOUT))

  internal val idleTimeBeforeTest
    get(): kotlin.time.Duration =
        getString(POOL_IDLE_TIME_BEFORE_TEST).orEmpty().run {
          if (this.isEmpty()) {
            (-1).milliseconds
          } else {
            kotlin.time.Duration.parseSimpleString(this)
          }
        }

  internal val maxConnectionLifetime
    get(): kotlin.time.Duration =
        kotlin.time.Duration.parseSimpleString(getString(POOL_MAX_CONNECTION_LIFETIME))

  internal val encrypted
    get(): Boolean = getString(SECURITY_ENCRYPTED).toBoolean()

  internal val certFiles
    get(): List<File> = getList(SECURITY_CERT_FILES).map { File(it) }

  internal val authenticationToken
    get(): AuthToken =
        when (ConfigUtils.getEnum<AuthenticationType>(this, AUTHENTICATION_TYPE)) {
          null -> throw ConnectException("Configuration '$AUTHENTICATION_TYPE' is not provided")
          AuthenticationType.NONE -> AuthTokens.none()
          AuthenticationType.BASIC ->
              AuthTokens.basic(
                  getString(AUTHENTICATION_BASIC_USERNAME),
                  getPassword(AUTHENTICATION_BASIC_PASSWORD).value(),
                  getString(AUTHENTICATION_BASIC_REALM))
          AuthenticationType.KERBEROS ->
              AuthTokens.kerberos(getPassword(AUTHENTICATION_KERBEROS_TICKET).value())
          AuthenticationType.BEARER ->
              AuthTokens.bearer(getPassword(AUTHENTICATION_BEARER_TOKEN).value())
          AuthenticationType.CUSTOM ->
              AuthTokens.custom(
                  getString(AUTHENTICATION_CUSTOM_PRINCIPAL),
                  getPassword(AUTHENTICATION_CUSTOM_CREDENTIALS).value(),
                  getString(AUTHENTICATION_CUSTOM_REALM),
                  getString(AUTHENTICATION_CUSTOM_SCHEME))
        }

  internal val trustStrategy
    get(): TrustStrategy {
      val strategy: TrustStrategy =
          when (ConfigUtils.getEnum<Strategy>(
              this,
              SECURITY_TRUST_STRATEGY,
          )) {
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
        connectionAcquisitionTimeout.inWholeMilliseconds, TimeUnit.MILLISECONDS)
    config.withConnectionTimeout(connectionTimeout.inWholeMilliseconds, TimeUnit.MILLISECONDS)
    config.withMaxConnectionPoolSize(maxConnectionPoolSize)
    config.withConnectionLivenessCheckTimeout(
        idleTimeBeforeTest.inWholeMilliseconds, TimeUnit.MILLISECONDS)
    config.withMaxConnectionLifetime(
        maxConnectionLifetime.inWholeMilliseconds, TimeUnit.MILLISECONDS)
    config.withMaxTransactionRetryTime(maxRetryTime.inWholeMilliseconds, TimeUnit.MILLISECONDS)

    if (uri.none { it.scheme.endsWith("+s", true) || it.scheme.endsWith("+ssc", true) }) {
      if (encrypted) {
        config.withEncryption()
        config.withTrustStrategy(trustStrategy)
      } else {
        config.withoutEncryption()
      }
    }

    GraphDatabase.driver(mainUri, authenticationToken, config.build())
  }

  fun sessionConfig(vararg bookmarks: Bookmark): SessionConfig {
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
        })

    return config.build()
  }

  open fun txConfig(): TransactionConfig =
      TransactionConfig.builder()
          .withMetadata(
              buildMap {
                this["app"] = connectorInformation(type.description)

                val metadata = telemetryData()
                if (metadata.isNotEmpty()) {
                  this["metadata"] = metadata
                }
              })
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

  companion object {
    val DEFAULT_MAX_RETRY_DURATION = 30.seconds

    const val URI = "neo4j.uri"

    const val DATABASE = "neo4j.database"

    const val AUTHENTICATION_TYPE = "neo4j.authentication.type"
    const val AUTHENTICATION_BASIC_USERNAME = "neo4j.authentication.basic.username"
    const val AUTHENTICATION_BASIC_PASSWORD = "neo4j.authentication.basic.password"
    const val AUTHENTICATION_BASIC_REALM = "neo4j.authentication.basic.realm"
    const val AUTHENTICATION_KERBEROS_TICKET = "neo4j.authentication.kerberos.ticket"
    const val AUTHENTICATION_BEARER_TOKEN = "neo4j.authentication.bearer.token"
    const val AUTHENTICATION_CUSTOM_SCHEME = "neo4j.authentication.custom.scheme"
    const val AUTHENTICATION_CUSTOM_PRINCIPAL = "neo4j.authentication.custom.principal"
    const val AUTHENTICATION_CUSTOM_CREDENTIALS = "neo4j.authentication.custom.credentials"
    const val AUTHENTICATION_CUSTOM_REALM = "neo4j.authentication.custom.realm"

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
