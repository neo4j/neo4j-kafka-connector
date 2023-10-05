/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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

import com.github.jcustenborder.kafka.connect.utils.config.ConfigKeyBuilder
import com.github.jcustenborder.kafka.connect.utils.config.ConfigUtils
import java.io.File
import java.net.URI
import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.function.Predicate
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.Range
import org.apache.kafka.common.config.ConfigDef.Type
import org.apache.kafka.common.config.ConfigException
import org.neo4j.connectors.kafka.configuration.helpers.Recommenders
import org.neo4j.connectors.kafka.configuration.helpers.Validators
import org.neo4j.driver.AccessMode
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Bookmark
import org.neo4j.driver.Config
import org.neo4j.driver.Config.TrustStrategy
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.SessionConfig
import org.neo4j.driver.TransactionConfig
import org.neo4j.driver.internal.async.pool.PoolSettings
import org.neo4j.driver.net.ServerAddress
import streams.kafka.connect.utils.PropertiesUtil

object ConfigGroup {
  const val ENCRYPTION = "Encryption"
  const val CONNECTION = "Connection"
  const val AUTHENTICATION = "Authentication"
  const val TOPIC_CYPHER_MAPPING = "Topic Cypher Mapping"
  const val ERROR_REPORTING = "Error Reporting"
  const val BATCH = "Batch Management"
  const val RETRY = "Retry Strategy"
  const val DEPRECATED = "Deprecated Properties (please check the documentation)"
}

open class DeprecatedNeo4jConfiguration(
    configDef: ConfigDef,
    originals: Map<*, *>,
    private val type: ConnectorType
) : AbstractConfig(configDef, originals) {
  val encryptionEnabled: Boolean
  val encryptionTrustStrategy: TrustStrategy.Strategy
  var encryptionCACertificateFile: File? = null

  val authenticationType: AuthenticationType
  val authenticationUsername: String
  val authenticationPassword: String
  val authenticationRealm: String
  val authenticationKerberosTicket: String

  val serverUri: List<URI>
  val connectionMaxConnectionLifetime: Long
  val connectionLivenessCheckTimeout: Long
  val connectionPoolMaxSize: Int
  val connectionAcquisitionTimeout: Long

  val retryBackoff: Long
  val retryMaxAttempts: Int

  val batchTimeout: Long
  val batchSize: Int

  val database: String

  init {
    database = getString(DATABASE)
    encryptionEnabled = getBoolean(ENCRYPTION_ENABLED)
    encryptionTrustStrategy =
        ConfigUtils.getEnum(TrustStrategy.Strategy::class.java, this, ENCRYPTION_TRUST_STRATEGY)
    val encryptionCACertificatePATH = getString(ENCRYPTION_CA_CERTIFICATE_PATH) ?: ""
    if (encryptionCACertificatePATH != "") {
      encryptionCACertificateFile = File(encryptionCACertificatePATH)
    }

    authenticationType =
        ConfigUtils.getEnum(AuthenticationType::class.java, this, AUTHENTICATION_TYPE)
    authenticationRealm = getString(AUTHENTICATION_BASIC_REALM)
    authenticationUsername = getString(AUTHENTICATION_BASIC_USERNAME)
    authenticationPassword = getPassword(AUTHENTICATION_BASIC_PASSWORD).value()
    authenticationKerberosTicket = getPassword(AUTHENTICATION_KERBEROS_TICKET).value()

    serverUri = getString(SERVER_URI).split(",").map { URI(it) }
    connectionLivenessCheckTimeout = getLong(CONNECTION_LIVENESS_CHECK_TIMEOUT_MSECS)
    connectionMaxConnectionLifetime = getLong(CONNECTION_MAX_CONNECTION_LIFETIME_MSECS)
    connectionPoolMaxSize = getInt(CONNECTION_POOL_MAX_SIZE)
    connectionAcquisitionTimeout = getLong(CONNECTION_MAX_CONNECTION_ACQUISITION_TIMEOUT_MSECS)

    retryBackoff = getLong(RETRY_BACKOFF_MSECS)
    retryMaxAttempts = getInt(RETRY_MAX_ATTEMPTS)

    batchTimeout = getLong(BATCH_TIMEOUT_MSECS)
    batchSize = getInt(BATCH_SIZE)
  }

  fun hasSecuredURI() =
      serverUri.any { it.scheme.endsWith("+s", true) || it.scheme.endsWith("+ssc", true) }

  fun createDriver(): Driver {
    val configBuilder = Config.builder()
    configBuilder.withUserAgent("neo4j-kafka-connect-$type/${PropertiesUtil.getVersion()}")

    if (!this.hasSecuredURI()) {
      if (this.encryptionEnabled) {
        configBuilder.withEncryption()
        val trustStrategy: Config.TrustStrategy =
            when (this.encryptionTrustStrategy) {
              Config.TrustStrategy.Strategy.TRUST_ALL_CERTIFICATES ->
                  Config.TrustStrategy.trustAllCertificates()
              Config.TrustStrategy.Strategy.TRUST_SYSTEM_CA_SIGNED_CERTIFICATES ->
                  Config.TrustStrategy.trustSystemCertificates()
              Config.TrustStrategy.Strategy.TRUST_CUSTOM_CA_SIGNED_CERTIFICATES ->
                  Config.TrustStrategy.trustCustomCertificateSignedBy(
                      this.encryptionCACertificateFile)
              else -> {
                throw ConfigException(
                    ENCRYPTION_TRUST_STRATEGY,
                    this.encryptionTrustStrategy.toString(),
                    "Encryption Trust Strategy is not supported.")
              }
            }
        configBuilder.withTrustStrategy(trustStrategy)
      } else {
        configBuilder.withoutEncryption()
      }
    }

    val authToken =
        when (this.authenticationType) {
          AuthenticationType.NONE -> AuthTokens.none()
          AuthenticationType.BASIC -> {
            if (this.authenticationRealm != "") {
              AuthTokens.basic(
                  this.authenticationUsername,
                  this.authenticationPassword,
                  this.authenticationRealm)
            } else {
              AuthTokens.basic(this.authenticationUsername, this.authenticationPassword)
            }
          }
          AuthenticationType.KERBEROS -> AuthTokens.kerberos(this.authenticationKerberosTicket)
          else ->
              throw ConfigException("unsupported authentication type ${this.authenticationType}")
        }
    configBuilder.withMaxConnectionPoolSize(this.connectionPoolMaxSize)
    configBuilder.withMaxConnectionLifetime(
        this.connectionMaxConnectionLifetime, TimeUnit.MILLISECONDS)
    configBuilder.withConnectionAcquisitionTimeout(
        this.connectionAcquisitionTimeout, TimeUnit.MILLISECONDS)
    configBuilder.withMaxTransactionRetryTime(this.retryBackoff, TimeUnit.MILLISECONDS)
    configBuilder.withConnectionLivenessCheckTimeout(
        this.connectionLivenessCheckTimeout, TimeUnit.MINUTES)
    configBuilder.withResolver { address ->
      this.serverUri.map { ServerAddress.of(it.host, if (it.port == -1) 7687 else it.port) }.toSet()
    }
    val neo4jConfig = configBuilder.build()

    return GraphDatabase.driver(this.serverUri.firstOrNull(), authToken, neo4jConfig)
  }

  fun createSessionConfig(bookmarks: List<Bookmark> = emptyList()): SessionConfig {
    val sessionConfigBuilder = SessionConfig.builder()
    if (this.database.isNotBlank()) {
      sessionConfigBuilder.withDatabase(this.database)
    }
    val accessMode =
        if (type == ConnectorType.SOURCE) {
          AccessMode.READ
        } else {
          AccessMode.WRITE
        }
    sessionConfigBuilder.withDefaultAccessMode(accessMode)
    sessionConfigBuilder.withBookmarks(bookmarks)
    return sessionConfigBuilder.build()
  }

  fun createTransactionConfig(): TransactionConfig {
    val batchTimeout = this.batchTimeout
    return if (batchTimeout > 0) {
      TransactionConfig.builder().withTimeout(Duration.ofMillis(batchTimeout)).build()
    } else {
      TransactionConfig.empty()
    }
  }

  companion object {
    @Deprecated("deprecated in favour of ${Neo4jConfiguration.URI}")
    const val SERVER_URI = "neo4j.server.uri"
    const val DATABASE = "neo4j.database"

    const val AUTHENTICATION_TYPE = "neo4j.authentication.type"
    const val AUTHENTICATION_BASIC_USERNAME = "neo4j.authentication.basic.username"
    const val AUTHENTICATION_BASIC_PASSWORD = "neo4j.authentication.basic.password"
    const val AUTHENTICATION_BASIC_REALM = "neo4j.authentication.basic.realm"
    const val AUTHENTICATION_KERBEROS_TICKET = "neo4j.authentication.kerberos.ticket"

    @Deprecated("deprecated in favour of ${Neo4jConfiguration.SECURITY_ENCRYPTED}")
    const val ENCRYPTION_ENABLED = "neo4j.encryption.enabled"
    @Deprecated("deprecated in favour of ${Neo4jConfiguration.SECURITY_TRUST_STRATEGY}")
    const val ENCRYPTION_TRUST_STRATEGY = "neo4j.encryption.trust.strategy"
    @Deprecated("deprecated in favour of ${Neo4jConfiguration.SECURITY_CERT_FILES}")
    const val ENCRYPTION_CA_CERTIFICATE_PATH = "neo4j.encryption.ca.certificate.path"

    @Deprecated("deprecated in favour of ${Neo4jConfiguration.POOL_MAX_CONNECTION_LIFETIME}")
    const val CONNECTION_MAX_CONNECTION_LIFETIME_MSECS = "neo4j.connection.max.lifetime.msecs"
    @Deprecated("deprecated in favour of ${Neo4jConfiguration.POOL_CONNECTION_ACQUISITION_TIMEOUT}")
    const val CONNECTION_MAX_CONNECTION_ACQUISITION_TIMEOUT_MSECS =
        "neo4j.connection.acquisition.timeout.msecs"
    @Deprecated("deprecated in favour of ${Neo4jConfiguration.POOL_IDLE_TIME_BEFORE_TEST}")
    const val CONNECTION_LIVENESS_CHECK_TIMEOUT_MSECS =
        "neo4j.connection.liveness.check.timeout.msecs"
    @Deprecated("deprecated in favour of ${Neo4jConfiguration.POOL_MAX_CONNECTION_POOL_SIZE}")
    const val CONNECTION_POOL_MAX_SIZE = "neo4j.connection.max.pool.size"

    @Deprecated("use connector specific configuration instead")
    const val BATCH_SIZE = "neo4j.batch.size"
    @Deprecated("use connector specific configuration instead")
    const val BATCH_TIMEOUT_MSECS = "neo4j.batch.timeout.msecs"

    @Deprecated("deprecated in favour of ${Neo4jConfiguration.MAX_TRANSACTION_RETRY_TIMEOUT}")
    const val RETRY_BACKOFF_MSECS = "neo4j.retry.backoff.msecs"
    @Deprecated("deprecated in favour of ${Neo4jConfiguration.MAX_TRANSACTION_RETRY_ATTEMPTS}")
    const val RETRY_MAX_ATTEMPTS = "neo4j.retry.max.attemps"

    const val CONNECTION_POOL_MAX_SIZE_DEFAULT = 100
    val BATCH_TIMEOUT_DEFAULT = TimeUnit.SECONDS.toMillis(0L)
    const val BATCH_SIZE_DEFAULT = 1000
    val RETRY_BACKOFF_DEFAULT = TimeUnit.SECONDS.toMillis(30L)
    const val RETRY_MAX_ATTEMPTS_DEFAULT = 5

    // Default values optimizations for Aura please look at:
    // https://aura.support.neo4j.com/hc/en-us/articles/1500002493281-Neo4j-Java-driver-settings-for-Aura
    val CONNECTION_MAX_CONNECTION_LIFETIME_MSECS_DEFAULT = Duration.ofMinutes(8).toMillis()
    val CONNECTION_LIVENESS_CHECK_TIMEOUT_MSECS_DEFAULT = Duration.ofMinutes(2).toMillis()

    fun config(): ConfigDef =
        ConfigDef()
            .define(
                ConfigKeyBuilder.of(AUTHENTICATION_TYPE, Type.STRING)
                    .documentation(PropertiesUtil.getProperty(AUTHENTICATION_TYPE))
                    .importance(Importance.HIGH)
                    .defaultValue(AuthenticationType.BASIC.toString())
                    .group(ConfigGroup.AUTHENTICATION)
                    .validator(Validators.enum(AuthenticationType::class.java))
                    .build())
            .define(
                ConfigKeyBuilder.of(AUTHENTICATION_BASIC_USERNAME, Type.STRING)
                    .documentation(PropertiesUtil.getProperty(AUTHENTICATION_BASIC_USERNAME))
                    .importance(Importance.HIGH)
                    .defaultValue("")
                    .group(ConfigGroup.AUTHENTICATION)
                    .recommender(
                        Recommenders.visibleIf(
                            AUTHENTICATION_TYPE,
                            Predicate.isEqual(AuthenticationType.BASIC.toString())))
                    .build())
            .define(
                ConfigKeyBuilder.of(AUTHENTICATION_BASIC_PASSWORD, Type.PASSWORD)
                    .documentation(PropertiesUtil.getProperty(AUTHENTICATION_BASIC_PASSWORD))
                    .importance(Importance.HIGH)
                    .defaultValue("")
                    .group(ConfigGroup.AUTHENTICATION)
                    .recommender(
                        Recommenders.visibleIf(
                            AUTHENTICATION_TYPE,
                            Predicate.isEqual(AuthenticationType.BASIC.toString())))
                    .build())
            .define(
                ConfigKeyBuilder.of(AUTHENTICATION_BASIC_REALM, Type.STRING)
                    .documentation(PropertiesUtil.getProperty(AUTHENTICATION_BASIC_REALM))
                    .importance(Importance.HIGH)
                    .defaultValue("")
                    .group(ConfigGroup.AUTHENTICATION)
                    .recommender(
                        Recommenders.visibleIf(
                            AUTHENTICATION_TYPE,
                            Predicate.isEqual(AuthenticationType.BASIC.toString())))
                    .build())
            .define(
                ConfigKeyBuilder.of(AUTHENTICATION_KERBEROS_TICKET, Type.PASSWORD)
                    .documentation(PropertiesUtil.getProperty(AUTHENTICATION_KERBEROS_TICKET))
                    .importance(Importance.HIGH)
                    .defaultValue("")
                    .group(ConfigGroup.AUTHENTICATION)
                    .recommender(
                        Recommenders.visibleIf(
                            AUTHENTICATION_TYPE,
                            Predicate.isEqual(AuthenticationType.KERBEROS.toString())))
                    .build())
            .define(
                ConfigKeyBuilder.of(SERVER_URI, Type.STRING)
                    .documentation(PropertiesUtil.getProperty(SERVER_URI))
                    .importance(Importance.HIGH)
                    .defaultValue("bolt://localhost:7687")
                    .group(ConfigGroup.CONNECTION)
                    .validator(
                        Validators.uri(
                            "bolt",
                            "bolt+routing",
                            "bolt+s",
                            "bolt+ssc",
                            "neo4j",
                            "neo4j+s",
                            "neo4j+ssc"))
                    .build())
            .define(
                ConfigKeyBuilder.of(CONNECTION_POOL_MAX_SIZE, Type.INT)
                    .documentation(PropertiesUtil.getProperty(CONNECTION_POOL_MAX_SIZE))
                    .importance(Importance.LOW)
                    .defaultValue(CONNECTION_POOL_MAX_SIZE_DEFAULT)
                    .group(ConfigGroup.CONNECTION)
                    .validator(Range.atLeast(1))
                    .build())
            .define(
                ConfigKeyBuilder.of(CONNECTION_MAX_CONNECTION_LIFETIME_MSECS, Type.LONG)
                    .documentation(
                        PropertiesUtil.getProperty(CONNECTION_MAX_CONNECTION_LIFETIME_MSECS))
                    .importance(Importance.LOW)
                    .defaultValue(CONNECTION_MAX_CONNECTION_LIFETIME_MSECS_DEFAULT)
                    .group(ConfigGroup.CONNECTION)
                    .validator(Range.atLeast(1))
                    .build())
            .define(
                ConfigKeyBuilder.of(CONNECTION_LIVENESS_CHECK_TIMEOUT_MSECS, Type.LONG)
                    .documentation(
                        PropertiesUtil.getProperty(CONNECTION_LIVENESS_CHECK_TIMEOUT_MSECS))
                    .importance(Importance.LOW)
                    .defaultValue(CONNECTION_LIVENESS_CHECK_TIMEOUT_MSECS_DEFAULT)
                    .group(ConfigGroup.CONNECTION)
                    .validator(Range.atLeast(1))
                    .build())
            .define(
                ConfigKeyBuilder.of(CONNECTION_MAX_CONNECTION_ACQUISITION_TIMEOUT_MSECS, Type.LONG)
                    .documentation(
                        PropertiesUtil.getProperty(
                            CONNECTION_MAX_CONNECTION_ACQUISITION_TIMEOUT_MSECS))
                    .importance(Importance.LOW)
                    .defaultValue(PoolSettings.DEFAULT_CONNECTION_ACQUISITION_TIMEOUT)
                    .group(ConfigGroup.CONNECTION)
                    .validator(Range.atLeast(1))
                    .build())
            .define(
                ConfigKeyBuilder.of(ENCRYPTION_ENABLED, Type.BOOLEAN)
                    .documentation(PropertiesUtil.getProperty(ENCRYPTION_ENABLED))
                    .importance(Importance.HIGH)
                    .defaultValue(false)
                    .group(ConfigGroup.ENCRYPTION)
                    .build())
            .define(
                ConfigKeyBuilder.of(ENCRYPTION_TRUST_STRATEGY, Type.STRING)
                    .documentation(PropertiesUtil.getProperty(ENCRYPTION_TRUST_STRATEGY))
                    .importance(Importance.MEDIUM)
                    .defaultValue(TrustStrategy.Strategy.TRUST_ALL_CERTIFICATES.toString())
                    .group(ConfigGroup.ENCRYPTION)
                    .validator(Validators.enum(TrustStrategy.Strategy::class.java))
                    .recommender(
                        Recommenders.visibleIf(ENCRYPTION_ENABLED, Predicate.isEqual(true)))
                    .build())
            .define(
                ConfigKeyBuilder.of(ENCRYPTION_CA_CERTIFICATE_PATH, Type.STRING)
                    .documentation(PropertiesUtil.getProperty(ENCRYPTION_CA_CERTIFICATE_PATH))
                    .importance(Importance.MEDIUM)
                    .defaultValue("")
                    .group(ConfigGroup.ENCRYPTION)
                    .validator(Validators.or(Validators.blank(), Validators.file()))
                    .recommender(
                        Recommenders.visibleIf(
                            ENCRYPTION_TRUST_STRATEGY,
                            Predicate.isEqual(
                                TrustStrategy.Strategy.TRUST_CUSTOM_CA_SIGNED_CERTIFICATES
                                    .toString())))
                    .build())
            .define(
                ConfigKeyBuilder.of(BATCH_SIZE, Type.INT)
                    .documentation(PropertiesUtil.getProperty(BATCH_SIZE))
                    .importance(Importance.LOW)
                    .defaultValue(BATCH_SIZE_DEFAULT)
                    .group(ConfigGroup.BATCH)
                    .validator(Range.atLeast(1))
                    .build())
            .define(
                ConfigKeyBuilder.of(BATCH_TIMEOUT_MSECS, Type.LONG)
                    .documentation(PropertiesUtil.getProperty(BATCH_TIMEOUT_MSECS))
                    .importance(Importance.LOW)
                    .defaultValue(BATCH_TIMEOUT_DEFAULT)
                    .group(ConfigGroup.BATCH)
                    .validator(Range.atLeast(0))
                    .build())
            .define(
                ConfigKeyBuilder.of(RETRY_BACKOFF_MSECS, Type.LONG)
                    .documentation(PropertiesUtil.getProperty(RETRY_BACKOFF_MSECS))
                    .importance(Importance.MEDIUM)
                    .defaultValue(RETRY_BACKOFF_DEFAULT)
                    .group(ConfigGroup.RETRY)
                    .validator(Range.atLeast(1))
                    .build())
            .define(
                ConfigKeyBuilder.of(RETRY_MAX_ATTEMPTS, Type.INT)
                    .documentation(PropertiesUtil.getProperty(RETRY_MAX_ATTEMPTS))
                    .importance(Importance.MEDIUM)
                    .defaultValue(RETRY_MAX_ATTEMPTS_DEFAULT)
                    .group(ConfigGroup.RETRY)
                    .validator(Range.atLeast(1))
                    .build())
            .define(
                ConfigKeyBuilder.of(DATABASE, Type.STRING)
                    .documentation(PropertiesUtil.getProperty(DATABASE))
                    .importance(Importance.HIGH)
                    .group(ConfigGroup.CONNECTION)
                    .defaultValue("")
                    .build())
  }
}
