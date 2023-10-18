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

import com.fasterxml.jackson.databind.util.ClassUtil.defaultValue
import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.function.Predicate
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.Range
import org.apache.kafka.common.config.ConfigDef.Type
import org.neo4j.connectors.kafka.configuration.helpers.ConfigKeyBuilder
import org.neo4j.connectors.kafka.configuration.helpers.Recommenders
import org.neo4j.connectors.kafka.configuration.helpers.Validators
import org.neo4j.connectors.kafka.utils.PropertiesUtil
import org.neo4j.driver.Config.TrustStrategy
import org.neo4j.driver.internal.async.pool.PoolSettings

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
                ConfigKeyBuilder.of(AUTHENTICATION_TYPE, Type.STRING) {
                  importance = Importance.HIGH
                  defaultValue = AuthenticationType.BASIC.toString()
                  group = ConfigGroup.AUTHENTICATION
                  validator = Validators.enum(AuthenticationType::class.java)
                })
            .define(
                ConfigKeyBuilder.of(AUTHENTICATION_BASIC_USERNAME, Type.STRING) {
                  importance = Importance.HIGH
                  defaultValue = ""
                  group = ConfigGroup.AUTHENTICATION
                  recommender =
                      Recommenders.visibleIf(
                          AUTHENTICATION_TYPE,
                          Predicate.isEqual(AuthenticationType.BASIC.toString()))
                })
            .define(
                ConfigKeyBuilder.of(AUTHENTICATION_BASIC_PASSWORD, Type.PASSWORD) {
                  importance = Importance.HIGH
                  defaultValue = ""
                  group = ConfigGroup.AUTHENTICATION
                  recommender =
                      Recommenders.visibleIf(
                          AUTHENTICATION_TYPE,
                          Predicate.isEqual(AuthenticationType.BASIC.toString()))
                })
            .define(
                ConfigKeyBuilder.of(AUTHENTICATION_BASIC_REALM, Type.STRING) {
                  importance = Importance.HIGH
                  defaultValue = ""
                  group = ConfigGroup.AUTHENTICATION
                  recommender =
                      Recommenders.visibleIf(
                          AUTHENTICATION_TYPE,
                          Predicate.isEqual(AuthenticationType.BASIC.toString()))
                })
            .define(
                ConfigKeyBuilder.of(AUTHENTICATION_KERBEROS_TICKET, Type.PASSWORD) {
                  importance = Importance.HIGH
                  defaultValue = ""
                  group = ConfigGroup.AUTHENTICATION
                  recommender =
                      Recommenders.visibleIf(
                          AUTHENTICATION_TYPE,
                          Predicate.isEqual(AuthenticationType.KERBEROS.toString()))
                })
            .define(
                ConfigKeyBuilder.of(SERVER_URI, Type.STRING) {
                  importance = Importance.HIGH
                  defaultValue = "bolt://localhost:7687"
                  group = ConfigGroup.CONNECTION
                  validator =
                      Validators.uri("bolt", "bolt+s", "bolt+ssc", "neo4j", "neo4j+s", "neo4j+ssc")
                })
            .define(
                ConfigKeyBuilder.of(CONNECTION_POOL_MAX_SIZE, Type.INT) {
                  importance = Importance.LOW
                  defaultValue = CONNECTION_POOL_MAX_SIZE_DEFAULT
                  group = ConfigGroup.CONNECTION
                  validator = Range.atLeast(1)
                })
            .define(
                ConfigKeyBuilder.of(CONNECTION_MAX_CONNECTION_LIFETIME_MSECS, Type.LONG) {
                  documentation =
                      PropertiesUtil.getProperty(CONNECTION_MAX_CONNECTION_LIFETIME_MSECS)
                  importance = Importance.LOW
                  defaultValue = CONNECTION_MAX_CONNECTION_LIFETIME_MSECS_DEFAULT
                  group = ConfigGroup.CONNECTION
                  validator = Range.atLeast(1)
                })
            .define(
                ConfigKeyBuilder.of(CONNECTION_LIVENESS_CHECK_TIMEOUT_MSECS, Type.LONG) {
                  documentation =
                      PropertiesUtil.getProperty(CONNECTION_LIVENESS_CHECK_TIMEOUT_MSECS)
                  importance = Importance.LOW
                  defaultValue = CONNECTION_LIVENESS_CHECK_TIMEOUT_MSECS_DEFAULT
                  group = ConfigGroup.CONNECTION
                  validator = Range.atLeast(1)
                })
            .define(
                ConfigKeyBuilder.of(
                    CONNECTION_MAX_CONNECTION_ACQUISITION_TIMEOUT_MSECS, Type.LONG) {
                      documentation =
                          PropertiesUtil.getProperty(
                              CONNECTION_MAX_CONNECTION_ACQUISITION_TIMEOUT_MSECS)
                      importance = Importance.LOW
                      defaultValue = PoolSettings.DEFAULT_CONNECTION_ACQUISITION_TIMEOUT
                      group = ConfigGroup.CONNECTION
                      validator = Range.atLeast(1)
                    })
            .define(
                ConfigKeyBuilder.of(ENCRYPTION_ENABLED, Type.BOOLEAN) {
                  importance = Importance.HIGH
                  defaultValue = false
                  group = ConfigGroup.ENCRYPTION
                })
            .define(
                ConfigKeyBuilder.of(ENCRYPTION_TRUST_STRATEGY, Type.STRING) {
                  importance = Importance.MEDIUM
                  defaultValue = TrustStrategy.Strategy.TRUST_ALL_CERTIFICATES.toString()
                  group = ConfigGroup.ENCRYPTION
                  validator = Validators.enum(TrustStrategy.Strategy::class.java)
                  recommender = Recommenders.visibleIf(ENCRYPTION_ENABLED, Predicate.isEqual(true))
                })
            .define(
                ConfigKeyBuilder.of(ENCRYPTION_CA_CERTIFICATE_PATH, Type.STRING) {
                  importance = Importance.MEDIUM
                  defaultValue = ""
                  group = ConfigGroup.ENCRYPTION
                  validator = Validators.or(Validators.blank(), Validators.file())
                  recommender =
                      Recommenders.visibleIf(
                          ENCRYPTION_TRUST_STRATEGY,
                          Predicate.isEqual(
                              TrustStrategy.Strategy.TRUST_CUSTOM_CA_SIGNED_CERTIFICATES
                                  .toString()))
                })
            .define(
                ConfigKeyBuilder.of(BATCH_SIZE, Type.INT) {
                  importance = Importance.LOW
                  defaultValue = BATCH_SIZE_DEFAULT
                  group = ConfigGroup.BATCH
                  validator = Range.atLeast(1)
                })
            .define(
                ConfigKeyBuilder.of(BATCH_TIMEOUT_MSECS, Type.LONG) {
                  importance = Importance.LOW
                  defaultValue = BATCH_TIMEOUT_DEFAULT
                  group = ConfigGroup.BATCH
                  validator = Range.atLeast(0)
                })
            .define(
                ConfigKeyBuilder.of(RETRY_BACKOFF_MSECS, Type.LONG) {
                  importance = Importance.MEDIUM
                  defaultValue = RETRY_BACKOFF_DEFAULT
                  group = ConfigGroup.RETRY
                  validator = Range.atLeast(1)
                })
            .define(
                ConfigKeyBuilder.of(RETRY_MAX_ATTEMPTS, Type.INT) {
                  importance = Importance.MEDIUM
                  defaultValue = RETRY_MAX_ATTEMPTS_DEFAULT
                  group = ConfigGroup.RETRY
                  validator = Range.atLeast(1)
                })
            .define(
                ConfigKeyBuilder.of(DATABASE, Type.STRING) {
                  importance = Importance.HIGH
                  group = ConfigGroup.CONNECTION
                  defaultValue = ""
                })
  }
}
