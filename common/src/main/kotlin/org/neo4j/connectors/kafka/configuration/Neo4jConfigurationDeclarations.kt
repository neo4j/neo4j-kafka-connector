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

import java.net.URI
import java.util.function.Predicate
import kotlin.time.Duration.Companion.milliseconds
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.Range
import org.apache.kafka.common.config.ConfigException
import org.neo4j.connectors.kafka.configuration.ConfigurationGroup.ADVANCED
import org.neo4j.connectors.kafka.configuration.ConfigurationGroup.CONNECTION
import org.neo4j.connectors.kafka.configuration.helpers.ConfigKeyBuilder
import org.neo4j.connectors.kafka.configuration.helpers.Recommenders
import org.neo4j.connectors.kafka.configuration.helpers.SIMPLE_DURATION_PATTERN
import org.neo4j.connectors.kafka.configuration.helpers.Validators
import org.neo4j.connectors.kafka.configuration.helpers.toSimpleString
import org.neo4j.driver.Config
import org.neo4j.driver.Config.TrustStrategy.Strategy

enum class ConfigurationGroup(val title: String) {
  CONNECTION("Neo4j Connection"),
  ADVANCED("Advanced")
}

fun ConfigDef.defineConnectionSettings(): ConfigDef =
    this.define(
            ConfigKeyBuilder.of(Neo4jConfiguration.URI, ConfigDef.Type.LIST) {
              displayName = "URI"
              group = CONNECTION.title
              importance = Importance.HIGH
              validator =
                  Validators.uri("neo4j", "neo4j+s", "neo4j+ssc", "bolt", "bolt+s", "bolt+ssc")
            })
        .define(
            ConfigKeyBuilder.of(Neo4jConfiguration.DATABASE, ConfigDef.Type.STRING) {
              displayName = "Database"
              group = CONNECTION.title
              importance = Importance.HIGH
              defaultValue = ""
              validator = ConfigDef.NonNullValidator()
            })
        .define(
            ConfigKeyBuilder.of(Neo4jConfiguration.AUTHENTICATION_TYPE, ConfigDef.Type.STRING) {
              defaultValue = AuthenticationType.BASIC.toString()
              displayName = "Authentication Type"
              group = CONNECTION.title
              importance = Importance.HIGH
              validator = Validators.enum(AuthenticationType::class.java)
              recommender = Recommenders.enum(AuthenticationType::class.java)
            })
        .define(
            ConfigKeyBuilder.of(
                Neo4jConfiguration.AUTHENTICATION_BASIC_USERNAME, ConfigDef.Type.STRING) {
                  displayName = "Username"
                  group = CONNECTION.title
                  importance = Importance.HIGH
                  defaultValue = ""
                  recommender =
                      Recommenders.visibleIf(
                          Neo4jConfiguration.AUTHENTICATION_TYPE,
                          Predicate.isEqual(AuthenticationType.BASIC.toString()))
                })
        .define(
            ConfigKeyBuilder.of(
                Neo4jConfiguration.AUTHENTICATION_BASIC_PASSWORD, ConfigDef.Type.PASSWORD) {
                  displayName = "Password"
                  group = CONNECTION.title
                  importance = Importance.HIGH
                  defaultValue = ""
                  recommender =
                      Recommenders.visibleIf(
                          Neo4jConfiguration.AUTHENTICATION_TYPE,
                          Predicate.isEqual(AuthenticationType.BASIC.toString()))
                })
        .define(
            ConfigKeyBuilder.of(
                Neo4jConfiguration.AUTHENTICATION_BASIC_REALM, ConfigDef.Type.STRING) {
                  displayName = "Realm"
                  group = CONNECTION.title
                  importance = Importance.HIGH
                  defaultValue = ""
                  recommender =
                      Recommenders.visibleIf(
                          Neo4jConfiguration.AUTHENTICATION_TYPE,
                          Predicate.isEqual(AuthenticationType.BASIC.toString()))
                })
        .define(
            ConfigKeyBuilder.of(
                Neo4jConfiguration.AUTHENTICATION_KERBEROS_TICKET, ConfigDef.Type.PASSWORD) {
                  displayName = "Kerberos Ticket"
                  group = CONNECTION.title
                  importance = Importance.HIGH
                  defaultValue = ""
                  recommender =
                      Recommenders.visibleIf(
                          Neo4jConfiguration.AUTHENTICATION_TYPE,
                          Predicate.isEqual(AuthenticationType.KERBEROS.toString()))
                })
        .define(
            ConfigKeyBuilder.of(
                Neo4jConfiguration.AUTHENTICATION_BEARER_TOKEN, ConfigDef.Type.PASSWORD) {
                  displayName = "Bearer Token"
                  group = CONNECTION.title
                  importance = Importance.HIGH
                  defaultValue = ""
                  recommender =
                      Recommenders.visibleIf(
                          Neo4jConfiguration.AUTHENTICATION_TYPE,
                          Predicate.isEqual(AuthenticationType.BEARER.toString()))
                })
        .define(
            ConfigKeyBuilder.of(
                Neo4jConfiguration.AUTHENTICATION_CUSTOM_PRINCIPAL, ConfigDef.Type.STRING) {
                  displayName = "Principal"
                  group = CONNECTION.title
                  importance = Importance.HIGH
                  defaultValue = ""
                  recommender =
                      Recommenders.visibleIf(
                          Neo4jConfiguration.AUTHENTICATION_TYPE,
                          Predicate.isEqual(AuthenticationType.CUSTOM.toString()))
                })
        .define(
            ConfigKeyBuilder.of(
                Neo4jConfiguration.AUTHENTICATION_CUSTOM_CREDENTIALS, ConfigDef.Type.PASSWORD) {
                  displayName = "Credentials"
                  group = CONNECTION.title
                  importance = Importance.HIGH
                  defaultValue = ""
                  recommender =
                      Recommenders.visibleIf(
                          Neo4jConfiguration.AUTHENTICATION_TYPE,
                          Predicate.isEqual(AuthenticationType.CUSTOM.toString()))
                })
        .define(
            ConfigKeyBuilder.of(
                Neo4jConfiguration.AUTHENTICATION_CUSTOM_REALM, ConfigDef.Type.STRING) {
                  displayName = "Realm"
                  group = CONNECTION.title
                  importance = Importance.HIGH
                  defaultValue = ""
                  recommender =
                      Recommenders.visibleIf(
                          Neo4jConfiguration.AUTHENTICATION_TYPE,
                          Predicate.isEqual(AuthenticationType.CUSTOM.toString()))
                })
        .define(
            ConfigKeyBuilder.of(
                Neo4jConfiguration.AUTHENTICATION_CUSTOM_SCHEME, ConfigDef.Type.STRING) {
                  displayName = "Scheme"
                  group = CONNECTION.title
                  importance = Importance.HIGH
                  defaultValue = ""
                  recommender =
                      Recommenders.visibleIf(
                          Neo4jConfiguration.AUTHENTICATION_TYPE,
                          Predicate.isEqual(AuthenticationType.CUSTOM.toString()))
                })

fun ConfigDef.defineEncryptionSettings(): ConfigDef =
    this.define(
            ConfigKeyBuilder.of(Neo4jConfiguration.SECURITY_ENCRYPTED, ConfigDef.Type.BOOLEAN) {
              displayName = "Encryption"
              group = ADVANCED.title
              importance = Importance.LOW
              recommender =
                  Recommenders.visibleIf(
                      Neo4jConfiguration.URI,
                      object : Predicate<Any?> {
                        override fun test(t: Any?): Boolean {
                          return when (t) {
                            null -> false
                            is String -> URI(t).scheme in arrayOf("bolt", "neo4j")
                            is List<*> -> t.any { test(it) }
                            else -> throw ConfigException("Must be a String or a List")
                          }
                        }
                      })
              defaultValue = false
            })
        .define(
            ConfigKeyBuilder.of(Neo4jConfiguration.SECURITY_TRUST_STRATEGY, ConfigDef.Type.STRING) {
              displayName = "Trust Strategy"
              group = ADVANCED.title
              importance = Importance.LOW
              validator = Validators.enum(Strategy::class.java)
              recommender =
                  Recommenders.and(
                      Recommenders.enum(Strategy::class.java),
                      Recommenders.visibleIf(
                          Neo4jConfiguration.URI,
                          object : Predicate<Any?> {
                            override fun test(t: Any?): Boolean {
                              return when (t) {
                                null -> false
                                is String -> URI(t).scheme in arrayOf("bolt", "neo4j")
                                is List<*> -> t.any { test(it) }
                                else -> throw ConfigException("Must be a String or a List")
                              }
                            }
                          }),
                      Recommenders.visibleIf(
                          Neo4jConfiguration.SECURITY_ENCRYPTED, Predicate.isEqual(true)))
              defaultValue = Strategy.TRUST_SYSTEM_CA_SIGNED_CERTIFICATES.toString()
            })
        .define(
            ConfigKeyBuilder.of(
                Neo4jConfiguration.SECURITY_HOST_NAME_VERIFICATION_ENABLED,
                ConfigDef.Type.BOOLEAN) {
                  displayName = "Hostname Verification"
                  group = ADVANCED.title
                  importance = Importance.LOW
                  recommender =
                      Recommenders.and(
                          Recommenders.visibleIf(
                              Neo4jConfiguration.URI,
                              object : Predicate<Any?> {
                                override fun test(t: Any?): Boolean {
                                  return when (t) {
                                    null -> false
                                    is String -> URI(t).scheme in arrayOf("bolt", "neo4j")
                                    is List<*> -> t.any { test(it) }
                                    else -> throw ConfigException("Must be a String or a List")
                                  }
                                }
                              }),
                          Recommenders.visibleIf(
                              Neo4jConfiguration.SECURITY_ENCRYPTED, Predicate.isEqual(true)))
                  defaultValue = true
                })
        .define(
            ConfigKeyBuilder.of(Neo4jConfiguration.SECURITY_CERT_FILES, ConfigDef.Type.LIST) {
              displayName = "Trusted Certificate List"

              group = ADVANCED.title
              importance = Importance.HIGH
              validator = Validators.or(Validators.blank(), Validators.file())
              recommender =
                  Recommenders.and(
                      Recommenders.visibleIf(
                          Neo4jConfiguration.URI,
                          object : Predicate<Any?> {
                            override fun test(t: Any?): Boolean {
                              return when (t) {
                                null -> false
                                is String -> URI(t).scheme in arrayOf("bolt", "neo4j")
                                is List<*> -> t.any { test(it) }
                                else -> throw ConfigException("Must be a String or a List")
                              }
                            }
                          }),
                      Recommenders.visibleIf(
                          Neo4jConfiguration.SECURITY_ENCRYPTED, Predicate.isEqual(true)),
                      Recommenders.visibleIf(
                          Neo4jConfiguration.SECURITY_TRUST_STRATEGY,
                          Predicate.isEqual(
                              Strategy.TRUST_CUSTOM_CA_SIGNED_CERTIFICATES.toString())))
              defaultValue = ""
            })

fun ConfigDef.definePoolSettings(): ConfigDef =
    this.define(
            ConfigKeyBuilder.of(Neo4jConfiguration.CONNECTION_TIMEOUT, ConfigDef.Type.STRING) {
              displayName = "Connection Timeout"
              group = ADVANCED.title
              importance = Importance.LOW
              validator = Validators.pattern(SIMPLE_DURATION_PATTERN)
              defaultValue =
                  Config.defaultConfig().connectionTimeoutMillis().milliseconds.toSimpleString()
            })
        .define(
            ConfigKeyBuilder.of(
                Neo4jConfiguration.POOL_MAX_CONNECTION_POOL_SIZE, ConfigDef.Type.INT) {
                  displayName = "Max Connection Pool Size"
                  group = ADVANCED.title
                  importance = Importance.LOW
                  validator = Range.atLeast(1)
                  defaultValue = Config.defaultConfig().maxConnectionPoolSize()
                })
        .define(
            ConfigKeyBuilder.of(
                Neo4jConfiguration.POOL_CONNECTION_ACQUISITION_TIMEOUT, ConfigDef.Type.STRING) {
                  displayName = "Connection Acquisition Timeout"
                  group = ADVANCED.title
                  importance = Importance.LOW
                  validator = Validators.pattern(SIMPLE_DURATION_PATTERN)
                  defaultValue =
                      Config.defaultConfig()
                          .connectionAcquisitionTimeoutMillis()
                          .milliseconds
                          .toSimpleString()
                })
        .define(
            ConfigKeyBuilder.of(
                Neo4jConfiguration.POOL_MAX_CONNECTION_LIFETIME, ConfigDef.Type.STRING) {
                  displayName = "Max Connection Lifetime"
                  group = ADVANCED.title
                  importance = Importance.LOW
                  validator = Validators.pattern(SIMPLE_DURATION_PATTERN)
                  defaultValue =
                      Config.defaultConfig()
                          .maxConnectionLifetimeMillis()
                          .milliseconds
                          .toSimpleString()
                })
        .define(
            ConfigKeyBuilder.of(
                Neo4jConfiguration.POOL_IDLE_TIME_BEFORE_TEST, ConfigDef.Type.STRING) {
                  displayName = "Idle Time Before Connection Test"
                  group = ADVANCED.title
                  importance = Importance.LOW
                  validator =
                      Validators.or(Validators.blank(), Validators.pattern(SIMPLE_DURATION_PATTERN))
                  defaultValue = ""
                })

fun ConfigDef.defineRetrySettings(): ConfigDef =
    this.define(
            ConfigKeyBuilder.of(
                Neo4jConfiguration.MAX_TRANSACTION_RETRY_TIMEOUT, ConfigDef.Type.STRING) {
                  displayName = "Transaction Retry Timeout"
                  group = ADVANCED.title
                  importance = Importance.LOW
                  validator = Validators.pattern(SIMPLE_DURATION_PATTERN)
                  defaultValue = Neo4jConfiguration.DEFAULT_MAX_RETRY_DURATION.toSimpleString()
                })
        .define(
            ConfigKeyBuilder.of(
                Neo4jConfiguration.MAX_TRANSACTION_RETRY_ATTEMPTS, ConfigDef.Type.INT) {
                  displayName = "Max Transaction Retry Attempts"
                  group = ADVANCED.title
                  importance = Importance.LOW
                  validator = Range.atLeast(1)
                  defaultValue = Neo4jConfiguration.DEFAULT_MAX_RETRY_ATTEMPTS
                })
