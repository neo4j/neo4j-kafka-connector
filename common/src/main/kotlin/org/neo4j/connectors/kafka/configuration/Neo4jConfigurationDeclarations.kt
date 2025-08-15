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
import org.neo4j.connectors.kafka.configuration.helpers.ConfigKeyBuilder
import org.neo4j.connectors.kafka.configuration.helpers.Recommenders
import org.neo4j.connectors.kafka.configuration.helpers.SIMPLE_DURATION_PATTERN
import org.neo4j.connectors.kafka.configuration.helpers.Validators
import org.neo4j.connectors.kafka.configuration.helpers.toSimpleString
import org.neo4j.driver.Config
import org.neo4j.driver.Config.TrustStrategy.Strategy

fun ConfigDef.defineConnectionSettings(): ConfigDef =
    this.define(
            ConfigKeyBuilder.of(Neo4jConfiguration.URI, ConfigDef.Type.LIST) {
              importance = Importance.HIGH
              group = Groups.CONNECTION.title
              validator =
                  Validators.uri("neo4j", "neo4j+s", "neo4j+ssc", "bolt", "bolt+s", "bolt+ssc")
            }
        )
        .define(
            ConfigKeyBuilder.of(Neo4jConfiguration.DATABASE, ConfigDef.Type.STRING) {
              importance = Importance.HIGH
              defaultValue = ""
              group = Groups.CONNECTION.title
              validator = ConfigDef.NonNullValidator()
            }
        )
        .define(
            ConfigKeyBuilder.of(Neo4jConfiguration.AUTHENTICATION_TYPE, ConfigDef.Type.STRING) {
              importance = Importance.HIGH
              defaultValue = AuthenticationType.BASIC.toString()
              group = Groups.CONNECTION.title
              validator = Validators.enum(AuthenticationType::class.java)
              recommender = Recommenders.enum(AuthenticationType::class.java)
            }
        )
        .define(
            ConfigKeyBuilder.of(
                Neo4jConfiguration.AUTHENTICATION_BASIC_USERNAME,
                ConfigDef.Type.STRING,
            ) {
              importance = Importance.HIGH
              defaultValue = ""
              group = Groups.CONNECTION.title
              recommender =
                  Recommenders.visibleIf(
                      Neo4jConfiguration.AUTHENTICATION_TYPE,
                      Predicate.isEqual(AuthenticationType.BASIC.toString()),
                  )
            }
        )
        .define(
            ConfigKeyBuilder.of(
                Neo4jConfiguration.AUTHENTICATION_BASIC_PASSWORD,
                ConfigDef.Type.PASSWORD,
            ) {
              importance = Importance.HIGH
              defaultValue = ""
              group = Groups.CONNECTION.title
              recommender =
                  Recommenders.visibleIf(
                      Neo4jConfiguration.AUTHENTICATION_TYPE,
                      Predicate.isEqual(AuthenticationType.BASIC.toString()),
                  )
            }
        )
        .define(
            ConfigKeyBuilder.of(
                Neo4jConfiguration.AUTHENTICATION_BASIC_REALM,
                ConfigDef.Type.STRING,
            ) {
              importance = Importance.LOW
              defaultValue = ""
              group = Groups.CONNECTION.title
              recommender =
                  Recommenders.visibleIf(
                      Neo4jConfiguration.AUTHENTICATION_TYPE,
                      Predicate.isEqual(AuthenticationType.BASIC.toString()),
                  )
            }
        )
        .define(
            ConfigKeyBuilder.of(
                Neo4jConfiguration.AUTHENTICATION_KERBEROS_TICKET,
                ConfigDef.Type.PASSWORD,
            ) {
              group = Groups.CONNECTION.title
              importance = Importance.HIGH
              defaultValue = ""
              recommender =
                  Recommenders.visibleIf(
                      Neo4jConfiguration.AUTHENTICATION_TYPE,
                      Predicate.isEqual(AuthenticationType.KERBEROS.toString()),
                  )
            }
        )
        .define(
            ConfigKeyBuilder.of(
                Neo4jConfiguration.AUTHENTICATION_BEARER_TOKEN,
                ConfigDef.Type.PASSWORD,
            ) {
              importance = Importance.HIGH
              defaultValue = ""
              group = Groups.CONNECTION.title
              recommender =
                  Recommenders.visibleIf(
                      Neo4jConfiguration.AUTHENTICATION_TYPE,
                      Predicate.isEqual(AuthenticationType.BEARER.toString()),
                  )
            }
        )
        .define(
            ConfigKeyBuilder.of(
                Neo4jConfiguration.AUTHENTICATION_CUSTOM_SCHEME,
                ConfigDef.Type.STRING,
            ) {
              importance = Importance.HIGH
              defaultValue = ""
              group = Groups.CONNECTION.title
              recommender =
                  Recommenders.visibleIf(
                      Neo4jConfiguration.AUTHENTICATION_TYPE,
                      Predicate.isEqual(AuthenticationType.CUSTOM.toString()),
                  )
            }
        )
        .define(
            ConfigKeyBuilder.of(
                Neo4jConfiguration.AUTHENTICATION_CUSTOM_PRINCIPAL,
                ConfigDef.Type.STRING,
            ) {
              importance = Importance.HIGH
              defaultValue = ""
              group = Groups.CONNECTION.title
              recommender =
                  Recommenders.visibleIf(
                      Neo4jConfiguration.AUTHENTICATION_TYPE,
                      Predicate.isEqual(AuthenticationType.CUSTOM.toString()),
                  )
            }
        )
        .define(
            ConfigKeyBuilder.of(
                Neo4jConfiguration.AUTHENTICATION_CUSTOM_CREDENTIALS,
                ConfigDef.Type.PASSWORD,
            ) {
              importance = Importance.HIGH
              defaultValue = ""
              group = Groups.CONNECTION.title
              recommender =
                  Recommenders.visibleIf(
                      Neo4jConfiguration.AUTHENTICATION_TYPE,
                      Predicate.isEqual(AuthenticationType.CUSTOM.toString()),
                  )
            }
        )
        .define(
            ConfigKeyBuilder.of(
                Neo4jConfiguration.AUTHENTICATION_CUSTOM_REALM,
                ConfigDef.Type.STRING,
            ) {
              importance = Importance.HIGH
              defaultValue = ""
              group = Groups.CONNECTION.title
              recommender =
                  Recommenders.visibleIf(
                      Neo4jConfiguration.AUTHENTICATION_TYPE,
                      Predicate.isEqual(AuthenticationType.CUSTOM.toString()),
                  )
            }
        )

fun ConfigDef.defineEncryptionSettings(): ConfigDef =
    this.define(
            ConfigKeyBuilder.of(Neo4jConfiguration.SECURITY_ENCRYPTED, ConfigDef.Type.STRING) {
              importance = Importance.LOW
              defaultValue = "false"
              group = Groups.CONNECTION_TLS.title
              validator = Validators.bool()
              recommender =
                  Recommenders.and(
                      Recommenders.bool(),
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
                          },
                      ),
                  )
            }
        )
        .define(
            ConfigKeyBuilder.of(Neo4jConfiguration.SECURITY_TRUST_STRATEGY, ConfigDef.Type.STRING) {
              importance = Importance.LOW
              defaultValue = Strategy.TRUST_SYSTEM_CA_SIGNED_CERTIFICATES.toString()
              group = Groups.CONNECTION_TLS.title
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
                          },
                      ),
                      Recommenders.visibleIf(
                          Neo4jConfiguration.SECURITY_ENCRYPTED,
                          Predicate.isEqual(true),
                      ),
                  )
            }
        )
        .define(
            ConfigKeyBuilder.of(
                Neo4jConfiguration.SECURITY_HOST_NAME_VERIFICATION_ENABLED,
                ConfigDef.Type.STRING,
            ) {
              importance = Importance.LOW
              defaultValue = "true"
              group = Groups.CONNECTION_TLS.title
              validator = Validators.bool()
              recommender =
                  Recommenders.and(
                      Recommenders.bool(),
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
                          },
                      ),
                      Recommenders.visibleIf(
                          Neo4jConfiguration.SECURITY_ENCRYPTED,
                          Predicate.isEqual(true),
                      ),
                  )
            }
        )
        .define(
            ConfigKeyBuilder.of(Neo4jConfiguration.SECURITY_CERT_FILES, ConfigDef.Type.LIST) {
              importance = Importance.LOW
              defaultValue = ""
              group = Groups.CONNECTION_TLS.title
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
                          },
                      ),
                      Recommenders.visibleIf(
                          Neo4jConfiguration.SECURITY_ENCRYPTED,
                          Predicate.isEqual(true),
                      ),
                      Recommenders.visibleIf(
                          Neo4jConfiguration.SECURITY_TRUST_STRATEGY,
                          Predicate.isEqual(Strategy.TRUST_CUSTOM_CA_SIGNED_CERTIFICATES.toString()),
                      ),
                  )
            }
        )

fun ConfigDef.definePoolSettings(): ConfigDef =
    this.define(
            ConfigKeyBuilder.of(Neo4jConfiguration.CONNECTION_TIMEOUT, ConfigDef.Type.STRING) {
              importance = Importance.LOW
              defaultValue =
                  Config.defaultConfig().connectionTimeoutMillis().milliseconds.toSimpleString()
              group = Groups.CONNECTION_ADVANCED.title
              validator = Validators.pattern(SIMPLE_DURATION_PATTERN)
            }
        )
        .define(
            ConfigKeyBuilder.of(
                Neo4jConfiguration.POOL_MAX_CONNECTION_POOL_SIZE,
                ConfigDef.Type.INT,
            ) {
              importance = Importance.LOW
              defaultValue = Config.defaultConfig().maxConnectionPoolSize()
              group = Groups.CONNECTION_ADVANCED.title
              validator = Range.atLeast(1)
            }
        )
        .define(
            ConfigKeyBuilder.of(
                Neo4jConfiguration.POOL_CONNECTION_ACQUISITION_TIMEOUT,
                ConfigDef.Type.STRING,
            ) {
              importance = Importance.LOW
              defaultValue =
                  Config.defaultConfig()
                      .connectionAcquisitionTimeoutMillis()
                      .milliseconds
                      .toSimpleString()
              group = Groups.CONNECTION_ADVANCED.title
              validator = Validators.pattern(SIMPLE_DURATION_PATTERN)
            }
        )
        .define(
            ConfigKeyBuilder.of(
                Neo4jConfiguration.POOL_MAX_CONNECTION_LIFETIME,
                ConfigDef.Type.STRING,
            ) {
              importance = Importance.LOW
              defaultValue =
                  Config.defaultConfig().maxConnectionLifetimeMillis().milliseconds.toSimpleString()
              group = Groups.CONNECTION_ADVANCED.title
              validator = Validators.pattern(SIMPLE_DURATION_PATTERN)
            }
        )
        .define(
            ConfigKeyBuilder.of(
                Neo4jConfiguration.POOL_IDLE_TIME_BEFORE_TEST,
                ConfigDef.Type.STRING,
            ) {
              importance = Importance.LOW
              defaultValue = ""
              group = Groups.CONNECTION_ADVANCED.title
              validator =
                  Validators.or(Validators.blank(), Validators.pattern(SIMPLE_DURATION_PATTERN))
            }
        )

fun ConfigDef.defineRetrySettings(): ConfigDef =
    this.define(
        ConfigKeyBuilder.of(
            Neo4jConfiguration.MAX_TRANSACTION_RETRY_TIMEOUT,
            ConfigDef.Type.STRING,
        ) {
          importance = Importance.LOW
          defaultValue = Neo4jConfiguration.DEFAULT_MAX_RETRY_DURATION.toSimpleString()
          group = Groups.CONNECTION_ADVANCED.title
          validator = Validators.pattern(SIMPLE_DURATION_PATTERN)
        }
    )
