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
package org.neo4j.connectors.kafka.sink

import java.util.function.Predicate
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import org.apache.kafka.common.config.Config
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.sink.SinkTask
import org.neo4j.connectors.kafka.configuration.ConfigGroup
import org.neo4j.connectors.kafka.configuration.ConnectorType
import org.neo4j.connectors.kafka.configuration.DeprecatedNeo4jConfiguration
import org.neo4j.connectors.kafka.configuration.Neo4jConfiguration
import org.neo4j.connectors.kafka.configuration.helpers.ConfigKeyBuilder
import org.neo4j.connectors.kafka.configuration.helpers.Recommenders
import org.neo4j.connectors.kafka.configuration.helpers.SIMPLE_DURATION_PATTERN
import org.neo4j.connectors.kafka.configuration.helpers.Validators
import org.neo4j.connectors.kafka.configuration.helpers.parseSimpleString
import org.neo4j.connectors.kafka.configuration.helpers.toSimpleString
import org.neo4j.connectors.kafka.service.TopicType
import org.neo4j.connectors.kafka.service.sink.strategy.SourceIdIngestionStrategyConfig
import org.neo4j.connectors.kafka.sink.utils.TopicUtils
import org.neo4j.connectors.kafka.sink.utils.Topics
import org.neo4j.connectors.kafka.utils.PropertiesUtil
import org.neo4j.cypherdsl.core.Cypher
import org.neo4j.cypherdsl.core.renderer.Configuration
import org.neo4j.cypherdsl.core.renderer.Dialect
import org.neo4j.cypherdsl.core.renderer.Renderer

class SinkConfiguration(originals: Map<String, *>) :
    Neo4jConfiguration(config(), originals, ConnectorType.SINK) {

  val parallelBatches
    get(): Boolean = getBoolean(BATCH_PARALLELIZE)

  val batchSize
    get(): Int = getInt(BATCH_SIZE)

  val batchTimeout
    get(): Duration = Duration.parseSimpleString(getString(BATCH_TIMEOUT))

  val topics: Topics by lazy { Topics.from(originals()) }

  val strategyMap: Map<TopicType, Any> by lazy { TopicUtils.toStrategyMap(topics) }

  val kafkaBrokerProperties: Map<String, Any?> by lazy {
    originals().filterKeys { it.startsWith("kafka.") }.mapKeys { it.key.substring("kafka.".length) }
  }

  val cypherBindTimestampAs
    get(): String = getString(CYPHER_BIND_TIMESTAMP_AS)

  val cypherBindHeaderAs
    get(): String = getString(CYPHER_BIND_HEADER_AS)

  val cypherBindKeyAs
    get(): String = getString(CYPHER_BIND_KEY_AS)

  val cypherBindValueAs
    get(): String = getString(CYPHER_BIND_VALUE_AS)

  val cypherBindValueAsEvent
    get(): Boolean = getBoolean(CYPHER_BIND_VALUE_AS_EVENT)

  val patternBindTimestampAs
    get(): String = getString(PATTERN_BIND_TIMESTAMP_AS)

  val patternBindHeaderAs
    get(): String = getString(PATTERN_BIND_HEADER_AS)

  val patternBindKeyAs
    get(): String = getString(PATTERN_BIND_KEY_AS)

  val patternBindValueAs
    get(): String = getString(PATTERN_BIND_VALUE_AS)

  val dialect: Dialect by lazy {
    session().use {
      val name = Cypher.name("name")
      val versions = Cypher.name("versions")
      val stmt =
          Cypher.call("dbms.components")
              .yield(name, versions)
              .where(name.eq(Cypher.anonParameter("Neo4j Kernel")))
              .returning(Cypher.valueAt(versions, 0))
              .build()

      val version = it.run(stmt.cypher, stmt.parameters).single().get(0).asString()
      if (version.startsWith("5")) {
        return@lazy Dialect.NEO4J_5
      } else if (version.startsWith("4")) {
        return@lazy Dialect.DEFAULT
      }

      throw ConfigException("unsupported Neo4j version: $version")
    }
  }

  val renderer: Renderer by lazy {
    Renderer.getRenderer(Configuration.newConfig().withDialect(dialect).build())
  }

  val topicNames: List<String>
    get() =
        originalsStrings()[SinkTask.TOPICS_CONFIG]?.split(',')?.map { it.trim() }?.toList()
            ?: emptyList()

  init {
    validateAllTopics(originals)
  }

  override fun userAgentComment(): String =
      SinkStrategyHandler.configuredStrategies(this).sorted().joinToString("; ")

  private fun validateAllTopics(originals: Map<*, *>) {
    TopicUtils.validate<ConfigException>(this.topics)
    val topics =
        if (originals.containsKey(SinkTask.TOPICS_CONFIG)) {
          originals[SinkTask.TOPICS_CONFIG].toString().split(",").map { it.trim() }.sorted()
        } else { // TODO manage regexp
          emptyList()
        }
    val allTopics = this.topics.allTopics().sorted()
    if (topics != allTopics) {
      throw ConfigException(
          "There is a mismatch between topics defined into the property `${SinkTask.TOPICS_CONFIG}` ($topics) and configured topics ($allTopics)")
    }
  }

  companion object {
    const val BATCH_SIZE = "neo4j.batch-size"
    const val BATCH_TIMEOUT = "neo4j.batch-timeout"
    const val BATCH_PARALLELIZE = "neo4j.batch-parallelize"

    const val CYPHER_TOPIC_PREFIX = "neo4j.cypher.topic."
    const val CYPHER_BIND_TIMESTAMP_AS = "neo4j.cypher.bind-timestamp-as"
    const val CYPHER_BIND_HEADER_AS = "neo4j.cypher.bind-header-as"
    const val CYPHER_BIND_KEY_AS = "neo4j.cypher.bind-key-as"
    const val CYPHER_BIND_VALUE_AS = "neo4j.cypher.bind-value-as"
    const val CYPHER_BIND_VALUE_AS_EVENT = "neo4j.cypher.bind-value-as-event"
    const val CDC_SOURCE_ID_TOPICS = "neo4j.cdc.source-id.topics"
    const val CDC_SOURCE_ID_LABEL_NAME = "neo4j.cdc.source-id.label-name"
    const val CDC_SOURCE_ID_PROPERTY_NAME = "neo4j.cdc.source-id.property-name"
    const val CDC_SCHEMA_TOPICS = "neo4j.cdc.schema.topics"
    const val PATTERN_BIND_TIMESTAMP_AS = "neo4j.pattern.bind-timestamp-as"
    const val PATTERN_BIND_HEADER_AS = "neo4j.pattern.bind-header-as"
    const val PATTERN_BIND_KEY_AS = "neo4j.pattern.bind-key-as"
    const val PATTERN_BIND_VALUE_AS = "neo4j.pattern.bind-value-as"
    const val PATTERN_NODE_TOPIC_PREFIX = "neo4j.pattern.node.topic."
    const val PATTERN_RELATIONSHIP_TOPIC_PREFIX = "neo4j.pattern.relationship.topic."
    const val PATTERN_NODE_MERGE_PROPERTIES = "neo4j.pattern.node.merge-properties"
    const val PATTERN_RELATIONSHIP_MERGE_PROPERTIES = "neo4j.pattern.relationship.merge-properties"
    const val CUD_TOPICS = "neo4j.cud.topics"

    private const val DEFAULT_BATCH_SIZE = 1000
    val DEFAULT_BATCH_TIMEOUT = 0.seconds
    private const val DEFAULT_BATCH_PARALLELIZE = true
    private const val DEFAULT_TOPIC_PATTERN_MERGE_NODE_PROPERTIES = false
    private const val DEFAULT_TOPIC_PATTERN_MERGE_RELATIONSHIP_PROPERTIES = false
    const val DEFAULT_BIND_TIMESTAMP_ALIAS = "__timestamp"
    const val DEFAULT_BIND_HEADER_ALIAS = "__header"
    const val DEFAULT_BIND_KEY_ALIAS = "__key"
    const val DEFAULT_BIND_VALUE_ALIAS = "__value"
    const val DEFAULT_CYPHER_BIND_VALUE_AS_EVENT = true

    @Suppress("DEPRECATION")
    @JvmStatic
    val KEY_REPLACEMENTS =
        mapOf(
            DeprecatedNeo4jSinkConfiguration.TOPIC_CYPHER_PREFIX to CYPHER_TOPIC_PREFIX,
            DeprecatedNeo4jSinkConfiguration.TOPIC_PATTERN_NODE_PREFIX to PATTERN_NODE_TOPIC_PREFIX,
            DeprecatedNeo4jSinkConfiguration.TOPIC_PATTERN_RELATIONSHIP_PREFIX to
                PATTERN_RELATIONSHIP_TOPIC_PREFIX)

    @Suppress("DEPRECATION")
    fun migrateSettings(oldSettings: Map<String, Any>): Map<String, String> {
      val migratedBase = migrateSettings(oldSettings, false)
      val migrated = HashMap<String, String>(migratedBase.size)

      migratedBase.forEach {
        when (it.key) {
          DeprecatedNeo4jConfiguration.BATCH_SIZE -> migrated[BATCH_SIZE] = it.value
          DeprecatedNeo4jConfiguration.BATCH_TIMEOUT_MSECS ->
              migrated[BATCH_TIMEOUT] = "${it.value}ms"
          DeprecatedNeo4jSinkConfiguration.BATCH_PARALLELIZE ->
              migrated[BATCH_PARALLELIZE] = it.value
          DeprecatedNeo4jSinkConfiguration.TOPIC_PATTERN_MERGE_NODE_PROPERTIES_ENABLED ->
              migrated[PATTERN_NODE_MERGE_PROPERTIES] = it.value
          DeprecatedNeo4jSinkConfiguration.TOPIC_PATTERN_MERGE_RELATIONSHIP_PROPERTIES_ENABLED ->
              migrated[PATTERN_RELATIONSHIP_MERGE_PROPERTIES] = it.value
          DeprecatedNeo4jSinkConfiguration.TOPIC_CDC_SOURCE_ID ->
              migrated[CDC_SOURCE_ID_TOPICS] = it.value.replaceLegacyDelimiter()
          DeprecatedNeo4jSinkConfiguration.TOPIC_CDC_SOURCE_ID_LABEL_NAME ->
              migrated[CDC_SOURCE_ID_LABEL_NAME] = it.value
          DeprecatedNeo4jSinkConfiguration.TOPIC_CDC_SOURCE_ID_ID_NAME ->
              migrated[CDC_SOURCE_ID_PROPERTY_NAME] = it.value
          DeprecatedNeo4jSinkConfiguration.TOPIC_CDC_SCHEMA ->
              migrated[CDC_SCHEMA_TOPICS] = it.value.replaceLegacyDelimiter()
          DeprecatedNeo4jSinkConfiguration.TOPIC_CUD ->
              migrated[CUD_TOPICS] = it.value.replaceLegacyDelimiter()
          else -> {
            val migratedKey = replaceLegacyPropertyKeys(it.key)
            if (!migrated.containsKey(migratedKey)) {
              migrated[migratedKey] = it.value
            }
          }
        }
      }

      return migrated
    }

    fun validate(config: Config) {
      Neo4jConfiguration.validate(config)

      // cypher bind variables
      val cypherAliasForTimestamp = config.value<String>(CYPHER_BIND_TIMESTAMP_AS).isNullOrEmpty()
      val cypherAliasForHeader = config.value<String>(CYPHER_BIND_HEADER_AS).isNullOrEmpty()
      val cypherAliasForKey = config.value<String>(CYPHER_BIND_KEY_AS).isNullOrEmpty()
      val cypherAliasForValue = config.value<String>(CYPHER_BIND_VALUE_AS).isNullOrEmpty()
      val cypherUseEventForValue = config.value<Boolean>(CYPHER_BIND_VALUE_AS_EVENT) ?: true
      if (!cypherUseEventForValue &&
          (cypherAliasForHeader &&
              cypherAliasForKey &&
              cypherAliasForValue &&
              cypherAliasForTimestamp)) {
        config
            .configValues()
            .filter {
              it.name() in
                  listOf(
                      CYPHER_BIND_TIMESTAMP_AS,
                      CYPHER_BIND_HEADER_AS,
                      CYPHER_BIND_KEY_AS,
                      CYPHER_BIND_VALUE_AS,
                      CYPHER_BIND_VALUE_AS_EVENT)
            }
            .forEach {
              it.addErrorMessage(
                  "At least one variable binding must be specified for Cypher strategies.")
            }
      }
    }

    @Suppress("UNCHECKED_CAST")
    private fun <T> Config.value(key: String): T? {
      return this.configValues().first { it.name() == key }.let { it.value() as T? }
    }

    fun config(): ConfigDef =
        Neo4jConfiguration.config()
            .define(
                ConfigKeyBuilder.of(CDC_SOURCE_ID_TOPICS, ConfigDef.Type.LIST) {
                  importance = ConfigDef.Importance.HIGH
                  defaultValue = ""
                  group = ConfigGroup.STRATEGIES
                })
            .define(
                ConfigKeyBuilder.of(CDC_SOURCE_ID_LABEL_NAME, ConfigDef.Type.STRING) {
                  importance = ConfigDef.Importance.HIGH
                  defaultValue = SourceIdIngestionStrategyConfig.DEFAULT.labelName
                  group = ConfigGroup.STRATEGIES
                  recommender =
                      Recommenders.visibleIfNotEmpty(Predicate.isEqual(CDC_SOURCE_ID_TOPICS))
                })
            .define(
                ConfigKeyBuilder.of(CDC_SOURCE_ID_PROPERTY_NAME, ConfigDef.Type.STRING) {
                  importance = ConfigDef.Importance.HIGH
                  defaultValue = SourceIdIngestionStrategyConfig.DEFAULT.idName
                  group = ConfigGroup.STRATEGIES
                  recommender =
                      Recommenders.visibleIfNotEmpty(Predicate.isEqual(CDC_SOURCE_ID_TOPICS))
                })
            .define(
                ConfigKeyBuilder.of(CDC_SCHEMA_TOPICS, ConfigDef.Type.LIST) {
                  importance = ConfigDef.Importance.HIGH
                  defaultValue = ""
                  group = ConfigGroup.STRATEGIES
                })
            .define(
                ConfigKeyBuilder.of(CUD_TOPICS, ConfigDef.Type.LIST) {
                  importance = ConfigDef.Importance.HIGH
                  defaultValue = ""
                  group = ConfigGroup.STRATEGIES
                })
            .define(
                ConfigKeyBuilder.of(PATTERN_NODE_MERGE_PROPERTIES, ConfigDef.Type.BOOLEAN) {
                  importance = ConfigDef.Importance.MEDIUM
                  defaultValue = DEFAULT_TOPIC_PATTERN_MERGE_NODE_PROPERTIES
                  group = ConfigGroup.STRATEGIES
                  recommender =
                      Recommenders.visibleIfNotEmpty { k ->
                        k.startsWith(PATTERN_NODE_TOPIC_PREFIX) ||
                            k.startsWith(PATTERN_RELATIONSHIP_TOPIC_PREFIX)
                      }
                })
            .define(
                ConfigKeyBuilder.of(PATTERN_RELATIONSHIP_MERGE_PROPERTIES, ConfigDef.Type.BOOLEAN) {
                  documentation = PropertiesUtil.getProperty(PATTERN_RELATIONSHIP_MERGE_PROPERTIES)
                  importance = ConfigDef.Importance.MEDIUM
                  defaultValue = DEFAULT_TOPIC_PATTERN_MERGE_RELATIONSHIP_PROPERTIES
                  group = ConfigGroup.STRATEGIES
                  recommender =
                      Recommenders.visibleIfNotEmpty { k ->
                        k.startsWith(PATTERN_NODE_TOPIC_PREFIX) ||
                            k.startsWith(PATTERN_RELATIONSHIP_TOPIC_PREFIX)
                      }
                })
            .define(
                ConfigKeyBuilder.of(BATCH_SIZE, ConfigDef.Type.INT) {
                  importance = ConfigDef.Importance.HIGH
                  validator = ConfigDef.Range.atLeast(1)
                  defaultValue = DEFAULT_BATCH_SIZE
                })
            .define(
                ConfigKeyBuilder.of(BATCH_TIMEOUT, ConfigDef.Type.STRING) {
                  importance = ConfigDef.Importance.HIGH
                  validator = Validators.pattern(SIMPLE_DURATION_PATTERN)
                  defaultValue = DEFAULT_BATCH_TIMEOUT.toSimpleString()
                })
            .define(
                ConfigKeyBuilder.of(BATCH_PARALLELIZE, ConfigDef.Type.BOOLEAN) {
                  importance = ConfigDef.Importance.MEDIUM
                  defaultValue = DEFAULT_BATCH_PARALLELIZE
                  group = ConfigGroup.BATCH
                })
            .define(
                ConfigKeyBuilder.of(CYPHER_BIND_TIMESTAMP_AS, ConfigDef.Type.STRING) {
                  importance = ConfigDef.Importance.MEDIUM
                  defaultValue = DEFAULT_BIND_TIMESTAMP_ALIAS
                  group = ConfigGroup.STRATEGIES
                  recommender =
                      Recommenders.visibleIfNotEmpty { k -> k.startsWith(CYPHER_TOPIC_PREFIX) }
                })
            .define(
                ConfigKeyBuilder.of(CYPHER_BIND_HEADER_AS, ConfigDef.Type.STRING) {
                  importance = ConfigDef.Importance.MEDIUM
                  defaultValue = DEFAULT_BIND_HEADER_ALIAS
                  group = ConfigGroup.STRATEGIES
                  recommender =
                      Recommenders.visibleIfNotEmpty { k -> k.startsWith(CYPHER_TOPIC_PREFIX) }
                })
            .define(
                ConfigKeyBuilder.of(CYPHER_BIND_KEY_AS, ConfigDef.Type.STRING) {
                  importance = ConfigDef.Importance.MEDIUM
                  defaultValue = DEFAULT_BIND_KEY_ALIAS
                  group = ConfigGroup.STRATEGIES
                  recommender =
                      Recommenders.visibleIfNotEmpty { k -> k.startsWith(CYPHER_TOPIC_PREFIX) }
                })
            .define(
                ConfigKeyBuilder.of(CYPHER_BIND_VALUE_AS, ConfigDef.Type.STRING) {
                  importance = ConfigDef.Importance.MEDIUM
                  defaultValue = DEFAULT_BIND_VALUE_ALIAS
                  group = ConfigGroup.STRATEGIES
                  recommender =
                      Recommenders.visibleIfNotEmpty { k -> k.startsWith(CYPHER_TOPIC_PREFIX) }
                })
            .define(
                ConfigKeyBuilder.of(CYPHER_BIND_VALUE_AS_EVENT, ConfigDef.Type.BOOLEAN) {
                  importance = ConfigDef.Importance.MEDIUM
                  defaultValue = DEFAULT_CYPHER_BIND_VALUE_AS_EVENT
                  validator = ConfigDef.NonNullValidator()
                  group = ConfigGroup.STRATEGIES
                  recommender =
                      Recommenders.visibleIfNotEmpty { k -> k.startsWith(CYPHER_TOPIC_PREFIX) }
                })
            .define(
                ConfigKeyBuilder.of(PATTERN_BIND_TIMESTAMP_AS, ConfigDef.Type.STRING) {
                  importance = ConfigDef.Importance.MEDIUM
                  defaultValue = DEFAULT_BIND_TIMESTAMP_ALIAS
                  group = ConfigGroup.STRATEGIES
                  recommender =
                      Recommenders.visibleIfNotEmpty { k ->
                        k.startsWith(PATTERN_NODE_TOPIC_PREFIX) ||
                            k.startsWith(PATTERN_RELATIONSHIP_TOPIC_PREFIX)
                      }
                })
            .define(
                ConfigKeyBuilder.of(PATTERN_BIND_HEADER_AS, ConfigDef.Type.STRING) {
                  importance = ConfigDef.Importance.MEDIUM
                  defaultValue = DEFAULT_BIND_HEADER_ALIAS
                  group = ConfigGroup.STRATEGIES
                  recommender =
                      Recommenders.visibleIfNotEmpty { k ->
                        k.startsWith(PATTERN_NODE_TOPIC_PREFIX) ||
                            k.startsWith(PATTERN_RELATIONSHIP_TOPIC_PREFIX)
                      }
                })
            .define(
                ConfigKeyBuilder.of(PATTERN_BIND_KEY_AS, ConfigDef.Type.STRING) {
                  importance = ConfigDef.Importance.MEDIUM
                  defaultValue = DEFAULT_BIND_KEY_ALIAS
                  group = ConfigGroup.STRATEGIES
                  recommender =
                      Recommenders.visibleIfNotEmpty { k ->
                        k.startsWith(PATTERN_NODE_TOPIC_PREFIX) ||
                            k.startsWith(PATTERN_RELATIONSHIP_TOPIC_PREFIX)
                      }
                })
            .define(
                ConfigKeyBuilder.of(PATTERN_BIND_VALUE_AS, ConfigDef.Type.STRING) {
                  importance = ConfigDef.Importance.MEDIUM
                  defaultValue = DEFAULT_BIND_VALUE_ALIAS
                  group = ConfigGroup.STRATEGIES
                  recommender =
                      Recommenders.visibleIfNotEmpty { k ->
                        k.startsWith(PATTERN_NODE_TOPIC_PREFIX) ||
                            k.startsWith(PATTERN_RELATIONSHIP_TOPIC_PREFIX)
                      }
                })

    private fun replaceLegacyPropertyKeys(key: String) =
        KEY_REPLACEMENTS.entries.fold(key) { k, replacement ->
          k.replace(replacement.key, replacement.value)
        }

    private fun String.replaceLegacyDelimiter() = this.replace(';', ',')
  }
}
