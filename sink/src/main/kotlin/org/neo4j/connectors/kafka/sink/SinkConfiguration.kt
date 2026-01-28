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
import kotlin.time.Duration.Companion.seconds
import org.apache.kafka.common.config.Config
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.sink.SinkTask
import org.jetbrains.annotations.TestOnly
import org.neo4j.caniuse.Neo4j
import org.neo4j.caniuse.Neo4jDetector
import org.neo4j.connectors.kafka.configuration.ConnectorType
import org.neo4j.connectors.kafka.configuration.Groups
import org.neo4j.connectors.kafka.configuration.Neo4jConfiguration
import org.neo4j.connectors.kafka.configuration.helpers.ConfigKeyBuilder
import org.neo4j.connectors.kafka.configuration.helpers.Recommenders
import org.neo4j.connectors.kafka.configuration.helpers.SIMPLE_DURATION_PATTERN
import org.neo4j.connectors.kafka.configuration.helpers.Validators
import org.neo4j.connectors.kafka.configuration.helpers.toSimpleString
import org.neo4j.cypherdsl.core.renderer.Renderer

class SinkConfiguration : Neo4jConfiguration {
  private var fixedRenderer: Renderer? = null

  constructor(original: Map<String, *>) : this(original, null)

  @TestOnly
  constructor(
      originals: Map<String, *>,
      renderer: Renderer?,
  ) : super(config(), originals, ConnectorType.SINK) {
    fixedRenderer = renderer

    validateAllTopics()
  }

  val batchSize
    get(): Int = getInt(BATCH_SIZE)

  val cypherBindTimestampAs
    get(): String = getString(CYPHER_BIND_TIMESTAMP_AS)

  val cypherBindHeaderAs
    get(): String = getString(CYPHER_BIND_HEADER_AS)

  val cypherBindKeyAs
    get(): String = getString(CYPHER_BIND_KEY_AS)

  val cypherBindValueAs
    get(): String = getString(CYPHER_BIND_VALUE_AS)

  val cypherBindValueAsEvent
    get(): Boolean = getString(CYPHER_BIND_VALUE_AS_EVENT).toBoolean()

  val patternBindTimestampAs
    get(): String = getString(PATTERN_BIND_TIMESTAMP_AS)

  val patternBindHeaderAs
    get(): String = getString(PATTERN_BIND_HEADER_AS)

  val patternBindKeyAs
    get(): String = getString(PATTERN_BIND_KEY_AS)

  val patternBindValueAs
    get(): String = getString(PATTERN_BIND_VALUE_AS)

  val neo4j: Neo4j by lazy { Neo4jDetector.detect(driver) }

  val renderer: Renderer by lazy { fixedRenderer ?: Cypher5Renderer(neo4j) }

  val topicNames: List<String>
    get() =
        originalsStrings()[SinkTask.TOPICS_CONFIG]?.split(',')?.map { it.trim() }?.toList()
            ?: emptyList()

  val topicHandlers: Map<String, SinkStrategyHandler> by lazy {
    SinkStrategyHandler.createFrom(this)
  }

  override fun userAgentComment(): String =
      SinkStrategyHandler.configuredStrategies(this).sorted().joinToString("; ")

  private fun validateAllTopics() {
    val sourceTopics = topicNames.toSet()
    val configuredTopics = topicHandlers.keys

    if (sourceTopics != configuredTopics) {
      throw ConfigException(
          "There is a mismatch between topics defined into the property `${SinkTask.TOPICS_CONFIG}` ($sourceTopics) and configured strategies ($configuredTopics)"
      )
    }
  }

  companion object {
    const val BATCH_SIZE = "neo4j.batch-size"
    const val BATCH_TIMEOUT = "neo4j.batch-timeout"

    const val CYPHER_TOPIC_PREFIX = "neo4j.cypher.topic."
    const val CYPHER_BIND_TIMESTAMP_AS = "neo4j.cypher.bind-timestamp-as"
    const val CYPHER_BIND_HEADER_AS = "neo4j.cypher.bind-header-as"
    const val CYPHER_BIND_KEY_AS = "neo4j.cypher.bind-key-as"
    const val CYPHER_BIND_VALUE_AS = "neo4j.cypher.bind-value-as"
    const val CYPHER_BIND_VALUE_AS_EVENT = "neo4j.cypher.bind-value-as-event"
    const val CDC_MAX_BATCHED_QUERIES = "neo4j.cdc.max-batched-queries"
    const val CDC_SOURCE_ID_TOPICS = "neo4j.cdc.source-id.topics"
    const val CDC_SOURCE_ID_LABEL_NAME = "neo4j.cdc.source-id.label-name"
    const val CDC_SOURCE_ID_PROPERTY_NAME = "neo4j.cdc.source-id.property-name"
    const val CDC_SCHEMA_TOPICS = "neo4j.cdc.schema.topics"
    const val PATTERN_BIND_TIMESTAMP_AS = "neo4j.pattern.bind-timestamp-as"
    const val PATTERN_BIND_HEADER_AS = "neo4j.pattern.bind-header-as"
    const val PATTERN_BIND_KEY_AS = "neo4j.pattern.bind-key-as"
    const val PATTERN_BIND_VALUE_AS = "neo4j.pattern.bind-value-as"
    const val PATTERN_TOPIC_PREFIX = "neo4j.pattern.topic."
    const val PATTERN_MERGE_NODE_PROPERTIES = "neo4j.pattern.merge-node-properties"
    const val PATTERN_MERGE_RELATIONSHIP_PROPERTIES = "neo4j.pattern.merge-relationship-properties"
    const val CUD_TOPICS = "neo4j.cud.topics"

    private const val DEFAULT_BATCH_SIZE = 1000
    private val DEFAULT_BATCH_TIMEOUT = 0.seconds
    private const val DEFAULT_TOPIC_PATTERN_MERGE_NODE_PROPERTIES = false
    private const val DEFAULT_TOPIC_PATTERN_MERGE_RELATIONSHIP_PROPERTIES = false
    const val DEFAULT_BIND_TIMESTAMP_ALIAS = "__timestamp"
    const val DEFAULT_BIND_HEADER_ALIAS = "__header"
    const val DEFAULT_BIND_KEY_ALIAS = "__key"
    const val DEFAULT_BIND_VALUE_ALIAS = "__value"
    const val DEFAULT_CYPHER_BIND_VALUE_AS_EVENT = true
    const val DEFAULT_CDC_MAX_BATCHED_QUERIES = 50
    const val DEFAULT_SOURCE_ID_LABEL_NAME = "SourceEvent"
    const val DEFAULT_SOURCE_ID_PROPERTY_NAME = "sourceId"

    fun validate(config: Config) {
      Neo4jConfiguration.validate(config)

      // cypher bind variables
      val cypherAliasForTimestamp = config.value<String>(CYPHER_BIND_TIMESTAMP_AS).isNullOrEmpty()
      val cypherAliasForHeader = config.value<String>(CYPHER_BIND_HEADER_AS).isNullOrEmpty()
      val cypherAliasForKey = config.value<String>(CYPHER_BIND_KEY_AS).isNullOrEmpty()
      val cypherAliasForValue = config.value<String>(CYPHER_BIND_VALUE_AS).isNullOrEmpty()
      val cypherUseEventForValue =
          config.value<String>(CYPHER_BIND_VALUE_AS_EVENT)?.toBoolean() ?: true
      if (
          !cypherUseEventForValue &&
              (cypherAliasForHeader &&
                  cypherAliasForKey &&
                  cypherAliasForValue &&
                  cypherAliasForTimestamp)
      ) {
        config
            .configValues()
            .filter {
              it.name() in
                  listOf(
                      CYPHER_BIND_TIMESTAMP_AS,
                      CYPHER_BIND_HEADER_AS,
                      CYPHER_BIND_KEY_AS,
                      CYPHER_BIND_VALUE_AS,
                      CYPHER_BIND_VALUE_AS_EVENT,
                  )
            }
            .forEach {
              it.addErrorMessage(
                  "At least one variable binding must be specified for Cypher strategies."
              )
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
                  importance = ConfigDef.Importance.MEDIUM
                  defaultValue = ""
                  group = Groups.CONNECTOR.title
                }
            )
            .define(
                ConfigKeyBuilder.of(CDC_SCHEMA_TOPICS, ConfigDef.Type.LIST) {
                  importance = ConfigDef.Importance.MEDIUM
                  defaultValue = ""
                  group = Groups.CONNECTOR.title
                }
            )
            .define(
                ConfigKeyBuilder.of(CUD_TOPICS, ConfigDef.Type.LIST) {
                  importance = ConfigDef.Importance.MEDIUM
                  defaultValue = ""
                  group = Groups.CONNECTOR.title
                }
            )
            .define(
                ConfigKeyBuilder.of(BATCH_SIZE, ConfigDef.Type.INT) {
                  importance = ConfigDef.Importance.MEDIUM
                  defaultValue = DEFAULT_BATCH_SIZE
                  group = Groups.CONNECTOR_ADVANCED.title
                  validator = ConfigDef.Range.atLeast(1)
                }
            )
            .define(
                ConfigKeyBuilder.of(BATCH_TIMEOUT, ConfigDef.Type.STRING) {
                  importance = ConfigDef.Importance.MEDIUM
                  defaultValue = DEFAULT_BATCH_TIMEOUT.toSimpleString()
                  group = Groups.CONNECTOR_ADVANCED.title
                  validator = Validators.pattern(SIMPLE_DURATION_PATTERN)
                }
            )
            .define(
                ConfigKeyBuilder.of(CDC_MAX_BATCHED_QUERIES, ConfigDef.Type.INT) {
                  importance = ConfigDef.Importance.LOW
                  defaultValue = DEFAULT_CDC_MAX_BATCHED_QUERIES
                  group = Groups.CONNECTOR_ADVANCED.title
                  recommender =
                      Recommenders.visibleIfNotEmpty(Predicate.isEqual(CDC_SOURCE_ID_TOPICS))
                }
            )
            .define(
                ConfigKeyBuilder.of(CDC_SOURCE_ID_LABEL_NAME, ConfigDef.Type.STRING) {
                  importance = ConfigDef.Importance.LOW
                  defaultValue = DEFAULT_SOURCE_ID_LABEL_NAME
                  group = Groups.CONNECTOR_ADVANCED.title
                  recommender =
                      Recommenders.visibleIfNotEmpty(
                          Predicate.isEqual<String>(CDC_SOURCE_ID_TOPICS)
                              .or(Predicate.isEqual(CDC_SCHEMA_TOPICS))
                      )
                }
            )
            .define(
                ConfigKeyBuilder.of(CDC_SOURCE_ID_PROPERTY_NAME, ConfigDef.Type.STRING) {
                  importance = ConfigDef.Importance.LOW
                  defaultValue = DEFAULT_SOURCE_ID_PROPERTY_NAME
                  group = Groups.CONNECTOR_ADVANCED.title
                  recommender =
                      Recommenders.visibleIfNotEmpty(Predicate.isEqual(CDC_SOURCE_ID_TOPICS))
                }
            )
            .define(
                ConfigKeyBuilder.of(CYPHER_BIND_TIMESTAMP_AS, ConfigDef.Type.STRING) {
                  importance = ConfigDef.Importance.MEDIUM
                  defaultValue = DEFAULT_BIND_TIMESTAMP_ALIAS
                  group = Groups.CONNECTOR_ADVANCED.title
                }
            )
            .define(
                ConfigKeyBuilder.of(CYPHER_BIND_HEADER_AS, ConfigDef.Type.STRING) {
                  importance = ConfigDef.Importance.LOW
                  defaultValue = DEFAULT_BIND_HEADER_ALIAS
                  group = Groups.CONNECTOR_ADVANCED.title
                }
            )
            .define(
                ConfigKeyBuilder.of(CYPHER_BIND_KEY_AS, ConfigDef.Type.STRING) {
                  importance = ConfigDef.Importance.LOW
                  defaultValue = DEFAULT_BIND_KEY_ALIAS
                  group = Groups.CONNECTOR_ADVANCED.title
                }
            )
            .define(
                ConfigKeyBuilder.of(CYPHER_BIND_VALUE_AS, ConfigDef.Type.STRING) {
                  importance = ConfigDef.Importance.LOW
                  defaultValue = DEFAULT_BIND_VALUE_ALIAS
                  group = Groups.CONNECTOR_ADVANCED.title
                }
            )
            .define(
                ConfigKeyBuilder.of(CYPHER_BIND_VALUE_AS_EVENT, ConfigDef.Type.STRING) {
                  importance = ConfigDef.Importance.LOW
                  defaultValue = DEFAULT_CYPHER_BIND_VALUE_AS_EVENT.toString()
                  group = Groups.CONNECTOR_ADVANCED.title
                  validator = Validators.bool()
                  recommender = Recommenders.bool()
                }
            )
            .define(
                ConfigKeyBuilder.of(PATTERN_BIND_TIMESTAMP_AS, ConfigDef.Type.STRING) {
                  importance = ConfigDef.Importance.LOW
                  defaultValue = DEFAULT_BIND_TIMESTAMP_ALIAS
                  group = Groups.CONNECTOR_ADVANCED.title
                }
            )
            .define(
                ConfigKeyBuilder.of(PATTERN_BIND_HEADER_AS, ConfigDef.Type.STRING) {
                  importance = ConfigDef.Importance.LOW
                  defaultValue = DEFAULT_BIND_HEADER_ALIAS
                  group = Groups.CONNECTOR_ADVANCED.title
                }
            )
            .define(
                ConfigKeyBuilder.of(PATTERN_BIND_KEY_AS, ConfigDef.Type.STRING) {
                  importance = ConfigDef.Importance.LOW
                  defaultValue = DEFAULT_BIND_KEY_ALIAS
                  group = Groups.CONNECTOR_ADVANCED.title
                }
            )
            .define(
                ConfigKeyBuilder.of(PATTERN_BIND_VALUE_AS, ConfigDef.Type.STRING) {
                  importance = ConfigDef.Importance.LOW
                  defaultValue = DEFAULT_BIND_VALUE_ALIAS
                  group = Groups.CONNECTOR_ADVANCED.title
                }
            )
            .define(
                ConfigKeyBuilder.of(PATTERN_MERGE_NODE_PROPERTIES, ConfigDef.Type.STRING) {
                  importance = ConfigDef.Importance.LOW
                  defaultValue = DEFAULT_TOPIC_PATTERN_MERGE_NODE_PROPERTIES.toString()
                  group = Groups.CONNECTOR_ADVANCED.title
                  validator = Validators.bool()
                  recommender = Recommenders.bool()
                }
            )
            .define(
                ConfigKeyBuilder.of(PATTERN_MERGE_RELATIONSHIP_PROPERTIES, ConfigDef.Type.STRING) {
                  importance = ConfigDef.Importance.LOW
                  defaultValue = DEFAULT_TOPIC_PATTERN_MERGE_RELATIONSHIP_PROPERTIES.toString()
                  group = Groups.CONNECTOR_ADVANCED.title
                  validator = Validators.bool()
                  recommender = Recommenders.bool()
                }
            )
  }
}
