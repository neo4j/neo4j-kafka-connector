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
package org.neo4j.connectors.kafka.sink

import java.util.function.Predicate
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
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

class SinkConfiguration(originals: Map<*, *>) :
    Neo4jConfiguration(config(), originals, ConnectorType.SINK) {

  val parallelBatches
    get(): Boolean = getBoolean(BATCH_PARALLELIZE)

  val batchSize
    get(): Int = getInt(BATCH_SIZE)

  val batchTimeout
    get(): Duration = Duration.parseSimpleString(getString(BATCH_TIMEOUT))

  val topics: Topics by lazy { Topics.from(originals(), "streams.sink." to "neo4j.") }

  val strategyMap: Map<TopicType, Any> by lazy { TopicUtils.toStrategyMap(topics) }

  val kafkaBrokerProperties: Map<String, Any?> by lazy {
    originals().filterKeys { it.startsWith("kafka.") }.mapKeys { it.key.substring("kafka.".length) }
  }

  init {
    validateAllTopics(originals)
  }

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

    const val TOPIC_CYPHER_PREFIX = "neo4j.topic.cypher."
    const val TOPIC_CDC_SOURCE_ID = "neo4j.topic.cdc.sourceId"
    const val TOPIC_CDC_SOURCE_ID_LABEL_NAME = "neo4j.topic.cdc.sourceId.labelName"
    const val TOPIC_CDC_SOURCE_ID_ID_NAME = "neo4j.topic.cdc.sourceId.idName"
    const val TOPIC_CDC_SCHEMA = "neo4j.topic.cdc.schema"
    const val TOPIC_PATTERN_NODE_PREFIX = "neo4j.topic.pattern.node."
    const val TOPIC_PATTERN_RELATIONSHIP_PREFIX = "neo4j.topic.pattern.relationship."
    const val TOPIC_PATTERN_MERGE_NODE_PROPERTIES = "neo4j.topic.pattern.merge-node-properties"
    const val TOPIC_PATTERN_MERGE_RELATIONSHIP_PROPERTIES =
        "neo4j.topic.pattern.merge-relationship-properties"
    const val TOPIC_CUD = "neo4j.topic.cud"

    const val DEFAULT_BATCH_SIZE = 1000
    val DEFAULT_BATCH_TIMEOUT = 0.seconds
    const val DEFAULT_BATCH_PARALLELIZE = true
    const val DEFAULT_TOPIC_PATTERN_MERGE_NODE_PROPERTIES = false
    const val DEFAULT_TOPIC_PATTERN_MERGE_RELATIONSHIP_PROPERTIES = false

    fun migrateSettings(oldSettings: Map<String, Any>): Map<String, String> {
      val migrated = Neo4jConfiguration.migrateSettings(oldSettings, true).toMutableMap()

      oldSettings.forEach {
        when (it.key) {
          DeprecatedNeo4jConfiguration.BATCH_SIZE -> migrated[BATCH_SIZE] = it.value.toString()
          DeprecatedNeo4jConfiguration.BATCH_TIMEOUT_MSECS ->
              migrated[BATCH_TIMEOUT] = "${it.value}ms"
          DeprecatedNeo4jSinkConfiguration.BATCH_PARALLELIZE ->
              migrated[BATCH_PARALLELIZE] = it.value.toString()
          DeprecatedNeo4jSinkConfiguration.TOPIC_PATTERN_MERGE_NODE_PROPERTIES_ENABLED ->
              migrated[TOPIC_PATTERN_MERGE_NODE_PROPERTIES] = it.value.toString()
          DeprecatedNeo4jSinkConfiguration.TOPIC_PATTERN_MERGE_RELATIONSHIP_PROPERTIES_ENABLED ->
              migrated[TOPIC_PATTERN_MERGE_RELATIONSHIP_PROPERTIES] = it.value.toString()
          else ->
              if (!migrated.containsKey(it.key)) {
                migrated[it.key] = it.value.toString()
              }
        }
      }

      return migrated
    }

    fun validate(config: org.apache.kafka.common.config.Config) {
      Neo4jConfiguration.validate(config)
    }

    fun config(): ConfigDef =
        Neo4jConfiguration.config()
            .define(
                ConfigKeyBuilder.of(TOPIC_CDC_SOURCE_ID, ConfigDef.Type.LIST) {
                  documentation = PropertiesUtil.getProperty(TOPIC_CDC_SOURCE_ID)
                  importance = ConfigDef.Importance.HIGH
                  defaultValue = ""
                  group = ConfigGroup.TOPIC_CYPHER_MAPPING
                })
            .define(
                ConfigKeyBuilder.of(TOPIC_CDC_SOURCE_ID_LABEL_NAME, ConfigDef.Type.STRING) {
                  documentation = PropertiesUtil.getProperty(TOPIC_CDC_SOURCE_ID_LABEL_NAME)
                  importance = ConfigDef.Importance.HIGH
                  defaultValue = SourceIdIngestionStrategyConfig.DEFAULT.labelName
                  group = ConfigGroup.TOPIC_CYPHER_MAPPING
                  recommender =
                      Recommenders.visibleIfNotEmpty(Predicate.isEqual(TOPIC_CDC_SOURCE_ID))
                })
            .define(
                ConfigKeyBuilder.of(TOPIC_CDC_SOURCE_ID_ID_NAME, ConfigDef.Type.STRING) {
                  documentation = PropertiesUtil.getProperty(TOPIC_CDC_SOURCE_ID_ID_NAME)
                  importance = ConfigDef.Importance.HIGH
                  defaultValue = SourceIdIngestionStrategyConfig.DEFAULT.idName
                  group = ConfigGroup.TOPIC_CYPHER_MAPPING
                  recommender =
                      Recommenders.visibleIfNotEmpty(Predicate.isEqual(TOPIC_CDC_SOURCE_ID))
                })
            .define(
                ConfigKeyBuilder.of(TOPIC_CDC_SCHEMA, ConfigDef.Type.LIST) {
                  documentation = PropertiesUtil.getProperty(TOPIC_CDC_SCHEMA)
                  importance = ConfigDef.Importance.HIGH
                  defaultValue = ""
                  group = ConfigGroup.TOPIC_CYPHER_MAPPING
                })
            .define(
                ConfigKeyBuilder.of(TOPIC_CUD, ConfigDef.Type.LIST) {
                  documentation = PropertiesUtil.getProperty(TOPIC_CUD)
                  importance = ConfigDef.Importance.HIGH
                  defaultValue = ""
                  group = ConfigGroup.TOPIC_CYPHER_MAPPING
                })
            .define(
                ConfigKeyBuilder.of(TOPIC_PATTERN_MERGE_NODE_PROPERTIES, ConfigDef.Type.BOOLEAN) {
                  documentation = PropertiesUtil.getProperty(TOPIC_PATTERN_MERGE_NODE_PROPERTIES)
                  importance = ConfigDef.Importance.MEDIUM
                  defaultValue = DEFAULT_TOPIC_PATTERN_MERGE_NODE_PROPERTIES
                  group = ConfigGroup.TOPIC_CYPHER_MAPPING
                  recommender =
                      Recommenders.visibleIfNotEmpty { k ->
                        k.startsWith(TOPIC_PATTERN_NODE_PREFIX) ||
                            k.startsWith(TOPIC_PATTERN_RELATIONSHIP_PREFIX)
                      }
                })
            .define(
                ConfigKeyBuilder.of(
                    TOPIC_PATTERN_MERGE_RELATIONSHIP_PROPERTIES, ConfigDef.Type.BOOLEAN) {
                      documentation =
                          PropertiesUtil.getProperty(TOPIC_PATTERN_MERGE_RELATIONSHIP_PROPERTIES)
                      importance = ConfigDef.Importance.MEDIUM
                      defaultValue = DEFAULT_TOPIC_PATTERN_MERGE_RELATIONSHIP_PROPERTIES
                      group = ConfigGroup.TOPIC_CYPHER_MAPPING
                      recommender =
                          Recommenders.visibleIfNotEmpty { k ->
                            k.startsWith(TOPIC_PATTERN_NODE_PREFIX) ||
                                k.startsWith(TOPIC_PATTERN_RELATIONSHIP_PREFIX)
                          }
                    })
            .define(
                ConfigKeyBuilder.of(BATCH_SIZE, ConfigDef.Type.INT) {
                  documentation = PropertiesUtil.getProperty(BATCH_SIZE)
                  importance = ConfigDef.Importance.HIGH
                  validator = ConfigDef.Range.atLeast(1)
                  defaultValue = DEFAULT_BATCH_SIZE
                })
            .define(
                ConfigKeyBuilder.of(BATCH_TIMEOUT, ConfigDef.Type.STRING) {
                  documentation = PropertiesUtil.getProperty(BATCH_TIMEOUT)
                  importance = ConfigDef.Importance.HIGH
                  validator = Validators.pattern(SIMPLE_DURATION_PATTERN)
                  defaultValue = DEFAULT_BATCH_TIMEOUT.toSimpleString()
                })
            .define(
                ConfigKeyBuilder.of(BATCH_PARALLELIZE, ConfigDef.Type.BOOLEAN) {
                  documentation = PropertiesUtil.getProperty(BATCH_PARALLELIZE)
                  importance = ConfigDef.Importance.MEDIUM
                  defaultValue = DEFAULT_BATCH_PARALLELIZE
                  group = ConfigGroup.BATCH
                })
  }
}
