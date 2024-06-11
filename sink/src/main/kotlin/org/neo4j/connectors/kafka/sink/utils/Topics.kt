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
package org.neo4j.connectors.kafka.sink.utils

import kotlin.reflect.jvm.javaType
import org.neo4j.connectors.kafka.sink.SinkConfiguration
import org.neo4j.connectors.kafka.sink.strategy.legacy.CUDIngestionStrategy
import org.neo4j.connectors.kafka.sink.strategy.legacy.NodePatternIngestionStrategy
import org.neo4j.connectors.kafka.sink.strategy.legacy.RelationshipPatternIngestionStrategy
import org.neo4j.connectors.kafka.sink.strategy.legacy.SchemaIngestionStrategy
import org.neo4j.connectors.kafka.sink.strategy.legacy.SourceIdIngestionStrategy
import org.neo4j.connectors.kafka.sink.strategy.legacy.SourceIdIngestionStrategyConfig
import org.neo4j.connectors.kafka.sink.strategy.legacy.TopicType
import org.neo4j.connectors.kafka.sink.strategy.legacy.TopicTypeGroup
import org.neo4j.connectors.kafka.sink.strategy.pattern.NodePattern
import org.neo4j.connectors.kafka.sink.strategy.pattern.Pattern
import org.neo4j.connectors.kafka.sink.strategy.pattern.RelationshipPattern

@Suppress("UNCHECKED_CAST")
data class Topics(
    val cypherTopics: Map<String, String> = emptyMap(),
    val cdcSourceIdTopics: Pair<Set<String>, SourceIdIngestionStrategyConfig> =
        (emptySet<String>() to SourceIdIngestionStrategyConfig()),
    val cdcSchemaTopics: Set<String> = emptySet(),
    val cudTopics: Set<String> = emptySet(),
    val nodePatternTopics: Map<String, NodePattern> = emptyMap(),
    val relPatternTopics: Map<String, RelationshipPattern> = emptyMap(),
    val mergeNodeProperties: Boolean = false,
    val mergeRelationshipProperties: Boolean = false,
    val invalid: List<String> = emptyList()
) {

  fun allTopics(): List<String> =
      this.asMap()
          .map {
            when (it.key.group) {
              TopicTypeGroup.CDC,
              TopicTypeGroup.CUD ->
                  if (it.key != TopicType.CDC_SOURCE_ID) {
                    (it.value as Set<String>).toList()
                  } else {
                    (it.value as Pair<Set<String>, SourceIdIngestionStrategyConfig>).first
                  }
              else -> (it.value as Map<String, Any>).keys.toList()
            }
          }
          .flatten()

  fun asMap(): Map<TopicType, Any> =
      mapOf(
          TopicType.CYPHER to cypherTopics,
          TopicType.CUD to cudTopics,
          TopicType.CDC_SCHEMA to cdcSchemaTopics,
          TopicType.CDC_SOURCE_ID to cdcSourceIdTopics,
          TopicType.PATTERN_NODE to nodePatternTopics,
          TopicType.PATTERN_RELATIONSHIP to relPatternTopics)

  companion object {
    fun from(config: Map<String, Any?>, invalidTopics: List<String> = emptyList()): Topics {
      val cypherTopics = TopicUtils.filterByPrefix(config, SinkConfiguration.CYPHER_TOPIC_PREFIX)
      val mergeNodeProperties =
          config[SinkConfiguration.PATTERN_NODE_MERGE_PROPERTIES].toString().toBoolean()
      val mergeRelProperties =
          config[SinkConfiguration.PATTERN_RELATIONSHIP_MERGE_PROPERTIES].toString().toBoolean()
      val nodePatternTopics =
          TopicUtils.filterByPrefix(
                  config, SinkConfiguration.PATTERN_NODE_TOPIC_PREFIX, invalidTopics)
              .mapValues {
                val pattern = Pattern.parse(it.value)
                if (pattern !is NodePattern) {
                  throw IllegalArgumentException(
                      "Unsupported pattern type for node strategy: $pattern")
                }
                pattern
              }
      val relPatternTopics =
          TopicUtils.filterByPrefix(
                  config, SinkConfiguration.PATTERN_RELATIONSHIP_TOPIC_PREFIX, invalidTopics)
              .mapValues {
                val pattern = Pattern.parse(it.value)
                if (pattern !is RelationshipPattern) {
                  throw IllegalArgumentException(
                      "Unsupported pattern type for relationship strategy: $pattern")
                }
                pattern
              }
      val cdcSourceIdTopics =
          TopicUtils.splitTopics(
              config[SinkConfiguration.CDC_SOURCE_ID_TOPICS] as? String, invalidTopics)
      val cdcSchemaTopics =
          TopicUtils.splitTopics(
              config[SinkConfiguration.CDC_SCHEMA_TOPICS] as? String, invalidTopics)
      val cudTopics =
          TopicUtils.splitTopics(config[SinkConfiguration.CUD_TOPICS] as? String, invalidTopics)
      val sourceIdStrategyConfig =
          SourceIdIngestionStrategyConfig(
              config
                  .getOrDefault(
                      SinkConfiguration.CDC_SOURCE_ID_LABEL_NAME,
                      SourceIdIngestionStrategyConfig.DEFAULT.labelName)
                  .toString(),
              config
                  .getOrDefault(
                      SinkConfiguration.CDC_SOURCE_ID_PROPERTY_NAME,
                      SourceIdIngestionStrategyConfig.DEFAULT.idName)
                  .toString())
      return Topics(
          cypherTopics,
          (cdcSourceIdTopics to sourceIdStrategyConfig),
          cdcSchemaTopics,
          cudTopics,
          nodePatternTopics,
          relPatternTopics,
          mergeNodeProperties,
          mergeRelProperties)
    }
  }
}

object TopicUtils {

  @JvmStatic val TOPIC_SEPARATOR = ","

  fun filterByPrefix(
      config: Map<*, *>,
      prefix: String,
      invalidTopics: List<String> = emptyList()
  ): Map<String, String> {
    return config
        .filterKeys { it.toString().startsWith(prefix) }
        .mapKeys { it.key.toString().replace(prefix, "") }
        .filterKeys { !invalidTopics.contains(it) }
        .mapValues { it.value.toString() }
  }

  fun splitTopics(
      cdcMergeTopicsString: String?,
      invalidTopics: List<String> = emptyList()
  ): Set<String> {
    return if (cdcMergeTopicsString.isNullOrBlank()) {
      emptySet()
    } else {
      cdcMergeTopicsString.split(TOPIC_SEPARATOR).filter { !invalidTopics.contains(it) }.toSet()
    }
  }

  inline fun <reified T : Throwable> validate(topics: Topics) {
    val exceptionStringConstructor =
        T::class.constructors.first {
          it.parameters.size == 1 && it.parameters[0].type.javaType == String::class.java
        }
    val crossDefinedTopics =
        topics.allTopics().groupBy({ it }, { 1 }).filterValues { it.sum() > 1 }.keys
    if (crossDefinedTopics.isNotEmpty()) {
      throw exceptionStringConstructor.call(
          "The following topics are cross defined: $crossDefinedTopics")
    }
  }

  @Suppress("UNCHECKED_CAST")
  fun toStrategyMap(topics: Topics): Map<TopicType, Any> {
    return topics
        .asMap()
        .filterKeys { it != TopicType.CYPHER }
        .mapValues { (type, config) ->
          when (type) {
            TopicType.CDC_SOURCE_ID -> {
              val (_, sourceIdStrategyConfig) =
                  (config as Pair<Set<String>, SourceIdIngestionStrategyConfig>)
              SourceIdIngestionStrategy(sourceIdStrategyConfig)
            }
            TopicType.CDC_SCHEMA -> SchemaIngestionStrategy()
            TopicType.CUD -> CUDIngestionStrategy()
            TopicType.PATTERN_NODE -> {
              val map = config as Map<String, NodePattern>
              map.mapValues { NodePatternIngestionStrategy(it.value, topics.mergeNodeProperties) }
            }
            TopicType.PATTERN_RELATIONSHIP -> {
              val map = config as Map<String, RelationshipPattern>
              map.mapValues {
                RelationshipPatternIngestionStrategy(
                    it.value, topics.mergeNodeProperties, topics.mergeRelationshipProperties)
              }
            }
            else -> throw RuntimeException("Unsupported topic type $type")
          }
        }
  }
}
