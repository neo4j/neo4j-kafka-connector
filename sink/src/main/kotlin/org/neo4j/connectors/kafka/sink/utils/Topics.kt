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
package org.neo4j.connectors.kafka.sink.utils

import java.util.Locale
import kotlin.reflect.jvm.javaType
import org.neo4j.connectors.kafka.service.TopicType
import org.neo4j.connectors.kafka.service.TopicTypeGroup
import org.neo4j.connectors.kafka.service.sink.strategy.CUDIngestionStrategy
import org.neo4j.connectors.kafka.service.sink.strategy.NodePatternConfiguration
import org.neo4j.connectors.kafka.service.sink.strategy.NodePatternIngestionStrategy
import org.neo4j.connectors.kafka.service.sink.strategy.RelationshipPatternConfiguration
import org.neo4j.connectors.kafka.service.sink.strategy.RelationshipPatternIngestionStrategy
import org.neo4j.connectors.kafka.service.sink.strategy.SchemaIngestionStrategy
import org.neo4j.connectors.kafka.service.sink.strategy.SourceIdIngestionStrategy
import org.neo4j.connectors.kafka.service.sink.strategy.SourceIdIngestionStrategyConfig
import org.neo4j.connectors.kafka.sink.DeprecatedNeo4jSinkConfiguration
import org.neo4j.connectors.kafka.sink.SinkConfiguration

@Suppress("UNCHECKED_CAST")
data class Topics(
    val cypherTopics: Map<String, String> = emptyMap(),
    val cdcSourceIdTopics: Pair<Set<String>, SourceIdIngestionStrategyConfig> =
        (emptySet<String>() to SourceIdIngestionStrategyConfig()),
    val cdcSchemaTopics: Set<String> = emptySet(),
    val cudTopics: Set<String> = emptySet(),
    val nodePatternTopics: Map<String, NodePatternConfiguration> = emptyMap(),
    val relPatternTopics: Map<String, RelationshipPatternConfiguration> = emptyMap(),
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
    fun from(
        originalConfig: Map<String, Any?>,
        dbName: String = "",
        invalidTopics: List<String> = emptyList()
    ): Topics {
      val config =
          originalConfig
              .filterKeys {
                if (dbName.isNotBlank()) it.lowercase(Locale.ROOT).endsWith(".to.$dbName")
                else !it.contains(".to.")
              }
              .mapKeys {
                if (dbName.isNotBlank()) it.key.replace(".to.$dbName", "", true) else it.key
              }
      val cypherTopics = TopicUtils.filterByPrefix(config, TopicType.CYPHER.key)
      val mergeNodeProperties =
          originalConfig[SinkConfiguration.PATTERN_MERGE_NODE_PROPERTIES].toString().toBoolean()
      val mergeRelProperties =
          originalConfig[SinkConfiguration.PATTERN_MERGE_RELATIONSHIP_PROPERTIES]
              .toString()
              .toBoolean()
      val nodePatternTopics =
          TopicUtils.filterByPrefix(config, TopicType.PATTERN_NODE.key, invalidTopics).mapValues {
            NodePatternConfiguration.parse(it.value, mergeNodeProperties)
          }
      val relPatternTopics =
          TopicUtils.filterByPrefix(config, TopicType.PATTERN_RELATIONSHIP.key, invalidTopics)
              .mapValues {
                RelationshipPatternConfiguration.parse(
                    it.value, mergeNodeProperties, mergeRelProperties)
              }
      val cdcSourceIdTopics =
          TopicUtils.splitTopics(config[TopicType.CDC_SOURCE_ID.key] as? String, invalidTopics)
      val cdcSchemaTopics =
          TopicUtils.splitTopics(config[TopicType.CDC_SCHEMA.key] as? String, invalidTopics)
      val cudTopics = TopicUtils.splitTopics(config[TopicType.CUD.key] as? String, invalidTopics)
      val sourceIdStrategyConfig =
          SourceIdIngestionStrategyConfig(
              originalConfig
                  .getOrDefault(
                      DeprecatedNeo4jSinkConfiguration.TOPIC_CDC_SOURCE_ID_LABEL_NAME,
                      SourceIdIngestionStrategyConfig.DEFAULT.labelName)
                  .toString(),
              originalConfig
                  .getOrDefault(
                      DeprecatedNeo4jSinkConfiguration.TOPIC_CDC_SOURCE_ID_ID_NAME,
                      SourceIdIngestionStrategyConfig.DEFAULT.idName)
                  .toString())
      val t =
          Topics(
              cypherTopics,
              (cdcSourceIdTopics to sourceIdStrategyConfig),
              cdcSchemaTopics,
              cudTopics,
              nodePatternTopics,
              relPatternTopics)
      return t
    }
  }
}

object TopicUtils {

  @JvmStatic val TOPIC_SEPARATOR = ";"

  fun filterByPrefix(
      config: Map<*, *>,
      prefix: String,
      invalidTopics: List<String> = emptyList()
  ): Map<String, String> {
    val fullPrefix = "$prefix."
    return config
        .filterKeys { it.toString().startsWith(fullPrefix) }
        .mapKeys { it.key.toString().replace(fullPrefix, "") }
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
              val map = config as Map<String, NodePatternConfiguration>
              map.mapValues { NodePatternIngestionStrategy(it.value) }
            }
            TopicType.PATTERN_RELATIONSHIP -> {
              val map = config as Map<String, RelationshipPatternConfiguration>
              map.mapValues { RelationshipPatternIngestionStrategy(it.value) }
            }
            else -> throw RuntimeException("Unsupported topic type $type")
          }
        }
  }
}
