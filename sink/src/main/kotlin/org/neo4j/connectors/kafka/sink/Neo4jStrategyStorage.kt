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

import org.neo4j.connectors.kafka.sink.strategy.legacy.CUDIngestionStrategy
import org.neo4j.connectors.kafka.sink.strategy.legacy.CypherTemplateStrategy
import org.neo4j.connectors.kafka.sink.strategy.legacy.IngestionStrategy
import org.neo4j.connectors.kafka.sink.strategy.legacy.NodePatternIngestionStrategy
import org.neo4j.connectors.kafka.sink.strategy.legacy.RelationshipPatternIngestionStrategy
import org.neo4j.connectors.kafka.sink.strategy.legacy.SchemaIngestionStrategy
import org.neo4j.connectors.kafka.sink.strategy.legacy.SourceIdIngestionStrategy
import org.neo4j.connectors.kafka.sink.strategy.legacy.StreamsStrategyStorage
import org.neo4j.connectors.kafka.sink.strategy.legacy.TopicType

class Neo4jStrategyStorage(val config: SinkConfiguration) : StreamsStrategyStorage() {
  private val topicConfigMap = config.topics.asMap()

  @Suppress("UNCHECKED_CAST")
  override fun getTopicType(topic: String): TopicType? =
      TopicType.entries.firstOrNull { topicType ->
        when (val topicConfig = topicConfigMap.getOrDefault(topicType, emptyList<Any>())) {
          is Collection<*> -> topicConfig.contains(topic)
          is Map<*, *> -> topicConfig.containsKey(topic)
          is Pair<*, *> -> (topicConfig.first as Set<String>).contains(topic)
          else -> false
        }
      }

  override fun getStrategy(topic: String): IngestionStrategy =
      when (val topicType = getTopicType(topic)) {
        TopicType.CDC_SOURCE_ID -> config.strategyMap[topicType] as SourceIdIngestionStrategy
        TopicType.CDC_SCHEMA -> SchemaIngestionStrategy()
        TopicType.CUD -> CUDIngestionStrategy()
        TopicType.PATTERN_NODE ->
            NodePatternIngestionStrategy(
                config.topics.nodePatternTopics.getValue(topic), config.topics.mergeNodeProperties)
        TopicType.PATTERN_RELATIONSHIP ->
            RelationshipPatternIngestionStrategy(
                config.topics.relPatternTopics.getValue(topic),
                config.topics.mergeNodeProperties,
                config.topics.mergeRelationshipProperties)
        TopicType.CYPHER -> CypherTemplateStrategy(config.topics.cypherTopics.getValue(topic))
        null -> throw RuntimeException("Topic Type not Found")
      }
}
