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
package streams.kafka.connect.sink

import streams.service.StreamsStrategyStorage
import streams.service.TopicType
import streams.service.sink.strategy.CUDIngestionStrategy
import streams.service.sink.strategy.CypherTemplateStrategy
import streams.service.sink.strategy.IngestionStrategy
import streams.service.sink.strategy.NodePatternIngestionStrategy
import streams.service.sink.strategy.RelationshipPatternIngestionStrategy
import streams.service.sink.strategy.SchemaIngestionStrategy
import streams.service.sink.strategy.SourceIdIngestionStrategy

class Neo4jStrategyStorage(val config: Neo4jSinkConnectorConfig) : StreamsStrategyStorage() {
  private val topicConfigMap = config.topics.asMap()

  override fun getTopicType(topic: String): TopicType? =
      TopicType.values().firstOrNull { topicType ->
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
            NodePatternIngestionStrategy(config.topics.nodePatternTopics.getValue(topic))
        TopicType.PATTERN_RELATIONSHIP ->
            RelationshipPatternIngestionStrategy(config.topics.relPatternTopics.getValue(topic))
        TopicType.CYPHER -> CypherTemplateStrategy(config.topics.cypherTopics.getValue(topic))
        null -> throw RuntimeException("Topic Type not Found")
      }
}
