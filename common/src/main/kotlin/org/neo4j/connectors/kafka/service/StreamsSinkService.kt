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
package org.neo4j.connectors.kafka.service

import org.neo4j.connectors.kafka.service.sink.strategy.IngestionStrategy

const val NEO4J_PREFIX: String = "neo4j"
const val CDC_PREFIX: String = "$NEO4J_PREFIX.cdc"

enum class TopicTypeGroup {
  CYPHER,
  CDC,
  PATTERN,
  CUD
}

enum class TopicType(val group: TopicTypeGroup, val key: String) {
  CDC_SOURCE_ID(group = TopicTypeGroup.CDC, key = "$CDC_PREFIX.sourceId.topics"),
  CYPHER(group = TopicTypeGroup.CYPHER, key = "$NEO4J_PREFIX.cypher.topic"),
  PATTERN_NODE(group = TopicTypeGroup.PATTERN, key = "$NEO4J_PREFIX.pattern.node.topic"),
  PATTERN_RELATIONSHIP(
      group = TopicTypeGroup.PATTERN, key = "$NEO4J_PREFIX.pattern.relationship.topic"),
  CDC_SCHEMA(group = TopicTypeGroup.CDC, key = "$CDC_PREFIX.schema.topics"),
  CUD(group = TopicTypeGroup.CUD, key = "$NEO4J_PREFIX.cud.topics")
}

data class StreamsSinkEntity(val key: Any?, val value: Any?)

abstract class StreamsStrategyStorage {
  abstract fun getTopicType(topic: String): TopicType?

  abstract fun getStrategy(topic: String): IngestionStrategy
}

abstract class StreamsSinkService(private val streamsStrategyStorage: StreamsStrategyStorage) {

  abstract fun write(query: String, events: Collection<Any>)

  private fun writeWithStrategy(data: Collection<StreamsSinkEntity>, strategy: IngestionStrategy) {
    strategy.mergeNodeEvents(data).forEach { write(it.query, it.events) }
    strategy.deleteNodeEvents(data).forEach { write(it.query, it.events) }

    strategy.mergeRelationshipEvents(data).forEach { write(it.query, it.events) }
    strategy.deleteRelationshipEvents(data).forEach { write(it.query, it.events) }
  }

  fun writeForTopic(topic: String, params: Collection<StreamsSinkEntity>) {
    writeWithStrategy(params, streamsStrategyStorage.getStrategy(topic))
  }
}
