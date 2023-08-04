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
package streams.service

import streams.service.sink.strategy.IngestionStrategy

const val STREAMS_TOPIC_KEY: String = "streams.sink.topic"
const val STREAMS_TOPIC_CDC_KEY: String = "streams.sink.topic.cdc"

enum class TopicTypeGroup {
  CYPHER,
  CDC,
  PATTERN,
  CUD
}

enum class TopicType(val group: TopicTypeGroup, val key: String) {
  CDC_SOURCE_ID(group = TopicTypeGroup.CDC, key = "$STREAMS_TOPIC_CDC_KEY.sourceId"),
  CYPHER(group = TopicTypeGroup.CYPHER, key = "$STREAMS_TOPIC_KEY.cypher"),
  PATTERN_NODE(group = TopicTypeGroup.PATTERN, key = "$STREAMS_TOPIC_KEY.pattern.node"),
  PATTERN_RELATIONSHIP(
      group = TopicTypeGroup.PATTERN, key = "$STREAMS_TOPIC_KEY.pattern.relationship"),
  CDC_SCHEMA(group = TopicTypeGroup.CDC, key = "$STREAMS_TOPIC_CDC_KEY.schema"),
  CUD(group = TopicTypeGroup.CUD, key = "$STREAMS_TOPIC_KEY.cud")
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
