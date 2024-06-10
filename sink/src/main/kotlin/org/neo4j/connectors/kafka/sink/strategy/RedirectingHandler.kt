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
package org.neo4j.connectors.kafka.sink.strategy

import org.neo4j.connectors.kafka.sink.ChangeQuery
import org.neo4j.connectors.kafka.sink.SinkMessage
import org.neo4j.connectors.kafka.sink.SinkStrategyHandler
import org.neo4j.connectors.kafka.sink.strategy.legacy.IngestionStrategy
import org.neo4j.connectors.kafka.sink.utils.toStreamsSinkEntity
import org.neo4j.driver.Query

abstract class RedirectingHandler(
    private val original: IngestionStrategy,
    private val batchSize: Int
) : SinkStrategyHandler {
  override fun handle(messages: Iterable<SinkMessage>): Iterable<Iterable<ChangeQuery>> {
    val events = messages.map { it.record.toStreamsSinkEntity() }

    return (original.mergeNodeEvents(events) +
            original.deleteNodeEvents(events) +
            original.mergeRelationshipEvents(events) +
            original.deleteRelationshipEvents(events))
        .map { q -> ChangeQuery(null, null, Query(q.query, mapOf("events" to q.events))) }
        .chunked(batchSize)
  }
}
