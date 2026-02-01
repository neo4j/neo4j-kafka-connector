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
package org.neo4j.connectors.kafka.sink.strategy.cdc.batch

import org.neo4j.cdc.client.model.ChangeEvent
import org.neo4j.cdc.client.model.EntityOperation
import org.neo4j.cdc.client.model.NodeEvent
import org.neo4j.cdc.client.model.RelationshipEvent
import org.neo4j.connectors.kafka.sink.ChangeQuery
import org.neo4j.connectors.kafka.sink.SinkMessage
import org.neo4j.connectors.kafka.sink.SinkStrategyHandler
import org.neo4j.connectors.kafka.sink.strategy.toChangeEvent
import org.neo4j.driver.Query
import org.slf4j.Logger
import org.slf4j.LoggerFactory

abstract class BatchedCdcHandler(
    private val maxBatchedStatements: Int,
    private val batchSize: Int,
) : SinkStrategyHandler {
  private val logger: Logger = LoggerFactory.getLogger(javaClass)

  data class MessageToEvent(
      val message: SinkMessage,
      val changeEvent: ChangeEvent,
      val cdcData: CdcData,
  )

  override fun handle(messages: Iterable<SinkMessage>): Iterable<Iterable<ChangeQuery>> {
    val changeEvents =
        messages
            .onEach { logger.trace("received message: {}", it) }
            .map {
              MessageToEvent(
                  it,
                  it.toChangeEvent(),
                  when (val event = it.toChangeEvent().event) {
                    is NodeEvent ->
                        when (event.operation) {
                          EntityOperation.CREATE -> transformCreate(event)
                          EntityOperation.UPDATE -> transformUpdate(event)
                          EntityOperation.DELETE -> transformDelete(event)
                          else ->
                              throw IllegalArgumentException("unknown operation ${event.operation}")
                        }

                    is RelationshipEvent ->
                        when (event.operation) {
                          EntityOperation.CREATE -> transformCreate(event)
                          EntityOperation.UPDATE -> transformUpdate(event)
                          EntityOperation.DELETE -> transformDelete(event)
                          else ->
                              throw IllegalArgumentException("unknown operation ${event.operation}")
                        }

                    else ->
                        throw IllegalArgumentException("unsupported event type ${event.eventType}")
                  },
              )
            }
    return listOf(splitEventsIntoBatches(changeEvents, maxBatchedStatements)).onEach {
      logger.trace("messages: {} ", it)
    }
  }

  private fun splitEventsIntoBatches(
      events: List<MessageToEvent>,
      maxBatchedStatements: Int,
  ): List<ChangeQuery> {
    val result = mutableListOf<ChangeQuery>()

    var currentGroupId = 0
    val queries = mutableMapOf<GroupingKey, Pair<Int, String>>()
    val currentEvents = mutableListOf<Map<String, Any>>()
    val currentMessages = mutableListOf<SinkMessage>()

    fun flush() {
      result.add(
          ChangeQuery(
              null,
              null,
              currentMessages.toList(),
              batchedStatement(queries, currentEvents),
          )
      )
      queries.clear()
      currentEvents.clear()
      currentMessages.clear()
    }

    events.forEach { event ->
      val key = event.cdcData.groupingBasedOn()
      if (!queries.containsKey(key) && (queries.size >= maxBatchedStatements)) {
        flush()
      }

      val (groupId, _) =
          queries.getOrPut(key) { currentGroupId++ to event.cdcData.buildStatement() }
      currentEvents.add(event.cdcData.toParams(groupId))
      currentMessages.add(event.message)
      if (currentEvents.size >= batchSize) {
        flush()
      }
    }

    // handle final batch, if any
    if (queries.isNotEmpty() && currentEvents.isNotEmpty()) {
      flush()
    }

    return result
  }

  private fun batchedStatement(
      queries: Map<GroupingKey, Pair<Int, String>>,
      events: List<Map<String, Any>>,
  ): Query {
    val query = buildString {
      append("CYPHER 25 ")
      append("UNWIND \$events AS $EVENT CALL ($EVENT) { ")
      queries.keys.sorted().forEach { key ->
        val (index, stmt) = queries[key]!!

        append("WHEN $EVENT.q = \$q$index THEN $stmt ")
      }
      append("} FINISH")
    }

    return Query(
        query,
        buildMap {
          queries.values.forEach { (index, _) -> put("q$index", index) }
          put("events", events)
        },
    )
  }

  protected abstract fun transformCreate(event: NodeEvent): CdcData

  protected abstract fun transformUpdate(event: NodeEvent): CdcData

  protected abstract fun transformDelete(event: NodeEvent): CdcData

  protected abstract fun transformCreate(event: RelationshipEvent): CdcData

  protected abstract fun transformUpdate(event: RelationshipEvent): CdcData

  protected abstract fun transformDelete(event: RelationshipEvent): CdcData
}
