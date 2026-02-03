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
package org.neo4j.connectors.kafka.sink.strategy.cdc.apoc

import kotlin.sequences.chunked
import kotlin.sequences.map
import kotlin.sequences.onEach
import kotlin.sequences.toList
import org.neo4j.caniuse.CanIUse.canIUse
import org.neo4j.caniuse.Cypher
import org.neo4j.caniuse.Neo4j
import org.neo4j.caniuse.Neo4jVersion
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

abstract class ApocCdcHandler(private val neo4j: Neo4j, private val batchSize: Int) :
    SinkStrategyHandler {
  private val logger: Logger = LoggerFactory.getLogger(javaClass)

  data class MessageToEvent(
      val message: SinkMessage,
      val changeEvent: ChangeEvent,
      val cdcData: CdcData,
  )

  override fun handle(messages: Iterable<SinkMessage>): Iterable<Iterable<ChangeQuery>> {
    return messages
        .asSequence()
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
                      else -> throw IllegalArgumentException("unknown operation ${event.operation}")
                    }

                is RelationshipEvent ->
                    when (event.operation) {
                      EntityOperation.CREATE -> transformCreate(event)
                      EntityOperation.UPDATE -> transformUpdate(event)
                      EntityOperation.DELETE -> transformDelete(event)
                      else -> throw IllegalArgumentException("unknown operation ${event.operation}")
                    }

                else -> throw IllegalArgumentException("unsupported event type ${event.eventType}")
              },
          )
        }
        .chunked(batchSize)
        .map { batch ->
          listOf(
              ChangeQuery(
                  null,
                  null,
                  batch.map { data -> data.message },
                  batchedStatement(batch.map { it.cdcData.toParams() }),
              )
          )
        }
        .onEach { logger.trace("mapped messages: '{}'", it) }
        .toList()
  }

  private fun batchedStatement(events: List<Map<String, Any>>): Query {
    val termination = if (neo4j.version >= Neo4jVersion(5, 19, 0)) "FINISH" else "RETURN 1"

    val query = buildString {
      append("UNWIND \$events AS $EVENT ")
      if (canIUse(Cypher.callSubqueryWithVariableScopeClause()).withNeo4j(neo4j))
          append("CALL ($EVENT) { ")
      else append("CALL { WITH $EVENT ")
      append("CALL apoc.cypher.doIt($EVENT.stmt, $EVENT.params) YIELD value $termination ")
      append("} $termination")
    }

    return Query(query, buildMap { put("events", events) })
  }

  protected abstract fun transformCreate(event: NodeEvent): CdcData

  protected abstract fun transformUpdate(event: NodeEvent): CdcData

  protected abstract fun transformDelete(event: NodeEvent): CdcData

  protected abstract fun transformCreate(event: RelationshipEvent): CdcData

  protected abstract fun transformUpdate(event: RelationshipEvent): CdcData

  protected abstract fun transformDelete(event: RelationshipEvent): CdcData
}
