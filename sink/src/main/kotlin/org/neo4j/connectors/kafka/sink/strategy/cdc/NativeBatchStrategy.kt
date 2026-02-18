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
package org.neo4j.connectors.kafka.sink.strategy.cdc

import org.neo4j.caniuse.CanIUse.canIUse
import org.neo4j.caniuse.Cypher
import org.neo4j.caniuse.Neo4j
import org.neo4j.caniuse.Neo4jVersion
import org.neo4j.cdc.client.model.ChangeEvent
import org.neo4j.connectors.kafka.sink.ChangeQuery
import org.neo4j.connectors.kafka.sink.SinkMessage
import org.neo4j.driver.Query
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class NativeBatchStrategy(
    private val neo4j: Neo4j,
    private val maxBatchedStatements: Int,
    private val batchSize: Int,
) : CdcBatchStrategy {
  private val logger: Logger = LoggerFactory.getLogger(javaClass)
  private val statementGenerator by lazy { DefaultCdcStatementGenerator(neo4j) }

  override fun handle(
      messages: Iterable<SinkMessage>,
      eventTransformer: (ChangeEvent) -> CdcData,
  ): Iterable<Iterable<ChangeQuery>> {
    val events =
        messages
            .onEach { logger.trace("received message: {}", it) }
            .map {
              val changeEvent = it.toChangeEvent()
              MessageToEvent(it, changeEvent, eventTransformer(changeEvent))
            }

    return listOf(splitEventsIntoBatches(events, maxBatchedStatements)).onEach {
      logger.trace("messages: {} ", it)
    }
  }

  private fun splitEventsIntoBatches(
      events: List<MessageToEvent>,
      maxBatchedStatements: Int,
  ): List<ChangeQuery> {
    val result = mutableListOf<ChangeQuery>()

    var currentGroupId = 0
    val queries = mutableMapOf<String, Int>()
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
      val query = statementGenerator.buildStatement(event.cdcData, "$EVENT.params")

      if (!queries.containsKey(query.text()) && (queries.size >= maxBatchedStatements)) {
        flush()
      }

      val queryId = queries.getOrPut(query.text()) { currentGroupId++ }
      currentEvents.add(mapOf("q" to queryId, "params" to query.parameters()))
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

  private fun batchedStatement(queries: Map<String, Int>, events: List<Map<String, Any>>): Query {
    val cypher25 = canIUse(Cypher.explicitCypher25Selection()).withNeo4j(neo4j)
    val termination =
        if (neo4j.version >= Neo4jVersion(5, 19, 0)) "FINISH" else "RETURN COUNT(1) AS total"

    val query = buildString {
      if (cypher25) {
        appendLine("CYPHER 25")
      }
      appendLine("UNWIND \$events AS $EVENT")
      if (canIUse(Cypher.callSubqueryWithVariableScopeClause()).withNeo4j(neo4j))
          appendLine("CALL (${EVENT}) {")
      else appendLine("CALL { WITH ${EVENT}")
      queries.keys.sorted().forEachIndexed { index, stmt ->
        if (cypher25) {
          appendLine("  WHEN $EVENT.q = \$q$index THEN {")
          appendLine("    $stmt")
          appendLine("  }")
        } else {
          if (index > 0) appendLine("  UNION ALL")

          val qId = queries[stmt]

          appendLine("  WITH * WHERE $EVENT.q = \$q$qId")
          appendLine("  $stmt")
          appendLine("  RETURN $index AS x")
        }
      }
      appendLine("}")
      append(termination)
    }

    return Query(
        query,
        buildMap {
          queries.values.forEach { id -> put("q$id", id) }
          put("events", events)
        },
    )
  }
}
