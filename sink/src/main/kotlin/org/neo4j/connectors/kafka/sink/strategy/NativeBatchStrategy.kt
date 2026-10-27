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

import org.neo4j.caniuse.CanIUse
import org.neo4j.caniuse.Cypher
import org.neo4j.caniuse.Neo4j
import org.neo4j.connectors.kafka.sink.ChangeQuery
import org.neo4j.connectors.kafka.sink.SinkMessage
import org.neo4j.connectors.kafka.sink.SinkStrategy
import org.neo4j.driver.Query
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class NativeBatchStrategy(
    private val neo4j: Neo4j,
    private val maxBatchedStatements: Int,
    private val batchSize: Int,
    private val eosOffsetLabel: String,
    private val strategy: SinkStrategy,
) : SinkBatchStrategy {
  private val logger: Logger = LoggerFactory.getLogger(javaClass)
  private val statementGenerator by lazy { DefaultSinkActionStatementGenerator(neo4j) }

  override fun handle(
      messages: Iterable<SinkMessage>,
      eventTransformer: (SinkMessage) -> SinkAction,
  ): Iterable<Iterable<ChangeQuery>> {
    val partitions = messages.map { it.record.kafkaPartition() }.distinct()
    require(partitions.size <= 1) { "batch must not span partitions, got $partitions" }

    val (topic, partition) =
        messages.firstOrNull()?.let { it.record.topic() to it.record.kafkaPartition() }
            ?: return emptyList()

    val events =
        messages
            .onEach { logger.trace("received message: {}", it) }
            .map { MessageToEvent(it, eventTransformer(it)) }

    return splitEventsIntoBatches(events, maxBatchedStatements, topic, partition).onEach {
      logger.trace("messages: {} ", it)
    }
  }

  private fun splitEventsIntoBatches(
      events: List<MessageToEvent>,
      maxBatchedStatements: Int,
      topic: String,
      partition: Int,
  ): List<List<ChangeQuery>> {
    val result = mutableListOf<List<ChangeQuery>>()

    val currentTransaction = mutableListOf<ChangeQuery>()
    var currentTransactionSize = 0

    var currentGroupId = 0
    val queries = mutableMapOf<String, Int>()
    val currentEvents = mutableListOf<Map<String, Any>>()
    val currentMessages = mutableListOf<SinkMessage>()

    fun flushQuery() {
      if (currentEvents.isEmpty()) return

      currentTransaction.add(
          ChangeQuery(
              null,
              null,
              currentMessages.toList(),
              batchedStatement(queries, currentEvents, topic, partition),
          )
      )
      currentGroupId = 0
      queries.clear()
      currentEvents.clear()
      currentMessages.clear()
    }

    fun flushTransaction() {
      flushQuery()
      if (currentTransaction.isEmpty()) return

      result.add(currentTransaction.toList())
      currentTransaction.clear()
      currentTransactionSize = 0
    }

    events.forEach { event ->
      val query = statementGenerator.buildStatement(event.sinkAction, "${EVENT}.params")

      if (!queries.containsKey(query.text()) && (queries.size >= maxBatchedStatements)) {
        flushQuery()
      }

      val queryId = queries.getOrPut(query.text()) { currentGroupId++ }
      currentEvents.add(
          mapOf(
              "q" to queryId,
              "offset" to event.message.record.kafkaOffset(),
              "params" to query.parameters(),
          )
      )
      currentMessages.add(event.message)
      if (++currentTransactionSize >= batchSize) {
        flushTransaction()
      }
    }

    // handle final batch, if any
    flushTransaction()

    return result
  }

  private fun batchedStatement(
      queries: Map<String, Int>,
      events: List<Map<String, Any>>,
      topic: String,
      partition: Int,
  ): Query {
    val cypher25 = CanIUse.canIUse(Cypher.explicitCypher25Selection()).withNeo4j(neo4j)
    val termination =
        if (CanIUse.canIUse(Cypher.finishClause()).withNeo4j(neo4j)) "FINISH"
        else "RETURN COUNT(1) AS total"
    val sortedQueries = queries.keys.sorted()
    val withVariableScope =
        CanIUse.canIUse(Cypher.callSubqueryWithVariableScopeClause()).withNeo4j(neo4j)

    val query = buildString {
      if (cypher25) {
        appendLine("CYPHER 25")
      }
      appendLine("UNWIND \$events AS ${EVENT}")
      if (eosOffsetLabel.isNotBlank()) {
        appendLine(
            "MERGE (k:$eosOffsetLabel {strategy: \$strategy, topic: \$topic, partition: \$partition}) ON CREATE SET k.offset = -1"
        )
        appendLine("WITH k, ${EVENT} WHERE ${EVENT}.offset > k.offset")
        appendLine("WITH k, ${EVENT} ORDER BY ${EVENT}.offset ASC")
      } else {
        appendLine("WITH ${EVENT} ORDER BY ${EVENT}.offset ASC")
      }
      if (withVariableScope) {
        appendLine("CALL (${EVENT}) {")
      } else {
        appendLine("CALL {")
      }
      sortedQueries.forEachIndexed { index, stmt ->
        if (cypher25) {
          appendLine("  WHEN ${EVENT}.q = \$q$index THEN {")
          appendLine("    $stmt")
          appendLine("  }")
        } else {
          if (index > 0) appendLine("  UNION ALL")
          if (!withVariableScope) {
            appendLine("  WITH ${EVENT}")
          }

          appendLine("  WITH ${EVENT} WHERE ${EVENT}.q = \$q$index")
          appendLine("  $stmt")
          appendLine("  RETURN $index AS x")
        }
      }
      appendLine("}")
      if (eosOffsetLabel.isNotBlank()) {
        appendLine("WITH k, max(${EVENT}.offset) AS newOffset SET k.offset = newOffset")
      }
      append(termination)
    }

    return Query(
        query,
        buildMap {
          sortedQueries.forEachIndexed { index, stmt -> put("q$index", queries[stmt]) }
          put("strategy", strategy.name)
          put("topic", topic)
          put("partition", partition)
          put("events", events)
        },
    )
  }
}
