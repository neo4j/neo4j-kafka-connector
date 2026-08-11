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
import org.neo4j.caniuse.Cypher as CanIUseCypher
import org.neo4j.caniuse.Neo4j
import org.neo4j.connectors.kafka.sink.ChangeQuery
import org.neo4j.connectors.kafka.sink.SinkMessage
import org.neo4j.connectors.kafka.sink.SinkStrategy
import org.neo4j.connectors.kafka.utils.CypherRenderer
import org.neo4j.cypherdsl.core.Cypher
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
  private val renderer = CypherRenderer(neo4j)

  override fun handle(
      messages: Iterable<SinkMessage>,
      eventTransformer: (SinkMessage) -> SinkAction,
  ): Iterable<Iterable<ChangeQuery>> {
    val (topic, partition) =
        messages.firstOrNull()?.let { it.record.topic() to it.record.kafkaPartition() }
            ?: return emptyList()

    val events =
        messages
            .onEach { logger.trace("received message: {}", it) }
            .map { MessageToEvent(it, eventTransformer(it)) }

    return listOf(splitEventsIntoBatches(events, maxBatchedStatements, topic, partition)).onEach {
      logger.trace("messages: {} ", it)
    }
  }

  private fun splitEventsIntoBatches(
      events: List<MessageToEvent>,
      maxBatchedStatements: Int,
      topic: String,
      partition: Int,
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
              batchedStatement(queries, currentEvents, topic, partition),
          )
      )
      queries.clear()
      currentEvents.clear()
      currentMessages.clear()
    }

    events.forEach { event ->
      val query = statementGenerator.buildStatement(event.sinkAction, "${EVENT}.params")

      if (!queries.containsKey(query.text()) && (queries.size >= maxBatchedStatements)) {
        flush()
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

  /**
   * Each per-record statement was already generated (and rendered to text) by
   * [DefaultSinkActionStatementGenerator] before reaching here - `stmt` below is that opaque,
   * already-rendered text, deduplicated and grouped by [queries]. There is no way to compose an
   * opaque pre-rendered fragment into a further Cypher-DSL clause chain without re-parsing it (and
   * no justification for adding a Cypher parser dependency just to immediately re-render the same
   * text), so the `UNION ALL`/`WHEN ... THEN` dispatch around those fragments remains text-based,
   * same as before - including the `CYPHER 25` conditional-apply syntax, which the Cypher-DSL does
   * not model in any form as of the pinned version. Everything that does not embed one of those
   * opaque fragments (the `UNWIND`, the exactly-once offset tracker, `FINISH` vs `RETURN COUNT`) is
   * still built and rendered through the DSL, same as [ApocBatchStrategy].
   */
  private fun batchedStatement(
      queries: Map<String, Int>,
      events: List<Map<String, Any>>,
      topic: String,
      partition: Int,
  ): Query {
    val cypher25 = CanIUse.canIUse(CanIUseCypher.explicitCypher25Selection()).withNeo4j(neo4j)
    val hasFinish = CanIUse.canIUse(CanIUseCypher.finishClause()).withNeo4j(neo4j)
    val termination = if (hasFinish) "FINISH" else "RETURN count(1) AS total"
    val sortedQueries = queries.keys.sorted()
    val withVariableScope =
        CanIUse.canIUse(CanIUseCypher.callSubqueryWithVariableScopeClause()).withNeo4j(neo4j)

    val query = buildString {
      if (cypher25) {
        appendLine("CYPHER 25")
      }
      appendLine("UNWIND \$events AS $EVENT")
      if (eosOffsetLabel.isNotBlank()) {
        appendLine(offsetTrackerMergeClause())
        appendLine("WITH k, $EVENT WHERE $EVENT.offset > k.offset")
        appendLine("WITH k, $EVENT ORDER BY $EVENT.offset ASC")
      } else {
        appendLine("WITH $EVENT ORDER BY $EVENT.offset ASC")
      }
      if (withVariableScope) {
        appendLine("CALL ($EVENT) {")
      } else {
        appendLine("CALL {")
      }
      sortedQueries.forEachIndexed { index, stmt ->
        if (cypher25) {
          appendLine("  WHEN $EVENT.q = \$q$index THEN {")
          appendLine("    $stmt")
          appendLine("  }")
        } else {
          if (index > 0) appendLine("  UNION ALL")
          if (!withVariableScope) {
            appendLine("  WITH $EVENT")
          }

          appendLine("  WITH $EVENT WHERE $EVENT.q = \$q$index")
          appendLine("  $stmt")
          appendLine("  RETURN $index AS x")
        }
      }
      appendLine("}")
      if (eosOffsetLabel.isNotBlank()) {
        appendLine("WITH k, max($EVENT.offset) AS newOffset SET k.offset = newOffset")
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

  private fun offsetTrackerMergeClause(): String {
    // eosOffsetLabel is the operator-configured, raw (unsanitized) label - handing it straight to
    // the DSL lets it escape the identifier itself, exactly once.
    val offsetNode =
        Cypher.node(eosOffsetLabel)
            .named("k")
            .withProperties(
                Cypher.mapOf(
                    "strategy",
                    Cypher.parameter("strategy"),
                    "topic",
                    Cypher.parameter("topic"),
                    "partition",
                    Cypher.parameter("partition"),
                )
            )
    val statement =
        Cypher.merge(offsetNode)
            .onCreate()
            .set(offsetNode.property("offset"), Cypher.literalOf<Any>(-1))
            .returning(Cypher.literalTrue())
            .build()
    return renderer.render(statement).removeSuffix(" RETURN true")
  }
}
