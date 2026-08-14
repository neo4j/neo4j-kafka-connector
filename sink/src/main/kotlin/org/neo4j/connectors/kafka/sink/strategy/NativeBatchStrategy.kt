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
import org.neo4j.cypherdsl.core.Statement
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
  private val envelope = BatchEnvelope(neo4j, eosOffsetLabel)
  private val cypher25 = CanIUse.canIUse(CanIUseCypher.explicitCypher25Selection()).withNeo4j(neo4j)
  private val hasFinish = CanIUse.canIUse(CanIUseCypher.finishClause()).withNeo4j(neo4j)
  private val withVariableScope =
      CanIUse.canIUse(CanIUseCypher.callSubqueryWithVariableScopeClause()).withNeo4j(neo4j)

  /**
   * One of a batch's distinct statements: the [id] its records carry in their `q` field, and the
   * statement's clauses in open form - `null` for a [CypherSinkAction], see [GeneratedStatement].
   */
  private data class DistinctStatement(val id: Int, val clauses: OpenStatement?)

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
    val queries = mutableMapOf<String, DistinctStatement>()
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
      // Statements are deduplicated - and later ordered - by their rendered text even when the
      // envelope goes on to compose them as clauses, so that the `q` index a record carries does
      // not depend on which of the two envelopes it ends up in.
      val generated = statementGenerator.generate(event.sinkAction, "${EVENT}.params")
      val statement = generated.query.text()

      if (!queries.containsKey(statement) && (queries.size >= maxBatchedStatements)) {
        flush()
      }

      val queryId =
          queries.getOrPut(statement) { DistinctStatement(currentGroupId++, generated.clauses) }.id
      currentEvents.add(
          mapOf(
              "q" to queryId,
              "offset" to event.message.record.kafkaOffset(),
              "params" to generated.query.parameters(),
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
   * The batch's distinct statements, deduplicated and ordered by [queries], dispatched on the `q`
   * index each record carries.
   *
   * Two of the three shapes this can take cannot be expressed with the Cypher-DSL, and are still
   * assembled as text: Cypher 25's conditional apply (`WHEN ... THEN { }`), which the DSL does not
   * model in any form as of the pinned version, and a [CypherSinkAction]'s query, which is opaque
   * operator-authored text with no DSL hole to append it into. Everything else - which is every
   * strategy other than [SinkStrategy.CYPHER] on a server without conditional apply - composes its
   * branches as clauses, through [dispatch].
   */
  private fun batchedStatement(
      queries: Map<String, DistinctStatement>,
      events: List<Map<String, Any>>,
      topic: String,
      partition: Int,
  ): Query {
    val sorted = queries.entries.sortedBy { it.key }
    val branches = sorted.map { it.value.clauses }

    val query =
        if (cypher25 || branches.any { it == null }) {
          textEnvelope(sorted.map { it.key })
        } else {
          renderer.render(
              envelope.around(dispatch(branches.filterNotNull()), importEvent = withVariableScope)
          )
        }

    return Query(
        query,
        buildMap {
          sorted.forEachIndexed { index, (_, statement) -> put("q$index", statement.id) }
          put("strategy", strategy.name)
          put("topic", topic)
          put("partition", partition)
          put("events", events)
        },
    )
  }

  /**
   * The subquery body: one [branch] per distinct statement, joined by `UNION ALL`. A batch often
   * holds a single distinct statement, and `UNION ALL` needs two operands, so that case is the
   * branch on its own - which is the same Cypher, since a branch already carries its own gate.
   */
  private fun dispatch(branches: List<OpenStatement>): Statement {
    val built = branches.mapIndexed { index, clauses -> branch(clauses, index) }
    return if (built.size == 1) built.single() else Cypher.unionAll(*built.toTypedArray())
  }

  /**
   * One branch of the dispatch, gated on the `q` index the statement's records carry and tagged
   * with it, since every branch of a `UNION` has to return the same thing.
   *
   * The clauses are rebuilt from [OpenStatement] per branch rather than reused as a built
   * statement: an aliased expression renders its `AS` only the first time a render pass meets it,
   * so a shared branch would silently lose its `_e` projection everywhere but the first.
   *
   * Without the variable scope clause each branch also has to import `e` for itself, with a bare
   * `WITH` carrying no `WHERE`. That one the DSL will not write: asking the subquery to import `e`
   * puts a single leading `WITH` in front of the first branch only, leaving the rest unbound - so
   * the import is spelled out here instead, and [BatchEnvelope.around] is told to leave it alone.
   */
  private fun branch(clauses: OpenStatement, index: Int): Statement {
    val opening = Cypher.with(envelope.event)
    val gate =
        (if (withVariableScope) opening else opening.with(envelope.event)).where(
            envelope.event.property("q").eq(Cypher.parameter("q$index"))
        )

    return clauses
        .appendTo { items -> gate.with(*items) }
        .returning(Cypher.literalOf<Any>(index).`as`("x"))
  }

  /** The envelope hand-written around already-rendered statements, for the two shapes above. */
  private fun textEnvelope(statements: List<String>): String {
    val termination = if (hasFinish) "FINISH" else "RETURN count(1) AS total"

    return buildString {
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
      statements.forEachIndexed { index, stmt ->
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
  }

  private fun offsetTrackerMergeClause(): String {
    val offsetNode = envelope.offsetTrackerNode()
    val statement =
        Cypher.merge(offsetNode)
            .onCreate()
            .set(offsetNode.property("offset"), Cypher.literalOf<Any>(-1))
            .returning(Cypher.literalTrue())
            .build()
    return renderer.render(statement).removeSuffix(" RETURN true")
  }
}
