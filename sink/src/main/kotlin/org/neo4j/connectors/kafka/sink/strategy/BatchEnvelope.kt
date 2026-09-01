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

import org.neo4j.caniuse.CanIUse.canIUse
import org.neo4j.caniuse.Cypher as CanIUseCypher
import org.neo4j.caniuse.Neo4j
import org.neo4j.cypherdsl.core.Cypher
import org.neo4j.cypherdsl.core.ExposesFinish
import org.neo4j.cypherdsl.core.ExposesReturning
import org.neo4j.cypherdsl.core.ExposesSubqueryCall
import org.neo4j.cypherdsl.core.Node
import org.neo4j.cypherdsl.core.Statement
import org.neo4j.cypherdsl.core.StatementBuilder
import org.neo4j.cypherdsl.core.SymbolicName

/**
 * The clauses both batch strategies wrap their per-record work in: unwind `${'$'}events`, gate the
 * batch on the exactly-once offset tracker when one is configured, order by Kafka offset, run a
 * subquery per event and finally advance the tracker. Only that subquery differs between them -
 * `apoc.cypher.doIt` for [ApocBatchStrategy], a `UNION ALL` over the batch's distinct statements
 * for [NativeBatchStrategy].
 */
internal class BatchEnvelope(neo4j: Neo4j, private val eosOffsetLabel: String) {

  /** The unwound event. The body passed to [around] reads its own inputs off this. */
  val event: SymbolicName = Cypher.name(EVENT)

  private val hasFinish = canIUse(CanIUseCypher.finishClause()).withNeo4j(neo4j)

  /**
   * [body] called once per event of the batch.
   *
   * [importEvent] asks the DSL to bring [event] into the subquery's scope, which it renders as the
   * variable scope clause or as a leading importing `WITH` depending on its dialect. Pass `false`
   * for a [body] that already opens with its own importing `WITH`: the DSL emits that leading
   * `WITH` once for the whole subquery, which is one short of what a `UNION` body needs.
   *
   * `FINISH` vs `RETURN count(1) AS total` is not a dialect concern the DSL knows about, and is
   * gated here instead.
   */
  fun around(body: Statement, importEvent: Boolean = true): Statement {
    val unwound = Cypher.unwind(Cypher.parameter("events")).`as`(event)

    if (eosOffsetLabel.isBlank()) {
      return terminate(
          unwound
              .with(event)
              .orderBy(event.property("offset"))
              .ascending()
              .callBody(body, importEvent)
      )
    }

    val tracker = offsetTrackerNode()
    return terminate(
        unwound
            .merge(tracker)
            .onCreate()
            .set(tracker.property("offset"), Cypher.literalOf<Any>(-1))
            .with(tracker, event)
            .where(event.property("offset").gt(tracker.property("offset")))
            .with(tracker, event)
            .orderBy(event.property("offset"))
            .ascending()
            .callBody(body, importEvent)
            .with(tracker, Cypher.max(event.property("offset")).`as`("newOffset"))
            .set(tracker.property("offset"), Cypher.name("newOffset"))
    )
  }

  private fun StatementBuilder.OngoingReading.callBody(
      body: Statement,
      importEvent: Boolean,
  ): ExposesSubqueryCall.BuildableSubquery = if (importEvent) call(body, event) else call(body)

  /**
   * The node the last committed offset of one topic-partition is tracked on. Exposed because
   * [NativeBatchStrategy] also has to emit this `MERGE` into an envelope [around] cannot build for
   * it.
   *
   * [eosOffsetLabel] is the operator-configured, raw (unsanitized) label - handing it straight to
   * the DSL lets it escape the identifier itself, exactly once.
   */
  fun offsetTrackerNode(): Node =
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

  /** `FINISH` when supported, otherwise `RETURN COUNT(1) AS total`. */
  private fun <T> terminate(ongoing: T): Statement where T : ExposesFinish, T : ExposesReturning =
      if (hasFinish) ongoing.finish().build()
      else ongoing.returning(Cypher.count(Cypher.literalOf<Any>(1)).`as`("total")).build()
}
