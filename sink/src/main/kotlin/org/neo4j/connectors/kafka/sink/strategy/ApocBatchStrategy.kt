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
import org.neo4j.cypherdsl.core.ExposesFinish
import org.neo4j.cypherdsl.core.ExposesReturning
import org.neo4j.cypherdsl.core.Statement
import org.neo4j.driver.Query
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class ApocBatchStrategy(
    private val neo4j: Neo4j,
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

    return messages
        .asSequence()
        .onEach { logger.trace("received message: {}", it) }
        .map { MessageToEvent(it, eventTransformer(it)) }
        .chunked(batchSize)
        .map { batch ->
          listOf(
              ChangeQuery(
                  null,
                  null,
                  batch.map { data -> data.message },
                  batchedStatement(topic, partition, batch),
              )
          )
        }
        .onEach { logger.trace("mapped messages: '{}'", it) }
        .toList()
  }

  private fun batchedStatement(topic: String, partition: Int, events: List<MessageToEvent>): Query {
    return Query(
        renderer.render(batchedCypherStatement()),
        buildMap {
          put(
              "events",
              events.map {
                val query = statementGenerator.buildStatement(it.sinkAction)

                mapOf(
                    "offset" to it.message.record.kafkaOffset(),
                    "stmt" to query.text(),
                    "params" to query.parameters(),
                )
              },
          )
          put("topic", topic)
          put("partition", partition)
          put("strategy", strategy.name)
        },
    )
  }

  /**
   * Cypher-DSL renders the very same [call] expression differently depending on that dialect.
   * `FINISH` vs `RETURN COUNT(1) AS total`, on the other hand, is not a dialect concern the DSL
   * knows about - it is gated explicitly below, same as before.
   */
  private fun batchedCypherStatement(): Statement {
    val hasFinish = CanIUse.canIUse(CanIUseCypher.finishClause()).withNeo4j(neo4j)
    val event = Cypher.name(EVENT)
    val subquery =
        Cypher.call("apoc.cypher.doIt")
            .withArgs(event.property("stmt"), event.property("params"))
            .yield("value")
            .returning(Cypher.count(Cypher.literalOf<Any>(1)).`as`("total"))
            .build()
    val unwound = Cypher.unwind(Cypher.parameter("events")).`as`(event)

    return if (eosOffsetLabel.isNotBlank()) {
      // eosOffsetLabel is the operator-configured, raw (unsanitized) label - handing it straight
      // to the DSL lets it escape the identifier itself, exactly once.
      val offsetTracker =
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
      val afterSet =
          unwound
              .merge(offsetTracker)
              .onCreate()
              .set(offsetTracker.property("offset"), Cypher.literalOf<Any>(-1))
              .with(offsetTracker, event)
              .where(event.property("offset").gt(offsetTracker.property("offset")))
              .with(offsetTracker, event)
              .orderBy(event.property("offset"))
              .ascending()
              .call(subquery, event)
              .with(offsetTracker, Cypher.max(event.property("offset")).`as`("newOffset"))
              .set(offsetTracker.property("offset"), Cypher.name("newOffset"))
      terminate(afterSet, hasFinish)
    } else {
      val afterCall =
          unwound.with(event).orderBy(event.property("offset")).ascending().call(subquery, event)
      terminate(afterCall, hasFinish)
    }
  }

  /** `FINISH` when supported, otherwise `RETURN COUNT(1) AS total`. */
  private fun <T> terminate(ongoing: T, hasFinish: Boolean): Statement where
  T : ExposesFinish,
  T : ExposesReturning {
    return if (hasFinish) ongoing.finish().build()
    else ongoing.returning(Cypher.count(Cypher.literalOf<Any>(1)).`as`("total")).build()
  }
}
