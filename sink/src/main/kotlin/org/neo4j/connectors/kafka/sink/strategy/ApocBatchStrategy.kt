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

class ApocBatchStrategy(
    private val neo4j: Neo4j,
    private val batchSize: Int,
    eosOffsetLabel: String,
    private val strategy: SinkStrategy,
) : SinkBatchStrategy {
  private val logger: Logger = LoggerFactory.getLogger(javaClass)
  private val statementGenerator by lazy { DefaultSinkActionStatementGenerator(neo4j) }
  private val renderer = CypherRenderer(neo4j)
  private val envelope = BatchEnvelope(neo4j, eosOffsetLabel)

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
        renderer.render(envelope.around(applyRecordStatement())),
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
   * The batch's per-event body: the record's own statement, which travels as a `stmt`/`params` pair
   * on the event rather than as code, handed to `apoc.cypher.doIt` to run.
   */
  private fun applyRecordStatement(): Statement =
      Cypher.call("apoc.cypher.doIt")
          .withArgs(envelope.event.property("stmt"), envelope.event.property("params"))
          .yield("value")
          .returning(Cypher.count(Cypher.literalOf<Any>(1)).`as`("total"))
          .build()
}
