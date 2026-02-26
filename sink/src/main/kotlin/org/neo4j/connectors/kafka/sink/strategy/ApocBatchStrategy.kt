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

class ApocBatchStrategy(
    private val neo4j: Neo4j,
    private val batchSize: Int,
    private val eosOffsetLabel: String,
    private val strategy: SinkStrategy,
) : SinkBatchStrategy {
  private val logger: Logger = LoggerFactory.getLogger(javaClass)
  private val statementGenerator by lazy { DefaultSinkDataStatementGenerator(neo4j) }

  override fun handle(
      messages: Iterable<SinkMessage>,
      eventTransformer: (SinkMessage) -> SinkData,
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
    val termination =
        if (CanIUse.canIUse(Cypher.finishClause()).withNeo4j(neo4j)) "FINISH"
        else "RETURN COUNT(1) AS total"

    val query = buildString {
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
      appendCallSubquery()
      if (eosOffsetLabel.isNotBlank()) {
        appendLine("WITH k, max(${EVENT}.offset) AS newOffset SET k.offset = newOffset")
      }
      append(termination)
    }

    return Query(
        query,
        buildMap {
          put(
              "events",
              events.map {
                val query = statementGenerator.buildStatement(it.sinkData)

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

  private fun StringBuilder.appendCallSubquery() {
    if (CanIUse.canIUse(Cypher.callSubqueryWithVariableScopeClause()).withNeo4j(neo4j))
        appendLine("CALL (${EVENT}) {")
    else appendLine("CALL { WITH ${EVENT}")
    appendLine(
        "  CALL apoc.cypher.doIt(${EVENT}.stmt, ${EVENT}.params) YIELD value RETURN COUNT(1) AS total"
    )
    appendLine("}")
  }
}
