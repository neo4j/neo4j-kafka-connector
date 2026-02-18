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

import org.neo4j.caniuse.Neo4j
import org.neo4j.cdc.client.model.ChangeEvent
import org.neo4j.connectors.kafka.sink.ChangeQuery
import org.neo4j.connectors.kafka.sink.SinkMessage
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class TransactionalBatchStrategy(neo4j: Neo4j) : CdcBatchStrategy {
  private val logger: Logger = LoggerFactory.getLogger(javaClass)
  private val statementGenerator by lazy { DefaultCdcStatementGenerator(neo4j) }

  override fun handle(
      messages: Iterable<SinkMessage>,
      eventTransformer: (ChangeEvent) -> CdcData,
  ): Iterable<Iterable<ChangeQuery>> {
    return messages
        .onEach { logger.trace("received message: {}", it) }
        .map {
          val changeEvent = it.toChangeEvent()
          MessageToEvent(it, changeEvent, eventTransformer(changeEvent))
        }
        .onEach { logger.trace("converted message: {} to {}", it.changeEvent.txId, it.changeEvent) }
        .groupBy(
            { it.changeEvent.txId },
            {
              ChangeQuery(
                  it.changeEvent.txId,
                  it.changeEvent.seq,
                  listOf(it.message),
                  statementGenerator.buildStatement(it.cdcData),
              )
            },
        )
        .onEach { logger.trace("mapped messages: {} to {}", it.key, it.value) }
        .values
  }
}
