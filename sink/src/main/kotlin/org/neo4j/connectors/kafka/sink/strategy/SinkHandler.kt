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

import org.neo4j.connectors.kafka.sink.ChangeQuery
import org.neo4j.connectors.kafka.sink.SinkMessage
import org.neo4j.connectors.kafka.sink.SinkStrategy
import org.neo4j.connectors.kafka.sink.SinkStrategyHandler

class SinkHandler(
    private val strategy: SinkStrategy,
    internal val batchStrategy: SinkBatchStrategy,
    internal val eventTransformer: SinkEventTransformer,
) : SinkStrategyHandler {

  override fun strategy(): SinkStrategy = strategy

  override fun handle(messages: Iterable<SinkMessage>): Iterable<Iterable<ChangeQuery>> {
    return batchStrategy.handle(messages) { eventTransformer.transform(it) }
  }
}

interface SinkBatchStrategy {
  fun handle(
      messages: Iterable<SinkMessage>,
      eventTransformer: (SinkMessage) -> SinkData,
  ): Iterable<Iterable<ChangeQuery>>
}

interface SinkEventTransformer {
  fun transform(message: SinkMessage): SinkData
}

data class MessageToEvent(val message: SinkMessage, val sinkData: SinkData)
