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

import org.apache.kafka.connect.data.Struct
import org.neo4j.cdc.client.model.ChangeEvent
import org.neo4j.connectors.kafka.data.StreamsTransactionEventExtensions.toChangeEvent
import org.neo4j.connectors.kafka.data.toChangeEvent
import org.neo4j.connectors.kafka.metrics.Metrics
import org.neo4j.connectors.kafka.sink.ChangeQuery
import org.neo4j.connectors.kafka.sink.SinkMessage
import org.neo4j.connectors.kafka.sink.SinkStrategy
import org.neo4j.connectors.kafka.sink.SinkStrategyHandler
import org.neo4j.connectors.kafka.sink.strategy.legacy.toStreamsSinkEntity
import org.neo4j.connectors.kafka.sink.strategy.legacy.toStreamsTransactionEvent

class CdcHandler(
    private val strategy: SinkStrategy,
    internal val batchStrategy: CdcBatchStrategy,
    internal val eventTransformer: CdcEventTransformer,
    metrics: Metrics,
) : SinkStrategyHandler {

  private val metricsData = CdcMetricsData(metrics)

  override fun strategy(): SinkStrategy = strategy

  override fun handle(messages: Iterable<SinkMessage>): Iterable<Iterable<ChangeQuery>> {
    return batchStrategy.handle(messages) { eventTransformer.transform(it) }
  }

  override fun postProcessLastMessageBatch(group: Iterable<ChangeQuery>) {
    group.lastOrNull()?.messages?.lastOrNull()?.toChangeEvent()
  }
}

internal fun SinkMessage.toChangeEvent(): ChangeEvent =
    when {
      this.isCdcMessage -> parseCdcChangeEvent(this)
      else -> parseStreamsChangeEvent(this)
    }

internal fun parseCdcChangeEvent(message: SinkMessage): ChangeEvent =
    when (val value = message.value) {
      is Struct -> value.toChangeEvent()
      else ->
          throw IllegalArgumentException(
              "unexpected message value type ${value?.javaClass?.name} in $message"
          )
    }

internal fun parseStreamsChangeEvent(message: SinkMessage): ChangeEvent {
  val event =
      message.record.toStreamsSinkEntity().toStreamsTransactionEvent { _ -> true }
          ?: throw IllegalArgumentException("unsupported change event message in $message")

  return event.toChangeEvent()
}
