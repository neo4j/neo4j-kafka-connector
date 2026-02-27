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
import org.neo4j.cdc.client.model.EntityOperation
import org.neo4j.cdc.client.model.NodeEvent
import org.neo4j.cdc.client.model.RelationshipEvent
import org.neo4j.connectors.kafka.configuration.ConnectorType.SINK
import org.neo4j.connectors.kafka.data.StreamsTransactionEventExtensions.toChangeEvent
import org.neo4j.connectors.kafka.data.toChangeEvent
import org.neo4j.connectors.kafka.metrics.CdcMetricsData
import org.neo4j.connectors.kafka.metrics.Metrics
import org.neo4j.connectors.kafka.sink.ChangeQuery
import org.neo4j.connectors.kafka.sink.SinkMessage
import org.neo4j.connectors.kafka.sink.strategy.SinkAction
import org.neo4j.connectors.kafka.sink.strategy.SinkEventTransformer
import org.neo4j.connectors.kafka.sink.strategy.legacy.toStreamsSinkEntity
import org.neo4j.connectors.kafka.sink.strategy.legacy.toStreamsTransactionEvent

interface CdcEventTransformer : SinkEventTransformer {
  override fun transform(message: SinkMessage): SinkAction {
    val changeEvent = message.toChangeEvent()
    return transform(changeEvent)
  }

  fun transform(event: ChangeEvent): SinkAction {
    return when (val e = event.event) {
      is NodeEvent ->
          when (e.operation) {
            EntityOperation.CREATE -> transformCreate(e)
            EntityOperation.UPDATE -> transformUpdate(e)
            EntityOperation.DELETE -> transformDelete(e)
            else -> throw IllegalArgumentException("unknown operation ${e.operation}")
          }
      is RelationshipEvent ->
          when (e.operation) {
            EntityOperation.CREATE -> transformCreate(e)
            EntityOperation.UPDATE -> transformUpdate(e)
            EntityOperation.DELETE -> transformDelete(e)
            else -> throw IllegalArgumentException("unknown operation ${e.operation}")
          }
      else -> throw IllegalArgumentException("unsupported event type ${e.eventType}")
    }
  }

  fun transformCreate(event: NodeEvent): SinkAction

  fun transformUpdate(event: NodeEvent): SinkAction

  fun transformDelete(event: NodeEvent): SinkAction

  fun transformCreate(event: RelationshipEvent): SinkAction

  fun transformUpdate(event: RelationshipEvent): SinkAction

  fun transformDelete(event: RelationshipEvent): SinkAction
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
