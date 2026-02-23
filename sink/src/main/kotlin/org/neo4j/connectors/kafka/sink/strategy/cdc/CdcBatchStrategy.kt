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

import org.neo4j.cdc.client.model.ChangeEvent
import org.neo4j.cdc.client.model.EntityOperation
import org.neo4j.cdc.client.model.NodeEvent
import org.neo4j.cdc.client.model.RelationshipEvent
import org.neo4j.connectors.kafka.sink.ChangeQuery
import org.neo4j.connectors.kafka.sink.SinkMessage

data class MessageToEvent(
    val message: SinkMessage,
    val changeEvent: ChangeEvent,
    val cdcData: CdcData,
)

interface CdcBatchStrategy {
  fun handle(
      messages: Iterable<SinkMessage>,
      eventTransformer: (ChangeEvent) -> CdcData,
  ): Iterable<Iterable<ChangeQuery>>
}

interface CdcEventTransformer {
  fun transform(event: ChangeEvent): CdcData {
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

  fun transformCreate(event: NodeEvent): CdcData

  fun transformUpdate(event: NodeEvent): CdcData

  fun transformDelete(event: NodeEvent): CdcData

  fun transformCreate(event: RelationshipEvent): CdcData

  fun transformUpdate(event: RelationshipEvent): CdcData

  fun transformDelete(event: RelationshipEvent): CdcData
}
