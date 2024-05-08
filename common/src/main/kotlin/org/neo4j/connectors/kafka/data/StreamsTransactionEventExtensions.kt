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
package org.neo4j.connectors.kafka.data

import java.time.Instant
import java.time.ZoneOffset
import java.time.ZonedDateTime
import org.neo4j.cdc.client.model.CaptureMode
import org.neo4j.cdc.client.model.ChangeEvent
import org.neo4j.cdc.client.model.ChangeIdentifier
import org.neo4j.cdc.client.model.EntityOperation
import org.neo4j.cdc.client.model.Metadata
import org.neo4j.cdc.client.model.Node
import org.neo4j.cdc.client.model.NodeEvent
import org.neo4j.cdc.client.model.NodeState
import org.neo4j.cdc.client.model.RelationshipEvent
import org.neo4j.cdc.client.model.RelationshipState
import org.neo4j.cdc.client.model.State
import org.neo4j.connectors.kafka.events.NodePayload
import org.neo4j.connectors.kafka.events.OperationType
import org.neo4j.connectors.kafka.events.RelationshipPayload
import org.neo4j.connectors.kafka.events.StreamsConstraintType
import org.neo4j.connectors.kafka.events.StreamsTransactionEvent

object StreamsTransactionEventExtensions {

  fun StreamsTransactionEvent.toChangeEvent(): ChangeEvent {
    val cdcOperation =
        when (this.meta.operation) {
          OperationType.created -> EntityOperation.CREATE
          OperationType.updated -> EntityOperation.UPDATE
          OperationType.deleted -> EntityOperation.DELETE
        }
    val cdcMetadata =
        Metadata(
            this.meta.username,
            this.meta.username,
            "unknown",
            CaptureMode.OFF,
            "unknown",
            "unknown",
            "unknown",
            ZonedDateTime.ofInstant(Instant.ofEpochMilli(this.meta.timestamp), ZoneOffset.UTC),
            ZonedDateTime.ofInstant(Instant.ofEpochMilli(this.meta.timestamp), ZoneOffset.UTC),
            emptyMap(),
            emptyMap())
    val cdcEvent =
        when (val payload = this.payload) {
          is NodePayload -> {
            val before = payload.before?.let { NodeState(it.labels, it.properties) }
            val after = payload.after?.let { NodeState(it.labels, it.properties) }
            val referenceState = extractState(this, before, after)

            val keys =
                this.schema.constraints
                    .filter { c -> c.type == StreamsConstraintType.UNIQUE }
                    .map { c ->
                      c.label!! to c.properties.associateWith { referenceState.properties[it] }
                    }
                    .groupBy { it.first }
                    .mapValues { it.value.map { p -> p.second } }

            NodeEvent(payload.id, cdcOperation, referenceState.labels, keys, before, after)
          }
          is RelationshipPayload -> {
            val before = payload.before?.let { RelationshipState(it.properties) }
            val after = payload.after?.let { RelationshipState(it.properties) }

            RelationshipEvent(
                payload.id,
                payload.label,
                Node(
                    payload.start.id,
                    payload.start.labels ?: emptyList(),
                    buildMap {
                      val label = payload.start.labels?.firstOrNull()
                      if (label != null) {
                        this[label] = listOf(payload.start.ids)
                      }
                    }),
                Node(
                    payload.end.id,
                    payload.end.labels ?: emptyList(),
                    buildMap {
                      val label = payload.end.labels?.firstOrNull()
                      if (label != null) {
                        this[label] = listOf(payload.end.ids)
                      }
                    }),
                emptyList(),
                cdcOperation,
                before,
                after)
          }
          else ->
              throw IllegalArgumentException("unexpected payload type ${payload.javaClass.name}")
        }

    return ChangeEvent(
        ChangeIdentifier("${this.meta.txId}:${this.meta.txEventId}"),
        this.meta.txId,
        this.meta.txEventId,
        cdcMetadata,
        cdcEvent)
  }

  private fun <T : State> extractState(event: StreamsTransactionEvent, before: T?, after: T?): T {
    val (name, referenceState) =
        when (event.meta.operation) {
          OperationType.created -> ("after" to after)
          OperationType.updated -> ("before" to before)
          OperationType.deleted -> ("before" to before)
        }
    require(referenceState != null) {
      "$name state should not be null for ${event.meta.operation} events"
    }
    return referenceState
  }
}
