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

import java.time.ZonedDateTime
import java.util.UUID
import kotlin.random.Random
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.connect.sink.SinkRecord
import org.neo4j.cdc.client.model.CaptureMode
import org.neo4j.cdc.client.model.ChangeEvent
import org.neo4j.cdc.client.model.ChangeIdentifier
import org.neo4j.cdc.client.model.EntityOperation
import org.neo4j.cdc.client.model.Event
import org.neo4j.cdc.client.model.Metadata
import org.neo4j.cdc.client.model.Node
import org.neo4j.cdc.client.model.NodeEvent
import org.neo4j.cdc.client.model.NodeState
import org.neo4j.cdc.client.model.RelationshipEvent
import org.neo4j.cdc.client.model.RelationshipState
import org.neo4j.connectors.kafka.data.ChangeEventExtensions.toConnectValue
import org.neo4j.connectors.kafka.data.Headers
import org.neo4j.connectors.kafka.sink.SinkMessage

object TestUtils {
  private val random = Random(System.currentTimeMillis())

  fun <T : Event> newChangeEventMessage(event: T, txId: Long, seq: Int): SinkMessage {
    val change =
        ChangeEvent(
            ChangeIdentifier("change-id"),
            txId,
            seq,
            Metadata(
                "service",
                "neo4j",
                "server-1",
                CaptureMode.DIFF,
                "bolt",
                "127.0.0.1:32000",
                "127.0.0.1:7687",
                ZonedDateTime.now().minusSeconds(1),
                ZonedDateTime.now(),
                mapOf("user" to "app_user", "app" to "hr"),
                emptyMap()),
            event)
    val changeConnect = change.toConnectValue()

    return SinkMessage(
        SinkRecord(
            "my-topic",
            0,
            null,
            null,
            changeConnect.schema(),
            changeConnect.value(),
            0,
            System.currentTimeMillis(),
            TimestampType.CREATE_TIME,
            Headers.from(change)))
  }

  fun randomChangeEvent(): Event =
      when (random.nextBits(1)) {
        0 -> {
          val id = random.nextInt()

          NodeEvent(
              UUID.randomUUID().toString(),
              EntityOperation.CREATE,
              listOf("Person"),
              mapOf("Person" to listOf(mapOf("id" to id))),
              null,
              NodeState(listOf("Person"), mapOf("id" to id)))
        }
        1 -> {
          val id = random.nextInt()
          val startId = random.nextInt()
          val endId = random.nextInt()

          RelationshipEvent(
              UUID.randomUUID().toString(),
              "KNOWS",
              Node(
                  UUID.randomUUID().toString(),
                  listOf("Person"),
                  mapOf("Person" to listOf(mapOf("id" to startId)))),
              Node(
                  UUID.randomUUID().toString(),
                  listOf("Person"),
                  mapOf("Person" to listOf(mapOf("id" to endId)))),
              listOf(mapOf("id" to id)),
              EntityOperation.CREATE,
              null,
              RelationshipState(emptyMap()))
        }
        else -> throw IllegalArgumentException("unexpected value from random.nextBits")
      }
}
