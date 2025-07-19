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

import io.kotest.matchers.shouldBe
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneOffset
import java.time.ZonedDateTime
import org.junit.jupiter.api.Test
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
import org.neo4j.connectors.kafka.data.StreamsTransactionEventExtensions.toChangeEvent
import org.neo4j.connectors.kafka.events.Constraint
import org.neo4j.connectors.kafka.events.Meta
import org.neo4j.connectors.kafka.events.NodeChange
import org.neo4j.connectors.kafka.events.NodePayload
import org.neo4j.connectors.kafka.events.OperationType
import org.neo4j.connectors.kafka.events.Payload
import org.neo4j.connectors.kafka.events.RelationshipChange
import org.neo4j.connectors.kafka.events.RelationshipNodeChange
import org.neo4j.connectors.kafka.events.RelationshipPayload
import org.neo4j.connectors.kafka.events.Schema
import org.neo4j.connectors.kafka.events.StreamsConstraintType
import org.neo4j.connectors.kafka.events.StreamsTransactionEvent

class StreamsTransactionEventExtensionsTest {

  @Test
  fun `should be able to convert node creation event without keys`() {
    val time = System.currentTimeMillis()
    val event =
        newStreamsEvent(
            time,
            OperationType.created,
            NodePayload(
                id = "0",
                before = null,
                after =
                    NodeChange(
                        properties = mapOf("name" to "john", "surname" to "doe"),
                        labels = listOf("User"),
                    ),
            ),
        )

    event.toChangeEvent() shouldBe
        ChangeEvent(
            ChangeIdentifier("1:0"),
            1,
            0,
            defaultExpectedMetadata(time),
            NodeEvent(
                "0",
                EntityOperation.CREATE,
                listOf("User"),
                emptyMap(),
                null,
                NodeState(listOf("User"), mapOf("name" to "john", "surname" to "doe")),
            ),
        )
  }

  @Test
  fun `should be able to convert node creation event with keys`() {
    val time = System.currentTimeMillis()
    val event =
        newStreamsEvent(
            time,
            OperationType.created,
            NodePayload(
                id = "0",
                before = null,
                after =
                    NodeChange(
                        properties =
                            mapOf(
                                "name" to "john",
                                "surname" to "doe",
                                "dob" to LocalDate.of(2000, 1, 1),
                            ),
                        labels = listOf("User"),
                    ),
            ),
            schema =
                Schema(
                    constraints =
                        listOf(
                            Constraint(
                                "User",
                                setOf("name", "surname"),
                                StreamsConstraintType.UNIQUE,
                            )
                        )
                ),
        )

    event.toChangeEvent() shouldBe
        ChangeEvent(
            ChangeIdentifier("1:0"),
            1,
            0,
            defaultExpectedMetadata(time),
            NodeEvent(
                "0",
                EntityOperation.CREATE,
                listOf("User"),
                mapOf("User" to listOf(mapOf("name" to "john", "surname" to "doe"))),
                null,
                NodeState(
                    listOf("User"),
                    mapOf("name" to "john", "surname" to "doe", "dob" to LocalDate.of(2000, 1, 1)),
                ),
            ),
        )
  }

  @Test
  fun `should be able to convert node update event`() {
    val time = System.currentTimeMillis()
    val event =
        newStreamsEvent(
            time,
            OperationType.updated,
            NodePayload(
                id = "0",
                before = NodeChange(mapOf("name" to "joe", "surname" to "doe"), listOf("User")),
                after =
                    NodeChange(
                        properties =
                            mapOf(
                                "name" to "john",
                                "surname" to "doe",
                                "dob" to LocalDate.of(2000, 1, 1),
                            ),
                        labels = listOf("User"),
                    ),
            ),
            schema =
                Schema(
                    constraints =
                        listOf(
                            Constraint(
                                "User",
                                setOf("name", "surname"),
                                StreamsConstraintType.UNIQUE,
                            )
                        )
                ),
        )

    event.toChangeEvent() shouldBe
        ChangeEvent(
            ChangeIdentifier("1:0"),
            1,
            0,
            defaultExpectedMetadata(time),
            NodeEvent(
                "0",
                EntityOperation.UPDATE,
                listOf("User"),
                mapOf("User" to listOf(mapOf("name" to "joe", "surname" to "doe"))),
                NodeState(listOf("User"), mapOf("name" to "joe", "surname" to "doe")),
                NodeState(
                    listOf("User"),
                    mapOf("name" to "john", "surname" to "doe", "dob" to LocalDate.of(2000, 1, 1)),
                ),
            ),
        )
  }

  @Test
  fun `should be able to convert node delete event`() {
    val time = System.currentTimeMillis()
    val event =
        newStreamsEvent(
            time,
            OperationType.deleted,
            NodePayload(
                id = "0",
                before =
                    NodeChange(
                        properties =
                            mapOf(
                                "name" to "john",
                                "surname" to "doe",
                                "dob" to LocalDate.of(2000, 1, 1),
                            ),
                        labels = listOf("User"),
                    ),
                after = null,
            ),
            schema =
                Schema(
                    constraints =
                        listOf(
                            Constraint(
                                "User",
                                setOf("name", "surname"),
                                StreamsConstraintType.UNIQUE,
                            )
                        )
                ),
        )

    event.toChangeEvent() shouldBe
        ChangeEvent(
            ChangeIdentifier("1:0"),
            1,
            0,
            defaultExpectedMetadata(time),
            NodeEvent(
                "0",
                EntityOperation.DELETE,
                listOf("User"),
                mapOf("User" to listOf(mapOf("name" to "john", "surname" to "doe"))),
                NodeState(
                    listOf("User"),
                    mapOf("name" to "john", "surname" to "doe", "dob" to LocalDate.of(2000, 1, 1)),
                ),
                null,
            ),
        )
  }

  @Test
  fun `should be able to convert relationship creation event`() {
    val time = System.currentTimeMillis()
    val event =
        newStreamsEvent(
            time,
            OperationType.created,
            RelationshipPayload(
                id = "0",
                label = "KNOWS",
                start =
                    RelationshipNodeChange(
                        "1",
                        listOf("Person"),
                        mapOf("name" to "joe", "surname" to "doe"),
                    ),
                end =
                    RelationshipNodeChange(
                        "2",
                        listOf("Person"),
                        mapOf("name" to "mary", "surname" to "doe"),
                    ),
                before = null,
                after = RelationshipChange(mapOf("since" to LocalDate.of(2000, 1, 1))),
            ),
        )

    event.toChangeEvent() shouldBe
        ChangeEvent(
            ChangeIdentifier("1:0"),
            1,
            0,
            defaultExpectedMetadata(time),
            RelationshipEvent(
                "0",
                "KNOWS",
                Node(
                    "1",
                    listOf("Person"),
                    mapOf("Person" to listOf(mapOf("name" to "joe", "surname" to "doe"))),
                ),
                Node(
                    "2",
                    listOf("Person"),
                    mapOf("Person" to listOf(mapOf("name" to "mary", "surname" to "doe"))),
                ),
                emptyList(),
                EntityOperation.CREATE,
                null,
                RelationshipState(mapOf("since" to LocalDate.of(2000, 1, 1))),
            ),
        )
  }

  @Test
  fun `should be able to convert relationship update event`() {
    val time = System.currentTimeMillis()
    val event =
        newStreamsEvent(
            time,
            OperationType.updated,
            RelationshipPayload(
                id = "0",
                label = "KNOWS",
                start =
                    RelationshipNodeChange(
                        "1",
                        listOf("Person", "Employee"),
                        mapOf("name" to "joe", "surname" to "doe"),
                    ),
                end =
                    RelationshipNodeChange(
                        "2",
                        listOf("Person", "Employee"),
                        mapOf("name" to "mary", "surname" to "doe"),
                    ),
                before = RelationshipChange(emptyMap()),
                after = RelationshipChange(mapOf("since" to LocalDate.of(2000, 1, 1))),
            ),
        )

    event.toChangeEvent() shouldBe
        ChangeEvent(
            ChangeIdentifier("1:0"),
            1,
            0,
            defaultExpectedMetadata(time),
            RelationshipEvent(
                "0",
                "KNOWS",
                Node(
                    "1",
                    listOf("Person", "Employee"),
                    mapOf("Person" to listOf(mapOf("name" to "joe", "surname" to "doe"))),
                ),
                Node(
                    "2",
                    listOf("Person", "Employee"),
                    mapOf("Person" to listOf(mapOf("name" to "mary", "surname" to "doe"))),
                ),
                emptyList(),
                EntityOperation.UPDATE,
                RelationshipState(emptyMap()),
                RelationshipState(mapOf("since" to LocalDate.of(2000, 1, 1))),
            ),
        )
  }

  @Test
  fun `should be able to convert relationship delete event`() {
    val time = System.currentTimeMillis()
    val event =
        newStreamsEvent(
            time,
            OperationType.deleted,
            RelationshipPayload(
                id = "0",
                label = "KNOWS",
                start =
                    RelationshipNodeChange(
                        "1",
                        listOf("Person"),
                        mapOf("name" to "joe", "surname" to "doe"),
                    ),
                end =
                    RelationshipNodeChange(
                        "2",
                        listOf("Person"),
                        mapOf("name" to "mary", "surname" to "doe"),
                    ),
                before = RelationshipChange(mapOf("since" to LocalDate.of(2000, 1, 1))),
                after = null,
            ),
        )

    event.toChangeEvent() shouldBe
        ChangeEvent(
            ChangeIdentifier("1:0"),
            1,
            0,
            defaultExpectedMetadata(time),
            RelationshipEvent(
                "0",
                "KNOWS",
                Node(
                    "1",
                    listOf("Person"),
                    mapOf("Person" to listOf(mapOf("name" to "joe", "surname" to "doe"))),
                ),
                Node(
                    "2",
                    listOf("Person"),
                    mapOf("Person" to listOf(mapOf("name" to "mary", "surname" to "doe"))),
                ),
                emptyList(),
                EntityOperation.DELETE,
                RelationshipState(mapOf("since" to LocalDate.of(2000, 1, 1))),
                null,
            ),
        )
  }

  private fun newStreamsEvent(
      time: Long,
      operationType: OperationType,
      payload: Payload,
      meta: Meta? = null,
      schema: Schema? = null,
  ): StreamsTransactionEvent =
      StreamsTransactionEvent(
          meta ?: defaultMetadata(time, operationType),
          payload,
          schema ?: Schema(),
      )

  private fun defaultExpectedMetadata(time: Long) =
      Metadata(
          "user",
          "user",
          "unknown",
          "unknown",
          CaptureMode.OFF,
          "unknown",
          "unknown",
          "unknown",
          ZonedDateTime.ofInstant(Instant.ofEpochMilli(time), ZoneOffset.UTC),
          ZonedDateTime.ofInstant(Instant.ofEpochMilli(time), ZoneOffset.UTC),
          emptyMap(),
          emptyMap(),
      )

  private fun defaultMetadata(time: Long, operationType: OperationType) =
      Meta(
          timestamp = time,
          username = "user",
          txId = 1,
          txEventId = 0,
          txEventsCount = 3,
          operation = operationType,
      )
}
