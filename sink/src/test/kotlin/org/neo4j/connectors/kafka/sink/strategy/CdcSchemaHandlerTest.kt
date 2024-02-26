/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import io.kotest.matchers.throwable.shouldHaveMessage
import java.time.LocalDate
import java.time.ZonedDateTime
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.connect.sink.SinkRecord
import org.junit.jupiter.api.Test
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
import org.neo4j.cypherdsl.core.renderer.Configuration
import org.neo4j.cypherdsl.core.renderer.Renderer
import org.neo4j.driver.Query

class CdcSchemaHandlerTest {

  @Test
  fun `should fail on empty keys`() {
    listOf(
            newChangeEventMessage(
                NodeEvent(
                    "node-element-id",
                    EntityOperation.CREATE,
                    emptyList(),
                    emptyMap(),
                    null,
                    NodeState(emptyList(), mapOf("name" to "john", "surname" to "doe"))),
                1,
                0),
            newChangeEventMessage(
                RelationshipEvent(
                    "rel-element-id",
                    "REL",
                    Node("start-element-id", listOf("Person"), emptyMap()),
                    Node("end-element-id", listOf("Person"), emptyMap()),
                    emptyList(),
                    EntityOperation.CREATE,
                    null,
                    RelationshipState(emptyMap())),
                1,
                0),
            newChangeEventMessage(
                RelationshipEvent(
                    "rel-element-id",
                    "REL",
                    Node(
                        "start-element-id",
                        listOf("Person"),
                        mapOf("Person" to listOf(mapOf("id" to 5L)))),
                    Node("end-element-id", listOf("Person"), emptyMap()),
                    emptyList(),
                    EntityOperation.CREATE,
                    null,
                    RelationshipState(emptyMap())),
                1,
                0),
            newChangeEventMessage(
                RelationshipEvent(
                    "rel-element-id",
                    "REL",
                    Node("start-element-id", listOf("Person"), emptyMap()),
                    Node(
                        "end-element-id",
                        listOf("Person"),
                        mapOf("Person" to listOf(mapOf("id" to 5L)))),
                    emptyList(),
                    EntityOperation.CREATE,
                    null,
                    RelationshipState(emptyMap())),
                1,
                0))
        .forEach {
          shouldThrow<IllegalArgumentException> {
                val handler =
                    CdcSchemaHandler(
                        "my-topic", Renderer.getRenderer(Configuration.defaultConfig()))

                handler.handle(listOf(it))
              }
              .also {
                it shouldHaveMessage
                    Regex(
                        "^schema strategy requires at least one node key associated with node aliased.*$")
              }
        }
  }

  @Test
  fun `should generate correct statement for node creation events`() {
    verify(
        listOf(
            newChangeEventMessage(
                NodeEvent(
                    "node-element-id",
                    EntityOperation.CREATE,
                    listOf("Person"),
                    mapOf("Person" to listOf(mapOf("name" to "john", "surname" to "doe"))),
                    null,
                    NodeState(
                        listOf("Person"),
                        mapOf(
                            "name" to "john",
                            "surname" to "doe",
                            "dob" to LocalDate.of(1990, 1, 1)))),
                1,
                0)),
        listOf(
            listOf(
                Query(
                    "MERGE (n:`Person` {name: ${'$'}nName, surname: ${'$'}nSurname}) SET n = ${'$'}nProps",
                    mapOf(
                        "nName" to "john",
                        "nSurname" to "doe",
                        "nProps" to
                            mapOf(
                                "name" to "john",
                                "surname" to "doe",
                                "dob" to LocalDate.of(1990, 1, 1)))))))

    verify(
        listOf(
            newChangeEventMessage(
                NodeEvent(
                    "node-element-id",
                    EntityOperation.CREATE,
                    listOf("Person", "Employee"),
                    mapOf("Person" to listOf(mapOf("name" to "john", "surname" to "doe"))),
                    null,
                    NodeState(
                        listOf("Person", "Employee"),
                        mapOf(
                            "name" to "john",
                            "surname" to "doe",
                            "dob" to LocalDate.of(1990, 1, 1)))),
                1,
                0)),
        listOf(
            listOf(
                Query(
                    "MERGE (n:`Person` {name: ${'$'}nName, surname: ${'$'}nSurname}) SET n = ${'$'}nProps SET n:`Employee`",
                    mapOf(
                        "nName" to "john",
                        "nSurname" to "doe",
                        "nProps" to
                            mapOf(
                                "name" to "john",
                                "surname" to "doe",
                                "dob" to LocalDate.of(1990, 1, 1)))))))
  }

  @Test
  fun `should generate correct statement for node update events`() {
    verify(
        listOf(
            newChangeEventMessage(
                NodeEvent(
                    "node-element-id",
                    EntityOperation.UPDATE,
                    listOf("Person", "Employee"),
                    mapOf("Person" to listOf(mapOf("name" to "john", "surname" to "doe"))),
                    NodeState(
                        listOf("Person", "Employee"), mapOf("name" to "joe", "surname" to "doe")),
                    NodeState(
                        listOf("Person", "Employee"),
                        mapOf(
                            "name" to "john",
                            "surname" to "doe",
                            "dob" to LocalDate.of(1990, 1, 1)))),
                1,
                0)),
        listOf(
            listOf(
                Query(
                    "MERGE (n:`Person` {name: ${'$'}nName, surname: ${'$'}nSurname}) SET n += ${'$'}nProps",
                    mapOf(
                        "nName" to "john",
                        "nSurname" to "doe",
                        "nProps" to mapOf("name" to "john", "dob" to LocalDate.of(1990, 1, 1)))))))

    verify(
        listOf(
            newChangeEventMessage(
                NodeEvent(
                    "node-element-id",
                    EntityOperation.UPDATE,
                    listOf("Person", "Employee"),
                    mapOf("Person" to listOf(mapOf("name" to "john", "surname" to "doe"))),
                    NodeState(
                        listOf("Person", "Employee"),
                        mapOf("name" to "joe", "surname" to "doe", "married" to true)),
                    NodeState(
                        listOf("Person", "Manager"),
                        mapOf(
                            "name" to "john",
                            "surname" to "doe",
                            "dob" to LocalDate.of(1990, 1, 1)))),
                1,
                0)),
        listOf(
            listOf(
                Query(
                    "MERGE (n:`Person` {name: ${'$'}nName, surname: ${'$'}nSurname}) SET n += ${'$'}nProps SET n:`Manager` REMOVE n:`Employee`",
                    mapOf(
                        "nName" to "john",
                        "nSurname" to "doe",
                        "nProps" to
                            mapOf(
                                "name" to "john",
                                "dob" to LocalDate.of(1990, 1, 1),
                                "married" to null))))))

    verify(
        listOf(
            newChangeEventMessage(
                NodeEvent(
                    "node-element-id",
                    EntityOperation.UPDATE,
                    listOf("Person", "Employee"),
                    mapOf(
                        "Person" to listOf(mapOf("name" to "john", "surname" to "doe")),
                        "Employee" to listOf(mapOf("id" to 5000L))),
                    NodeState(
                        listOf("Person", "Employee"),
                        mapOf("name" to "joe", "surname" to "doe", "married" to true)),
                    NodeState(
                        listOf("Person", "Manager"),
                        mapOf(
                            "name" to "john",
                            "surname" to "doe",
                            "dob" to LocalDate.of(1990, 1, 1)))),
                1,
                0)),
        listOf(
            listOf(
                Query(
                    "MERGE (n:`Person`:`Employee` {name: ${'$'}nName, surname: ${'$'}nSurname, id: ${'$'}nId}) SET n += ${'$'}nProps SET n:`Manager` REMOVE n:`Employee`",
                    mapOf(
                        "nId" to 5000L,
                        "nName" to "john",
                        "nSurname" to "doe",
                        "nProps" to
                            mapOf(
                                "name" to "john",
                                "dob" to LocalDate.of(1990, 1, 1),
                                "married" to null))))))
  }

  @Test
  fun `should generate correct statement for node deletion events`() {
    verify(
        listOf(
            newChangeEventMessage(
                NodeEvent(
                    "node-element-id",
                    EntityOperation.DELETE,
                    listOf("Person"),
                    mapOf("Person" to listOf(mapOf("name" to "joe", "surname" to "doe"))),
                    NodeState(
                        emptyList(),
                        mapOf(
                            "name" to "joe",
                            "surname" to "doe",
                            "dob" to LocalDate.of(2000, 1, 1))),
                    null),
                1,
                0)),
        listOf(
            listOf(
                Query(
                    "MATCH (n:`Person` {name: ${'$'}nName, surname: ${'$'}nSurname}) DETACH DELETE n",
                    mapOf("nName" to "joe", "nSurname" to "doe")))))
  }

  @Test
  fun `should generate correct statement for relationship creation events`() {
    verify(
        listOf(
            newChangeEventMessage(
                RelationshipEvent(
                    "rel-element-id",
                    "KNOWS",
                    Node(
                        "start-element-id",
                        listOf("Person"),
                        mapOf("Person" to listOf(mapOf("id" to 1L)))),
                    Node(
                        "end-element-id",
                        listOf("Person"),
                        mapOf("Person" to listOf(mapOf("id" to 2L)))),
                    emptyList(),
                    EntityOperation.CREATE,
                    null,
                    RelationshipState(mapOf("since" to LocalDate.of(2000, 1, 1)))),
                1,
                0)),
        listOf(
            listOf(
                Query(
                    "MERGE (start:`Person` {id: ${'$'}startId}) " +
                        "MERGE (end:`Person` {id: ${'$'}endId}) " +
                        "MERGE (start)-[r:`KNOWS` {}]->(end) " +
                        "SET r = ${'$'}rProps",
                    mapOf(
                        "startId" to 1L,
                        "endId" to 2L,
                        "rProps" to mapOf("since" to LocalDate.of(2000, 1, 1)))))))
  }

  @Test
  fun `should generate correct statement for relationship update events`() {
    verify(
        listOf(
            newChangeEventMessage(
                RelationshipEvent(
                    "rel-element-id",
                    "KNOWS",
                    Node(
                        "start-element-id",
                        listOf("Person", "Employee"),
                        mapOf(
                            "Person" to listOf(mapOf("id" to 1L)),
                            "Employee" to listOf(mapOf("contractId" to 5000L)))),
                    Node(
                        "end-element-id",
                        listOf("Person", "Employee"),
                        mapOf(
                            "Person" to listOf(mapOf("id" to 2L)),
                            "Employee" to listOf(mapOf("contractId" to 5001L)))),
                    emptyList(),
                    EntityOperation.UPDATE,
                    RelationshipState(mapOf("since" to LocalDate.of(2000, 1, 1))),
                    RelationshipState(mapOf("since" to LocalDate.of(1999, 1, 1)))),
                1,
                0)),
        listOf(
            listOf(
                Query(
                    "MERGE (start:`Person`:`Employee` {id: ${'$'}startId, contractId: ${'$'}startContractId}) " +
                        "MERGE (end:`Person`:`Employee` {id: ${'$'}endId, contractId: ${'$'}endContractId}) " +
                        "MERGE (start)-[r:`KNOWS` {}]->(end) " +
                        "SET r += ${'$'}rProps",
                    mapOf(
                        "startId" to 1L,
                        "startContractId" to 5000L,
                        "endId" to 2L,
                        "endContractId" to 5001L,
                        "rProps" to mapOf("since" to LocalDate.of(1999, 1, 1)))))))

    verify(
        listOf(
            newChangeEventMessage(
                RelationshipEvent(
                    "rel-element-id",
                    "KNOWS",
                    Node(
                        "start-element-id",
                        listOf("Person", "Employee"),
                        mapOf(
                            "Person" to listOf(mapOf("id" to 1L)),
                            "Employee" to listOf(mapOf("contractId" to 5000L)))),
                    Node(
                        "end-element-id",
                        listOf("Person"),
                        mapOf("Person" to listOf(mapOf("id" to 2L)))),
                    listOf(mapOf("id" to 1001L)),
                    EntityOperation.UPDATE,
                    RelationshipState(mapOf("name" to "john", "surname" to "doe")),
                    RelationshipState(mapOf("name" to "joe", "surname" to "doe"))),
                1,
                0)),
        listOf(
            listOf(
                Query(
                    "MERGE (start:`Person`:`Employee` {id: ${'$'}startId, contractId: ${'$'}startContractId}) " +
                        "MERGE (end:`Person` {id: ${'$'}endId}) " +
                        "MERGE (start)-[r:`KNOWS` {id: ${'$'}rId}]->(end) " +
                        "SET r += ${'$'}rProps",
                    mapOf(
                        "startId" to 1L,
                        "startContractId" to 5000L,
                        "endId" to 2L,
                        "rId" to 1001L,
                        "rProps" to mapOf("name" to "joe"))))))
  }

  @Test
  fun `should generate correct statement for relationship deletion events`() {
    verify(
        listOf(
            newChangeEventMessage(
                RelationshipEvent(
                    "rel-element-id",
                    "KNOWS",
                    Node(
                        "start-element-id",
                        listOf("Person"),
                        mapOf("Person" to listOf(mapOf("id" to 1L)))),
                    Node(
                        "end-element-id",
                        listOf("Person"),
                        mapOf("Person" to listOf(mapOf("id" to 2L)))),
                    emptyList(),
                    EntityOperation.DELETE,
                    RelationshipState(mapOf("name" to "john", "surname" to "doe")),
                    null),
                1,
                0)),
        listOf(
            listOf(
                Query(
                    "MATCH (start:`Person` {id: ${'$'}startId}) " +
                        "MATCH (end:`Person` {id: ${'$'}endId}) " +
                        "MATCH (start)-[r:`KNOWS` {}]->(end) " +
                        "DELETE r",
                    mapOf("startId" to 1L, "endId" to 2L)))))

    verify(
        listOf(
            newChangeEventMessage(
                RelationshipEvent(
                    "rel-element-id",
                    "KNOWS",
                    Node(
                        "start-element-id",
                        listOf("Person", "Employee"),
                        mapOf(
                            "Person" to listOf(mapOf("id" to 1L)),
                            "Employee" to listOf(mapOf("contractId" to 5000L)))),
                    Node(
                        "end-element-id",
                        listOf("Person"),
                        mapOf("Person" to listOf(mapOf("id" to 2L)))),
                    listOf(mapOf("id" to 1001L)),
                    EntityOperation.DELETE,
                    RelationshipState(mapOf("name" to "john", "surname" to "doe")),
                    null),
                1,
                0)),
        listOf(
            listOf(
                Query(
                    "MATCH (start:`Person`:`Employee` {id: ${'$'}startId, contractId: ${'$'}startContractId}) " +
                        "MATCH (end:`Person` {id: ${'$'}endId}) " +
                        "MATCH (start)-[r:`KNOWS` {id: ${'$'}rId}]->(end) " +
                        "DELETE r",
                    mapOf(
                        "startId" to 1L,
                        "startContractId" to 5000L,
                        "endId" to 2L,
                        "rId" to 1001L)))))
  }

  private fun verify(messages: Iterable<SinkMessage>, expected: Iterable<Iterable<Query>>) {
    val handler = CdcSchemaHandler("my-topic", Renderer.getRenderer(Configuration.defaultConfig()))

    val result = handler.handle(messages)

    result shouldBe expected
  }

  private fun <T : Event> newChangeEventMessage(event: T, txId: Long, seq: Int): SinkMessage {
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
}
