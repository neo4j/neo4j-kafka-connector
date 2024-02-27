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

import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.collections.shouldMatchInOrder
import io.kotest.matchers.shouldBe
import java.time.LocalDate
import org.junit.jupiter.api.Test
import org.neo4j.cdc.client.model.EntityOperation
import org.neo4j.cdc.client.model.Node
import org.neo4j.cdc.client.model.NodeEvent
import org.neo4j.cdc.client.model.NodeState
import org.neo4j.cdc.client.model.RelationshipEvent
import org.neo4j.cdc.client.model.RelationshipState
import org.neo4j.connectors.kafka.sink.ChangeQuery
import org.neo4j.connectors.kafka.sink.SinkMessage
import org.neo4j.connectors.kafka.sink.strategy.TestUtils.newChangeEventMessage
import org.neo4j.connectors.kafka.sink.strategy.TestUtils.randomChangeEvent
import org.neo4j.cypherdsl.core.renderer.Configuration
import org.neo4j.cypherdsl.core.renderer.Renderer
import org.neo4j.driver.Query

class CdcSourceIdHandlerTest {

  @Test
  fun `should generate correct statement for node creation events`() {
    verify(
        listOf(
            newChangeEventMessage(
                NodeEvent(
                    "node-element-id",
                    EntityOperation.CREATE,
                    emptyList(),
                    emptyMap(),
                    null,
                    NodeState(emptyList(), mapOf("name" to "john", "surname" to "doe"))),
                0,
                1)),
        listOf(
            listOf(
                ChangeQuery(
                    0,
                    1,
                    Query(
                        "MERGE (n:`SourceEvent` {sourceElementId: ${'$'}nElementId}) SET n += ${'$'}nProps",
                        mapOf(
                            "nElementId" to "node-element-id",
                            "nProps" to mapOf("name" to "john", "surname" to "doe")))))))

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
                0,
                1)),
        listOf(
            listOf(
                ChangeQuery(
                    0,
                    1,
                    Query(
                        "MERGE (n:`SourceEvent` {sourceElementId: ${'$'}nElementId}) SET n += ${'$'}nProps SET n:`Person`",
                        mapOf(
                            "nElementId" to "node-element-id",
                            "nProps" to
                                mapOf(
                                    "name" to "john",
                                    "surname" to "doe",
                                    "dob" to LocalDate.of(1990, 1, 1))))))))

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
                0,
                1)),
        listOf(
            listOf(
                ChangeQuery(
                    0,
                    1,
                    Query(
                        "MERGE (n:`SourceEvent` {sourceElementId: ${'$'}nElementId}) SET n += ${'$'}nProps SET n:`Person`:`Employee`",
                        mapOf(
                            "nElementId" to "node-element-id",
                            "nProps" to
                                mapOf(
                                    "name" to "john",
                                    "surname" to "doe",
                                    "dob" to LocalDate.of(1990, 1, 1))))))))
  }

  @Test
  fun `should generate correct statement for node update events`() {
    verify(
        listOf(
            newChangeEventMessage(
                NodeEvent(
                    "node-element-id",
                    EntityOperation.UPDATE,
                    emptyList(),
                    emptyMap(),
                    NodeState(emptyList(), mapOf("name" to "john")),
                    NodeState(
                        emptyList(), mapOf("name" to "joe", "dob" to LocalDate.of(2000, 1, 1)))),
                0,
                1)),
        listOf(
            listOf(
                ChangeQuery(
                    0,
                    1,
                    Query(
                        "MERGE (n:`SourceEvent` {sourceElementId: ${'$'}nElementId}) SET n += ${'$'}nProps",
                        mapOf(
                            "nElementId" to "node-element-id",
                            "nProps" to
                                mapOf("name" to "joe", "dob" to LocalDate.of(2000, 1, 1))))))))

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
                0,
                1)),
        listOf(
            listOf(
                ChangeQuery(
                    0,
                    1,
                    Query(
                        "MERGE (n:`SourceEvent` {sourceElementId: ${'$'}nElementId}) SET n += ${'$'}nProps",
                        mapOf(
                            "nElementId" to "node-element-id",
                            "nProps" to
                                mapOf("name" to "john", "dob" to LocalDate.of(1990, 1, 1))))))))

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
                0,
                1)),
        listOf(
            listOf(
                ChangeQuery(
                    0,
                    1,
                    Query(
                        "MERGE (n:`SourceEvent` {sourceElementId: ${'$'}nElementId}) SET n += ${'$'}nProps SET n:`Manager` REMOVE n:`Employee`",
                        mapOf(
                            "nElementId" to "node-element-id",
                            "nProps" to
                                mapOf(
                                    "name" to "john",
                                    "dob" to LocalDate.of(1990, 1, 1),
                                    "married" to null)))))))
  }

  @Test
  fun `should generate correct statement for node deletion events`() {
    verify(
        listOf(
            newChangeEventMessage(
                NodeEvent(
                    "node-element-id",
                    EntityOperation.DELETE,
                    emptyList(),
                    emptyMap(),
                    NodeState(
                        emptyList(), mapOf("name" to "joe", "dob" to LocalDate.of(2000, 1, 1))),
                    null),
                0,
                1)),
        listOf(
            listOf(
                ChangeQuery(
                    0,
                    1,
                    Query(
                        "MATCH (n:`SourceEvent` {sourceElementId: ${'$'}nElementId}) DETACH DELETE n",
                        mapOf("nElementId" to "node-element-id"))))))
  }

  @Test
  fun `should generate correct statement for relationship creation events`() {
    verify(
        listOf(
            newChangeEventMessage(
                RelationshipEvent(
                    "rel-element-id",
                    "REL",
                    Node("start-element-id", emptyList(), emptyMap()),
                    Node("end-element-id", emptyList(), emptyMap()),
                    emptyList(),
                    EntityOperation.CREATE,
                    null,
                    RelationshipState(mapOf("name" to "john", "surname" to "doe"))),
                0,
                1)),
        listOf(
            listOf(
                ChangeQuery(
                    0,
                    1,
                    Query(
                        "MERGE (start:`SourceEvent` {sourceElementId: ${'$'}startElementId}) " +
                            "MERGE (end:`SourceEvent` {sourceElementId: ${'$'}endElementId}) " +
                            "MERGE (start)-[r:`REL` {sourceElementId: ${'$'}rElementId}]->(end) " +
                            "SET r += ${'$'}rProps",
                        mapOf(
                            "startElementId" to "start-element-id",
                            "endElementId" to "end-element-id",
                            "rElementId" to "rel-element-id",
                            "rProps" to mapOf("name" to "john", "surname" to "doe")))))))
  }

  @Test
  fun `should generate correct statement for relationship update events`() {
    verify(
        listOf(
            newChangeEventMessage(
                RelationshipEvent(
                    "rel-element-id",
                    "REL",
                    Node("start-element-id", emptyList(), emptyMap()),
                    Node("end-element-id", emptyList(), emptyMap()),
                    emptyList(),
                    EntityOperation.UPDATE,
                    RelationshipState(emptyMap()),
                    RelationshipState(mapOf("name" to "john", "surname" to "doe"))),
                0,
                1)),
        listOf(
            listOf(
                ChangeQuery(
                    0,
                    1,
                    Query(
                        "MERGE (start:`SourceEvent` {sourceElementId: ${'$'}startElementId}) " +
                            "MERGE (end:`SourceEvent` {sourceElementId: ${'$'}endElementId}) " +
                            "MERGE (start)-[r:`REL` {sourceElementId: ${'$'}rElementId}]->(end) " +
                            "SET r += ${'$'}rProps",
                        mapOf(
                            "startElementId" to "start-element-id",
                            "endElementId" to "end-element-id",
                            "rElementId" to "rel-element-id",
                            "rProps" to mapOf("name" to "john", "surname" to "doe")))))))

    verify(
        listOf(
            newChangeEventMessage(
                RelationshipEvent(
                    "rel-element-id",
                    "REL",
                    Node("start-element-id", emptyList(), emptyMap()),
                    Node("end-element-id", emptyList(), emptyMap()),
                    emptyList(),
                    EntityOperation.UPDATE,
                    RelationshipState(mapOf("name" to "john", "surname" to "doe")),
                    RelationshipState(mapOf("name" to "joe", "surname" to "doe"))),
                0,
                1)),
        listOf(
            listOf(
                ChangeQuery(
                    0,
                    1,
                    Query(
                        "MERGE (start:`SourceEvent` {sourceElementId: ${'$'}startElementId}) " +
                            "MERGE (end:`SourceEvent` {sourceElementId: ${'$'}endElementId}) " +
                            "MERGE (start)-[r:`REL` {sourceElementId: ${'$'}rElementId}]->(end) " +
                            "SET r += ${'$'}rProps",
                        mapOf(
                            "startElementId" to "start-element-id",
                            "endElementId" to "end-element-id",
                            "rElementId" to "rel-element-id",
                            "rProps" to mapOf("name" to "joe")))))))
  }

  @Test
  fun `should generate correct statement for relationship deletion events`() {
    verify(
        listOf(
            newChangeEventMessage(
                RelationshipEvent(
                    "rel-element-id",
                    "REL",
                    Node("start-element-id", emptyList(), emptyMap()),
                    Node("end-element-id", emptyList(), emptyMap()),
                    emptyList(),
                    EntityOperation.DELETE,
                    RelationshipState(emptyMap()),
                    null),
                0,
                1)),
        listOf(
            listOf(
                ChangeQuery(
                    0,
                    1,
                    Query(
                        "MATCH ()-[r:`REL` {sourceElementId: ${'$'}rElementId}]->() DELETE r",
                        mapOf("rElementId" to "rel-element-id"))))))
  }

  @Test
  fun `should split changes into transactional boundaries`() {
    val handler = CdcSchemaHandler("my-topic", Renderer.getRenderer(Configuration.defaultConfig()))

    val result =
        handler.handle(
            listOf(
                newChangeEventMessage(randomChangeEvent(), 0, 0),
                newChangeEventMessage(randomChangeEvent(), 0, 1),
                newChangeEventMessage(randomChangeEvent(), 0, 2),
                newChangeEventMessage(randomChangeEvent(), 1, 0),
                newChangeEventMessage(randomChangeEvent(), 1, 1),
                newChangeEventMessage(randomChangeEvent(), 2, 0)))

    result
        .shouldHaveSize(3)
        .shouldMatchInOrder(
            { first ->
              first
                  .shouldHaveSize(3)
                  .shouldMatchInOrder(
                      { q1 ->
                        q1.txId shouldBe 0
                        q1.seq shouldBe 0
                      },
                      { q2 ->
                        q2.txId shouldBe 0
                        q2.seq shouldBe 1
                      },
                      { q3 ->
                        q3.txId shouldBe 0
                        q3.seq shouldBe 2
                      })
            },
            { second ->
              second
                  .shouldHaveSize(2)
                  .shouldMatchInOrder(
                      { q1 ->
                        q1.txId shouldBe 1
                        q1.seq shouldBe 0
                      },
                      { q2 ->
                        q2.txId shouldBe 1
                        q2.seq shouldBe 1
                      })
            },
            { third ->
              third
                  .shouldHaveSize(1)
                  .shouldMatchInOrder({ q1 ->
                    q1.txId shouldBe 2
                    q1.seq shouldBe 0
                  })
            })
  }

  private fun verify(messages: Iterable<SinkMessage>, expected: Iterable<Iterable<ChangeQuery>>) {
    val handler =
        CdcSourceIdHandler(
            "my-topic",
            Renderer.getRenderer(Configuration.defaultConfig()),
            "SourceEvent",
            "sourceElementId")

    val result = handler.handle(messages)

    result shouldBe expected
  }
}
