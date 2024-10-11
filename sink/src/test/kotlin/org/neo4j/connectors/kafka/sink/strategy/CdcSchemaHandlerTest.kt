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

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.collections.shouldMatchInOrder
import io.kotest.matchers.shouldBe
import io.kotest.matchers.throwable.shouldHaveMessage
import java.time.LocalDate
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.neo4j.cdc.client.model.EntityOperation
import org.neo4j.cdc.client.model.Node
import org.neo4j.cdc.client.model.NodeEvent
import org.neo4j.cdc.client.model.NodeState
import org.neo4j.cdc.client.model.RelationshipEvent
import org.neo4j.cdc.client.model.RelationshipState
import org.neo4j.connectors.kafka.data.StreamsTransactionEventExtensions.toChangeEvent
import org.neo4j.connectors.kafka.events.StreamsTransactionEvent
import org.neo4j.connectors.kafka.exceptions.InvalidDataException
import org.neo4j.connectors.kafka.sink.ChangeQuery
import org.neo4j.connectors.kafka.sink.SinkMessage
import org.neo4j.connectors.kafka.sink.strategy.TestUtils.newChangeEventMessage
import org.neo4j.connectors.kafka.sink.strategy.TestUtils.randomChangeEvent
import org.neo4j.connectors.kafka.utils.JSONUtils
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
                    NodeState(emptyList(), mapOf("name" to "john", "surname" to "doe")),
                ),
                1,
                0,
            ),
            newChangeEventMessage(
                RelationshipEvent(
                    "rel-element-id",
                    "REL",
                    Node("start-element-id", listOf("Person"), emptyMap()),
                    Node("end-element-id", listOf("Person"), emptyMap()),
                    emptyList(),
                    EntityOperation.CREATE,
                    null,
                    RelationshipState(emptyMap()),
                ),
                1,
                0,
            ),
            newChangeEventMessage(
                RelationshipEvent(
                    "rel-element-id",
                    "REL",
                    Node(
                        "start-element-id",
                        listOf("Person"),
                        mapOf("Person" to listOf(mapOf("id" to 5L))),
                    ),
                    Node("end-element-id", listOf("Person"), emptyMap()),
                    emptyList(),
                    EntityOperation.CREATE,
                    null,
                    RelationshipState(emptyMap()),
                ),
                1,
                0,
            ),
            newChangeEventMessage(
                RelationshipEvent(
                    "rel-element-id",
                    "REL",
                    Node("start-element-id", listOf("Person"), emptyMap()),
                    Node(
                        "end-element-id",
                        listOf("Person"),
                        mapOf("Person" to listOf(mapOf("id" to 5L))),
                    ),
                    emptyList(),
                    EntityOperation.CREATE,
                    null,
                    RelationshipState(emptyMap()),
                ),
                1,
                0,
            ),
        )
        .forEach {
          shouldThrow<InvalidDataException> {
                val handler =
                    CdcSchemaHandler(
                        "my-topic",
                        Renderer.getRenderer(Configuration.defaultConfig()),
                    )

                handler.handle(listOf(it))
              }
              .also {
                it shouldHaveMessage
                    Regex(
                        "^schema strategy requires at least one node key with valid properties on node aliased '(n|start|end)'.$",
                    )
              }
        }
  }

  @Test
  fun `should generate correct statement for node creation events`() {
    val sinkMessage =
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
                        "dob" to LocalDate.of(1990, 1, 1),
                    ),
                ),
            ),
            1,
            0,
        )
    verify(
        listOf(sinkMessage),
        listOf(
            listOf(
                ChangeQuery(
                    1,
                    0,
                    listOf(sinkMessage),
                    Query(
                        "CREATE (n:`Person` {name: ${'$'}nName, surname: ${'$'}nSurname}) SET n = ${'$'}nProps",
                        mapOf(
                            "nName" to "john",
                            "nSurname" to "doe",
                            "nProps" to
                                mapOf(
                                    "name" to "john",
                                    "surname" to "doe",
                                    "dob" to LocalDate.of(1990, 1, 1),
                                ),
                        ),
                    ),
                ),
            ),
        ),
    )

    val sinkMessage1 =
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
                        "dob" to LocalDate.of(1990, 1, 1),
                    ),
                ),
            ),
            1,
            0,
        )
    verify(
        listOf(sinkMessage1),
        listOf(
            listOf(
                ChangeQuery(
                    1,
                    0,
                    listOf(sinkMessage1),
                    Query(
                        "CREATE (n:`Person` {name: ${'$'}nName, surname: ${'$'}nSurname}) SET n = ${'$'}nProps SET n:`Employee`",
                        mapOf(
                            "nName" to "john",
                            "nSurname" to "doe",
                            "nProps" to
                                mapOf(
                                    "name" to "john",
                                    "surname" to "doe",
                                    "dob" to LocalDate.of(1990, 1, 1),
                                ),
                        ),
                    ),
                ),
            ),
        ),
    )
  }

  @Test
  fun `should generate correct statement for node update events`() {
    val sinkMessage =
        newChangeEventMessage(
            NodeEvent(
                "node-element-id",
                EntityOperation.UPDATE,
                listOf("Person", "Employee"),
                mapOf("Person" to listOf(mapOf("name" to "john", "surname" to "doe"))),
                NodeState(listOf("Person", "Employee"), mapOf("name" to "joe", "surname" to "doe")),
                NodeState(
                    listOf("Person", "Employee"),
                    mapOf(
                        "name" to "john",
                        "surname" to "doe",
                        "dob" to LocalDate.of(1990, 1, 1),
                    ),
                ),
            ),
            1,
            0,
        )
    verify(
        listOf(sinkMessage),
        listOf(
            listOf(
                ChangeQuery(
                    1,
                    0,
                    listOf(sinkMessage),
                    Query(
                        "MATCH (n:`Person` {name: ${'$'}nName, surname: ${'$'}nSurname}) SET n += ${'$'}nProps",
                        mapOf(
                            "nName" to "john",
                            "nSurname" to "doe",
                            "nProps" to mapOf("name" to "john", "dob" to LocalDate.of(1990, 1, 1)),
                        ),
                    ),
                ),
            ),
        ),
    )

    val sinkMessage1 =
        newChangeEventMessage(
            NodeEvent(
                "node-element-id",
                EntityOperation.UPDATE,
                listOf("Person", "Employee"),
                mapOf("Person" to listOf(mapOf("name" to "john", "surname" to "doe"))),
                NodeState(
                    listOf("Person", "Employee"),
                    mapOf("name" to "joe", "surname" to "doe", "married" to true),
                ),
                NodeState(
                    listOf("Person", "Manager"),
                    mapOf(
                        "name" to "john",
                        "surname" to "doe",
                        "dob" to LocalDate.of(1990, 1, 1),
                    ),
                ),
            ),
            1,
            0,
        )
    verify(
        listOf(sinkMessage1),
        listOf(
            listOf(
                ChangeQuery(
                    1,
                    0,
                    listOf(sinkMessage1),
                    Query(
                        "MATCH (n:`Person` {name: ${'$'}nName, surname: ${'$'}nSurname}) SET n += ${'$'}nProps SET n:`Manager` REMOVE n:`Employee`",
                        mapOf(
                            "nName" to "john",
                            "nSurname" to "doe",
                            "nProps" to
                                mapOf(
                                    "name" to "john",
                                    "dob" to LocalDate.of(1990, 1, 1),
                                    "married" to null,
                                ),
                        ),
                    ),
                ),
            ),
        ),
    )

    val sinkMessage2 =
        newChangeEventMessage(
            NodeEvent(
                "node-element-id",
                EntityOperation.UPDATE,
                listOf("Person", "Employee"),
                mapOf(
                    "Person" to listOf(mapOf("name" to "john", "surname" to "doe")),
                    "Employee" to listOf(mapOf("id" to 5000L)),
                ),
                NodeState(
                    listOf("Person", "Employee"),
                    mapOf("name" to "joe", "surname" to "doe", "married" to true),
                ),
                NodeState(
                    listOf("Person", "Manager"),
                    mapOf(
                        "name" to "john",
                        "surname" to "doe",
                        "dob" to LocalDate.of(1990, 1, 1),
                    ),
                ),
            ),
            1,
            0,
        )
    verify(
        listOf(sinkMessage2),
        listOf(
            listOf(
                ChangeQuery(
                    1,
                    0,
                    listOf(sinkMessage2),
                    Query(
                        "MATCH (n:`Person`:`Employee` {name: ${'$'}nName, surname: ${'$'}nSurname, id: ${'$'}nId}) SET n += ${'$'}nProps SET n:`Manager` REMOVE n:`Employee`",
                        mapOf(
                            "nId" to 5000L,
                            "nName" to "john",
                            "nSurname" to "doe",
                            "nProps" to
                                mapOf(
                                    "name" to "john",
                                    "dob" to LocalDate.of(1990, 1, 1),
                                    "married" to null,
                                ),
                        ),
                    ),
                ),
            ),
        ),
    )
  }

  @Test
  fun `should generate correct statement for node deletion events`() {
    val sinkMessage =
        newChangeEventMessage(
            NodeEvent(
                "node-element-id",
                EntityOperation.DELETE,
                listOf("Person"),
                mapOf("Person" to listOf(mapOf("name" to "joe", "surname" to "doe"))),
                NodeState(
                    emptyList(),
                    mapOf("name" to "joe", "surname" to "doe", "dob" to LocalDate.of(2000, 1, 1)),
                ),
                null,
            ),
            1,
            0,
        )
    verify(
        listOf(sinkMessage),
        listOf(
            listOf(
                ChangeQuery(
                    1,
                    0,
                    listOf(sinkMessage),
                    Query(
                        "MATCH (n:`Person` {name: ${'$'}nName, surname: ${'$'}nSurname}) DETACH DELETE n",
                        mapOf("nName" to "joe", "nSurname" to "doe"),
                    ),
                ),
            ),
        ),
    )
  }

  @Test
  fun `should generate correct statement for relationship creation events`() {
    val sinkMessage =
        newChangeEventMessage(
            RelationshipEvent(
                "rel-element-id",
                "KNOWS",
                Node(
                    "start-element-id",
                    listOf("Person"),
                    mapOf("Person" to listOf(mapOf("id" to 1L))),
                ),
                Node(
                    "end-element-id",
                    listOf("Person"),
                    mapOf("Person" to listOf(mapOf("id" to 2L))),
                ),
                emptyList(),
                EntityOperation.CREATE,
                null,
                RelationshipState(mapOf("since" to LocalDate.of(2000, 1, 1))),
            ),
            1,
            0,
        )
    verify(
        listOf(sinkMessage),
        listOf(
            listOf(
                ChangeQuery(
                    1,
                    0,
                    listOf(sinkMessage),
                    Query(
                        "MATCH (start:`Person` {id: ${'$'}startId}) " +
                            "MATCH (end:`Person` {id: ${'$'}endId}) " +
                            "CREATE (start)-[r:`KNOWS` {}]->(end) " +
                            "SET r = ${'$'}rProps",
                        mapOf(
                            "startId" to 1L,
                            "endId" to 2L,
                            "rProps" to mapOf("since" to LocalDate.of(2000, 1, 1)),
                        ),
                    ),
                ),
            ),
        ),
    )
  }

  @Test
  fun `should generate correct statement for relationship update events`() {
    val sinkMessage =
        newChangeEventMessage(
            RelationshipEvent(
                "rel-element-id",
                "KNOWS",
                Node(
                    "start-element-id",
                    listOf("Person", "Employee"),
                    mapOf(
                        "Person" to listOf(mapOf("id" to 1L)),
                        "Employee" to listOf(mapOf("contractId" to 5000L)),
                    ),
                ),
                Node(
                    "end-element-id",
                    listOf("Person", "Employee"),
                    mapOf(
                        "Person" to listOf(mapOf("id" to 2L)),
                        "Employee" to listOf(mapOf("contractId" to 5001L)),
                    ),
                ),
                emptyList(),
                EntityOperation.UPDATE,
                RelationshipState(mapOf("since" to LocalDate.of(2000, 1, 1))),
                RelationshipState(mapOf("since" to LocalDate.of(1999, 1, 1))),
            ),
            1,
            0,
        )
    verify(
        listOf(sinkMessage),
        listOf(
            listOf(
                ChangeQuery(
                    1,
                    0,
                    listOf(sinkMessage),
                    Query(
                        "MATCH (start:`Person`:`Employee` {id: ${'$'}startId, contractId: ${'$'}startContractId}) " +
                            "MATCH (end:`Person`:`Employee` {id: ${'$'}endId, contractId: ${'$'}endContractId}) " +
                            "MATCH (start)-[r:`KNOWS` {}]->(end) " +
                            "SET r += ${'$'}rProps",
                        mapOf(
                            "startId" to 1L,
                            "startContractId" to 5000L,
                            "endId" to 2L,
                            "endContractId" to 5001L,
                            "rProps" to mapOf("since" to LocalDate.of(1999, 1, 1)),
                        ),
                    ),
                ),
            ),
        ),
    )

    val sinkMessage1 =
        newChangeEventMessage(
            RelationshipEvent(
                "rel-element-id",
                "KNOWS",
                Node(
                    "start-element-id",
                    listOf("Person", "Employee"),
                    mapOf(
                        "Person" to listOf(mapOf("id" to 1L)),
                        "Employee" to listOf(mapOf("contractId" to 5000L)),
                    ),
                ),
                Node(
                    "end-element-id",
                    listOf("Person"),
                    mapOf("Person" to listOf(mapOf("id" to 2L))),
                ),
                listOf(mapOf("id" to 1001L)),
                EntityOperation.UPDATE,
                RelationshipState(mapOf("name" to "john", "surname" to "doe")),
                RelationshipState(mapOf("name" to "joe", "surname" to "doe")),
            ),
            1,
            0,
        )
    verify(
        listOf(sinkMessage1),
        listOf(
            listOf(
                ChangeQuery(
                    1,
                    0,
                    listOf(sinkMessage1),
                    Query(
                        "MATCH (start)-[r:`KNOWS` {id: ${'$'}rId}]->(end) " +
                            "SET r += ${'$'}rProps",
                        mapOf(
                            "rId" to 1001L,
                            "rProps" to mapOf("name" to "joe"),
                        ),
                    ),
                ),
            ),
        ),
    )
  }

  @Test
  fun `should generate correct statement for relationship deletion events`() {
    val sinkMessage =
        newChangeEventMessage(
            RelationshipEvent(
                "rel-element-id",
                "KNOWS",
                Node(
                    "start-element-id",
                    listOf("Person"),
                    mapOf("Person" to listOf(mapOf("id" to 1L))),
                ),
                Node(
                    "end-element-id",
                    listOf("Person"),
                    mapOf("Person" to listOf(mapOf("id" to 2L))),
                ),
                emptyList(),
                EntityOperation.DELETE,
                RelationshipState(mapOf("name" to "john", "surname" to "doe")),
                null,
            ),
            1,
            0,
        )
    verify(
        listOf(sinkMessage),
        listOf(
            listOf(
                ChangeQuery(
                    1,
                    0,
                    listOf(sinkMessage),
                    Query(
                        "MATCH (start:`Person` {id: ${'$'}startId}) " +
                            "MATCH (end:`Person` {id: ${'$'}endId}) " +
                            "MATCH (start)-[r:`KNOWS` {}]->(end) " +
                            "DELETE r",
                        mapOf("startId" to 1L, "endId" to 2L),
                    ),
                ),
            ),
        ),
    )

    val sinkMessage1 =
        newChangeEventMessage(
            RelationshipEvent(
                "rel-element-id",
                "KNOWS",
                Node(
                    "start-element-id",
                    listOf("Person", "Employee"),
                    mapOf(
                        "Person" to listOf(mapOf("id" to 1L)),
                        "Employee" to listOf(mapOf("contractId" to 5000L)),
                    ),
                ),
                Node(
                    "end-element-id",
                    listOf("Person"),
                    mapOf("Person" to listOf(mapOf("id" to 2L))),
                ),
                listOf(mapOf("id" to 1001L)),
                EntityOperation.DELETE,
                RelationshipState(mapOf("name" to "john", "surname" to "doe")),
                null,
            ),
            1,
            0,
        )
    verify(
        listOf(sinkMessage1),
        listOf(
            listOf(
                ChangeQuery(
                    1,
                    0,
                    listOf(sinkMessage1),
                    Query(
                        "MATCH (start)-[r:`KNOWS` {id: ${'$'}rId}]->(end) " + "DELETE r",
                        mapOf(
                            "rId" to 1001L,
                        ),
                    ),
                ),
            ),
        ),
    )
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
                newChangeEventMessage(randomChangeEvent(), 2, 0),
            ),
        )

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
                      },
                  )
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
                      },
                  )
            },
            { third ->
              third
                  .shouldHaveSize(1)
                  .shouldMatchInOrder(
                      { q1 ->
                        q1.txId shouldBe 2
                        q1.seq shouldBe 0
                      },
                  )
            },
        )
  }

  @Test
  fun `should fail on null 'after' field with node create operation`() {
    val handler = CdcSchemaHandler("my-topic", Renderer.getRenderer(Configuration.defaultConfig()))

    val nodeChangeEventMessage =
        newChangeEventMessage(
            NodeEvent(
                "person1",
                EntityOperation.CREATE,
                listOf("Person"),
                mapOf("Person" to listOf(mapOf("id" to 1L))),
                null,
                null,
            ),
            1,
            0,
        )

    assertThrows<InvalidDataException> {
      handler.handle(listOf(nodeChangeEventMessage))
    } shouldHaveMessage "create operation requires 'after' field in the event object"
  }

  @Test
  fun `should fail on null 'after' field with relationship create operation`() {
    val handler = CdcSchemaHandler("my-topic", Renderer.getRenderer(Configuration.defaultConfig()))

    val relationshipChangeEventMessage =
        newChangeEventMessage(
            RelationshipEvent(
                "rel-element-id",
                "REL",
                Node(
                    "start-element-id",
                    listOf("Person"),
                    mapOf("Person" to listOf(mapOf("id" to 1L))),
                ),
                Node(
                    "end-element-id",
                    listOf("Person"),
                    mapOf("Person" to listOf(mapOf("id" to 2L))),
                ),
                emptyList(),
                EntityOperation.CREATE,
                null,
                null,
            ),
            1,
            0,
        )

    assertThrows<InvalidDataException> {
      handler.handle(listOf(relationshipChangeEventMessage))
    } shouldHaveMessage "create operation requires 'after' field in the event object"
  }

  @Test
  fun `should fail on null 'before' field with node update operation`() {
    val handler = CdcSchemaHandler("my-topic", Renderer.getRenderer(Configuration.defaultConfig()))

    val nodeChangeEventMessage =
        newChangeEventMessage(
            NodeEvent(
                "person1",
                EntityOperation.UPDATE,
                listOf("Person"),
                mapOf("Person" to listOf(mapOf("id" to 1L))),
                null,
                NodeState(
                    listOf("Person", "Employee"),
                    mapOf("name" to "joe", "surname" to "doe"),
                ),
            ),
            1,
            0,
        )

    assertThrows<InvalidDataException> {
      handler.handle(listOf(nodeChangeEventMessage))
    } shouldHaveMessage "update operation requires 'before' field in the event object"
  }

  @Test
  fun `should fail on null 'before' field with relationship update operation`() {
    val handler = CdcSchemaHandler("my-topic", Renderer.getRenderer(Configuration.defaultConfig()))

    val relationshipChangeEventMessage =
        newChangeEventMessage(
            RelationshipEvent(
                "rel-element-id",
                "REL",
                Node(
                    "start-element-id",
                    listOf("Person"),
                    mapOf("Person" to listOf(mapOf("id" to 1L))),
                ),
                Node(
                    "end-element-id",
                    listOf("Person"),
                    mapOf("Person" to listOf(mapOf("id" to 2L))),
                ),
                emptyList(),
                EntityOperation.UPDATE,
                null,
                RelationshipState(mapOf("name" to "john", "surname" to "doe")),
            ),
            1,
            0,
        )

    assertThrows<InvalidDataException> {
      handler.handle(listOf(relationshipChangeEventMessage))
    } shouldHaveMessage "update operation requires 'before' field in the event object"
  }

  @Test
  fun `should fail on null 'after' field with node update operation`() {
    val handler = CdcSchemaHandler("my-topic", Renderer.getRenderer(Configuration.defaultConfig()))

    val nodeChangeEventMessage =
        newChangeEventMessage(
            NodeEvent(
                "person1",
                EntityOperation.UPDATE,
                listOf("Person"),
                mapOf("Person" to listOf(mapOf("id" to 1L))),
                NodeState(listOf("Person", "Employee"), mapOf("name" to "joe", "surname" to "doe")),
                null,
            ),
            1,
            0,
        )

    assertThrows<InvalidDataException> {
      handler.handle(listOf(nodeChangeEventMessage))
    } shouldHaveMessage "update operation requires 'after' field in the event object"
  }

  @Test
  fun `should fail on null 'after' field with relationship update operation`() {
    val handler = CdcSchemaHandler("my-topic", Renderer.getRenderer(Configuration.defaultConfig()))

    val relationshipChangeEventMessage =
        newChangeEventMessage(
            RelationshipEvent(
                "rel-element-id",
                "REL",
                Node(
                    "start-element-id",
                    listOf("Person"),
                    mapOf("Person" to listOf(mapOf("id" to 1L))),
                ),
                Node(
                    "end-element-id",
                    listOf("Person"),
                    mapOf("Person" to listOf(mapOf("id" to 2L))),
                ),
                emptyList(),
                EntityOperation.UPDATE,
                RelationshipState(mapOf("name" to "john", "surname" to "doe")),
                null,
            ),
            1,
            0,
        )

    assertThrows<InvalidDataException> {
      handler.handle(listOf(relationshipChangeEventMessage))
    } shouldHaveMessage "update operation requires 'after' field in the event object"
  }

  @Test
  fun `should generate correct statement for node deletion events containing null keys property values`() {
    val sinkMessage =
        newChangeEventMessage(
            NodeEvent(
                "node-element-id",
                EntityOperation.DELETE,
                listOf("Person"),
                mapOf("Person" to listOf(mapOf("name" to "john"), mapOf("invalid" to null))),
                NodeState(emptyList(), mapOf("name" to "john")),
                null,
            ),
            1,
            0,
        )
    verify(
        listOf(sinkMessage),
        listOf(
            listOf(
                ChangeQuery(
                    1,
                    0,
                    listOf(sinkMessage),
                    Query(
                        "MATCH (n:`Person` {name: ${'$'}nName}) DETACH DELETE n",
                        mapOf("nName" to "john"),
                    ),
                ),
            ),
        ),
    )
  }

  @Test
  fun `should generate correct statement for node creation events containing null keys property values`() {
    val sinkMessage =
        newChangeEventMessage(
            event =
                NodeEvent(
                    "node-element-id",
                    EntityOperation.CREATE,
                    listOf("Person"),
                    mapOf("Person" to listOf(mapOf("name" to "john"), mapOf("invalid" to null))),
                    null,
                    NodeState(
                        listOf("Person"),
                        mapOf(
                            "name" to "john",
                        ),
                    ),
                ),
            txId = 1,
            seq = 0,
        )
    verify(
        listOf(sinkMessage),
        listOf(
            listOf(
                ChangeQuery(
                    1,
                    0,
                    listOf(sinkMessage),
                    Query(
                        "CREATE (n:`Person` {name: ${'$'}nName}) SET n = ${'$'}nProps",
                        mapOf(
                            "nName" to "john",
                            "nProps" to
                                mapOf(
                                    "name" to "john",
                                ),
                        ),
                    ),
                ),
            ),
        ),
    )
  }

  @Test
  fun `should fail when node keys is empty`() {
    // given a sink message which contains a key with no properties
    val sinkMessage =
        newChangeEventMessage(
            NodeEvent(
                "node-element-id",
                EntityOperation.DELETE,
                listOf("Person"),
                mapOf("Person" to emptyList()),
                NodeState(emptyList(), mapOf("name" to "john")),
                null,
            ),
            1,
            0,
        )

    assertInvalidDataException(sinkMessage, "n")
  }

  @Test
  fun `should fail when relationship start keys is empty for create even if relationship has keys`() {
    val sinkMessage =
        newChangeEventMessage(
            RelationshipEvent(
                "rel-element-id",
                "KNOWS",
                Node(
                    "start-element-id",
                    listOf("Person"),
                    emptyMap<String, List<Map<String, Any>>>(),
                ),
                Node(
                    "end-element-id",
                    listOf("Person"),
                    mapOf("Person" to listOf(mapOf("name" to "john"))),
                ),
                listOf(mapOf("id" to 1L)),
                EntityOperation.CREATE,
                null,
                RelationshipState(mapOf("id" to 1L, "since" to LocalDate.of(2000, 1, 1))),
            ),
            1,
            0,
        )

    assertInvalidDataException(sinkMessage, "start")
  }

  @Test
  fun `should fail when relationship end keys is empty for create even if relationship has keys`() {
    val sinkMessage =
        newChangeEventMessage(
            RelationshipEvent(
                "rel-element-id",
                "KNOWS",
                Node(
                    "start-element-id",
                    listOf("Person"),
                    mapOf("Person" to listOf(mapOf("name" to "john"))),
                ),
                Node(
                    "end-element-id",
                    listOf("Person"),
                    mapOf("Person" to emptyList()),
                ),
                listOf(mapOf("id" to 1L)),
                EntityOperation.CREATE,
                null,
                RelationshipState(mapOf("id" to 1L, "since" to LocalDate.of(2000, 1, 1))),
            ),
            1,
            0,
        )

    assertInvalidDataException(sinkMessage, "end")
  }

  @Test
  fun `should fail when relationship start keys is empty for create`() {
    val sinkMessage =
        newChangeEventMessage(
            RelationshipEvent(
                "rel-element-id",
                "KNOWS",
                Node(
                    "start-element-id",
                    listOf("Person"),
                    emptyMap<String, List<Map<String, Any>>>(),
                ),
                Node(
                    "end-element-id",
                    listOf("Person"),
                    mapOf("Person" to listOf(mapOf("name" to "john"))),
                ),
                emptyList(),
                EntityOperation.CREATE,
                null,
                RelationshipState(mapOf("since" to LocalDate.of(2000, 1, 1))),
            ),
            1,
            0,
        )

    assertInvalidDataException(sinkMessage, "start")
  }

  @Test
  fun `should fail when relationship start keys is effectively empty for create`() {
    val sinkMessage =
        newChangeEventMessage(
            RelationshipEvent(
                "rel-element-id",
                "KNOWS",
                Node(
                    "start-element-id",
                    listOf("Person"),
                    mapOf("Person" to emptyList<Map<String, Any>>()),
                ),
                Node(
                    "end-element-id",
                    listOf("Person"),
                    mapOf("Person" to listOf(mapOf("name" to "john"))),
                ),
                emptyList(),
                EntityOperation.CREATE,
                null,
                RelationshipState(mapOf("since" to LocalDate.of(2000, 1, 1))),
            ),
            1,
            0,
        )

    assertInvalidDataException(sinkMessage, "start")
  }

  @Test
  fun `should fail when relationship end keys is empty for create`() {
    val sinkMessage =
        newChangeEventMessage(
            RelationshipEvent(
                "rel-element-id",
                "KNOWS",
                Node(
                    "start-element-id",
                    listOf("Person"),
                    mapOf("Person" to listOf(mapOf("name" to "john"))),
                ),
                Node(
                    "end-element-id",
                    listOf("Person"),
                    emptyMap<String, List<Map<String, Any>>>(),
                ),
                emptyList(),
                EntityOperation.CREATE,
                null,
                RelationshipState(mapOf("since" to LocalDate.of(2000, 1, 1))),
            ),
            1,
            0,
        )

    assertInvalidDataException(sinkMessage, "end")
  }

  @Test
  fun `should fail when relationship end keys is effectively empty for create`() {
    val sinkMessage =
        newChangeEventMessage(
            RelationshipEvent(
                "rel-element-id",
                "KNOWS",
                Node(
                    "start-element-id",
                    listOf("Person"),
                    mapOf("Person" to listOf(mapOf("name" to "john"))),
                ),
                Node(
                    "end-element-id",
                    listOf("Person"),
                    mapOf("Person" to emptyList<Map<String, Any>>()),
                ),
                emptyList(),
                EntityOperation.CREATE,
                null,
                RelationshipState(mapOf("since" to LocalDate.of(2000, 1, 1))),
            ),
            1,
            0,
        )

    assertInvalidDataException(sinkMessage, "end")
  }

  @Test
  fun `should generate correct statement when relationship start keys is empty but relationship has its own keys for update`() {
    val sinkMessage =
        newChangeEventMessage(
            RelationshipEvent(
                "rel-element-id",
                "KNOWS",
                Node(
                    "start-element-id",
                    listOf("Person"),
                    emptyMap<String, List<Map<String, Any>>>(),
                ),
                Node(
                    "end-element-id",
                    listOf("Person"),
                    mapOf("Person" to listOf(mapOf("name" to "john"))),
                ),
                listOf(mapOf("id" to 1L)),
                EntityOperation.UPDATE,
                RelationshipState(mapOf("id" to 1L)),
                RelationshipState(mapOf("id" to 1L, "since" to LocalDate.of(2000, 1, 1))),
            ),
            1,
            0,
        )

    verify(
        listOf(sinkMessage),
        listOf(
            listOf(
                ChangeQuery(
                    1,
                    0,
                    listOf(sinkMessage),
                    Query(
                        "MATCH (start)-[r:`KNOWS` {id: ${'$'}rId}]->(end) " +
                            "SET r += ${'$'}rProps",
                        mapOf(
                            "rId" to 1L,
                            "rProps" to
                                mapOf(
                                    "since" to LocalDate.of(2000, 1, 1),
                                ),
                        ),
                    ),
                ),
            ),
        ),
    )
  }

  @Test
  fun `should generate correct statement when relationship end keys is empty but relationship has its own keys for update`() {
    val sinkMessage =
        newChangeEventMessage(
            RelationshipEvent(
                "rel-element-id",
                "KNOWS",
                Node(
                    "start-element-id",
                    listOf("Person"),
                    mapOf("Person" to listOf(mapOf("name" to "john"))),
                ),
                Node(
                    "end-element-id",
                    listOf("Person"),
                    emptyMap<String, List<Map<String, Any>>>(),
                ),
                listOf(mapOf("id" to 1L)),
                EntityOperation.UPDATE,
                RelationshipState(mapOf("id" to 1L)),
                RelationshipState(mapOf("id" to 1L, "since" to LocalDate.of(2000, 1, 1))),
            ),
            1,
            0,
        )

    verify(
        listOf(sinkMessage),
        listOf(
            listOf(
                ChangeQuery(
                    1,
                    0,
                    listOf(sinkMessage),
                    Query(
                        "MATCH (start)-[r:`KNOWS` {id: ${'$'}rId}]->(end) " +
                            "SET r += ${'$'}rProps",
                        mapOf(
                            "rId" to 1L,
                            "rProps" to
                                mapOf(
                                    "since" to LocalDate.of(2000, 1, 1),
                                ),
                        ),
                    ),
                ),
            ),
        ),
    )
  }

  @Test
  fun `should fail when relationship start keys is empty for update`() {
    val sinkMessage =
        newChangeEventMessage(
            RelationshipEvent(
                "rel-element-id",
                "KNOWS",
                Node(
                    "start-element-id",
                    listOf("Person"),
                    emptyMap<String, List<Map<String, Any>>>(),
                ),
                Node(
                    "end-element-id",
                    listOf("Person"),
                    mapOf("Person" to listOf(mapOf("name" to "john"))),
                ),
                emptyList(),
                EntityOperation.UPDATE,
                RelationshipState(emptyMap<String, Any>()),
                RelationshipState(mapOf("since" to LocalDate.of(2000, 1, 1))),
            ),
            1,
            0,
        )

    assertInvalidDataException(sinkMessage, "start")
  }

  @Test
  fun `should fail when relationship start keys is effectively empty for update`() {
    val sinkMessage =
        newChangeEventMessage(
            RelationshipEvent(
                "rel-element-id",
                "KNOWS",
                Node(
                    "start-element-id",
                    listOf("Person"),
                    mapOf("Person" to emptyList<Map<String, Any>>()),
                ),
                Node(
                    "end-element-id",
                    listOf("Person"),
                    mapOf("Person" to listOf(mapOf("name" to "john"))),
                ),
                emptyList(),
                EntityOperation.UPDATE,
                RelationshipState(emptyMap<String, Any>()),
                RelationshipState(mapOf("since" to LocalDate.of(2000, 1, 1))),
            ),
            1,
            0,
        )

    assertInvalidDataException(sinkMessage, "start")
  }

  @Test
  fun `should fail when relationship end keys is empty for update`() {
    val sinkMessage =
        newChangeEventMessage(
            RelationshipEvent(
                "rel-element-id",
                "KNOWS",
                Node(
                    "start-element-id",
                    listOf("Person"),
                    mapOf("Person" to listOf(mapOf("name" to "john"))),
                ),
                Node(
                    "end-element-id",
                    listOf("Person"),
                    emptyMap<String, List<Map<String, Any>>>(),
                ),
                emptyList(),
                EntityOperation.UPDATE,
                RelationshipState(emptyMap<String, Any>()),
                RelationshipState(mapOf("since" to LocalDate.of(2000, 1, 1))),
            ),
            1,
            0,
        )

    assertInvalidDataException(sinkMessage, "end")
  }

  @Test
  fun `should fail when relationship end keys is effectively empty for update`() {
    val sinkMessage =
        newChangeEventMessage(
            RelationshipEvent(
                "rel-element-id",
                "KNOWS",
                Node(
                    "start-element-id",
                    listOf("Person"),
                    mapOf("Person" to listOf(mapOf("name" to "john"))),
                ),
                Node(
                    "end-element-id",
                    listOf("Person"),
                    mapOf("Person" to emptyList<Map<String, Any>>()),
                ),
                emptyList(),
                EntityOperation.UPDATE,
                RelationshipState(emptyMap<String, Any>()),
                RelationshipState(mapOf("since" to LocalDate.of(2000, 1, 1))),
            ),
            1,
            0,
        )

    assertInvalidDataException(sinkMessage, "end")
  }

  @Test
  fun `should generate correct statement when relationship start keys is empty but relationship has its own keys for deletion`() {
    val sinkMessage =
        newChangeEventMessage(
            RelationshipEvent(
                "rel-element-id",
                "KNOWS",
                Node(
                    "start-element-id",
                    listOf("Person"),
                    emptyMap<String, List<Map<String, Any>>>(),
                ),
                Node(
                    "end-element-id",
                    listOf("Person"),
                    mapOf("Person" to listOf(mapOf("name" to "john"))),
                ),
                listOf(mapOf("id" to 1L)),
                EntityOperation.DELETE,
                RelationshipState(mapOf("id" to 1L)),
                null,
            ),
            1,
            0,
        )

    verify(
        listOf(sinkMessage),
        listOf(
            listOf(
                ChangeQuery(
                    1,
                    0,
                    listOf(sinkMessage),
                    Query(
                        "MATCH (start)-[r:`KNOWS` {id: ${'$'}rId}]->(end) " + "DELETE r",
                        mapOf(
                            "rId" to 1L,
                        ),
                    ),
                ),
            ),
        ),
    )
  }

  @Test
  fun `should generate correct statement when relationship end keys is empty but relationship has its own keys for deletion`() {
    val sinkMessage =
        newChangeEventMessage(
            RelationshipEvent(
                "rel-element-id",
                "KNOWS",
                Node(
                    "start-element-id",
                    listOf("Person"),
                    mapOf("Person" to listOf(mapOf("name" to "john"))),
                ),
                Node(
                    "end-element-id",
                    listOf("Person"),
                    emptyMap<String, List<Map<String, Any>>>(),
                ),
                listOf(mapOf("id" to 1L)),
                EntityOperation.DELETE,
                RelationshipState(mapOf("id" to 1L)),
                null,
            ),
            1,
            0,
        )

    verify(
        listOf(sinkMessage),
        listOf(
            listOf(
                ChangeQuery(
                    1,
                    0,
                    listOf(sinkMessage),
                    Query(
                        "MATCH (start)-[r:`KNOWS` {id: ${'$'}rId}]->(end) " + "DELETE r",
                        mapOf(
                            "rId" to 1L,
                        ),
                    ),
                ),
            ),
        ),
    )
  }

  @Test
  fun `should fail when relationship start keys is empty for deletion`() {
    val sinkMessage =
        newChangeEventMessage(
            RelationshipEvent(
                "rel-element-id",
                "KNOWS",
                Node(
                    "start-element-id",
                    listOf("Person"),
                    emptyMap<String, List<Map<String, Any>>>(),
                ),
                Node(
                    "end-element-id",
                    listOf("Person"),
                    mapOf("Person" to listOf(mapOf("name" to "john"))),
                ),
                emptyList(),
                EntityOperation.DELETE,
                RelationshipState(emptyMap<String, Any>()),
                null,
            ),
            1,
            0,
        )

    assertInvalidDataException(sinkMessage, "start")
  }

  @Test
  fun `should fail when relationship start keys is effectively empty for deletion`() {
    val sinkMessage =
        newChangeEventMessage(
            RelationshipEvent(
                "rel-element-id",
                "KNOWS",
                Node(
                    "start-element-id",
                    listOf("Person"),
                    mapOf("Person" to emptyList<Map<String, Any>>()),
                ),
                Node(
                    "end-element-id",
                    listOf("Person"),
                    mapOf("Person" to listOf(mapOf("name" to "john"))),
                ),
                emptyList(),
                EntityOperation.DELETE,
                RelationshipState(emptyMap<String, Any>()),
                null,
            ),
            1,
            0,
        )

    assertInvalidDataException(sinkMessage, "start")
  }

  @Test
  fun `should fail when relationship end keys is empty for deletion`() {
    val sinkMessage =
        newChangeEventMessage(
            RelationshipEvent(
                "rel-element-id",
                "KNOWS",
                Node(
                    "start-element-id",
                    listOf("Person"),
                    mapOf("Person" to listOf(mapOf("name" to "john"))),
                ),
                Node(
                    "end-element-id",
                    listOf("Person"),
                    emptyMap<String, List<Map<String, Any>>>(),
                ),
                emptyList(),
                EntityOperation.DELETE,
                RelationshipState(emptyMap<String, Any>()),
                null,
            ),
            1,
            0,
        )

    assertInvalidDataException(sinkMessage, "end")
  }

  @Test
  fun `should fail when relationship end keys is effectively empty for deletion`() {
    val sinkMessage =
        newChangeEventMessage(
            RelationshipEvent(
                "rel-element-id",
                "KNOWS",
                Node(
                    "start-element-id",
                    listOf("Person"),
                    mapOf("Person" to listOf(mapOf("name" to "john"))),
                ),
                Node(
                    "end-element-id",
                    listOf("Person"),
                    mapOf("Person" to emptyList<Map<String, Any>>()),
                ),
                emptyList(),
                EntityOperation.DELETE,
                RelationshipState(emptyMap<String, Any>()),
                null,
            ),
            1,
            0,
        )

    assertInvalidDataException(sinkMessage, "end")
  }

  @Test
  fun `should fail when streams node keys is empty`() {
    val streamsMessage =
        """
      {
        "meta": {
          "timestamp": 1728643218066,
          "username": "neo4j",
          "txId": 18,
          "txEventId": 0,
          "txEventsCount": 1,
          "operation": "deleted",
          "source": {
            "hostname": "neo4j03"
          }
        },
        "payload": {
          "id": "0",
          "before": {
            "properties": {
              "first_name": "Ali",
              "last_name": "Ince",
              "id": 1
            },
            "labels": [
              "Person"
            ]
          },
          "after": null,
          "type": "node"
        },
        "schema": {
          "properties": {
            "first_name": "String",
            "last_name": "String",
            "id": "Long"
          },
          "constraints": []
        }
      }
    """
            .trimIndent()
    val event: StreamsTransactionEvent = JSONUtils.asStreamsTransactionEvent(streamsMessage)
    val sinkMessage = newChangeEventMessage(event.toChangeEvent().event, 1, 0)

    assertInvalidDataException(sinkMessage, "n")
  }

  @Test
  fun `should fail when streams relationship start node keys is empty`() {
    val streamsMessage =
        """
      {
        "meta": {
          "timestamp": 1728641686524,
          "username": "neo4j",
          "txId": 17,
          "txEventId": 0,
          "txEventsCount": 1,
          "operation": "deleted",
          "source": {
            "hostname": "neo4j03"
          }
        },
        "payload": {
          "id": "0",
          "start": {
            "id": "0",
            "labels": [
              "Person"
            ],
            "ids": {}
          },
          "end": {
            "id": "2",
            "labels": [
              "Company"
            ],
            "ids": {
              "name": "Neo4j"
            }
          },
          "before": {
            "properties": {}
          },
          "after": null,
          "label": "WORKS_FOR",
          "type": "relationship"
        },
        "schema": {
          "properties": {},
          "constraints": []
        }
      }
    """
            .trimIndent()
    val event: StreamsTransactionEvent = JSONUtils.asStreamsTransactionEvent(streamsMessage)
    val sinkMessage = newChangeEventMessage(event.toChangeEvent().event, 1, 0)

    assertInvalidDataException(sinkMessage, "start")
  }

  @Test
  fun `should fail when streams relationship end node keys is empty`() {
    val streamsMessage =
        """
      {
        "meta": {
          "timestamp": 1728641686524,
          "username": "neo4j",
          "txId": 17,
          "txEventId": 0,
          "txEventsCount": 1,
          "operation": "deleted",
          "source": {
            "hostname": "neo4j03"
          }
        },
        "payload": {
          "id": "0",
          "start": {
            "id": "0",
            "labels": [
              "Person"
            ],
            "ids": {
              "name": "john"
            }
          },
          "end": {
            "id": "2",
            "labels": [
              "Company"
            ],
            "ids": {}
          },
          "before": {
            "properties": {}
          },
          "after": null,
          "label": "WORKS_FOR",
          "type": "relationship"
        },
        "schema": {
          "properties": {},
          "constraints": []
        }
      }
    """
            .trimIndent()
    val event: StreamsTransactionEvent = JSONUtils.asStreamsTransactionEvent(streamsMessage)
    val sinkMessage = newChangeEventMessage(event.toChangeEvent().event, 1, 0)

    assertInvalidDataException(sinkMessage, "end")
  }

  private fun assertInvalidDataException(sinkMessage: SinkMessage, alias: String) {
    shouldThrow<InvalidDataException> {
          val handler =
              CdcSchemaHandler("my-topic", Renderer.getRenderer(Configuration.defaultConfig()))

          handler.handle(listOf(sinkMessage))
        }
        .also {
          it shouldHaveMessage
              Regex(
                  "^schema strategy requires at least one node key with valid properties on node aliased '$alias'.$",
              )
        }
  }

  private fun verify(messages: Iterable<SinkMessage>, expected: Iterable<Iterable<ChangeQuery>>) {
    val handler = CdcSchemaHandler("my-topic", Renderer.getRenderer(Configuration.defaultConfig()))

    val result = handler.handle(messages)

    result shouldBe expected
  }
}
