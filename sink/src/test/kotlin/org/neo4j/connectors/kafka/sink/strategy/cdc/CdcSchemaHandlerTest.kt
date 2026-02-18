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

import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.collections.shouldMatchInOrder
import io.kotest.matchers.shouldBe
import java.time.LocalDate
import org.junit.jupiter.api.Test
import org.neo4j.caniuse.Neo4j
import org.neo4j.caniuse.Neo4jDeploymentType
import org.neo4j.caniuse.Neo4jEdition
import org.neo4j.caniuse.Neo4jVersion
import org.neo4j.cdc.client.model.EntityOperation
import org.neo4j.cdc.client.model.Node
import org.neo4j.cdc.client.model.NodeEvent
import org.neo4j.cdc.client.model.NodeState
import org.neo4j.cdc.client.model.RelationshipEvent
import org.neo4j.cdc.client.model.RelationshipState
import org.neo4j.connectors.kafka.sink.ChangeQuery
import org.neo4j.connectors.kafka.sink.SinkMessage
import org.neo4j.connectors.kafka.sink.SinkStrategy
import org.neo4j.connectors.kafka.sink.strategy.TestUtils.newChangeEventMessage
import org.neo4j.connectors.kafka.sink.strategy.TestUtils.randomChangeEvent
import org.neo4j.driver.Query

class CdcSchemaHandlerTest {
  companion object {
    private val NEO4J_2026_1 =
        Neo4j(Neo4jVersion(2026, 1), Neo4jEdition.ENTERPRISE, Neo4jDeploymentType.SELF_MANAGED)
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
                    mapOf("name" to "john", "surname" to "doe", "dob" to LocalDate.of(1990, 1, 1)),
                ),
            ),
            1,
            0,
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
                        "WITH ${'$'}e AS _e MERGE (n:${'$'}(_e.matchLabels) {`name`: _e.matchProperties.`name`, `surname`: _e.matchProperties.`surname`}) SET n += _e.setProperties SET n:$(_e.addLabels) REMOVE n:$(_e.removeLabels)",
                        mapOf(
                            "e" to
                                mapOf(
                                    "matchLabels" to listOf("Person"),
                                    "matchProperties" to
                                        mapOf("name" to "john", "surname" to "doe"),
                                    "setProperties" to
                                        mapOf(
                                            "name" to "john",
                                            "surname" to "doe",
                                            "dob" to LocalDate.of(1990, 1, 1),
                                        ),
                                    "addLabels" to emptyList<String>(),
                                    "removeLabels" to emptyList<String>(),
                                )
                        ),
                    ),
                )
            )
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
                    mapOf("name" to "john", "surname" to "doe", "dob" to LocalDate.of(1990, 1, 1)),
                ),
            ),
            1,
            0,
            1,
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
                        "WITH ${'$'}e AS _e MERGE (n:${'$'}(_e.matchLabels) {`name`: _e.matchProperties.`name`, `surname`: _e.matchProperties.`surname`}) SET n += _e.setProperties SET n:$(_e.addLabels) REMOVE n:$(_e.removeLabels)",
                        mapOf(
                            "e" to
                                mapOf(
                                    "matchLabels" to listOf("Person"),
                                    "matchProperties" to
                                        mapOf("name" to "john", "surname" to "doe"),
                                    "setProperties" to
                                        mapOf(
                                            "name" to "john",
                                            "surname" to "doe",
                                            "dob" to LocalDate.of(1990, 1, 1),
                                        ),
                                    "addLabels" to listOf("Employee"),
                                    "removeLabels" to emptyList<String>(),
                                )
                        ),
                    ),
                )
            )
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
                    mapOf("name" to "john", "surname" to "doe", "dob" to LocalDate.of(1990, 1, 1)),
                ),
            ),
            1,
            0,
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
                        "WITH ${'$'}e AS _e MERGE (n:${'$'}(_e.matchLabels) {`name`: _e.matchProperties.`name`, `surname`: _e.matchProperties.`surname`}) SET n += _e.setProperties SET n:$(_e.addLabels) REMOVE n:$(_e.removeLabels)",
                        mapOf(
                            "e" to
                                mapOf(
                                    "matchLabels" to listOf("Person"),
                                    "matchProperties" to
                                        mapOf("name" to "john", "surname" to "doe"),
                                    "setProperties" to
                                        mapOf("name" to "john", "dob" to LocalDate.of(1990, 1, 1)),
                                    "addLabels" to emptyList<String>(),
                                    "removeLabels" to emptyList<String>(),
                                )
                        ),
                    ),
                )
            )
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
                    mapOf("name" to "john", "surname" to "doe", "dob" to LocalDate.of(1990, 1, 1)),
                ),
            ),
            1,
            0,
            1,
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
                        "WITH ${'$'}e AS _e MERGE (n:${'$'}(_e.matchLabels) {`name`: _e.matchProperties.`name`, `surname`: _e.matchProperties.`surname`}) SET n += _e.setProperties SET n:$(_e.addLabels) REMOVE n:$(_e.removeLabels)",
                        mapOf(
                            "e" to
                                mapOf(
                                    "matchLabels" to listOf("Person"),
                                    "matchProperties" to
                                        mapOf("name" to "john", "surname" to "doe"),
                                    "setProperties" to
                                        mapOf(
                                            "name" to "john",
                                            "dob" to LocalDate.of(1990, 1, 1),
                                            "married" to null,
                                        ),
                                    "addLabels" to listOf("Manager"),
                                    "removeLabels" to listOf("Employee"),
                                )
                        ),
                    ),
                )
            )
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
                    mapOf("name" to "john", "surname" to "doe", "dob" to LocalDate.of(1990, 1, 1)),
                ),
            ),
            1,
            0,
            2,
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
                        "WITH ${'$'}e AS _e MERGE (n:${'$'}(_e.matchLabels) {`id`: _e.matchProperties.`id`, `name`: _e.matchProperties.`name`, `surname`: _e.matchProperties.`surname`}) SET n += _e.setProperties SET n:$(_e.addLabels) REMOVE n:$(_e.removeLabels)",
                        mapOf(
                            "e" to
                                mapOf(
                                    "matchLabels" to listOf("Person", "Employee"),
                                    "matchProperties" to
                                        mapOf("id" to 5000L, "name" to "john", "surname" to "doe"),
                                    "setProperties" to
                                        mapOf(
                                            "name" to "john",
                                            "dob" to LocalDate.of(1990, 1, 1),
                                            "married" to null,
                                        ),
                                    "addLabels" to listOf("Manager"),
                                    "removeLabels" to listOf("Employee"),
                                )
                        ),
                    ),
                )
            )
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
                        "WITH ${'$'}e AS _e MATCH (n:${'$'}(_e.matchLabels) {`name`: _e.matchProperties.`name`, `surname`: _e.matchProperties.`surname`}) DELETE n",
                        mapOf(
                            "e" to
                                mapOf(
                                    "matchLabels" to listOf("Person"),
                                    "matchProperties" to mapOf("name" to "joe", "surname" to "doe"),
                                )
                        ),
                    ),
                )
            )
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
                        "WITH ${'$'}e AS _e MATCH (start:${'$'}(_e.start.matchLabels) {`id`: _e.start.matchProperties.`id`}) MATCH (end:${'$'}(_e.end.matchLabels) {`id`: _e.end.matchProperties.`id`}) MERGE (start)-[r:${'$'}(_e.matchType) {`since`: _e.matchProperties.`since`}]->(end) SET r += _e.setProperties",
                        mapOf(
                            "e" to
                                mapOf(
                                    "start" to
                                        mapOf(
                                            "matchLabels" to listOf("Person"),
                                            "matchProperties" to mapOf("id" to 1L),
                                        ),
                                    "end" to
                                        mapOf(
                                            "matchLabels" to listOf("Person"),
                                            "matchProperties" to mapOf("id" to 2L),
                                        ),
                                    "matchType" to "KNOWS",
                                    "matchProperties" to mapOf("since" to LocalDate.of(2000, 1, 1)),
                                    "setProperties" to mapOf("since" to LocalDate.of(2000, 1, 1)),
                                )
                        ),
                    ),
                )
            )
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
                        "WITH ${'$'}e AS _e MATCH (start:${'$'}(_e.start.matchLabels) {`contractId`: _e.start.matchProperties.`contractId`, `id`: _e.start.matchProperties.`id`}) " +
                            "MATCH (end:${'$'}(_e.end.matchLabels) {`contractId`: _e.end.matchProperties.`contractId`, `id`: _e.end.matchProperties.`id`}) " +
                            "MATCH (start)-[r:${'$'}(_e.matchType) {`since`: _e.matchProperties.`since`}]->(end) " +
                            "WITH _e, r LIMIT 1 " +
                            "SET r += _e.setProperties",
                        mapOf(
                            "e" to
                                mapOf(
                                    "start" to
                                        mapOf(
                                            "matchLabels" to listOf("Person", "Employee"),
                                            "matchProperties" to
                                                mapOf("id" to 1L, "contractId" to 5000L),
                                        ),
                                    "end" to
                                        mapOf(
                                            "matchLabels" to listOf("Person", "Employee"),
                                            "matchProperties" to
                                                mapOf("id" to 2L, "contractId" to 5001L),
                                        ),
                                    "matchType" to "KNOWS",
                                    "matchProperties" to mapOf("since" to LocalDate.of(2000, 1, 1)),
                                    "setProperties" to mapOf("since" to LocalDate.of(1999, 1, 1)),
                                )
                        ),
                    ),
                )
            )
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
                        "WITH ${'$'}e AS _e MATCH (start:${'$'}(_e.start.matchLabels) {`contractId`: _e.start.matchProperties.`contractId`, `id`: _e.start.matchProperties.`id`})-" +
                            "[r:${'$'}(_e.matchType) {`id`: _e.matchProperties.`id`}]->" +
                            "(end:${'$'}(_e.end.matchLabels) {`id`: _e.end.matchProperties.`id`}) " +
                            "SET r += _e.setProperties",
                        mapOf(
                            "e" to
                                mapOf(
                                    "start" to
                                        mapOf(
                                            "matchLabels" to listOf("Person", "Employee"),
                                            "matchProperties" to
                                                mapOf("id" to 1L, "contractId" to 5000L),
                                        ),
                                    "end" to
                                        mapOf(
                                            "matchLabels" to listOf("Person"),
                                            "matchProperties" to mapOf("id" to 2L),
                                        ),
                                    "matchType" to "KNOWS",
                                    "matchProperties" to mapOf("id" to 1001L),
                                    "setProperties" to mapOf("name" to "joe"),
                                )
                        ),
                    ),
                )
            )
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
                        "WITH ${'$'}e AS _e MATCH (start:${'$'}(_e.start.matchLabels) {`id`: _e.start.matchProperties.`id`}) MATCH (end:${'$'}(_e.end.matchLabels) {`id`: _e.end.matchProperties.`id`}) MATCH (start)-[r:${'$'}(_e.matchType) {`name`: _e.matchProperties.`name`, `surname`: _e.matchProperties.`surname`}]->(end) WITH _e, r LIMIT 1 DELETE r",
                        mapOf(
                            "e" to
                                mapOf(
                                    "start" to
                                        mapOf(
                                            "matchLabels" to listOf("Person"),
                                            "matchProperties" to mapOf("id" to 1L),
                                        ),
                                    "end" to
                                        mapOf(
                                            "matchLabels" to listOf("Person"),
                                            "matchProperties" to mapOf("id" to 2L),
                                        ),
                                    "matchType" to "KNOWS",
                                    "matchProperties" to mapOf("name" to "john", "surname" to "doe"),
                                )
                        ),
                    ),
                )
            )
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
            1,
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
                        "WITH ${'$'}e AS _e MATCH ()-[r:${'$'}(_e.matchType) {`id`: _e.matchProperties.`id`}]->() DELETE r",
                        mapOf(
                            "e" to
                                mapOf(
                                    "matchType" to "KNOWS",
                                    "matchProperties" to mapOf("id" to 1001L),
                                )
                        ),
                    ),
                )
            )
        ),
    )
  }

  @Test
  fun `should split changes into transactional boundaries`() {
    val handler =
        CdcHandler(
            SinkStrategy.CDC_SCHEMA,
            TransactionalBatchStrategy(NEO4J_2026_1),
            CdcSchemaEventTransformer("my-topic"),
        )

    val result =
        handler.handle(
            listOf(
                newChangeEventMessage(randomChangeEvent(), 0, 0, 0),
                newChangeEventMessage(randomChangeEvent(), 0, 1, 1),
                newChangeEventMessage(randomChangeEvent(), 0, 2, 2),
                newChangeEventMessage(randomChangeEvent(), 1, 0, 3),
                newChangeEventMessage(randomChangeEvent(), 1, 1, 4),
                newChangeEventMessage(randomChangeEvent(), 2, 0, 5),
            )
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
                  .shouldMatchInOrder({ q1 ->
                    q1.txId shouldBe 2
                    q1.seq shouldBe 0
                  })
            },
        )
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
                        "WITH ${'$'}e AS _e MATCH (n:${'$'}(_e.matchLabels) {`name`: _e.matchProperties.`name`}) DELETE n",
                        mapOf(
                            "e" to
                                mapOf(
                                    "matchLabels" to listOf("Person"),
                                    "matchProperties" to mapOf("name" to "john"),
                                )
                        ),
                    ),
                )
            )
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
                    NodeState(listOf("Person"), mapOf("name" to "john")),
                ),
            txId = 1,
            seq = 0,
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
                        "WITH ${'$'}e AS _e MERGE (n:${'$'}(_e.matchLabels) {`name`: _e.matchProperties.`name`}) SET n += _e.setProperties SET n:$(_e.addLabels) REMOVE n:$(_e.removeLabels)",
                        mapOf(
                            "e" to
                                mapOf(
                                    "matchLabels" to listOf("Person"),
                                    "matchProperties" to mapOf("name" to "john"),
                                    "setProperties" to mapOf("name" to "john"),
                                    "addLabels" to emptyList<String>(),
                                    "removeLabels" to emptyList<String>(),
                                )
                        ),
                    ),
                )
            )
        ),
    )
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
                        "WITH ${'$'}e AS _e MATCH (start)-[r:${'$'}(_e.matchType) {`id`: _e.matchProperties.`id`}]->(end:${'$'}(_e.end.matchLabels) {`name`: _e.end.matchProperties.`name`}) " +
                            "SET r += _e.setProperties",
                        mapOf(
                            "e" to
                                mapOf(
                                    "end" to
                                        mapOf(
                                            "matchLabels" to listOf("Person"),
                                            "matchProperties" to mapOf("name" to "john"),
                                        ),
                                    "matchType" to "KNOWS",
                                    "matchProperties" to mapOf("id" to 1L),
                                    "setProperties" to mapOf("since" to LocalDate.of(2000, 1, 1)),
                                )
                        ),
                    ),
                )
            )
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
                        "WITH ${'$'}e AS _e MATCH (start:${'$'}(_e.start.matchLabels) {`name`: _e.start.matchProperties.`name`})-[r:${'$'}(_e.matchType) {`id`: _e.matchProperties.`id`}]->(end) SET r += _e.setProperties",
                        mapOf(
                            "e" to
                                mapOf(
                                    "start" to
                                        mapOf(
                                            "matchLabels" to listOf("Person"),
                                            "matchProperties" to mapOf("name" to "john"),
                                        ),
                                    "matchType" to "KNOWS",
                                    "matchProperties" to mapOf("id" to 1L),
                                    "setProperties" to mapOf("since" to LocalDate.of(2000, 1, 1)),
                                )
                        ),
                    ),
                )
            )
        ),
    )
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
                        "WITH ${'$'}e AS _e MATCH ()-[r:${'$'}(_e.matchType) {`id`: _e.matchProperties.`id`}]->() " +
                            "DELETE r",
                        mapOf(
                            "e" to
                                mapOf(
                                    "matchType" to "KNOWS",
                                    "matchProperties" to mapOf("id" to 1L),
                                )
                        ),
                    ),
                )
            )
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
                        "WITH ${'$'}e AS _e MATCH ()-[r:${'$'}(_e.matchType) {`id`: _e.matchProperties.`id`}]->() DELETE r",
                        mapOf(
                            "e" to
                                mapOf(
                                    "matchType" to "KNOWS",
                                    "matchProperties" to mapOf("id" to 1L),
                                )
                        ),
                    ),
                )
            )
        ),
    )
  }

  private fun verify(messages: Iterable<SinkMessage>, expected: Iterable<Iterable<ChangeQuery>>) {
    val handler =
        CdcHandler(
            SinkStrategy.CDC_SCHEMA,
            TransactionalBatchStrategy(NEO4J_2026_1),
            CdcSchemaEventTransformer("my-topic"),
        )

    val result = handler.handle(messages)

    result shouldBe expected
  }
}
