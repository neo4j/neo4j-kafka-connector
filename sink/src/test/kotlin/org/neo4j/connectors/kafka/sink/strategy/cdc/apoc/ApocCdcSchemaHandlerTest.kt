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
package org.neo4j.connectors.kafka.sink.strategy.cdc.apoc

import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.collections.shouldMatchInOrder
import io.kotest.matchers.shouldBe
import java.time.LocalDate
import kotlin.collections.emptyList
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
import org.neo4j.connectors.kafka.sink.strategy.cdc.ApocBatchStrategy
import org.neo4j.connectors.kafka.sink.strategy.cdc.CdcHandler
import org.neo4j.connectors.kafka.sink.strategy.cdc.CdcSchemaEventTransformer
import org.neo4j.driver.Query

class ApocCdcSchemaHandlerWithEOSTest :
    ApocCdcSchemaHandlerTest(
        "__KafkaOffset",
        """
        |UNWIND ${'$'}events AS e
        |MERGE (k:__KafkaOffset {strategy: ${'$'}strategy, topic: ${'$'}topic, partition: ${'$'}partition}) ON CREATE SET k.offset = -1
        |WITH k, e WHERE e.offset > k.offset
        |WITH k, e ORDER BY e.offset ASC
        |CALL (e) {
        |  CALL apoc.cypher.doIt(e.stmt, e.params) YIELD value RETURN COUNT(1) AS total
        |}
        |WITH k, max(e.offset) AS newOffset SET k.offset = newOffset
        |FINISH
        """
            .trimMargin(),
    )

class ApocCdcSchemaHandlerWithoutEOSTest :
    ApocCdcSchemaHandlerTest(
        "",
        """
        |UNWIND ${'$'}events AS e
        |WITH e ORDER BY e.offset ASC
        |CALL (e) {
        |  CALL apoc.cypher.doIt(e.stmt, e.params) YIELD value RETURN COUNT(1) AS total
        |}
        |FINISH
        """
            .trimMargin(),
    )

abstract class ApocCdcSchemaHandlerTest(val eosOffsetLabel: String, val expectedQuery: String) {
  private val neo4j =
      Neo4j(Neo4jVersion(2025, 12, 1), Neo4jEdition.ENTERPRISE, Neo4jDeploymentType.SELF_MANAGED)

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
                    null,
                    null,
                    listOf(sinkMessage),
                    Query(
                        expectedQuery,
                        mapOf(
                            "events" to
                                listOf(
                                    mapOf(
                                        "offset" to 0L,
                                        "stmt" to
                                            "WITH ${'$'}e AS _e MERGE (n:`Person` {`name`: _e.matchProperties.`name`, `surname`: _e.matchProperties.`surname`}) SET n += _e.setProperties SET n:\$(_e.addLabels) REMOVE n:\$(_e.removeLabels)",
                                        "params" to
                                            mapOf(
                                                "e" to
                                                    mapOf(
                                                        "matchProperties" to
                                                            mapOf(
                                                                "name" to "john",
                                                                "surname" to "doe",
                                                            ),
                                                        "setProperties" to
                                                            mapOf(
                                                                "name" to "john",
                                                                "surname" to "doe",
                                                                "dob" to LocalDate.of(1990, 1, 1),
                                                            ),
                                                        "addLabels" to emptySet<String>(),
                                                        "removeLabels" to emptySet<String>(),
                                                    )
                                            ),
                                    )
                                ),
                            "topic" to "my-topic",
                            "partition" to 0,
                            "strategy" to "CDC_SCHEMA",
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
                    null,
                    null,
                    listOf(sinkMessage1),
                    Query(
                        expectedQuery,
                        mapOf(
                            "events" to
                                listOf(
                                    mapOf(
                                        "offset" to 1L,
                                        "stmt" to
                                            "WITH ${'$'}e AS _e MERGE (n:`Person` {`name`: _e.matchProperties.`name`, `surname`: _e.matchProperties.`surname`}) SET n += _e.setProperties SET n:\$(_e.addLabels) REMOVE n:\$(_e.removeLabels)",
                                        "params" to
                                            mapOf(
                                                "e" to
                                                    mapOf(
                                                        "matchProperties" to
                                                            mapOf(
                                                                "name" to "john",
                                                                "surname" to "doe",
                                                            ),
                                                        "setProperties" to
                                                            mapOf(
                                                                "name" to "john",
                                                                "surname" to "doe",
                                                                "dob" to LocalDate.of(1990, 1, 1),
                                                            ),
                                                        "addLabels" to setOf("Employee"),
                                                        "removeLabels" to emptySet<String>(),
                                                    )
                                            ),
                                    )
                                ),
                            "topic" to "my-topic",
                            "partition" to 0,
                            "strategy" to "CDC_SCHEMA",
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
                    null,
                    null,
                    listOf(sinkMessage),
                    Query(
                        expectedQuery,
                        mapOf(
                            "events" to
                                listOf(
                                    mapOf(
                                        "offset" to 0L,
                                        "stmt" to
                                            "WITH ${'$'}e AS _e MERGE (n:`Person` {`name`: _e.matchProperties.`name`, `surname`: _e.matchProperties.`surname`}) SET n += _e.setProperties SET n:\$(_e.addLabels) REMOVE n:\$(_e.removeLabels)",
                                        "params" to
                                            mapOf(
                                                "e" to
                                                    mapOf(
                                                        "matchProperties" to
                                                            mapOf(
                                                                "name" to "john",
                                                                "surname" to "doe",
                                                            ),
                                                        "setProperties" to
                                                            mapOf(
                                                                "name" to "john",
                                                                "dob" to LocalDate.of(1990, 1, 1),
                                                            ),
                                                        "addLabels" to emptySet<String>(),
                                                        "removeLabels" to emptySet<String>(),
                                                    )
                                            ),
                                    )
                                ),
                            "topic" to "my-topic",
                            "partition" to 0,
                            "strategy" to "CDC_SCHEMA",
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
                    null,
                    null,
                    listOf(sinkMessage1),
                    Query(
                        expectedQuery,
                        mapOf(
                            "events" to
                                listOf(
                                    mapOf(
                                        "offset" to 1L,
                                        "stmt" to
                                            "WITH ${'$'}e AS _e MERGE (n:`Person` {`name`: _e.matchProperties.`name`, `surname`: _e.matchProperties.`surname`}) SET n += _e.setProperties SET n:\$(_e.addLabels) REMOVE n:\$(_e.removeLabels)",
                                        "params" to
                                            mapOf(
                                                "e" to
                                                    mapOf(
                                                        "matchProperties" to
                                                            mapOf(
                                                                "name" to "john",
                                                                "surname" to "doe",
                                                            ),
                                                        "setProperties" to
                                                            mapOf(
                                                                "name" to "john",
                                                                "dob" to LocalDate.of(1990, 1, 1),
                                                                "married" to null,
                                                            ),
                                                        "addLabels" to setOf("Manager"),
                                                        "removeLabels" to setOf("Employee"),
                                                    )
                                            ),
                                    )
                                ),
                            "topic" to "my-topic",
                            "partition" to 0,
                            "strategy" to "CDC_SCHEMA",
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
                    null,
                    null,
                    listOf(sinkMessage2),
                    Query(
                        expectedQuery,
                        mapOf(
                            "events" to
                                listOf(
                                    mapOf(
                                        "offset" to 2L,
                                        "stmt" to
                                            "WITH ${'$'}e AS _e MERGE (n:`Employee`:`Person` {`id`: _e.matchProperties.`id`, `name`: _e.matchProperties.`name`, `surname`: _e.matchProperties.`surname`}) SET n += _e.setProperties SET n:\$(_e.addLabels) REMOVE n:\$(_e.removeLabels)",
                                        "params" to
                                            mapOf(
                                                "e" to
                                                    mapOf(
                                                        "matchProperties" to
                                                            mapOf(
                                                                "name" to "john",
                                                                "surname" to "doe",
                                                                "id" to 5000L,
                                                            ),
                                                        "setProperties" to
                                                            mapOf(
                                                                "name" to "john",
                                                                "dob" to LocalDate.of(1990, 1, 1),
                                                                "married" to null,
                                                            ),
                                                        "addLabels" to setOf("Manager"),
                                                        "removeLabels" to setOf("Employee"),
                                                    )
                                            ),
                                    )
                                ),
                            "topic" to "my-topic",
                            "partition" to 0,
                            "strategy" to "CDC_SCHEMA",
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
                    null,
                    null,
                    listOf(sinkMessage),
                    Query(
                        expectedQuery,
                        mapOf(
                            "events" to
                                listOf(
                                    mapOf(
                                        "offset" to 0L,
                                        "stmt" to
                                            "WITH ${'$'}e AS _e MATCH (n:`Person` {`name`: _e.matchProperties.`name`, `surname`: _e.matchProperties.`surname`}) DELETE n",
                                        "params" to
                                            mapOf(
                                                "e" to
                                                    mapOf(
                                                        "matchProperties" to
                                                            mapOf(
                                                                "name" to "joe",
                                                                "surname" to "doe",
                                                            )
                                                    )
                                            ),
                                    )
                                ),
                            "topic" to "my-topic",
                            "partition" to 0,
                            "strategy" to "CDC_SCHEMA",
                        ),
                    ),
                )
            )
        ),
    )
  }

  @Test
  fun `should generate correct statement for relationship creation events for key-less relationships`() {
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
                    null,
                    null,
                    listOf(sinkMessage),
                    Query(
                        expectedQuery,
                        mapOf(
                            "events" to
                                listOf(
                                    mapOf(
                                        "offset" to 0L,
                                        "stmt" to
                                            "WITH ${'$'}e AS _e MATCH (start:`Person` {`id`: _e.start.matchProperties.`id`}) MATCH (end:`Person` {`id`: _e.end.matchProperties.`id`}) MERGE (start)-[r:`KNOWS` {`since`: _e.matchProperties.`since`}]->(end) SET r += _e.setProperties",
                                        "params" to
                                            mapOf(
                                                "e" to
                                                    mapOf(
                                                        "start" to
                                                            mapOf(
                                                                "matchProperties" to
                                                                    mapOf("id" to 1L)
                                                            ),
                                                        "end" to
                                                            mapOf(
                                                                "matchProperties" to
                                                                    mapOf("id" to 2L)
                                                            ),
                                                        "matchProperties" to
                                                            mapOf(
                                                                "since" to LocalDate.of(2000, 1, 1)
                                                            ),
                                                        "setProperties" to
                                                            mapOf(
                                                                "since" to LocalDate.of(2000, 1, 1)
                                                            ),
                                                    )
                                            ),
                                    )
                                ),
                            "topic" to "my-topic",
                            "partition" to 0,
                            "strategy" to "CDC_SCHEMA",
                        ),
                    ),
                )
            )
        ),
    )
  }

  @Test
  fun `should generate correct statement for relationship creation events for relationships with key`() {
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
                listOf(mapOf("id" to 1001L)),
                EntityOperation.CREATE,
                null,
                RelationshipState(mapOf("id" to 1001L, "since" to LocalDate.of(2000, 1, 1))),
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
                    null,
                    null,
                    listOf(sinkMessage),
                    Query(
                        expectedQuery,
                        mapOf(
                            "events" to
                                listOf(
                                    mapOf(
                                        "offset" to 0L,
                                        "stmt" to
                                            "WITH ${'$'}e AS _e MATCH (start:`Person` {`id`: _e.start.matchProperties.`id`}) MATCH (end:`Person` {`id`: _e.end.matchProperties.`id`}) MERGE (start)-[r:`KNOWS` {`id`: _e.matchProperties.`id`}]->(end) SET r += _e.setProperties",
                                        "params" to
                                            mapOf(
                                                "e" to
                                                    mapOf(
                                                        "start" to
                                                            mapOf(
                                                                "matchProperties" to
                                                                    mapOf("id" to 1L)
                                                            ),
                                                        "end" to
                                                            mapOf(
                                                                "matchProperties" to
                                                                    mapOf("id" to 2L)
                                                            ),
                                                        "matchProperties" to mapOf("id" to 1001L),
                                                        "setProperties" to
                                                            mapOf(
                                                                "id" to 1001L,
                                                                "since" to LocalDate.of(2000, 1, 1),
                                                            ),
                                                    )
                                            ),
                                    )
                                ),
                            "topic" to "my-topic",
                            "partition" to 0,
                            "strategy" to "CDC_SCHEMA",
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
                    null,
                    null,
                    listOf(sinkMessage),
                    Query(
                        expectedQuery,
                        mapOf(
                            "events" to
                                listOf(
                                    mapOf(
                                        "offset" to 0L,
                                        "stmt" to
                                            "WITH ${'$'}e AS _e MATCH (start:`Employee`:`Person` {`contractId`: _e.start.matchProperties.`contractId`, `id`: _e.start.matchProperties.`id`}) MATCH (end:`Employee`:`Person` {`contractId`: _e.end.matchProperties.`contractId`, `id`: _e.end.matchProperties.`id`}) MATCH (start)-[r:`KNOWS` {`since`: _e.matchProperties.`since`}]->(end) WITH _e, r LIMIT 1 SET r += _e.setProperties",
                                        "params" to
                                            mapOf(
                                                "e" to
                                                    mapOf(
                                                        "start" to
                                                            mapOf(
                                                                "matchProperties" to
                                                                    mapOf(
                                                                        "id" to 1L,
                                                                        "contractId" to 5000L,
                                                                    )
                                                            ),
                                                        "end" to
                                                            mapOf(
                                                                "matchProperties" to
                                                                    mapOf(
                                                                        "id" to 2L,
                                                                        "contractId" to 5001L,
                                                                    )
                                                            ),
                                                        "matchProperties" to
                                                            mapOf(
                                                                "since" to LocalDate.of(2000, 1, 1)
                                                            ),
                                                        "setProperties" to
                                                            mapOf(
                                                                "since" to LocalDate.of(1999, 1, 1)
                                                            ),
                                                    )
                                            ),
                                    )
                                ),
                            "topic" to "my-topic",
                            "partition" to 0,
                            "strategy" to "CDC_SCHEMA",
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
                    null,
                    null,
                    listOf(sinkMessage1),
                    Query(
                        expectedQuery,
                        mapOf(
                            "events" to
                                listOf(
                                    mapOf(
                                        "offset" to 0L,
                                        "stmt" to
                                            "WITH ${'$'}e AS _e MATCH (start:`Employee`:`Person` {`contractId`: _e.start.matchProperties.`contractId`, `id`: _e.start.matchProperties.`id`})-[r:`KNOWS` {`id`: _e.matchProperties.`id`}]->(end:`Person` {`id`: _e.end.matchProperties.`id`}) SET r += _e.setProperties",
                                        "params" to
                                            mapOf(
                                                "e" to
                                                    mapOf(
                                                        "start" to
                                                            mapOf(
                                                                "matchProperties" to
                                                                    mapOf(
                                                                        "id" to 1L,
                                                                        "contractId" to 5000L,
                                                                    )
                                                            ),
                                                        "end" to
                                                            mapOf(
                                                                "matchProperties" to
                                                                    mapOf("id" to 2L)
                                                            ),
                                                        "matchProperties" to mapOf("id" to 1001L),
                                                        "setProperties" to mapOf("name" to "joe"),
                                                    )
                                            ),
                                    )
                                ),
                            "topic" to "my-topic",
                            "partition" to 0,
                            "strategy" to "CDC_SCHEMA",
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
                    null,
                    null,
                    listOf(sinkMessage),
                    Query(
                        expectedQuery,
                        mapOf(
                            "events" to
                                listOf(
                                    mapOf(
                                        "offset" to 0L,
                                        "stmt" to
                                            "WITH ${'$'}e AS _e MATCH (start:`Person` {`id`: _e.start.matchProperties.`id`}) MATCH (end:`Person` {`id`: _e.end.matchProperties.`id`}) MATCH (start)-[r:`KNOWS` {`name`: _e.matchProperties.`name`, `surname`: _e.matchProperties.`surname`}]->(end) WITH _e, r LIMIT 1 DELETE r",
                                        "params" to
                                            mapOf(
                                                "e" to
                                                    mapOf(
                                                        "start" to
                                                            mapOf(
                                                                "matchProperties" to
                                                                    mapOf("id" to 1L)
                                                            ),
                                                        "end" to
                                                            mapOf(
                                                                "matchProperties" to
                                                                    mapOf("id" to 2L)
                                                            ),
                                                        "matchProperties" to
                                                            mapOf(
                                                                "name" to "john",
                                                                "surname" to "doe",
                                                            ),
                                                    )
                                            ),
                                    )
                                ),
                            "topic" to "my-topic",
                            "partition" to 0,
                            "strategy" to "CDC_SCHEMA",
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
                    null,
                    null,
                    listOf(sinkMessage1),
                    Query(
                        expectedQuery,
                        mapOf(
                            "events" to
                                listOf(
                                    mapOf(
                                        "offset" to 1L,
                                        "stmt" to
                                            "WITH ${'$'}e AS _e MATCH ()-[r:`KNOWS` {`id`: _e.matchProperties.`id`}]->() DELETE r",
                                        "params" to
                                            mapOf(
                                                "e" to
                                                    mapOf("matchProperties" to mapOf("id" to 1001L))
                                            ),
                                    )
                                ),
                            "topic" to "my-topic",
                            "partition" to 0,
                            "strategy" to "CDC_SCHEMA",
                        ),
                    ),
                )
            )
        ),
    )
  }

  @Test
  fun `should split changes over batch size`() {
    val handler =
        CdcHandler(
            SinkStrategy.CDC_SCHEMA,
            ApocBatchStrategy(neo4j, 2, eosOffsetLabel, SinkStrategy.CDC_SCHEMA),
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
                newChangeEventMessage(randomChangeEvent(), 3, 0, 6),
            )
        )

    // With the new apoc.cypher.doIt approach, each batch produces one ChangeQuery
    // So 7 messages with batchSize=2 produces 4 batches (2+2+2+1)
    result.shouldHaveSize(4)
    result.shouldMatchInOrder(
        { batch ->
          batch.shouldHaveSize(1)
          batch.first().messages shouldHaveSize 2
        },
        { batch ->
          batch.shouldHaveSize(1)
          batch.first().messages shouldHaveSize 2
        },
        { batch ->
          batch.shouldHaveSize(1)
          batch.first().messages shouldHaveSize 2
        },
        { batch ->
          batch.shouldHaveSize(1)
          batch.first().messages shouldHaveSize 1
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
                    null,
                    null,
                    listOf(sinkMessage),
                    Query(
                        expectedQuery,
                        mapOf(
                            "events" to
                                listOf(
                                    mapOf(
                                        "offset" to 0L,
                                        "stmt" to
                                            "WITH ${'$'}e AS _e MATCH (n:`Person` {`name`: _e.matchProperties.`name`}) DELETE n",
                                        "params" to
                                            mapOf(
                                                "e" to
                                                    mapOf(
                                                        "matchProperties" to mapOf("name" to "john")
                                                    )
                                            ),
                                    )
                                ),
                            "topic" to "my-topic",
                            "partition" to 0,
                            "strategy" to "CDC_SCHEMA",
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
                    null,
                    null,
                    listOf(sinkMessage),
                    Query(
                        expectedQuery,
                        mapOf(
                            "events" to
                                listOf(
                                    mapOf(
                                        "offset" to 0L,
                                        "stmt" to
                                            "WITH ${'$'}e AS _e MERGE (n:`Person` {`name`: _e.matchProperties.`name`}) SET n += _e.setProperties SET n:\$(_e.addLabels) REMOVE n:\$(_e.removeLabels)",
                                        "params" to
                                            mapOf(
                                                "e" to
                                                    mapOf(
                                                        "matchProperties" to
                                                            mapOf("name" to "john"),
                                                        "setProperties" to mapOf("name" to "john"),
                                                        "addLabels" to emptySet<String>(),
                                                        "removeLabels" to emptySet<String>(),
                                                    )
                                            ),
                                    )
                                ),
                            "topic" to "my-topic",
                            "partition" to 0,
                            "strategy" to "CDC_SCHEMA",
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
                    null,
                    null,
                    listOf(sinkMessage),
                    Query(
                        expectedQuery,
                        mapOf(
                            "events" to
                                listOf(
                                    mapOf(
                                        "offset" to 0L,
                                        "stmt" to
                                            "WITH ${'$'}e AS _e MATCH (start)-[r:`KNOWS` {`id`: _e.matchProperties.`id`}]->(end:`Person` {`name`: _e.end.matchProperties.`name`}) SET r += _e.setProperties",
                                        "params" to
                                            mapOf(
                                                "e" to
                                                    mapOf(
                                                        "end" to
                                                            mapOf(
                                                                "matchProperties" to
                                                                    mapOf("name" to "john")
                                                            ),
                                                        "matchProperties" to mapOf("id" to 1L),
                                                        "setProperties" to
                                                            mapOf(
                                                                "since" to LocalDate.of(2000, 1, 1)
                                                            ),
                                                    )
                                            ),
                                    )
                                ),
                            "topic" to "my-topic",
                            "partition" to 0,
                            "strategy" to "CDC_SCHEMA",
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
                    null,
                    null,
                    listOf(sinkMessage),
                    Query(
                        expectedQuery,
                        mapOf(
                            "events" to
                                listOf(
                                    mapOf(
                                        "offset" to 0L,
                                        "stmt" to
                                            "WITH ${'$'}e AS _e MATCH (start:`Person` {`name`: _e.start.matchProperties.`name`})-[r:`KNOWS` {`id`: _e.matchProperties.`id`}]->(end) SET r += _e.setProperties",
                                        "params" to
                                            mapOf(
                                                "e" to
                                                    mapOf(
                                                        "start" to
                                                            mapOf(
                                                                "matchProperties" to
                                                                    mapOf("name" to "john")
                                                            ),
                                                        "matchProperties" to mapOf("id" to 1L),
                                                        "setProperties" to
                                                            mapOf(
                                                                "since" to LocalDate.of(2000, 1, 1)
                                                            ),
                                                    )
                                            ),
                                    )
                                ),
                            "topic" to "my-topic",
                            "partition" to 0,
                            "strategy" to "CDC_SCHEMA",
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
                    null,
                    null,
                    listOf(sinkMessage),
                    Query(
                        expectedQuery,
                        mapOf(
                            "events" to
                                listOf(
                                    mapOf(
                                        "offset" to 0L,
                                        "stmt" to
                                            "WITH ${'$'}e AS _e MATCH ()-[r:`KNOWS` {`id`: _e.matchProperties.`id`}]->() DELETE r",
                                        "params" to
                                            mapOf(
                                                "e" to mapOf("matchProperties" to mapOf("id" to 1L))
                                            ),
                                    )
                                ),
                            "topic" to "my-topic",
                            "partition" to 0,
                            "strategy" to "CDC_SCHEMA",
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
                    null,
                    null,
                    listOf(sinkMessage),
                    Query(
                        expectedQuery,
                        mapOf(
                            "events" to
                                listOf(
                                    mapOf(
                                        "offset" to 0L,
                                        "stmt" to
                                            "WITH ${'$'}e AS _e MATCH ()-[r:`KNOWS` {`id`: _e.matchProperties.`id`}]->() DELETE r",
                                        "params" to
                                            mapOf(
                                                "e" to mapOf("matchProperties" to mapOf("id" to 1L))
                                            ),
                                    )
                                ),
                            "topic" to "my-topic",
                            "partition" to 0,
                            "strategy" to "CDC_SCHEMA",
                        ),
                    ),
                )
            )
        ),
    )
  }

  @Test
  fun `should generate correct statement when relationship start and end keys are empty but relationship has its own keys for deletion`() {
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
                    null,
                    null,
                    listOf(sinkMessage),
                    Query(
                        expectedQuery,
                        mapOf(
                            "events" to
                                listOf(
                                    mapOf(
                                        "offset" to 0L,
                                        "stmt" to
                                            "WITH ${'$'}e AS _e MATCH ()-[r:`KNOWS` {`id`: _e.matchProperties.`id`}]->() DELETE r",
                                        "params" to
                                            mapOf(
                                                "e" to mapOf("matchProperties" to mapOf("id" to 1L))
                                            ),
                                    )
                                ),
                            "topic" to "my-topic",
                            "partition" to 0,
                            "strategy" to "CDC_SCHEMA",
                        ),
                    ),
                )
            )
        ),
    )
  }

  @Test
  fun `should generate correct statement when relationship start and end keys are empty but relationship has its own keys for update`() {
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
                    emptyMap<String, List<Map<String, Any>>>(),
                ),
                listOf(mapOf("id" to 1L)),
                EntityOperation.UPDATE,
                RelationshipState(mapOf("id" to 1L)),
                RelationshipState(mapOf("id" to 1L, "since" to LocalDate.of(2021, 1, 1))),
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
                    null,
                    null,
                    listOf(sinkMessage),
                    Query(
                        expectedQuery,
                        mapOf(
                            "events" to
                                listOf(
                                    mapOf(
                                        "offset" to 0L,
                                        "stmt" to
                                            "WITH ${'$'}e AS _e MATCH (start)-[r:`KNOWS` {`id`: _e.matchProperties.`id`}]->(end) SET r += _e.setProperties",
                                        "params" to
                                            mapOf(
                                                "e" to
                                                    mapOf(
                                                        "matchProperties" to mapOf("id" to 1L),
                                                        "setProperties" to
                                                            mapOf<String, Any>(
                                                                "since" to LocalDate.of(2021, 1, 1)
                                                            ),
                                                    )
                                            ),
                                    )
                                ),
                            "topic" to "my-topic",
                            "partition" to 0,
                            "strategy" to "CDC_SCHEMA",
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
            ApocBatchStrategy(neo4j, 1000, eosOffsetLabel, SinkStrategy.CDC_SCHEMA),
            CdcSchemaEventTransformer("my-topic"),
        )

    val result = handler.handle(messages)

    result.zip(expected).forEach { (res, exp) ->
      res.zip(exp).forEach { (r, e) ->
        r.txId shouldBe e.txId
        r.seq shouldBe e.seq
        r.messages shouldBe e.messages
        r.query.text() shouldBe e.query.text()
        r.query.parameters().asMap() shouldBe e.query.parameters().asMap()
      }
    }
    result shouldBe expected
  }
}
