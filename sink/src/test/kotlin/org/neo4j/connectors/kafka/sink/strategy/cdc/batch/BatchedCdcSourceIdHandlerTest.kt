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
package org.neo4j.connectors.kafka.sink.strategy.cdc.batch

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
import org.neo4j.connectors.kafka.exceptions.InvalidDataException
import org.neo4j.connectors.kafka.sink.ChangeQuery
import org.neo4j.connectors.kafka.sink.SinkMessage
import org.neo4j.connectors.kafka.sink.strategy.TestUtils.createKnowsRelationshipEvent
import org.neo4j.connectors.kafka.sink.strategy.TestUtils.createNodePersonEvent
import org.neo4j.connectors.kafka.sink.strategy.TestUtils.newChangeEventMessage
import org.neo4j.connectors.kafka.sink.strategy.TestUtils.randomChangeEvent
import org.neo4j.connectors.kafka.sink.strategy.TestUtils.updateKnowsRelationshipEvent
import org.neo4j.driver.Query

class BatchedCdcSourceIdHandlerTest {

  @Test
  fun `should generate correct statement for node creation events`() {
    val sinkMessage =
        newChangeEventMessage(
            NodeEvent(
                "node-element-id",
                EntityOperation.CREATE,
                emptyList(),
                emptyMap(),
                null,
                NodeState(emptyList(), mapOf("name" to "john", "surname" to "doe")),
            ),
            0,
            1,
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
                        "CYPHER 25 UNWIND \$events AS e CALL (e) { WHEN e.q = \$q0 THEN " +
                            "MERGE (n:\$(e.matchLabels) {sourceElementId: e.matchProperties.sourceElementId}) " +
                            "SET n += e.setProperties " +
                            "SET n:\$(e.addLabels) " +
                            "REMOVE n:\$(e.removeLabels) } FINISH",
                        mapOf(
                            "q0" to 0,
                            "events" to
                                listOf(
                                    mapOf(
                                        "q" to 0,
                                        "matchLabels" to listOf("SourceEvent"),
                                        "matchProperties" to
                                            mapOf("sourceElementId" to "node-element-id"),
                                        "setProperties" to
                                            mapOf("name" to "john", "surname" to "doe"),
                                        "addLabels" to emptyList<String>(),
                                        "removeLabels" to emptyList<String>(),
                                    )
                                ),
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
                listOf("Person"),
                mapOf("Person" to listOf(mapOf("name" to "john", "surname" to "doe"))),
                null,
                NodeState(
                    listOf("Person"),
                    mapOf("name" to "john", "surname" to "doe", "dob" to LocalDate.of(1990, 1, 1)),
                ),
            ),
            0,
            1,
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
                        "CYPHER 25 UNWIND \$events AS e CALL (e) { WHEN e.q = \$q0 THEN " +
                            "MERGE (n:\$(e.matchLabels) {sourceElementId: e.matchProperties.sourceElementId}) " +
                            "SET n += e.setProperties " +
                            "SET n:\$(e.addLabels) " +
                            "REMOVE n:\$(e.removeLabels) } FINISH",
                        mapOf(
                            "q0" to 0,
                            "events" to
                                listOf(
                                    mapOf(
                                        "q" to 0,
                                        "matchLabels" to listOf("SourceEvent"),
                                        "matchProperties" to
                                            mapOf("sourceElementId" to "node-element-id"),
                                        "setProperties" to
                                            mapOf(
                                                "name" to "john",
                                                "surname" to "doe",
                                                "dob" to LocalDate.of(1990, 1, 1),
                                            ),
                                        "addLabels" to listOf("Person"),
                                        "removeLabels" to emptyList<String>(),
                                    )
                                ),
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
                EntityOperation.CREATE,
                listOf("Person", "Employee"),
                mapOf("Person" to listOf(mapOf("name" to "john", "surname" to "doe"))),
                null,
                NodeState(
                    listOf("Person", "Employee"),
                    mapOf("name" to "john", "surname" to "doe", "dob" to LocalDate.of(1990, 1, 1)),
                ),
            ),
            0,
            1,
            0,
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
                        "CYPHER 25 UNWIND \$events AS e CALL (e) { WHEN e.q = \$q0 THEN " +
                            "MERGE (n:\$(e.matchLabels) {sourceElementId: e.matchProperties.sourceElementId}) " +
                            "SET n += e.setProperties " +
                            "SET n:\$(e.addLabels) " +
                            "REMOVE n:\$(e.removeLabels) } FINISH",
                        mapOf(
                            "q0" to 0,
                            "events" to
                                listOf(
                                    mapOf(
                                        "q" to 0,
                                        "matchLabels" to listOf("SourceEvent"),
                                        "matchProperties" to
                                            mapOf("sourceElementId" to "node-element-id"),
                                        "setProperties" to
                                            mapOf(
                                                "name" to "john",
                                                "surname" to "doe",
                                                "dob" to LocalDate.of(1990, 1, 1),
                                            ),
                                        "addLabels" to listOf("Person", "Employee"),
                                        "removeLabels" to emptyList<String>(),
                                    )
                                ),
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
                emptyList(),
                emptyMap(),
                NodeState(emptyList(), mapOf("name" to "john")),
                NodeState(emptyList(), mapOf("name" to "joe", "dob" to LocalDate.of(2000, 1, 1))),
            ),
            0,
            1,
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
                        "CYPHER 25 UNWIND \$events AS e CALL (e) { WHEN e.q = \$q0 THEN " +
                            "MERGE (n:\$(e.matchLabels) {sourceElementId: e.matchProperties.sourceElementId}) " +
                            "SET n += e.setProperties " +
                            "SET n:\$(e.addLabels) " +
                            "REMOVE n:\$(e.removeLabels) } FINISH",
                        mapOf(
                            "q0" to 0,
                            "events" to
                                listOf(
                                    mapOf(
                                        "q" to 0,
                                        "matchLabels" to listOf("SourceEvent"),
                                        "matchProperties" to
                                            mapOf("sourceElementId" to "node-element-id"),
                                        "setProperties" to
                                            mapOf(
                                                "name" to "joe",
                                                "dob" to LocalDate.of(2000, 1, 1),
                                            ),
                                        "addLabels" to emptyList<String>(),
                                        "removeLabels" to emptyList<String>(),
                                    )
                                ),
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
                NodeState(listOf("Person", "Employee"), mapOf("name" to "joe", "surname" to "doe")),
                NodeState(
                    listOf("Person", "Employee"),
                    mapOf("name" to "john", "surname" to "doe", "dob" to LocalDate.of(1990, 1, 1)),
                ),
            ),
            0,
            1,
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
                        "CYPHER 25 UNWIND \$events AS e CALL (e) { WHEN e.q = \$q0 THEN " +
                            "MERGE (n:\$(e.matchLabels) {sourceElementId: e.matchProperties.sourceElementId}) " +
                            "SET n += e.setProperties " +
                            "SET n:\$(e.addLabels) " +
                            "REMOVE n:\$(e.removeLabels) } FINISH",
                        mapOf(
                            "q0" to 0,
                            "events" to
                                listOf(
                                    mapOf(
                                        "q" to 0,
                                        "matchLabels" to listOf("SourceEvent"),
                                        "matchProperties" to
                                            mapOf("sourceElementId" to "node-element-id"),
                                        "setProperties" to
                                            mapOf(
                                                "name" to "john",
                                                "dob" to LocalDate.of(1990, 1, 1),
                                            ),
                                        "addLabels" to emptyList<String>(),
                                        "removeLabels" to emptyList<String>(),
                                    )
                                ),
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
            0,
            1,
            0,
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
                        "CYPHER 25 UNWIND \$events AS e CALL (e) { WHEN e.q = \$q0 THEN " +
                            "MERGE (n:\$(e.matchLabels) {sourceElementId: e.matchProperties.sourceElementId}) " +
                            "SET n += e.setProperties " +
                            "SET n:\$(e.addLabels) " +
                            "REMOVE n:\$(e.removeLabels) } FINISH",
                        mapOf(
                            "q0" to 0,
                            "events" to
                                listOf(
                                    mapOf(
                                        "q" to 0,
                                        "matchLabels" to listOf("SourceEvent"),
                                        "matchProperties" to
                                            mapOf("sourceElementId" to "node-element-id"),
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
                emptyList(),
                emptyMap(),
                NodeState(emptyList(), mapOf("name" to "joe", "dob" to LocalDate.of(2000, 1, 1))),
                null,
            ),
            0,
            1,
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
                        "CYPHER 25 UNWIND \$events AS e CALL (e) { WHEN e.q = \$q0 THEN " +
                            "MATCH (n:\$(e.matchLabels) {sourceElementId: e.matchProperties.sourceElementId}) " +
                            "DELETE n } FINISH",
                        mapOf(
                            "q0" to 0,
                            "events" to
                                listOf(
                                    mapOf(
                                        "q" to 0,
                                        "matchLabels" to listOf("SourceEvent"),
                                        "matchProperties" to
                                            mapOf("sourceElementId" to "node-element-id"),
                                        "setProperties" to emptyMap<String, Any>(),
                                        "addLabels" to emptyList<String>(),
                                        "removeLabels" to emptyList<String>(),
                                    )
                                ),
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
                "REL",
                Node("start-element-id", emptyList(), emptyMap()),
                Node("end-element-id", emptyList(), emptyMap()),
                emptyList(),
                EntityOperation.CREATE,
                null,
                RelationshipState(mapOf("name" to "john", "surname" to "doe")),
            ),
            0,
            1,
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
                        "CYPHER 25 UNWIND \$events AS e CALL (e) { WHEN e.q = \$q0 THEN " +
                            "" +
                            "MATCH (start:\$(e.start.matchLabels) {sourceElementId: e.start.matchProperties.sourceElementId}) " +
                            "MATCH (end:\$(e.end.matchLabels) {sourceElementId: e.end.matchProperties.sourceElementId}) " +
                            "MERGE (start)-[r:\$(e.matchType) {sourceElementId: e.matchProperties.sourceElementId}]->(end) " +
                            "SET r += e.setProperties } FINISH",
                        mapOf(
                            "q0" to 0,
                            "events" to
                                listOf(
                                    mapOf(
                                        "q" to 0,
                                        "start" to
                                            mapOf(
                                                "matchLabels" to listOf("SourceEvent"),
                                                "matchProperties" to
                                                    mapOf("sourceElementId" to "start-element-id"),
                                            ),
                                        "end" to
                                            mapOf(
                                                "matchLabels" to listOf("SourceEvent"),
                                                "matchProperties" to
                                                    mapOf("sourceElementId" to "end-element-id"),
                                            ),
                                        "matchType" to "REL",
                                        "matchProperties" to
                                            mapOf("sourceElementId" to "rel-element-id"),
                                        "setProperties" to
                                            mapOf("name" to "john", "surname" to "doe"),
                                    )
                                ),
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
                "REL",
                Node("start-element-id", emptyList(), emptyMap()),
                Node("end-element-id", emptyList(), emptyMap()),
                emptyList(),
                EntityOperation.UPDATE,
                RelationshipState(emptyMap()),
                RelationshipState(mapOf("name" to "john", "surname" to "doe")),
            ),
            0,
            1,
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
                        "CYPHER 25 UNWIND \$events AS e CALL (e) { WHEN e.q = \$q0 THEN " +
                            "MATCH (:\$(e.start.matchLabels) {sourceElementId: e.start.matchProperties.sourceElementId})-[r:\$(e.matchType) {sourceElementId: e.matchProperties.sourceElementId}]->(:\$(e.end.matchLabels) {sourceElementId: e.end.matchProperties.sourceElementId}) " +
                            "SET r += e.setProperties } FINISH",
                        mapOf(
                            "q0" to 0,
                            "events" to
                                listOf(
                                    mapOf(
                                        "q" to 0,
                                        "start" to
                                            mapOf(
                                                "matchLabels" to listOf("SourceEvent"),
                                                "matchProperties" to
                                                    mapOf("sourceElementId" to "start-element-id"),
                                            ),
                                        "end" to
                                            mapOf(
                                                "matchLabels" to listOf("SourceEvent"),
                                                "matchProperties" to
                                                    mapOf("sourceElementId" to "end-element-id"),
                                            ),
                                        "matchType" to "REL",
                                        "matchProperties" to
                                            mapOf("sourceElementId" to "rel-element-id"),
                                        "setProperties" to
                                            mapOf("name" to "john", "surname" to "doe"),
                                    )
                                ),
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
                "REL",
                Node("start-element-id", emptyList(), emptyMap()),
                Node("end-element-id", emptyList(), emptyMap()),
                emptyList(),
                EntityOperation.UPDATE,
                RelationshipState(mapOf("name" to "john", "surname" to "doe")),
                RelationshipState(mapOf("name" to "joe", "surname" to "doe")),
            ),
            0,
            1,
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
                        "CYPHER 25 UNWIND \$events AS e CALL (e) { WHEN e.q = \$q0 THEN " +
                            "MATCH (:\$(e.start.matchLabels) {sourceElementId: e.start.matchProperties.sourceElementId})-[r:\$(e.matchType) {sourceElementId: e.matchProperties.sourceElementId}]->(:\$(e.end.matchLabels) {sourceElementId: e.end.matchProperties.sourceElementId}) " +
                            "SET r += e.setProperties } FINISH",
                        mapOf(
                            "q0" to 0,
                            "events" to
                                listOf(
                                    mapOf(
                                        "q" to 0,
                                        "start" to
                                            mapOf(
                                                "matchLabels" to listOf("SourceEvent"),
                                                "matchProperties" to
                                                    mapOf("sourceElementId" to "start-element-id"),
                                            ),
                                        "end" to
                                            mapOf(
                                                "matchLabels" to listOf("SourceEvent"),
                                                "matchProperties" to
                                                    mapOf("sourceElementId" to "end-element-id"),
                                            ),
                                        "matchType" to "REL",
                                        "matchProperties" to
                                            mapOf("sourceElementId" to "rel-element-id"),
                                        "setProperties" to mapOf("name" to "joe"),
                                    )
                                ),
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
                "REL",
                Node("start-element-id", emptyList(), emptyMap()),
                Node("end-element-id", emptyList(), emptyMap()),
                emptyList(),
                EntityOperation.DELETE,
                RelationshipState(emptyMap()),
                null,
            ),
            0,
            1,
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
                        "CYPHER 25 UNWIND \$events AS e CALL (e) { WHEN e.q = \$q0 THEN " +
                            "MATCH ()-[r:\$(e.matchType) {sourceElementId: e.matchProperties.sourceElementId}]->() " +
                            "DELETE r } FINISH",
                        mapOf(
                            "q0" to 0,
                            "events" to
                                listOf(
                                    mapOf(
                                        "q" to 0,
                                        "matchType" to "REL",
                                        "matchProperties" to
                                            mapOf("sourceElementId" to "rel-element-id"),
                                        "start" to
                                            mapOf(
                                                "matchLabels" to listOf<String>(),
                                                "matchProperties" to emptyMap<String, Any>(),
                                            ),
                                        "end" to
                                            mapOf(
                                                "matchLabels" to listOf<String>(),
                                                "matchProperties" to emptyMap<String, Any>(),
                                            ),
                                        "setProperties" to emptyMap<String, Any>(),
                                    )
                                ),
                        ),
                    ),
                )
            )
        ),
    )
  }

  @Test
  fun `should split over batch size`() {
    val handler = createHandler(batchSize = 2)

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

    result.shouldHaveSize(1)
    result
        .first()
        .shouldHaveSize(4)
        .shouldMatchInOrder(
            { it.messages shouldHaveSize 2 },
            { it.messages shouldHaveSize 2 },
            { it.messages shouldHaveSize 2 },
            { it.messages shouldHaveSize 1 },
        )
  }

  @Test
  fun `should split changes over max batched statement count`() {
    val handler = createHandler(maxBatchedStatements = 2)

    val result =
        handler.handle(
            listOf(
                newChangeEventMessage(createNodePersonEvent("id", 42), 0, 0, 0),
                newChangeEventMessage(createNodePersonEvent("name", "John"), 0, 1, 1),
                newChangeEventMessage(createNodePersonEvent("id", 23), 1, 0, 2),
                newChangeEventMessage(createKnowsRelationshipEvent(1, 2, 3), 2, 0, 3),
                newChangeEventMessage(updateKnowsRelationshipEvent(2, 3, 4), 3, 0, 4),
            )
        )

    result.shouldHaveSize(1)
    result
        .first()
        .shouldHaveSize(2)
        .shouldMatchInOrder({ it.messages shouldHaveSize 4 }, { it.messages shouldHaveSize 1 })
  }

  @Test
  fun `should fail on null 'after' field with node create operation`() {
    val handler = createHandler()

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
            0,
        )

    assertThrows<InvalidDataException> {
      handler.handle(listOf(nodeChangeEventMessage))
    } shouldHaveMessage "create operation requires 'after' field in the event object"
  }

  @Test
  fun `should fail on null 'after' field with relationship create operation`() {
    val handler = createHandler()

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
            0,
        )

    assertThrows<InvalidDataException> {
      handler.handle(listOf(relationshipChangeEventMessage))
    } shouldHaveMessage "create operation requires 'after' field in the event object"
  }

  @Test
  fun `should fail on null 'before' field with node update operation`() {
    val handler = createHandler()

    val nodeChangeEventMessage =
        newChangeEventMessage(
            NodeEvent(
                "person1",
                EntityOperation.UPDATE,
                listOf("Person"),
                mapOf("Person" to listOf(mapOf("id" to 1L))),
                null,
                NodeState(listOf("Person", "Employee"), mapOf("name" to "joe", "surname" to "doe")),
            ),
            1,
            0,
            0,
        )

    assertThrows<InvalidDataException> {
      handler.handle(listOf(nodeChangeEventMessage))
    } shouldHaveMessage "update operation requires 'before' field in the event object"
  }

  @Test
  fun `should fail on null 'before' field with relationship update operation`() {
    val handler = createHandler()

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
            0,
        )

    assertThrows<InvalidDataException> {
      handler.handle(listOf(relationshipChangeEventMessage))
    } shouldHaveMessage "update operation requires 'before' field in the event object"
  }

  @Test
  fun `should fail on null 'after' field with node update operation`() {
    val handler = createHandler()

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
            0,
        )

    assertThrows<InvalidDataException> {
      handler.handle(listOf(nodeChangeEventMessage))
    } shouldHaveMessage "update operation requires 'after' field in the event object"
  }

  @Test
  fun `should fail on null 'after' field with relationship update operation`() {
    val handler = createHandler()

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
            0,
        )

    assertThrows<InvalidDataException> {
      handler.handle(listOf(relationshipChangeEventMessage))
    } shouldHaveMessage "update operation requires 'after' field in the event object"
  }

  private fun createHandler(
      maxBatchedStatements: Int = 1000,
      batchSize: Int = 1000,
  ): BatchedCdcSourceIdHandler =
      BatchedCdcSourceIdHandler(
          "my-topic",
          maxBatchedStatements,
          batchSize,
          "SourceEvent",
          "sourceElementId",
      )

  private fun verify(messages: Iterable<SinkMessage>, expected: Iterable<Iterable<ChangeQuery>>) {
    val handler = createHandler()
    val result = handler.handle(messages)
    result shouldBe expected
  }
}
