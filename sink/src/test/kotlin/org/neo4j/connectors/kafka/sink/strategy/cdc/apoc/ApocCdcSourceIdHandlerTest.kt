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
import io.kotest.matchers.throwable.shouldHaveMessage
import java.time.LocalDate
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
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
import org.neo4j.connectors.kafka.exceptions.InvalidDataException
import org.neo4j.connectors.kafka.sink.ChangeQuery
import org.neo4j.connectors.kafka.sink.SinkMessage
import org.neo4j.connectors.kafka.sink.strategy.TestUtils.newChangeEventMessage
import org.neo4j.connectors.kafka.sink.strategy.TestUtils.randomChangeEvent
import org.neo4j.driver.Query

class ApocCdcSourceIdHandlerWithEOSTest :
    ApocCdcSourceIdHandlerTest(
        "__KafkaOffset",
        """
        |UNWIND ${'$'}events AS e
        |MERGE (k:__KafkaOffset {strategy: ${'$'}strategy, topic: ${'$'}topic, partition: ${'$'}partition}) ON CREATE SET k.offset = -1
        |WITH k, e WHERE e.offset > k.offset
        |WITH k, e ORDER BY e.offset ASC
        |CALL (e) {
        |  CALL apoc.cypher.doIt(e.stmt, e.params) YIELD value FINISH
        |}
        |WITH k, max(e.offset) AS newOffset SET k.offset = newOffset
        |FINISH
        """
            .trimMargin(),
    )

class ApocCdcSourceIdHandlerWithoutEOSTest :
    ApocCdcSourceIdHandlerTest(
        "",
        """
        |UNWIND ${'$'}events AS e
        |WITH e ORDER BY e.offset ASC
        |CALL (e) {
        |  CALL apoc.cypher.doIt(e.stmt, e.params) YIELD value FINISH
        |}
        |FINISH
        """
            .trimMargin(),
    )

abstract class ApocCdcSourceIdHandlerTest(val eosOffsetLabel: String, val expectedQuery: String) {
  private val neo4j =
      Neo4j(Neo4jVersion(2025, 12, 1), Neo4jEdition.ENTERPRISE, Neo4jDeploymentType.SELF_MANAGED)

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
                        expectedQuery,
                        mapOf(
                            "events" to
                                listOf(
                                    mapOf(
                                        "offset" to 0L,
                                        "stmt" to
                                            "MERGE (n:SourceEvent {sourceElementId: \$e.matchProperties.sourceElementId}) SET n += \$e.setProperties SET n:\$(\$e.addLabels) REMOVE n:\$(\$e.removeLabels)",
                                        "params" to
                                            mapOf(
                                                "e" to
                                                    mapOf(
                                                        "matchProperties" to
                                                            mapOf(
                                                                "sourceElementId" to
                                                                    "node-element-id"
                                                            ),
                                                        "setProperties" to
                                                            mapOf(
                                                                "name" to "john",
                                                                "surname" to "doe",
                                                            ),
                                                        "addLabels" to emptySet<String>(),
                                                        "removeLabels" to emptySet<String>(),
                                                    )
                                            ),
                                    )
                                ),
                            "topic" to "my-topic",
                            "partition" to 0,
                            "strategy" to "CDC_SOURCE_ID",
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
                                            "MERGE (n:SourceEvent {sourceElementId: \$e.matchProperties.sourceElementId}) SET n += \$e.setProperties SET n:\$(\$e.addLabels) REMOVE n:\$(\$e.removeLabels)",
                                        "params" to
                                            mapOf(
                                                "e" to
                                                    mapOf(
                                                        "matchProperties" to
                                                            mapOf(
                                                                "sourceElementId" to
                                                                    "node-element-id"
                                                            ),
                                                        "setProperties" to
                                                            mapOf(
                                                                "name" to "john",
                                                                "surname" to "doe",
                                                                "dob" to LocalDate.of(1990, 1, 1),
                                                            ),
                                                        "addLabels" to setOf("Person"),
                                                        "removeLabels" to emptySet<String>(),
                                                    )
                                            ),
                                    )
                                ),
                            "topic" to "my-topic",
                            "partition" to 0,
                            "strategy" to "CDC_SOURCE_ID",
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
                                            "MERGE (n:SourceEvent {sourceElementId: \$e.matchProperties.sourceElementId}) SET n += \$e.setProperties SET n:\$(\$e.addLabels) REMOVE n:\$(\$e.removeLabels)",
                                        "params" to
                                            mapOf(
                                                "e" to
                                                    mapOf(
                                                        "matchProperties" to
                                                            mapOf(
                                                                "sourceElementId" to
                                                                    "node-element-id"
                                                            ),
                                                        "setProperties" to
                                                            mapOf(
                                                                "name" to "john",
                                                                "surname" to "doe",
                                                                "dob" to LocalDate.of(1990, 1, 1),
                                                            ),
                                                        "addLabels" to setOf("Person", "Employee"),
                                                        "removeLabels" to emptySet<String>(),
                                                    )
                                            ),
                                    )
                                ),
                            "topic" to "my-topic",
                            "partition" to 0,
                            "strategy" to "CDC_SOURCE_ID",
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
                        expectedQuery,
                        mapOf(
                            "events" to
                                listOf(
                                    mapOf(
                                        "offset" to 0L,
                                        "stmt" to
                                            "MERGE (n:SourceEvent {sourceElementId: \$e.matchProperties.sourceElementId}) SET n += \$e.setProperties SET n:\$(\$e.addLabels) REMOVE n:\$(\$e.removeLabels)",
                                        "params" to
                                            mapOf(
                                                "e" to
                                                    mapOf(
                                                        "matchProperties" to
                                                            mapOf(
                                                                "sourceElementId" to
                                                                    "node-element-id"
                                                            ),
                                                        "setProperties" to
                                                            mapOf(
                                                                "name" to "joe",
                                                                "dob" to LocalDate.of(2000, 1, 1),
                                                            ),
                                                        "addLabels" to emptySet<String>(),
                                                        "removeLabels" to emptySet<String>(),
                                                    )
                                            ),
                                    )
                                ),
                            "topic" to "my-topic",
                            "partition" to 0,
                            "strategy" to "CDC_SOURCE_ID",
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
                                            "MERGE (n:SourceEvent {sourceElementId: \$e.matchProperties.sourceElementId}) SET n += \$e.setProperties SET n:\$(\$e.addLabels) REMOVE n:\$(\$e.removeLabels)",
                                        "params" to
                                            mapOf(
                                                "e" to
                                                    mapOf(
                                                        "matchProperties" to
                                                            mapOf(
                                                                "sourceElementId" to
                                                                    "node-element-id"
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
                            "strategy" to "CDC_SOURCE_ID",
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
                                            "MERGE (n:SourceEvent {sourceElementId: \$e.matchProperties.sourceElementId}) SET n += \$e.setProperties SET n:\$(\$e.addLabels) REMOVE n:\$(\$e.removeLabels)",
                                        "params" to
                                            mapOf(
                                                "e" to
                                                    mapOf(
                                                        "matchProperties" to
                                                            mapOf(
                                                                "sourceElementId" to
                                                                    "node-element-id"
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
                            "strategy" to "CDC_SOURCE_ID",
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
                        expectedQuery,
                        mapOf(
                            "events" to
                                listOf(
                                    mapOf(
                                        "offset" to 0L,
                                        "stmt" to
                                            "MATCH (n:SourceEvent {sourceElementId: \$e.matchProperties.sourceElementId}) DELETE n",
                                        "params" to
                                            mapOf(
                                                "e" to
                                                    mapOf(
                                                        "matchProperties" to
                                                            mapOf(
                                                                "sourceElementId" to
                                                                    "node-element-id"
                                                            ),
                                                        "setProperties" to emptyMap<String, Any>(),
                                                        "addLabels" to emptySet<String>(),
                                                        "removeLabels" to emptySet<String>(),
                                                    )
                                            ),
                                    )
                                ),
                            "topic" to "my-topic",
                            "partition" to 0,
                            "strategy" to "CDC_SOURCE_ID",
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
                        expectedQuery,
                        mapOf(
                            "events" to
                                listOf(
                                    mapOf(
                                        "offset" to 0L,
                                        "stmt" to
                                            "MATCH (start:SourceEvent {sourceElementId: \$e.start.matchProperties.sourceElementId}) MATCH (end:SourceEvent {sourceElementId: \$e.end.matchProperties.sourceElementId}) MERGE (start)-[r:REL {sourceElementId: \$e.matchProperties.sourceElementId}]->(end) SET r += \$e.setProperties",
                                        "params" to
                                            mapOf(
                                                "e" to
                                                    mapOf(
                                                        "start" to
                                                            mapOf(
                                                                "matchProperties" to
                                                                    mapOf(
                                                                        "sourceElementId" to
                                                                            "start-element-id"
                                                                    )
                                                            ),
                                                        "end" to
                                                            mapOf(
                                                                "matchProperties" to
                                                                    mapOf(
                                                                        "sourceElementId" to
                                                                            "end-element-id"
                                                                    )
                                                            ),
                                                        "matchProperties" to
                                                            mapOf(
                                                                "sourceElementId" to
                                                                    "rel-element-id"
                                                            ),
                                                        "setProperties" to
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
                            "strategy" to "CDC_SOURCE_ID",
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
                        expectedQuery,
                        mapOf(
                            "events" to
                                listOf(
                                    mapOf(
                                        "offset" to 0L,
                                        "stmt" to
                                            "MATCH (:SourceEvent {sourceElementId: \$e.start.matchProperties.sourceElementId})-[r:REL {sourceElementId: \$e.matchProperties.sourceElementId}]->(:SourceEvent {sourceElementId: \$e.end.matchProperties.sourceElementId}) SET r += \$e.setProperties",
                                        "params" to
                                            mapOf(
                                                "e" to
                                                    mapOf(
                                                        "start" to
                                                            mapOf(
                                                                "matchProperties" to
                                                                    mapOf(
                                                                        "sourceElementId" to
                                                                            "start-element-id"
                                                                    )
                                                            ),
                                                        "end" to
                                                            mapOf(
                                                                "matchProperties" to
                                                                    mapOf(
                                                                        "sourceElementId" to
                                                                            "end-element-id"
                                                                    )
                                                            ),
                                                        "matchProperties" to
                                                            mapOf(
                                                                "sourceElementId" to
                                                                    "rel-element-id"
                                                            ),
                                                        "setProperties" to
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
                            "strategy" to "CDC_SOURCE_ID",
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
                                            "MATCH (:SourceEvent {sourceElementId: \$e.start.matchProperties.sourceElementId})-[r:REL {sourceElementId: \$e.matchProperties.sourceElementId}]->(:SourceEvent {sourceElementId: \$e.end.matchProperties.sourceElementId}) SET r += \$e.setProperties",
                                        "params" to
                                            mapOf(
                                                "e" to
                                                    mapOf(
                                                        "start" to
                                                            mapOf(
                                                                "matchProperties" to
                                                                    mapOf(
                                                                        "sourceElementId" to
                                                                            "start-element-id"
                                                                    )
                                                            ),
                                                        "end" to
                                                            mapOf(
                                                                "matchProperties" to
                                                                    mapOf(
                                                                        "sourceElementId" to
                                                                            "end-element-id"
                                                                    )
                                                            ),
                                                        "matchProperties" to
                                                            mapOf(
                                                                "sourceElementId" to
                                                                    "rel-element-id"
                                                            ),
                                                        "setProperties" to mapOf("name" to "joe"),
                                                    )
                                            ),
                                    )
                                ),
                            "topic" to "my-topic",
                            "partition" to 0,
                            "strategy" to "CDC_SOURCE_ID",
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
                        expectedQuery,
                        mapOf(
                            "events" to
                                listOf(
                                    mapOf(
                                        "offset" to 0L,
                                        "stmt" to
                                            "MATCH ()-[r:REL {sourceElementId: \$e.matchProperties.sourceElementId}]->() DELETE r",
                                        "params" to
                                            mapOf(
                                                "e" to
                                                    mapOf(
                                                        "start" to
                                                            mapOf(
                                                                "matchProperties" to
                                                                    emptyMap<String, Any>()
                                                            ),
                                                        "end" to
                                                            mapOf(
                                                                "matchProperties" to
                                                                    emptyMap<String, Any>()
                                                            ),
                                                        "matchProperties" to
                                                            mapOf(
                                                                "sourceElementId" to
                                                                    "rel-element-id"
                                                            ),
                                                        "setProperties" to emptyMap<String, Any>(),
                                                    )
                                            ),
                                    )
                                ),
                            "topic" to "my-topic",
                            "partition" to 0,
                            "strategy" to "CDC_SOURCE_ID",
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

  private fun createHandler(batchSize: Int = 1000): ApocCdcSourceIdHandler =
      ApocCdcSourceIdHandler(
          "my-topic",
          neo4j,
          batchSize,
          eosOffsetLabel,
          "SourceEvent",
          "sourceElementId",
      )

  private fun verify(messages: Iterable<SinkMessage>, expected: Iterable<Iterable<ChangeQuery>>) {
    val handler = createHandler()
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
