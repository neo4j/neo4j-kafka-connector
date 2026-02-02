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
package org.neo4j.connectors.kafka.sink

import io.kotest.assertions.nondeterministic.eventually
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import java.time.LocalDate
import kotlin.time.Duration.Companion.seconds
import org.junit.jupiter.api.Assumptions
import org.junit.jupiter.api.Test
import org.neo4j.caniuse.Neo4j
import org.neo4j.caniuse.Neo4jVersion
import org.neo4j.connectors.kafka.events.Constraint
import org.neo4j.connectors.kafka.events.Meta
import org.neo4j.connectors.kafka.events.NodeChange
import org.neo4j.connectors.kafka.events.NodePayload
import org.neo4j.connectors.kafka.events.OperationType
import org.neo4j.connectors.kafka.events.RelationshipChange
import org.neo4j.connectors.kafka.events.RelationshipNodeChange
import org.neo4j.connectors.kafka.events.RelationshipPayload
import org.neo4j.connectors.kafka.events.Schema
import org.neo4j.connectors.kafka.events.StreamsConstraintType
import org.neo4j.connectors.kafka.events.StreamsTransactionEvent
import org.neo4j.connectors.kafka.testing.TestSupport.runTest
import org.neo4j.connectors.kafka.testing.format.KafkaConverter
import org.neo4j.connectors.kafka.testing.format.KeyValueConverter
import org.neo4j.connectors.kafka.testing.kafka.ConvertingKafkaProducer
import org.neo4j.connectors.kafka.testing.sink.CdcSourceIdStrategy
import org.neo4j.connectors.kafka.testing.sink.Neo4jSink
import org.neo4j.connectors.kafka.testing.sink.SchemaCompatibilityMode
import org.neo4j.connectors.kafka.testing.sink.TopicProducer
import org.neo4j.driver.Session

@KeyValueConverter(key = KafkaConverter.STRING, value = KafkaConverter.STRING)
class Neo4jCdcSourceIdFromStreamsMessageIT {

  companion object {
    private const val TOPIC = "source-id"
  }

  @Neo4jSink(cdcSourceId = [CdcSourceIdStrategy(TOPIC, "SourceEvent", "sourceId")])
  @Test
  fun `should create node`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
      neo4j: Neo4j,
  ) = runTest {
    Assumptions.assumeTrue { neo4j.version >= Neo4jVersion(5, 19, 0) }

    producer.publish(
        StreamsTransactionEvent(
            meta = newMetadata(operation = OperationType.created),
            payload =
                NodePayload(
                    id = "person1",
                    before = null,
                    after =
                        NodeChange(
                            properties = mapOf("id" to 1L, "name" to "john"),
                            labels = listOf("Person"),
                        ),
                ),
            schema = Schema(),
        )
    )

    eventually(30.seconds) {
      val result =
          session
              .run(
                  "MATCH (n:SourceEvent {sourceId: ${'$'}sourceId}) RETURN n",
                  mapOf("sourceId" to "person1"),
              )
              .single()

      result.get("n").asNode() should
          {
            it.labels().toList() shouldContainExactlyInAnyOrder listOf("SourceEvent", "Person")
            it.asMap() shouldBe mapOf("sourceId" to "person1", "id" to 1L, "name" to "john")
          }
    }
  }

  @Neo4jSink(cdcSourceId = [CdcSourceIdStrategy(TOPIC, "SourceEvent", "sourceId")])
  @Test
  fun `should update node`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
      neo4j: Neo4j,
  ) = runTest {
    Assumptions.assumeTrue { neo4j.version >= Neo4jVersion(5, 19, 0) }

    session
        .run(
            "CREATE (n:SourceEvent:Person) SET n = ${'$'}props",
            mapOf("props" to mapOf("sourceId" to "person1", "id" to 1L, "name" to "john")),
        )
        .consume()

    producer.publish(
        StreamsTransactionEvent(
            meta = newMetadata(operation = OperationType.updated),
            payload =
                NodePayload(
                    id = "person1",
                    before = NodeChange(mapOf("id" to 1L, "name" to "john"), listOf("Person")),
                    after =
                        NodeChange(
                            properties =
                                mapOf(
                                    "id" to 5L,
                                    "name" to "john",
                                    "surname" to "doe",
                                    "dob" to LocalDate.of(2000, 1, 1).toEpochDay(),
                                ),
                            labels = listOf("Person", "Employee"),
                        ),
                ),
            schema =
                Schema(
                    constraints =
                        listOf(Constraint("Person", setOf("id"), StreamsConstraintType.UNIQUE))
                ),
        )
    )

    eventually(30.seconds) {
      val result =
          session
              .run(
                  "MATCH (n:SourceEvent {sourceId: ${'$'}sourceId}) RETURN n",
                  mapOf("sourceId" to "person1"),
              )
              .single()

      result.get("n").asNode() should
          {
            it.labels().toList() shouldContainExactlyInAnyOrder
                listOf("SourceEvent", "Person", "Employee")
            it.asMap() shouldBe
                mapOf(
                    "sourceId" to "person1",
                    "id" to 5L,
                    "name" to "john",
                    "surname" to "doe",
                    "dob" to LocalDate.of(2000, 1, 1).toEpochDay(),
                )
          }
    }
  }

  @Neo4jSink(cdcSourceId = [CdcSourceIdStrategy(TOPIC, "SourceEvent", "sourceId")])
  @Test
  fun `should delete node`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
      neo4j: Neo4j,
  ) = runTest {
    Assumptions.assumeTrue { neo4j.version >= Neo4jVersion(5, 19, 0) }

    session
        .run(
            "CREATE (n:SourceEvent) SET n = ${'$'}props",
            mapOf("props" to mapOf("sourceId" to "person1", "id" to 1L, "name" to "john")),
        )
        .consume()

    producer.publish(
        StreamsTransactionEvent(
            meta = newMetadata(operation = OperationType.deleted),
            payload =
                NodePayload(
                    id = "person1",
                    before = NodeChange(mapOf("id" to 1L, "name" to "john"), emptyList()),
                    after = null,
                ),
            schema = Schema(),
        )
    )

    eventually(30.seconds) {
      val result =
          session
              .run(
                  "MATCH (n:SourceEvent {sourceId: ${'$'}sourceId}) RETURN n",
                  mapOf("sourceId" to "person1"),
              )
              .list()

      result shouldHaveSize 0
    }
  }

  @Neo4jSink(cdcSourceId = [CdcSourceIdStrategy(TOPIC, "SourceEvent", "sourceId")])
  @Test
  fun `should create relationship`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
      neo4j: Neo4j,
  ) = runTest {
    Assumptions.assumeTrue { neo4j.version >= Neo4jVersion(5, 19, 0) }

    session
        .run(
            """
            CREATE (n1:SourceEvent:Person) SET n1 = ${'$'}person1
            CREATE (n2:SourceEvent:Person) SET n2 = ${'$'}person2
            """,
            mapOf(
                "person1" to mapOf("sourceId" to "person1", "name" to "john", "surname" to "doe"),
                "person2" to mapOf("sourceId" to "person2", "name" to "mary", "surname" to "doe"),
            ),
        )
        .consume()

    producer.publish(
        StreamsTransactionEvent(
            meta = newMetadata(operation = OperationType.created),
            payload =
                RelationshipPayload(
                    id = "knows1",
                    label = "KNOWS",
                    start = RelationshipNodeChange("person1", listOf("Person"), emptyMap()),
                    end = RelationshipNodeChange("person2", listOf("Person"), emptyMap()),
                    before = null,
                    after =
                        RelationshipChange(mapOf("since" to LocalDate.of(2000, 1, 1).toEpochDay())),
                ),
            schema = Schema(),
        )
    )

    eventually(30.seconds) {
      val result =
          session
              .run(
                  "MATCH (start)-[r:KNOWS {sourceId: ${'$'}sourceId}]->(end) RETURN start, r, end",
                  mapOf("sourceId" to "knows1"),
              )
              .single()

      result.get("r").asRelationship() should
          {
            it.asMap() shouldBe
                mapOf("sourceId" to "knows1", "since" to LocalDate.of(2000, 1, 1).toEpochDay())
          }
      result.get("start").asNode() should
          {
            it.labels().toList() shouldContainExactlyInAnyOrder listOf("SourceEvent", "Person")
            it.asMap() shouldBe mapOf("sourceId" to "person1", "name" to "john", "surname" to "doe")
          }
      result.get("end").asNode() should
          {
            it.labels().toList() shouldContainExactlyInAnyOrder listOf("SourceEvent", "Person")
            it.asMap() shouldBe mapOf("sourceId" to "person2", "name" to "mary", "surname" to "doe")
          }
    }
  }

  @Neo4jSink(cdcSourceId = [CdcSourceIdStrategy(TOPIC, "SourceEvent", "sourceId")])
  @Test
  fun `should update relationship`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
      neo4j: Neo4j,
  ) = runTest {
    Assumptions.assumeTrue { neo4j.version >= Neo4jVersion(5, 19, 0) }

    session
        .run(
            """
            CREATE (n1:SourceEvent:Person) SET n1 = ${'$'}person1
            CREATE (n2:SourceEvent:Person) SET n2 = ${'$'}person2
            CREATE (n1)-[r:KNOWS]->(n2) SET r = ${'$'}knows
            """,
            mapOf(
                "person1" to mapOf("sourceId" to "person1", "name" to "john", "surname" to "doe"),
                "person2" to mapOf("sourceId" to "person2", "name" to "mary", "surname" to "doe"),
                "knows" to
                    mapOf(
                        "sourceId" to "knows1",
                        "since" to LocalDate.of(2000, 1, 1).toEpochDay(),
                        "type" to "friend",
                    ),
            ),
        )
        .consume()

    producer.publish(
        StreamsTransactionEvent(
            meta = newMetadata(operation = OperationType.updated),
            payload =
                RelationshipPayload(
                    id = "knows1",
                    label = "KNOWS",
                    start = RelationshipNodeChange("person1", listOf("Person"), emptyMap()),
                    end = RelationshipNodeChange("person2", listOf("Person"), emptyMap()),
                    before =
                        RelationshipChange(
                            mapOf(
                                "since" to LocalDate.of(2000, 1, 1).toEpochDay(),
                                "type" to "friend",
                            )
                        ),
                    after =
                        RelationshipChange(mapOf("since" to LocalDate.of(1999, 1, 1).toEpochDay())),
                ),
            schema = Schema(),
        )
    )

    eventually(30.seconds) {
      val result =
          session
              .run(
                  "MATCH (start)-[r:KNOWS {sourceId: ${'$'}sourceId}]->(end) RETURN start, r, end",
                  mapOf("sourceId" to "knows1"),
              )
              .single()

      result.get("r").asRelationship() should
          {
            it.asMap() shouldBe
                mapOf("sourceId" to "knows1", "since" to LocalDate.of(1999, 1, 1).toEpochDay())
          }
      result.get("start").asNode() should
          {
            it.labels().toList() shouldContainExactlyInAnyOrder listOf("SourceEvent", "Person")
            it.asMap() shouldBe mapOf("sourceId" to "person1", "name" to "john", "surname" to "doe")
          }
      result.get("end").asNode() should
          {
            it.labels().toList() shouldContainExactlyInAnyOrder listOf("SourceEvent", "Person")
            it.asMap() shouldBe mapOf("sourceId" to "person2", "name" to "mary", "surname" to "doe")
          }
    }
  }

  @Neo4jSink(cdcSourceId = [CdcSourceIdStrategy(TOPIC, "SourceEvent", "sourceId")])
  @Test
  fun `should delete relationship`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
      neo4j: Neo4j,
  ) = runTest {
    Assumptions.assumeTrue { neo4j.version >= Neo4jVersion(5, 19, 0) }

    session
        .run(
            "CREATE (n1:SourceEvent:Person) SET n1 = ${'$'}person1 " +
                "CREATE (n2:SourceEvent:Person) SET n2 = ${'$'}person2 " +
                "CREATE (n1)-[r:KNOWS]->(n2) SET r = ${'$'}knows",
            mapOf(
                "person1" to mapOf("sourceId" to "person1", "name" to "john", "surname" to "doe"),
                "person2" to mapOf("sourceId" to "person2", "name" to "mary", "surname" to "doe"),
                "knows" to
                    mapOf(
                        "sourceId" to "knows1",
                        "since" to LocalDate.of(2000, 1, 1).toEpochDay(),
                        "type" to "friend",
                    ),
            ),
        )
        .consume()

    producer.publish(
        StreamsTransactionEvent(
            meta = newMetadata(operation = OperationType.deleted),
            payload =
                RelationshipPayload(
                    id = "knows1",
                    label = "KNOWS",
                    start = RelationshipNodeChange("person1", listOf("Person"), emptyMap()),
                    end = RelationshipNodeChange("person2", listOf("Person"), emptyMap()),
                    before =
                        RelationshipChange(
                            mapOf(
                                "since" to LocalDate.of(2000, 1, 1).toEpochDay(),
                                "type" to "friend",
                            )
                        ),
                    after = null,
                ),
            schema = Schema(),
        )
    )

    eventually(30.seconds) {
      val result =
          session
              .run(
                  "MATCH ()-[r:KNOWS {sourceId: ${'$'}sourceId}]->() RETURN count(r)",
                  mapOf("sourceId" to "knows1"),
              )
              .single()

      result.get(0).asInt() shouldBe 0
    }
  }

  @Neo4jSink(
      schemaControlKeyCompatibility = SchemaCompatibilityMode.NONE,
      schemaControlValueCompatibility = SchemaCompatibilityMode.NONE,
      cdcSourceId = [CdcSourceIdStrategy(TOPIC, "SourceEvent", "sourceId")],
  )
  @Test
  fun `should sync continuous changes`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
      neo4j: Neo4j,
  ) = runTest {
    Assumptions.assumeTrue { neo4j.version >= Neo4jVersion(5, 19, 0) }

    producer.publish(
        StreamsTransactionEvent(
            meta =
                newMetadata(
                    txId = 1,
                    txEventId = 0,
                    txEventsCount = 1,
                    operation = OperationType.created,
                ),
            payload =
                NodePayload(
                    id = "person1",
                    before = null,
                    after =
                        NodeChange(
                            properties = mapOf("id" to 1L, "name" to "john", "surname" to "doe"),
                            labels = listOf("Person"),
                        ),
                ),
            schema =
                Schema(
                    constraints =
                        listOf(Constraint("Person", setOf("id"), StreamsConstraintType.UNIQUE))
                ),
        )
    )
    producer.publish(
        StreamsTransactionEvent(
            meta =
                newMetadata(
                    txId = 2,
                    txEventId = 0,
                    txEventsCount = 1,
                    operation = OperationType.created,
                ),
            payload =
                NodePayload(
                    id = "person2",
                    before = null,
                    after =
                        NodeChange(
                            properties = mapOf("id" to 2L, "name" to "mary", "surname" to "doe"),
                            labels = listOf("Person"),
                        ),
                ),
            schema =
                Schema(
                    constraints =
                        listOf(Constraint("Person", setOf("id"), StreamsConstraintType.UNIQUE))
                ),
        )
    )

    producer.publish(
        StreamsTransactionEvent(
            meta =
                newMetadata(
                    txId = 3,
                    txEventId = 0,
                    txEventsCount = 1,
                    operation = OperationType.created,
                ),
            payload =
                RelationshipPayload(
                    id = "knows1",
                    label = "KNOWS",
                    start = RelationshipNodeChange("person1", listOf("Person"), mapOf("id" to 1L)),
                    end = RelationshipNodeChange("person2", listOf("Person"), mapOf("id" to 2L)),
                    before = null,
                    after = RelationshipChange(emptyMap()),
                ),
            schema = Schema(),
        )
    )

    eventually(30.seconds) {
      val result =
          session
              .run(
                  """
            MATCH (start:SourceEvent {sourceId: ${'$'}startId})
            MATCH (end:SourceEvent {sourceId: ${'$'}endId})
            MATCH (start)-[r:KNOWS {sourceId: ${'$'}rId}]->(end)
            RETURN start, r, end
          """,
                  mapOf("startId" to "person1", "rId" to "knows1", "endId" to "person2"),
              )
              .single()

      result.get("r").asRelationship() should { it.asMap() shouldBe mapOf("sourceId" to "knows1") }
      result.get("start").asNode() should
          {
            it.labels().toList() shouldContainExactlyInAnyOrder listOf("SourceEvent", "Person")
            it.asMap() shouldBe
                mapOf("sourceId" to "person1", "id" to 1L, "name" to "john", "surname" to "doe")
          }
      result.get("end").asNode() should
          {
            it.labels().toList() shouldContainExactlyInAnyOrder listOf("SourceEvent", "Person")
            it.asMap() shouldBe
                mapOf("sourceId" to "person2", "id" to 2L, "name" to "mary", "surname" to "doe")
          }
    }

    producer.publish(
        StreamsTransactionEvent(
            meta =
                newMetadata(
                    txId = 4,
                    txEventId = 0,
                    txEventsCount = 1,
                    operation = OperationType.updated,
                ),
            payload =
                RelationshipPayload(
                    id = "knows1",
                    label = "KNOWS",
                    start = RelationshipNodeChange("person1", listOf("Person"), mapOf("id" to 1L)),
                    end = RelationshipNodeChange("person2", listOf("Person"), mapOf("id" to 2L)),
                    before = RelationshipChange(emptyMap()),
                    after =
                        RelationshipChange(mapOf("since" to LocalDate.of(2000, 1, 1).toEpochDay())),
                ),
            schema = Schema(),
        )
    )

    eventually(30.seconds) {
      val result =
          session
              .run(
                  """
            MATCH (start:SourceEvent {sourceId: ${'$'}startId})
            MATCH (end:SourceEvent {sourceId: ${'$'}endId})
            MATCH (start)-[r:KNOWS {sourceId: ${'$'}rId}]->(end)
            RETURN r
          """,
                  mapOf("startId" to "person1", "rId" to "knows1", "endId" to "person2"),
              )
              .single()

      result.get("r").asRelationship() should
          {
            it.asMap() shouldBe
                mapOf("sourceId" to "knows1", "since" to LocalDate.of(2000, 1, 1).toEpochDay())
          }
    }

    producer.publish(
        StreamsTransactionEvent(
            meta =
                newMetadata(
                    txId = 5,
                    txEventId = 0,
                    txEventsCount = 3,
                    operation = OperationType.deleted,
                ),
            payload =
                RelationshipPayload(
                    id = "knows1",
                    label = "KNOWS",
                    start = RelationshipNodeChange("person1", listOf("Person"), mapOf("id" to 1L)),
                    end = RelationshipNodeChange("person2", listOf("Person"), mapOf("id" to 2L)),
                    before =
                        RelationshipChange(mapOf("since" to LocalDate.of(2000, 1, 1).toEpochDay())),
                    after = null,
                ),
            schema = Schema(),
        )
    )
    producer.publish(
        StreamsTransactionEvent(
            meta =
                newMetadata(
                    txId = 5,
                    txEventId = 1,
                    txEventsCount = 3,
                    operation = OperationType.deleted,
                ),
            payload =
                NodePayload(
                    id = "person1",
                    before =
                        NodeChange(
                            properties = mapOf("id" to 1L, "name" to "john", "surname" to "doe"),
                            labels = listOf("Person"),
                        ),
                    after = null,
                ),
            schema =
                Schema(
                    constraints =
                        listOf(Constraint("Person", setOf("id"), StreamsConstraintType.UNIQUE))
                ),
        )
    )
    producer.publish(
        StreamsTransactionEvent(
            meta =
                newMetadata(
                    txId = 5,
                    txEventId = 2,
                    txEventsCount = 3,
                    operation = OperationType.deleted,
                ),
            payload =
                NodePayload(
                    id = "person2",
                    before =
                        NodeChange(
                            properties = mapOf("id" to 2L, "name" to "mary", "surname" to "doe"),
                            labels = listOf("Person"),
                        ),
                    after = null,
                ),
            schema =
                Schema(
                    constraints =
                        listOf(Constraint("Person", setOf("id"), StreamsConstraintType.UNIQUE))
                ),
        )
    )

    eventually(30.seconds) {
      val result = session.run("MATCH (n) RETURN count(n)").single()

      result.get(0).asInt() shouldBe 0
    }
  }

  private fun newMetadata(
      txId: Long = 1,
      txEventId: Int = 0,
      txEventsCount: Int = 1,
      operation: OperationType,
  ) =
      Meta(
          timestamp = System.currentTimeMillis(),
          username = "user",
          txId = txId,
          txEventId = txEventId,
          txEventsCount = txEventsCount,
          operation = operation,
      )
}
