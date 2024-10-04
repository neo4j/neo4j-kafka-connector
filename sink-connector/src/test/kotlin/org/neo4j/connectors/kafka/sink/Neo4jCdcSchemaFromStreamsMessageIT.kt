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

import io.kotest.assertions.nondeterministic.continually
import io.kotest.assertions.nondeterministic.eventually
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import java.time.LocalDate
import kotlin.time.Duration.Companion.seconds
import org.junit.jupiter.api.Test
import org.neo4j.connectors.kafka.events.Constraint
import org.neo4j.connectors.kafka.events.EntityType
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
import org.neo4j.connectors.kafka.testing.sink.CdcSchemaStrategy
import org.neo4j.connectors.kafka.testing.sink.Neo4jSink
import org.neo4j.connectors.kafka.testing.sink.SchemaCompatibilityMode
import org.neo4j.connectors.kafka.testing.sink.TopicProducer
import org.neo4j.driver.Session

@KeyValueConverter(key = KafkaConverter.STRING, value = KafkaConverter.STRING)
class Neo4jCdcSchemaFromStreamsMessageIT {

  companion object {
    private const val TOPIC = "schema"
  }

  @Neo4jSink(cdcSchema = [CdcSchemaStrategy(TOPIC)])
  @Test
  fun `should create node`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
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
                            labels = listOf("Person"))),
            schema = defaultSchema()))

    eventually(30.seconds) {
      val result =
          session.run("MATCH (n:Person {id: ${'$'}id}) RETURN n", mapOf("id" to 1L)).single()

      result.get("n").asNode() should
          {
            it.labels() shouldBe listOf("Person")
            it.asMap() shouldBe mapOf("id" to 1L, "name" to "john")
          }
    }
  }

  @Neo4jSink(cdcSchema = [CdcSchemaStrategy(TOPIC)])
  @Test
  fun `should update node`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session
        .run(
            "CREATE (n:Person) SET n = ${'$'}props",
            mapOf("props" to mapOf("id" to 1L, "name" to "john")))
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
                                    "dob" to LocalDate.of(2000, 1, 1).toEpochDay()),
                            labels = listOf("Person", "Employee"))),
            schema = defaultSchema()))

    eventually(30.seconds) {
      val result =
          session.run("MATCH (n:Person {id: ${'$'}id}) RETURN n", mapOf("id" to 5L)).single()

      result.get("n").asNode() should
          {
            it.labels() shouldBe listOf("Person", "Employee")
            it.asMap() shouldBe
                mapOf(
                    "id" to 5L,
                    "name" to "john",
                    "surname" to "doe",
                    "dob" to LocalDate.of(2000, 1, 1).toEpochDay())
          }
    }
  }

  @Neo4jSink(cdcSchema = [CdcSchemaStrategy(TOPIC)])
  @Test
  fun `should delete node`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session
        .run(
            "CREATE (n:Person) SET n = ${'$'}props",
            mapOf("props" to mapOf("id" to 1L, "name" to "john")))
        .consume()

    producer.publish(
        StreamsTransactionEvent(
            meta = newMetadata(operation = OperationType.deleted),
            payload =
                NodePayload(
                    id = "person1",
                    before = NodeChange(mapOf("id" to 1L, "name" to "john"), emptyList()),
                    after = null),
            schema = defaultSchema()))

    eventually(30.seconds) {
      val result = session.run("MATCH (n:Person {id: ${'$'}id}) RETURN n", mapOf("id" to 1L)).list()

      result shouldHaveSize 0
    }
  }

  @Neo4jSink(cdcSchema = [CdcSchemaStrategy(TOPIC)])
  @Test
  fun `should create a node with a null unique constraint property value`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {

    // given a creation event
    // with a unique constraint referencing a property that doesn't exist
    val event =
        StreamsTransactionEvent(
            meta = newMetadata(operation = OperationType.created),
            payload =
                NodePayload(
                    id = "1",
                    type = EntityType.node,
                    before = null,
                    after =
                        NodeChange(
                            mapOf(
                                "first_name" to "john",
                                "last_name" to "smith",
                                "email" to "john@smith.org",
                            ),
                            listOf("Person")),
                ),
            schema =
                Schema(
                    properties =
                        mapOf(
                            "first_name" to "String",
                            "last_name" to "String",
                            "email" to "String",
                        ),
                    constraints =
                        listOf(
                            Constraint("Person", setOf("email"), StreamsConstraintType.UNIQUE),
                            Constraint(
                                "Person",
                                setOf("email"),
                                StreamsConstraintType.NODE_PROPERTY_EXISTS),
                            Constraint("Person", setOf("invalid"), StreamsConstraintType.UNIQUE)),
                ))

    // when the event is published
    producer.publish(event)

    // then a new node should exist
    eventually(30.seconds) {
      val result =
          session
              .run(
                  "MATCH (n:Person {first_name: ${'$'}first_name}) RETURN n",
                  mapOf("first_name" to "john"))
              .list()

      result shouldHaveSize 1
    }
  }

  @Neo4jSink(cdcSchema = [CdcSchemaStrategy(TOPIC)])
  @Test
  fun `should update node with a null unique constraint property value`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {

    // given a database with a single node
    session
        .run(
            "CREATE (n:Person) SET n = ${'$'}props",
            mapOf(
                "props" to
                    mapOf(
                        "first_name" to "john",
                        "last_name" to "smith",
                        "email" to "john@smith.org",
                    )))
        .consume()

    // and an update event adding a new property and label
    // which contains a non-existent constraint
    val updateEvent =
        StreamsTransactionEvent(
            meta = newMetadata(operation = OperationType.updated),
            payload =
                NodePayload(
                    id = "Person",
                    before =
                        NodeChange(
                            mapOf(
                                "first_name" to "john",
                                "last_name" to "smith",
                                "email" to "john@smith.org",
                            ),
                            listOf("Person")),
                    after =
                        NodeChange(
                            properties =
                                mapOf(
                                    "first_name" to "john",
                                    "last_name" to "smith",
                                    "email" to "john@smith.org",
                                    "location" to "London"),
                            labels = listOf("Person", "Employee"))),
            schema =
                Schema(
                    constraints =
                        listOf(
                            Constraint("Person", setOf("email"), StreamsConstraintType.UNIQUE),
                            Constraint(
                                "Person",
                                setOf("email"),
                                StreamsConstraintType.NODE_PROPERTY_EXISTS),
                            Constraint("Person", setOf("invalid"), StreamsConstraintType.UNIQUE)),
                ))

    // when the message is published
    producer.publish(updateEvent)

    // then the node should exist with its additional properties and labels
    eventually(30.seconds) {
      val result =
          session
              .run(
                  "MATCH (n:Person {first_name: ${'$'}first_name}) RETURN n",
                  mapOf("first_name" to "john"))
              .single()

      result.get("n").asNode() should
          {
            it.labels() shouldBe listOf("Person", "Employee")
            it.asMap() shouldBe
                mapOf(
                    "first_name" to "john",
                    "last_name" to "smith",
                    "email" to "john@smith.org",
                    "location" to "London")
          }
    }
  }

  @Neo4jSink(cdcSchema = [CdcSchemaStrategy(TOPIC)])
  @Test
  fun `should delete a node with a null unique constraint property value`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {

    // given a database containing 1 node
    session
        .run(
            "CREATE (n:Person) SET n = ${'$'}props",
            mapOf(
                "props" to
                    mapOf(
                        "first_name" to "john",
                        "last_name" to "smith",
                        "email" to "john@smith.org",
                    )))
        .consume()

    // and a deletion event and with a unique constraint referencing a property that doesn't exist
    val event =
        StreamsTransactionEvent(
            meta = newMetadata(operation = OperationType.deleted),
            payload =
                NodePayload(
                    id = "1",
                    type = EntityType.node,
                    after = null,
                    before =
                        NodeChange(
                            mapOf(
                                "first_name" to "john",
                                "last_name" to "smith",
                                "email" to "john@smith.org",
                            ),
                            listOf("Person")),
                ),
            schema =
                Schema(
                    properties =
                        mapOf(
                            "first_name" to "String",
                            "last_name" to "String",
                            "email" to "String",
                        ),
                    constraints =
                        listOf(
                            Constraint("Person", setOf("email"), StreamsConstraintType.UNIQUE),
                            Constraint(
                                "Person",
                                setOf("email"),
                                StreamsConstraintType.NODE_PROPERTY_EXISTS),
                            Constraint("Person", setOf("invalid"), StreamsConstraintType.UNIQUE)),
                ))

    // when the event is published
    producer.publish(event)

    // then the node should no longer exist
    eventually(10.seconds) {
      val result =
          session
              .run(
                  "MATCH (n:Person {first_name: ${'$'}first_name}) RETURN n",
                  mapOf("first_name" to "john"))
              .list()

      result shouldHaveSize 0
    }
  }

  @Neo4jSink(cdcSchema = [CdcSchemaStrategy(TOPIC)])
  @Test
  fun `should fail to delete a node when no valid unique constraints are provided`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {

    // given a database containing 1 node
    session
        .run(
            "CREATE (n:Person) SET n = ${'$'}props",
            mapOf(
                "props" to
                    mapOf(
                        "first_name" to "john",
                        "last_name" to "smith",
                        "email" to "john@smith.org",
                    )))
        .consume()

    // and a deletion event and with a multiple unique constraints which do not have a valid
    // property
    val event =
        StreamsTransactionEvent(
            meta = newMetadata(operation = OperationType.deleted),
            payload =
                NodePayload(
                    id = "1",
                    type = EntityType.node,
                    after = null,
                    before =
                        NodeChange(
                            mapOf(
                                "first_name" to "john",
                                "last_name" to "smith",
                            ),
                            listOf("Person")),
                ),
            schema =
                Schema(
                    properties =
                        mapOf(
                            "first_name" to "String",
                            "last_name" to "String",
                        ),
                    constraints =
                        listOf(
                            Constraint("Person", setOf("email"), StreamsConstraintType.UNIQUE),
                            Constraint(
                                "Person",
                                setOf("email"),
                                StreamsConstraintType.NODE_PROPERTY_EXISTS),
                            Constraint("Person", setOf("invalid"), StreamsConstraintType.UNIQUE)),
                ))

    // when the event is published
    producer.publish(event)

    // then the node should not be deleted and should still exist
    continually(10.seconds) {
      val result =
          session
              .run(
                  "MATCH (n:Person {first_name: ${'$'}first_name}) RETURN n",
                  mapOf("first_name" to "john"))
              .list()

      result shouldHaveSize 1
    }
  }

  @Neo4jSink(cdcSchema = [CdcSchemaStrategy(TOPIC)])
  @Test
  fun `should create relationship`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session
        .run(
            """
            CREATE (n1:Person) SET n1 = ${'$'}person1
            CREATE (n2:Person) SET n2 = ${'$'}person2
            """,
            mapOf(
                "person1" to mapOf("id" to 1L, "name" to "john", "surname" to "doe"),
                "person2" to mapOf("id" to 2L, "name" to "mary", "surname" to "doe")))
        .consume()

    producer.publish(
        StreamsTransactionEvent(
            meta = newMetadata(operation = OperationType.created),
            payload =
                RelationshipPayload(
                    id = "knows1",
                    label = "KNOWS",
                    start = RelationshipNodeChange("person1", listOf("Person"), mapOf("id" to 1L)),
                    end = RelationshipNodeChange("person2", listOf("Person"), mapOf("id" to 2L)),
                    before = null,
                    after =
                        RelationshipChange(
                            mapOf("since" to LocalDate.of(2000, 1, 1).toEpochDay()))),
            schema = Schema()))

    eventually(30.seconds) {
      val result = session.run("MATCH (start)-[r:KNOWS]->(end) RETURN start, r, end").single()

      result.get("r").asRelationship() should
          {
            it.asMap() shouldBe mapOf("since" to LocalDate.of(2000, 1, 1).toEpochDay())
          }
      result.get("start").asNode() should
          {
            it.labels() shouldBe listOf("Person")
            it.asMap() shouldBe mapOf("id" to 1L, "name" to "john", "surname" to "doe")
          }
      result.get("end").asNode() should
          {
            it.labels() shouldBe listOf("Person")
            it.asMap() shouldBe mapOf("id" to 2L, "name" to "mary", "surname" to "doe")
          }
    }
  }

  @Neo4jSink(cdcSchema = [CdcSchemaStrategy(TOPIC)])
  @Test
  fun `should update relationship`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session
        .run(
            """
            CREATE (n1:Person) SET n1 = ${'$'}person1
            CREATE (n2:Person) SET n2 = ${'$'}person2
            CREATE (n1)-[r:KNOWS]->(n2) SET r = ${'$'}knows
            """,
            mapOf(
                "person1" to mapOf("id" to 1L, "name" to "john", "surname" to "doe"),
                "person2" to mapOf("id" to 2L, "name" to "mary", "surname" to "doe"),
                "knows" to
                    mapOf("since" to LocalDate.of(2000, 1, 1).toEpochDay(), "type" to "friend")))
        .consume()

    producer.publish(
        StreamsTransactionEvent(
            meta = newMetadata(operation = OperationType.updated),
            payload =
                RelationshipPayload(
                    id = "knows1",
                    label = "KNOWS",
                    start = RelationshipNodeChange("person1", listOf("Person"), mapOf("id" to 1L)),
                    end = RelationshipNodeChange("person2", listOf("Person"), mapOf("id" to 2L)),
                    before =
                        RelationshipChange(
                            mapOf(
                                "since" to LocalDate.of(2000, 1, 1).toEpochDay(),
                                "type" to "friend")),
                    after =
                        RelationshipChange(
                            mapOf("since" to LocalDate.of(1999, 1, 1).toEpochDay()))),
            schema = Schema()))

    eventually(30.seconds) {
      val result = session.run("MATCH (start)-[r:KNOWS]->(end) RETURN start, r, end").single()

      result.get("r").asRelationship() should
          {
            it.asMap() shouldBe mapOf("since" to LocalDate.of(1999, 1, 1).toEpochDay())
          }
      result.get("start").asNode() should
          {
            it.labels() shouldBe listOf("Person")
            it.asMap() shouldBe mapOf("id" to 1L, "name" to "john", "surname" to "doe")
          }
      result.get("end").asNode() should
          {
            it.labels() shouldBe listOf("Person")
            it.asMap() shouldBe mapOf("id" to 2L, "name" to "mary", "surname" to "doe")
          }
    }
  }

  @Neo4jSink(cdcSchema = [CdcSchemaStrategy(TOPIC)])
  @Test
  fun `should delete relationship`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session
        .run(
            """
            CREATE (n1:Person) SET n1 = ${'$'}person1
            CREATE (n2:Person) SET n2 = ${'$'}person2
            CREATE (n1)-[r:KNOWS]->(n2) SET r = ${'$'}knows
            """,
            mapOf(
                "person1" to mapOf("id" to 1L, "name" to "john", "surname" to "doe"),
                "person2" to mapOf("id" to 2L, "name" to "mary", "surname" to "doe"),
                "knows" to
                    mapOf("since" to LocalDate.of(2000, 1, 1).toEpochDay(), "type" to "friend")))
        .consume()

    producer.publish(
        StreamsTransactionEvent(
            meta = newMetadata(operation = OperationType.deleted),
            payload =
                RelationshipPayload(
                    id = "knows1",
                    label = "KNOWS",
                    start = RelationshipNodeChange("person1", listOf("Person"), mapOf("id" to 1L)),
                    end = RelationshipNodeChange("person2", listOf("Person"), mapOf("id" to 2L)),
                    before =
                        RelationshipChange(
                            mapOf(
                                "since" to LocalDate.of(2000, 1, 1).toEpochDay(),
                                "type" to "friend")),
                    after = null),
            schema = Schema()))

    eventually(30.seconds) {
      val result = session.run("MATCH ()-[r:KNOWS]->() RETURN count(r)").single()

      result.get(0).asInt() shouldBe 0
    }
  }

  @Neo4jSink(
      schemaControlKeyCompatibility = SchemaCompatibilityMode.NONE,
      schemaControlValueCompatibility = SchemaCompatibilityMode.NONE,
      cdcSchema = [CdcSchemaStrategy(TOPIC)])
  @Test
  fun `should sync continuous changes`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    producer.publish(
        StreamsTransactionEvent(
            meta =
                newMetadata(
                    txId = 1, txEventId = 0, txEventsCount = 1, operation = OperationType.created),
            payload =
                NodePayload(
                    id = "person1",
                    before = null,
                    after =
                        NodeChange(
                            properties = mapOf("id" to 1L, "name" to "john", "surname" to "doe"),
                            labels = listOf("Person"))),
            schema = defaultSchema()))

    producer.publish(
        StreamsTransactionEvent(
            meta =
                newMetadata(
                    txId = 2, txEventId = 0, txEventsCount = 1, operation = OperationType.created),
            payload =
                NodePayload(
                    id = "person2",
                    before = null,
                    after =
                        NodeChange(
                            properties = mapOf("id" to 2L, "name" to "mary", "surname" to "doe"),
                            labels = listOf("Person"))),
            schema = defaultSchema()))

    producer.publish(
        StreamsTransactionEvent(
            meta =
                newMetadata(
                    txId = 3, txEventId = 0, txEventsCount = 1, operation = OperationType.created),
            payload =
                RelationshipPayload(
                    id = "knows1",
                    label = "KNOWS",
                    start = RelationshipNodeChange("person1", listOf("Person"), mapOf("id" to 1L)),
                    end = RelationshipNodeChange("person2", listOf("Person"), mapOf("id" to 2L)),
                    before = null,
                    after = RelationshipChange(emptyMap())),
            schema = Schema()))

    eventually(30.seconds) {
      val result =
          session
              .run(
                  """
            MATCH (start:Person {id: ${'$'}startId})
            MATCH (end:Person {id: ${'$'}endId})
            MATCH (start)-[r:KNOWS]->(end)
            RETURN start, r, end
          """,
                  mapOf("startId" to 1L, "endId" to 2L))
              .single()

      result.get("r").asRelationship() should { it.asMap() shouldBe emptyMap() }
      result.get("start").asNode() should
          {
            it.labels() shouldBe listOf("Person")
            it.asMap() shouldBe mapOf("id" to 1L, "name" to "john", "surname" to "doe")
          }
      result.get("end").asNode() should
          {
            it.labels() shouldBe listOf("Person")
            it.asMap() shouldBe mapOf("id" to 2L, "name" to "mary", "surname" to "doe")
          }
    }

    producer.publish(
        StreamsTransactionEvent(
            meta =
                newMetadata(
                    txId = 4, txEventId = 0, txEventsCount = 1, operation = OperationType.updated),
            payload =
                RelationshipPayload(
                    id = "knows1",
                    label = "KNOWS",
                    start = RelationshipNodeChange("person1", listOf("Person"), mapOf("id" to 1L)),
                    end = RelationshipNodeChange("person2", listOf("Person"), mapOf("id" to 2L)),
                    before = RelationshipChange(emptyMap()),
                    after =
                        RelationshipChange(
                            mapOf("since" to LocalDate.of(2000, 1, 1).toEpochDay()))),
            schema = Schema()))

    eventually(30.seconds) {
      val result =
          session
              .run(
                  """
            MATCH (start:Person {id: ${'$'}startId})
            MATCH (end:Person {id: ${'$'}endId})
            MATCH (start)-[r:KNOWS]->(end)
            RETURN r
          """,
                  mapOf("startId" to 1L, "endId" to 2L))
              .single()

      result.get("r").asRelationship() should
          {
            it.asMap() shouldBe mapOf("since" to LocalDate.of(2000, 1, 1).toEpochDay())
          }
    }

    producer.publish(
        StreamsTransactionEvent(
            meta =
                newMetadata(
                    txId = 5, txEventId = 0, txEventsCount = 3, operation = OperationType.deleted),
            payload =
                RelationshipPayload(
                    id = "knows1",
                    label = "KNOWS",
                    start = RelationshipNodeChange("person1", listOf("Person"), mapOf("id" to 1L)),
                    end = RelationshipNodeChange("person2", listOf("Person"), mapOf("id" to 2L)),
                    before =
                        RelationshipChange(mapOf("since" to LocalDate.of(2000, 1, 1).toEpochDay())),
                    after = null),
            schema = Schema()))

    producer.publish(
        StreamsTransactionEvent(
            meta =
                newMetadata(
                    txId = 5, txEventId = 1, txEventsCount = 3, operation = OperationType.deleted),
            payload =
                NodePayload(
                    id = "person1",
                    before =
                        NodeChange(
                            properties = mapOf("id" to 1L, "name" to "john", "surname" to "doe"),
                            labels = listOf("Person")),
                    after = null),
            schema = defaultSchema()))

    producer.publish(
        StreamsTransactionEvent(
            meta =
                newMetadata(
                    txId = 5, txEventId = 2, txEventsCount = 3, operation = OperationType.deleted),
            payload =
                NodePayload(
                    id = "person2",
                    before =
                        NodeChange(
                            properties = mapOf("id" to 2L, "name" to "mary", "surname" to "doe"),
                            labels = listOf("Person")),
                    after = null),
            schema = defaultSchema()))

    eventually(30.seconds) {
      val result = session.run("MATCH (n) RETURN count(n)").single()

      result.get(0).asInt() shouldBe 0
    }
  }

  private fun defaultSchema() =
      Schema(
          constraints = listOf(Constraint("Person", setOf("id"), StreamsConstraintType.UNIQUE)),
      )

  private fun newMetadata(
      txId: Long = 1,
      txEventId: Int = 0,
      txEventsCount: Int = 1,
      operation: OperationType
  ) =
      Meta(
          timestamp = System.currentTimeMillis(),
          username = "user",
          txId = txId,
          txEventId = txEventId,
          txEventsCount = txEventsCount,
          operation = operation)
}
