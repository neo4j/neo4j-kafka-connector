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
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import java.time.LocalDate
import java.time.ZonedDateTime
import kotlin.time.Duration.Companion.seconds
import org.junit.jupiter.api.Test
import org.neo4j.caniuse.Neo4j
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
import org.neo4j.connectors.kafka.testing.TestSupport.runTest
import org.neo4j.connectors.kafka.testing.createRelationshipKeyConstraint
import org.neo4j.connectors.kafka.testing.format.KafkaConverter
import org.neo4j.connectors.kafka.testing.format.KeyValueConverter
import org.neo4j.connectors.kafka.testing.kafka.ConvertingKafkaProducer
import org.neo4j.connectors.kafka.testing.sink.CdcSchemaStrategy
import org.neo4j.connectors.kafka.testing.sink.Neo4jSink
import org.neo4j.connectors.kafka.testing.sink.Neo4jSinkRegistration
import org.neo4j.connectors.kafka.testing.sink.SchemaCompatibilityMode
import org.neo4j.connectors.kafka.testing.sink.TopicProducer
import org.neo4j.driver.Session

abstract class Neo4jCdcSchemaIT {

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
        newEvent(
            0,
            0,
            NodeEvent(
                "person1",
                EntityOperation.CREATE,
                listOf("Person"),
                mapOf("Person" to listOf(mapOf("id" to 5L))),
                null,
                NodeState(emptyList(), mapOf("id" to 5L, "name" to "john", "surname" to "doe")),
            ),
        ),
    )

    eventually(30.seconds) {
      val result =
          session.run("MATCH (n:Person {id: ${'$'}id}) RETURN n", mapOf("id" to 5)).single()

      result.get("n").asNode() should
          {
            it.labels() shouldBe listOf("Person")
            it.asMap() shouldBe mapOf("id" to 5L, "name" to "john", "surname" to "doe")
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
            mapOf("props" to mapOf("id" to 1L, "name" to "john")),
        )
        .consume()

    producer.publish(
        newEvent(
            0,
            0,
            NodeEvent(
                "person1",
                EntityOperation.UPDATE,
                listOf("Person"),
                mapOf("Person" to listOf(mapOf("id" to 1L))),
                NodeState(listOf("Person"), mapOf("id" to 1L, "name" to "john")),
                NodeState(
                    listOf("Person", "Employee"),
                    mapOf(
                        "id" to 5L,
                        "name" to "john",
                        "surname" to "doe",
                        "dob" to LocalDate.of(2000, 1, 1),
                    ),
                ),
            ),
        ),
    )

    eventually(30.seconds) {
      val result =
          session.run("MATCH (n:Person {id: ${'$'}id}) RETURN n", mapOf("id" to 5)).single()

      result.get("n").asNode() should
          {
            it.labels() shouldBe listOf("Person", "Employee")
            it.asMap() shouldBe
                mapOf(
                    "id" to 5L,
                    "name" to "john",
                    "surname" to "doe",
                    "dob" to LocalDate.of(2000, 1, 1),
                )
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
            mapOf("props" to mapOf("id" to 1L, "name" to "john")),
        )
        .consume()

    producer.publish(
        newEvent(
            0,
            0,
            NodeEvent(
                "person1",
                EntityOperation.DELETE,
                listOf("Person"),
                mapOf("Person" to listOf(mapOf("id" to 1L))),
                NodeState(emptyList(), mapOf("id" to 1L, "name" to "john")),
                null,
            ),
        ),
    )

    eventually(30.seconds) {
      val result = session.run("MATCH (n:Person {id: ${'$'}id}) RETURN n", mapOf("id" to 1)).list()

      result shouldHaveSize 0
    }
  }

  @Neo4jSink(cdcSchema = [CdcSchemaStrategy(TOPIC)])
  @Test
  fun `should fail to delete a node when keys is empty `(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
      sink: Neo4jSinkRegistration
  ) = runTest {
    session
        .run(
            "CREATE (n:Person) SET n = ${'$'}props",
            mapOf("props" to mapOf("id" to 1L, "name" to "john")),
        )
        .consume()

    producer.publish(
        newEvent(
            0,
            0,
            NodeEvent(
                "person1",
                EntityOperation.DELETE,
                listOf("Person"),
                emptyMap<String, List<Map<String, Any>>>(),
                NodeState(emptyList(), mapOf("id" to 1L, "name" to "john")),
                null,
            ),
        ),
    )

    // sink connector should transition into FAILED state
    eventually(30.seconds) {
      val tasks = sink.getConnectorTasksForStatusCheck()
      tasks shouldHaveSize 1
      tasks.get(0).get("state").asText() shouldBe "FAILED"
    }

    // and the node should not be deleted and should still exist
    val result = session.run("MATCH (n:Person {id: ${'$'}id}) RETURN n", mapOf("id" to 1)).list()

    result shouldHaveSize 1
  }

  @Neo4jSink(cdcSchema = [CdcSchemaStrategy(TOPIC)])
  @Test
  fun `should fail to delete a node when keys is effectively empty `(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
      sink: Neo4jSinkRegistration
  ) = runTest {
    session
        .run(
            "CREATE (n:Person) SET n = ${'$'}props",
            mapOf("props" to mapOf("id" to 1L, "name" to "john")),
        )
        .consume()

    producer.publish(
        newEvent(
            0,
            0,
            NodeEvent(
                "person1",
                EntityOperation.DELETE,
                listOf("Person"),
                mapOf("Person" to emptyList<Map<String, Any>>()),
                NodeState(emptyList(), mapOf("id" to 1L, "name" to "john")),
                null,
            ),
        ),
    )

    // sink connector should transition into FAILED state
    eventually(30.seconds) {
      val tasks = sink.getConnectorTasksForStatusCheck()
      tasks shouldHaveSize 1
      tasks.get(0).get("state").asText() shouldBe "FAILED"
    }

    // and the node should not be deleted and should still exist
    val result = session.run("MATCH (n:Person {id: ${'$'}id}) RETURN n", mapOf("id" to 1)).list()

    result shouldHaveSize 1
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
                "person2" to mapOf("id" to 2L, "name" to "mary", "surname" to "doe"),
            ),
        )
        .consume()

    producer.publish(
        newEvent(
            0,
            0,
            RelationshipEvent(
                "knows1",
                "KNOWS",
                Node("person1", listOf("Person"), mapOf("Person" to listOf(mapOf("id" to 1L)))),
                Node("person2", listOf("Person"), mapOf("Person" to listOf(mapOf("id" to 2L)))),
                emptyList(),
                EntityOperation.CREATE,
                null,
                RelationshipState(mapOf("since" to LocalDate.of(2000, 1, 1))),
            ),
        ),
    )

    eventually(30.seconds) {
      val result = session.run("MATCH (start)-[r:KNOWS]->(end) RETURN start, r, end").single()

      result.get("r").asRelationship() should
          {
            it.asMap() shouldBe mapOf("since" to LocalDate.of(2000, 1, 1))
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
  fun `should fail to create a relationship when start node keys is empty`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
      sink: Neo4jSinkRegistration,
  ) = runTest {
    session
        .run(
            """
            CREATE (n1:Person) SET n1 = ${'$'}person1
            CREATE (n2:Person) SET n2 = ${'$'}person2
            """,
            mapOf(
                "person1" to mapOf("id" to 1L, "name" to "john", "surname" to "doe"),
                "person2" to mapOf("id" to 2L, "name" to "mary", "surname" to "doe"),
            ),
        )
        .consume()

    producer.publish(
        newEvent(
            0,
            0,
            RelationshipEvent(
                "knows1",
                "KNOWS",
                Node("person1", listOf("Person"), emptyMap()),
                Node("person2", listOf("Person"), mapOf("Person" to listOf(mapOf("id" to 2L)))),
                emptyList(),
                EntityOperation.CREATE,
                null,
                RelationshipState(mapOf("since" to LocalDate.of(2000, 1, 1), "type" to "friend")),
            ),
        ),
    )

    // sink connector should transition into FAILED state
    eventually(30.seconds) {
      val tasks = sink.getConnectorTasksForStatusCheck()
      tasks shouldHaveSize 1
      tasks.get(0).get("state").asText() shouldBe "FAILED"
    }

    // and relationship should not be created
    val result = session.run("MATCH (start)-[r:KNOWS]->(end) RETURN count(r)").single()
    result.get(0).asInt() shouldBe 0
  }

  @Neo4jSink(cdcSchema = [CdcSchemaStrategy(TOPIC)])
  @Test
  fun `should fail to create a relationship when start node keys is effectively empty`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
      sink: Neo4jSinkRegistration,
  ) = runTest {
    session
        .run(
            """
            CREATE (n1:Person) SET n1 = ${'$'}person1
            CREATE (n2:Person) SET n2 = ${'$'}person2
            """,
            mapOf(
                "person1" to mapOf("id" to 1L, "name" to "john", "surname" to "doe"),
                "person2" to mapOf("id" to 2L, "name" to "mary", "surname" to "doe"),
            ),
        )
        .consume()

    producer.publish(
        newEvent(
            0,
            0,
            RelationshipEvent(
                "knows1",
                "KNOWS",
                Node("person1", listOf("Person"), mapOf("Person" to emptyList())),
                Node("person2", listOf("Person"), mapOf("Person" to listOf(mapOf("id" to 2L)))),
                emptyList(),
                EntityOperation.CREATE,
                null,
                RelationshipState(mapOf("since" to LocalDate.of(2000, 1, 1), "type" to "friend")),
            ),
        ),
    )

    // sink connector should transition into FAILED state
    eventually(30.seconds) {
      val tasks = sink.getConnectorTasksForStatusCheck()
      tasks shouldHaveSize 1
      tasks.get(0).get("state").asText() shouldBe "FAILED"
    }

    // and relationship should not be created
    val result = session.run("MATCH (start)-[r:KNOWS]->(end) RETURN count(r)").single()
    result.get(0).asInt() shouldBe 0
  }

  @Neo4jSink(cdcSchema = [CdcSchemaStrategy(TOPIC)])
  @Test
  fun `should fail to create a relationship when end node keys is empty`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
      sink: Neo4jSinkRegistration,
  ) = runTest {
    session
        .run(
            """
            CREATE (n1:Person) SET n1 = ${'$'}person1
            CREATE (n2:Person) SET n2 = ${'$'}person2
            """,
            mapOf(
                "person1" to mapOf("id" to 1L, "name" to "john", "surname" to "doe"),
                "person2" to mapOf("id" to 2L, "name" to "mary", "surname" to "doe"),
            ),
        )
        .consume()

    producer.publish(
        newEvent(
            0,
            0,
            RelationshipEvent(
                "knows1",
                "KNOWS",
                Node("person1", listOf("Person"), mapOf("Person" to listOf(mapOf("id" to 1L)))),
                Node("person2", listOf("Person"), emptyMap()),
                emptyList(),
                EntityOperation.CREATE,
                null,
                RelationshipState(mapOf("since" to LocalDate.of(2000, 1, 1), "type" to "friend")),
            ),
        ),
    )

    // sink connector should transition into FAILED state
    eventually(30.seconds) {
      val tasks = sink.getConnectorTasksForStatusCheck()
      tasks shouldHaveSize 1
      tasks.get(0).get("state").asText() shouldBe "FAILED"
    }

    // and relationship should not be created
    val result = session.run("MATCH (start)-[r:KNOWS]->(end) RETURN count(r)").single()
    result.get(0).asInt() shouldBe 0
  }

  @Neo4jSink(cdcSchema = [CdcSchemaStrategy(TOPIC)])
  @Test
  fun `should fail to create a relationship when end node keys is effectively empty`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
      sink: Neo4jSinkRegistration,
  ) = runTest {
    session
        .run(
            """
            CREATE (n1:Person) SET n1 = ${'$'}person1
            CREATE (n2:Person) SET n2 = ${'$'}person2
            """,
            mapOf(
                "person1" to mapOf("id" to 1L, "name" to "john", "surname" to "doe"),
                "person2" to mapOf("id" to 2L, "name" to "mary", "surname" to "doe"),
            ),
        )
        .consume()

    producer.publish(
        newEvent(
            0,
            0,
            RelationshipEvent(
                "knows1",
                "KNOWS",
                Node("person1", listOf("Person"), mapOf("Person" to listOf(mapOf("id" to 2L)))),
                Node("person2", listOf("Person"), mapOf("Person" to emptyList())),
                emptyList(),
                EntityOperation.CREATE,
                null,
                RelationshipState(mapOf("since" to LocalDate.of(2000, 1, 1), "type" to "friend")),
            ),
        ),
    )

    // sink connector should transition into FAILED state
    eventually(30.seconds) {
      val tasks = sink.getConnectorTasksForStatusCheck()
      tasks shouldHaveSize 1
      tasks.get(0).get("state").asText() shouldBe "FAILED"
    }

    // and relationship should not be created
    val result = session.run("MATCH ()-[r:KNOWS]->() RETURN count(r)").single()
    result.get(0).asInt() shouldBe 0
  }

  @Neo4jSink(cdcSchema = [CdcSchemaStrategy(TOPIC)])
  @Test
  fun `should fail to create a relationship when start and end node keys is empty even if it has its own keys`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
      sink: Neo4jSinkRegistration,
  ) = runTest {
    session
        .run(
            """
            CREATE (n1:Person) SET n1 = ${'$'}person1
            CREATE (n2:Person) SET n2 = ${'$'}person2
            """,
            mapOf(
                "person1" to mapOf("id" to 1L, "name" to "john", "surname" to "doe"),
                "person2" to mapOf("id" to 2L, "name" to "mary", "surname" to "doe"),
            ),
        )
        .consume()

    producer.publish(
        newEvent(
            0,
            0,
            RelationshipEvent(
                "knows1",
                "KNOWS",
                Node("person1", listOf("Person"), mapOf("Person" to emptyList())),
                Node("person2", listOf("Person"), emptyMap()),
                listOf(mapOf("id" to 1L)),
                EntityOperation.CREATE,
                null,
                RelationshipState(
                    mapOf(
                        "id" to 1L,
                        "since" to LocalDate.of(2000, 1, 1),
                        "type" to "friend",
                    ),
                ),
            ),
        ),
    )

    // sink connector should transition into FAILED state
    eventually(30.seconds) {
      val tasks = sink.getConnectorTasksForStatusCheck()
      tasks shouldHaveSize 1
      tasks.get(0).get("state").asText() shouldBe "FAILED"
    }

    // and relationship should not be created
    val result = session.run("MATCH ()-[r:KNOWS]->() RETURN count(r)").single()
    result.get(0).asInt() shouldBe 0
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
                "knows" to mapOf("since" to LocalDate.of(2000, 1, 1), "type" to "friend"),
            ),
        )
        .consume()

    producer.publish(
        newEvent(
            0,
            0,
            RelationshipEvent(
                "knows1",
                "KNOWS",
                Node("person1", listOf("Person"), mapOf("Person" to listOf(mapOf("id" to 1L)))),
                Node("person2", listOf("Person"), mapOf("Person" to listOf(mapOf("id" to 2L)))),
                emptyList(),
                EntityOperation.UPDATE,
                RelationshipState(mapOf("since" to LocalDate.of(2000, 1, 1), "type" to "friend")),
                RelationshipState(mapOf("since" to LocalDate.of(1999, 1, 1))),
            ),
        ),
    )

    eventually(30.seconds) {
      val result = session.run("MATCH (start)-[r:KNOWS]->(end) RETURN start, r, end").single()

      result.get("r").asRelationship() should
          {
            it.asMap() shouldBe mapOf("since" to LocalDate.of(1999, 1, 1))
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
  fun `should fail to update a relationship when start node keys is empty`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
      sink: Neo4jSinkRegistration,
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
                "knows" to mapOf("since" to LocalDate.of(2000, 1, 1), "type" to "friend"),
            ),
        )
        .consume()

    producer.publish(
        newEvent(
            0,
            0,
            RelationshipEvent(
                "knows1",
                "KNOWS",
                Node("person1", listOf("Person"), emptyMap()),
                Node("person2", listOf("Person"), mapOf("Person" to listOf(mapOf("id" to 2L)))),
                emptyList(),
                EntityOperation.UPDATE,
                RelationshipState(mapOf("since" to LocalDate.of(2000, 1, 1), "type" to "friend")),
                RelationshipState(mapOf("since" to LocalDate.of(1999, 1, 1))),
            ),
        ),
    )

    // sink connector should transition into FAILED state
    eventually(30.seconds) {
      val tasks = sink.getConnectorTasksForStatusCheck()
      tasks shouldHaveSize 1
      tasks.get(0).get("state").asText() shouldBe "FAILED"
    }

    // and relationship should not be updated
    val result = session.run("MATCH (start)-[r:KNOWS]->(end) RETURN start, r, end").single()

    result.get("r").asRelationship() should
        {
          it.asMap() shouldBe mapOf("since" to LocalDate.of(2000, 1, 1), "type" to "friend")
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

  @Neo4jSink(cdcSchema = [CdcSchemaStrategy(TOPIC)])
  @Test
  fun `should fail to update a relationship when start node keys is effectively empty`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
      sink: Neo4jSinkRegistration,
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
                "knows" to mapOf("since" to LocalDate.of(2000, 1, 1), "type" to "friend"),
            ),
        )
        .consume()

    producer.publish(
        newEvent(
            0,
            0,
            RelationshipEvent(
                "knows1",
                "KNOWS",
                Node("person1", listOf("Person"), mapOf("Person" to emptyList())),
                Node("person2", listOf("Person"), mapOf("Person" to listOf(mapOf("id" to 2L)))),
                emptyList(),
                EntityOperation.UPDATE,
                RelationshipState(mapOf("since" to LocalDate.of(2000, 1, 1), "type" to "friend")),
                RelationshipState(mapOf("since" to LocalDate.of(1999, 1, 1))),
            ),
        ),
    )

    // sink connector should transition into FAILED state
    eventually(30.seconds) {
      val tasks = sink.getConnectorTasksForStatusCheck()
      tasks shouldHaveSize 1
      tasks.get(0).get("state").asText() shouldBe "FAILED"
    }

    // and relationship should not be updated
    val result = session.run("MATCH (start)-[r:KNOWS]->(end) RETURN start, r, end").single()

    result.get("r").asRelationship() should
        {
          it.asMap() shouldBe mapOf("since" to LocalDate.of(2000, 1, 1), "type" to "friend")
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

  @Neo4jSink(cdcSchema = [CdcSchemaStrategy(TOPIC)])
  @Test
  fun `should fail to update a relationship when end node keys is empty`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
      sink: Neo4jSinkRegistration,
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
                "knows" to mapOf("since" to LocalDate.of(2000, 1, 1), "type" to "friend"),
            ),
        )
        .consume()

    producer.publish(
        newEvent(
            0,
            0,
            RelationshipEvent(
                "knows1",
                "KNOWS",
                Node("person1", listOf("Person"), mapOf("Person" to listOf(mapOf("id" to 1L)))),
                Node("person2", listOf("Person"), emptyMap()),
                emptyList(),
                EntityOperation.UPDATE,
                RelationshipState(mapOf("since" to LocalDate.of(2000, 1, 1), "type" to "friend")),
                RelationshipState(mapOf("since" to LocalDate.of(1999, 1, 1))),
            ),
        ),
    )

    // sink connector should transition into FAILED state
    eventually(30.seconds) {
      val tasks = sink.getConnectorTasksForStatusCheck()
      tasks shouldHaveSize 1
      tasks.get(0).get("state").asText() shouldBe "FAILED"
    }

    // and relationship should not be updated
    val result = session.run("MATCH (start)-[r:KNOWS]->(end) RETURN start, r, end").single()

    result.get("r").asRelationship() should
        {
          it.asMap() shouldBe mapOf("since" to LocalDate.of(2000, 1, 1), "type" to "friend")
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

  @Neo4jSink(cdcSchema = [CdcSchemaStrategy(TOPIC)])
  @Test
  fun `should fail to update a relationship when end node keys is effectively empty`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
      sink: Neo4jSinkRegistration,
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
                "knows" to mapOf("since" to LocalDate.of(2000, 1, 1), "type" to "friend"),
            ),
        )
        .consume()
    producer.publish(
        newEvent(
            0,
            0,
            RelationshipEvent(
                "knows1",
                "KNOWS",
                Node("person1", listOf("Person"), mapOf("Person" to listOf(mapOf("id" to 2L)))),
                Node("person2", listOf("Person"), mapOf("Person" to emptyList())),
                emptyList(),
                EntityOperation.UPDATE,
                RelationshipState(mapOf("since" to LocalDate.of(2000, 1, 1), "type" to "friend")),
                RelationshipState(mapOf("since" to LocalDate.of(1999, 1, 1))),
            ),
        ),
    )

    // sink connector should transition into FAILED state
    eventually(30.seconds) {
      val tasks = sink.getConnectorTasksForStatusCheck()
      tasks shouldHaveSize 1
      tasks.get(0).get("state").asText() shouldBe "FAILED"
    }

    // and relationship should not be deleted
    val result = session.run("MATCH ()-[r:KNOWS]->() RETURN count(r)").single()
    result.get(0).asInt() shouldBe 1
  }

  @Neo4jSink(cdcSchema = [CdcSchemaStrategy(TOPIC)])
  @Test
  fun `should update a relationship when start and end node keys are empty but it has its own keys`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
      sink: Neo4jSinkRegistration,
      neo4j: Neo4j
  ) = runTest {
    session.createRelationshipKeyConstraint(neo4j, "KNOWS_KEY", "KNOWS", "id")
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
                    mapOf(
                        "id" to 3L,
                        "since" to LocalDate.of(2000, 1, 1),
                        "type" to "friend",
                    ),
            ),
        )
        .consume()

    producer.publish(
        newEvent(
            0,
            0,
            RelationshipEvent(
                "knows1",
                "KNOWS",
                Node("person1", listOf("Person"), mapOf("Person" to emptyList())),
                Node("person2", listOf("Person"), emptyMap()),
                listOf(mapOf("id" to 3L)),
                EntityOperation.UPDATE,
                RelationshipState(
                    mapOf(
                        "id" to 3L,
                        "since" to LocalDate.of(2000, 1, 1),
                        "type" to "friend",
                    ),
                ),
                RelationshipState(mapOf("id" to 3L, "since" to LocalDate.of(1999, 1, 1))),
            ),
        ),
    )

    eventually(30.seconds) {
      val result = session.run("MATCH (start)-[r:KNOWS]->(end) RETURN start, r, end").single()

      result.get("r").asRelationship() should
          {
            it.asMap() shouldBe mapOf("id" to 3L, "since" to LocalDate.of(1999, 1, 1))
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
                "knows" to mapOf("since" to LocalDate.of(2000, 1, 1), "type" to "friend"),
            ),
        )
        .consume()

    producer.publish(
        newEvent(
            0,
            0,
            RelationshipEvent(
                "knows1",
                "KNOWS",
                Node("person1", listOf("Person"), mapOf("Person" to listOf(mapOf("id" to 1L)))),
                Node("person2", listOf("Person"), mapOf("Person" to listOf(mapOf("id" to 2L)))),
                emptyList(),
                EntityOperation.DELETE,
                RelationshipState(mapOf("since" to LocalDate.of(1999, 1, 1))),
                null,
            ),
        ),
    )

    eventually(30.seconds) {
      val result = session.run("MATCH ()-[r:KNOWS]->() RETURN count(r)").single()

      result.get(0).asInt() shouldBe 0
    }
  }

  @Neo4jSink(cdcSchema = [CdcSchemaStrategy(TOPIC)])
  @Test
  fun `should fail to delete a relationship when start node keys is empty`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
      sink: Neo4jSinkRegistration,
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
                "knows" to mapOf("since" to LocalDate.of(2000, 1, 1), "type" to "friend"),
            ),
        )
        .consume()

    producer.publish(
        newEvent(
            0,
            0,
            RelationshipEvent(
                "knows1",
                "KNOWS",
                Node("person1", listOf("Person"), emptyMap()),
                Node("person2", listOf("Person"), mapOf("Person" to listOf(mapOf("id" to 2L)))),
                emptyList(),
                EntityOperation.DELETE,
                RelationshipState(mapOf("since" to LocalDate.of(1999, 1, 1))),
                null,
            ),
        ),
    )

    // sink connector should transition into FAILED state
    eventually(30.seconds) {
      val tasks = sink.getConnectorTasksForStatusCheck()
      tasks shouldHaveSize 1
      tasks.get(0).get("state").asText() shouldBe "FAILED"
    }

    // and relationship should not be deleted
    val result = session.run("MATCH ()-[r:KNOWS]->() RETURN count(r)").single()
    result.get(0).asInt() shouldBe 1
  }

  @Neo4jSink(cdcSchema = [CdcSchemaStrategy(TOPIC)])
  @Test
  fun `should fail to delete a relationship when start node keys is effectively empty`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
      sink: Neo4jSinkRegistration,
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
                "knows" to mapOf("since" to LocalDate.of(2000, 1, 1), "type" to "friend"),
            ),
        )
        .consume()

    producer.publish(
        newEvent(
            0,
            0,
            RelationshipEvent(
                "knows1",
                "KNOWS",
                Node("person1", listOf("Person"), mapOf("Person" to emptyList<Map<String, Any>>())),
                Node("person2", listOf("Person"), mapOf("Person" to listOf(mapOf("id" to 2L)))),
                emptyList(),
                EntityOperation.DELETE,
                RelationshipState(mapOf("since" to LocalDate.of(1999, 1, 1))),
                null,
            ),
        ),
    )

    // sink connector should transition into FAILED state
    eventually(30.seconds) {
      val tasks = sink.getConnectorTasksForStatusCheck()
      tasks shouldHaveSize 1
      tasks.get(0).get("state").asText() shouldBe "FAILED"
    }

    // and relationship should not be deleted
    val result = session.run("MATCH ()-[r:KNOWS]->() RETURN count(r)").single()
    result.get(0).asInt() shouldBe 1
  }

  @Neo4jSink(cdcSchema = [CdcSchemaStrategy(TOPIC)])
  @Test
  fun `should fail to delete a relationship when end node keys is empty`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
      sink: Neo4jSinkRegistration,
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
                "knows" to mapOf("since" to LocalDate.of(2000, 1, 1), "type" to "friend"),
            ),
        )
        .consume()

    producer.publish(
        newEvent(
            0,
            0,
            RelationshipEvent(
                "knows1",
                "KNOWS",
                Node("person1", listOf("Person"), mapOf("Person" to listOf(mapOf("id" to 2L)))),
                Node("person2", listOf("Person"), emptyMap()),
                emptyList(),
                EntityOperation.DELETE,
                RelationshipState(mapOf("since" to LocalDate.of(1999, 1, 1))),
                null,
            ),
        ),
    )

    // sink connector should transition into FAILED state
    eventually(30.seconds) {
      val tasks = sink.getConnectorTasksForStatusCheck()
      tasks shouldHaveSize 1
      tasks.get(0).get("state").asText() shouldBe "FAILED"
    }

    // and relationship should not be deleted
    val result = session.run("MATCH ()-[r:KNOWS]->() RETURN count(r)").single()
    result.get(0).asInt() shouldBe 1
  }

  @Neo4jSink(cdcSchema = [CdcSchemaStrategy(TOPIC)])
  @Test
  fun `should fail to delete a relationship when end node keys is effectively empty`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
      sink: Neo4jSinkRegistration,
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
                "knows" to mapOf("since" to LocalDate.of(2000, 1, 1), "type" to "friend"),
            ),
        )
        .consume()

    producer.publish(
        newEvent(
            0,
            0,
            RelationshipEvent(
                "knows1",
                "KNOWS",
                Node("person1", listOf("Person"), mapOf("Person" to listOf(mapOf("id" to 2L)))),
                Node("person2", listOf("Person"), mapOf("Person" to emptyList<Map<String, Any>>())),
                emptyList(),
                EntityOperation.DELETE,
                RelationshipState(mapOf("since" to LocalDate.of(1999, 1, 1))),
                null,
            ),
        ),
    )

    // sink connector should transition into FAILED state
    eventually(30.seconds) {
      val tasks = sink.getConnectorTasksForStatusCheck()
      tasks shouldHaveSize 1
      tasks.get(0).get("state").asText() shouldBe "FAILED"
    }

    // and relationship should not be deleted
    val result = session.run("MATCH ()-[r:KNOWS]->() RETURN count(r)").single()
    result.get(0).asInt() shouldBe 1
  }

  @Neo4jSink(cdcSchema = [CdcSchemaStrategy(TOPIC)])
  @Test
  fun `should delete a relationship when start and end node keys are empty but it has its own keys`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
      sink: Neo4jSinkRegistration,
      neo4j: Neo4j
  ) = runTest {
    session.createRelationshipKeyConstraint(neo4j, "KNOWS_KEY", "KNOWS", "id")
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
                    mapOf(
                        "id" to 3L,
                        "since" to LocalDate.of(2000, 1, 1),
                        "type" to "friend",
                    ),
            ),
        )
        .consume()

    producer.publish(
        newEvent(
            0,
            0,
            RelationshipEvent(
                "knows1",
                "KNOWS",
                Node("person1", listOf("Person"), emptyMap()),
                Node("person2", listOf("Person"), mapOf("Person" to emptyList())),
                listOf(mapOf("id" to 3L)),
                EntityOperation.DELETE,
                RelationshipState(mapOf("id" to 3L, "since" to LocalDate.of(1999, 1, 1))),
                null,
            ),
        ),
    )

    eventually(30.seconds) {
      val result = session.run("MATCH ()-[r:KNOWS]->() RETURN count(r)").single()

      result.get(0).asInt() shouldBe 0
    }
  }

  @Neo4jSink(
      schemaControlKeyCompatibility = SchemaCompatibilityMode.NONE,
      schemaControlValueCompatibility = SchemaCompatibilityMode.NONE,
      cdcSchema = [CdcSchemaStrategy(TOPIC)],
  )
  @Test
  fun `should sync continuous changes`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    producer.publish(
        newEvent(
            0,
            0,
            NodeEvent(
                "person1",
                EntityOperation.CREATE,
                listOf("Person"),
                mapOf("Person" to listOf(mapOf("id" to 1L))),
                null,
                NodeState(
                    listOf("Person"),
                    mapOf("id" to 1L, "name" to "john", "surname" to "doe"),
                ),
            ),
        ),
    )

    producer.publish(
        newEvent(
            1,
            0,
            NodeEvent(
                "person2",
                EntityOperation.CREATE,
                listOf("Person"),
                mapOf("Person" to listOf(mapOf("id" to 2L))),
                null,
                NodeState(
                    listOf("Person"),
                    mapOf("id" to 2L, "name" to "mary", "surname" to "doe"),
                ),
            ),
        ),
    )

    producer.publish(
        newEvent(
            2,
            0,
            RelationshipEvent(
                "knows1",
                "KNOWS",
                Node("person1", listOf("Person"), mapOf("Person" to listOf(mapOf("id" to 1L)))),
                Node("person2", listOf("Person"), mapOf("Person" to listOf(mapOf("id" to 2L)))),
                emptyList(),
                EntityOperation.CREATE,
                null,
                RelationshipState(emptyMap()),
            ),
        ),
    )

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
                  mapOf("startId" to 1L, "endId" to 2L),
              )
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
        newEvent(
            3,
            0,
            RelationshipEvent(
                "knows1",
                "KNOWS",
                Node("person1", listOf("Person"), mapOf("Person" to listOf(mapOf("id" to 1L)))),
                Node("person2", listOf("Person"), mapOf("Person" to listOf(mapOf("id" to 2L)))),
                emptyList(),
                EntityOperation.UPDATE,
                RelationshipState(emptyMap()),
                RelationshipState(mapOf("since" to LocalDate.of(2000, 1, 1))),
            ),
        ),
    )

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
                  mapOf("startId" to 1L, "endId" to 2L),
              )
              .single()

      result.get("r").asRelationship() should
          {
            it.asMap() shouldBe mapOf("since" to LocalDate.of(2000, 1, 1))
          }
    }

    producer.publish(
        newEvent(
            4,
            0,
            RelationshipEvent(
                "knows1",
                "KNOWS",
                Node("person1", listOf("Person"), mapOf("Person" to listOf(mapOf("id" to 1L)))),
                Node("person2", listOf("Person"), mapOf("Person" to listOf(mapOf("id" to 2L)))),
                emptyList(),
                EntityOperation.DELETE,
                RelationshipState(mapOf("since" to LocalDate.of(2000, 1, 1))),
                null,
            ),
        ),
    )
    producer.publish(
        newEvent(
            4,
            1,
            NodeEvent(
                "person1",
                EntityOperation.DELETE,
                listOf("Person"),
                mapOf("Person" to listOf(mapOf("id" to 1L))),
                NodeState(
                    listOf("Person"),
                    mapOf("id" to 1L, "name" to "john", "surname" to "doe"),
                ),
                null,
            ),
        ),
    )
    producer.publish(
        newEvent(
            4,
            2,
            NodeEvent(
                "person2",
                EntityOperation.DELETE,
                listOf("Person"),
                mapOf("Person" to listOf(mapOf("id" to 2L))),
                NodeState(
                    listOf("Person"),
                    mapOf("id" to 2L, "name" to "mary", "surname" to "doe"),
                ),
                null,
            ),
        ),
    )

    eventually(30.seconds) {
      val result = session.run("MATCH (n) RETURN count(n)").single()

      result.get(0).asInt() shouldBe 0
    }
  }

  private fun newEvent(txId: Long, seq: Int, event: Event): ChangeEvent =
      ChangeEvent(
          ChangeIdentifier("$txId:$seq"),
          txId,
          seq,
          Metadata(
              "neo4j",
              "neo4j",
              "server-id",
              "neo4j",
              CaptureMode.DIFF,
              "bolt",
              "localhost:32000",
              "localhost:7687",
              ZonedDateTime.now().minusSeconds(5),
              ZonedDateTime.now(),
              emptyMap(),
              emptyMap(),
          ),
          event,
      )
}

@KeyValueConverter(key = KafkaConverter.AVRO, value = KafkaConverter.AVRO)
class Neo4jCdcSchemaAvroIT : Neo4jCdcSchemaIT()

@KeyValueConverter(key = KafkaConverter.JSON_SCHEMA, value = KafkaConverter.JSON_SCHEMA)
class Neo4jCdcSchemaJsonSchemaIT : Neo4jCdcSchemaIT()

@KeyValueConverter(key = KafkaConverter.JSON_EMBEDDED, value = KafkaConverter.JSON_EMBEDDED)
class Neo4jCdcSchemaJsonEmbeddedIT : Neo4jCdcSchemaIT()

@KeyValueConverter(key = KafkaConverter.PROTOBUF, value = KafkaConverter.PROTOBUF)
class Neo4jCdcSchemaProtobufIT : Neo4jCdcSchemaIT()
