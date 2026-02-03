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

import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.instanceOf
import java.time.LocalDate
import java.util.UUID
import org.apache.kafka.connect.sink.ErrantRecordReporter
import org.apache.kafka.connect.sink.SinkTaskContext
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.neo4j.caniuse.CanIUse.canIUse
import org.neo4j.caniuse.Cypher
import org.neo4j.caniuse.Neo4j
import org.neo4j.caniuse.Neo4jDetector
import org.neo4j.cdc.client.model.EntityOperation
import org.neo4j.cdc.client.model.Node
import org.neo4j.cdc.client.model.NodeEvent
import org.neo4j.cdc.client.model.NodeState
import org.neo4j.cdc.client.model.RelationshipEvent
import org.neo4j.cdc.client.model.RelationshipState
import org.neo4j.connectors.kafka.sink.Neo4jSinkTask
import org.neo4j.connectors.kafka.sink.strategy.TestUtils.newChangeEventMessage
import org.neo4j.connectors.kafka.testing.DatabaseSupport.createDatabase
import org.neo4j.connectors.kafka.testing.DatabaseSupport.dropDatabase
import org.neo4j.connectors.kafka.testing.neo4jDatabase
import org.neo4j.connectors.kafka.testing.neo4jImage
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.Session
import org.neo4j.driver.SessionConfig
import org.testcontainers.containers.Neo4jContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

@Testcontainers
class ApocCdcSourceIdHandlerTaskIT {
  companion object {
    @Container
    val container: Neo4jContainer<*> =
        Neo4jContainer(neo4jImage())
            .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
            .withPlugins("apoc")
            .withExposedPorts(7687)
            .withoutAuthentication()
            .waitingFor(neo4jDatabase())

    private lateinit var driver: Driver
    private lateinit var neo4j: Neo4j

    @BeforeAll
    @JvmStatic
    fun setUpContainer() {
      driver = GraphDatabase.driver(container.boltUrl, AuthTokens.none())
      neo4j = Neo4jDetector.detect(driver)
    }

    @AfterAll
    @JvmStatic
    fun tearDownContainer() {
      driver.close()
    }
  }

  private lateinit var task: Neo4jSinkTask
  private lateinit var db: String
  private lateinit var session: Session

  @AfterEach
  fun after() {
    if (this::db.isInitialized) driver.dropDatabase(db)
    if (this::session.isInitialized) session.close()
    if (this::task.isInitialized) task.stop()
  }

  @BeforeEach
  fun before() {
    assumeTrue {
      canIUse(Cypher.setDynamicLabels()).withNeo4j(neo4j) &&
          canIUse(Cypher.setDynamicLabels()).withNeo4j(neo4j)
    }

    db = "test-${UUID.randomUUID()}"
    driver.createDatabase(db)
    session = driver.session(SessionConfig.forDatabase(db))

    // Create constraint for SourceEvent nodes
    session.run("CREATE CONSTRAINT FOR (n:SourceEvent) REQUIRE n.sourceId IS KEY").consume()

    task = Neo4jSinkTask()
    task.initialize(newTaskContext())
    task.start(
        mapOf(
            "topics" to "my-topic",
            "neo4j.database" to db,
            "neo4j.uri" to container.boltUrl,
            "neo4j.authentication.type" to "NONE",
            "neo4j.cdc.source-id.topics" to "my-topic",
        )
    )

    task.config.topicHandlers["my-topic"] shouldBe instanceOf(ApocCdcSourceIdHandler::class)
  }

  @Test
  fun `should create a simple node`() {
    task.put(
        listOf(
            newChangeEventMessage(
                    NodeEvent(
                        "node-1",
                        EntityOperation.CREATE,
                        emptyList(),
                        emptyMap(),
                        null,
                        NodeState(emptyList(), mapOf("name" to "Alice")),
                    ),
                    1,
                    0,
                )
                .record
        )
    )

    session
        .run("MATCH (n:SourceEvent {sourceId: 'node-1'}) RETURN n{.*}")
        .single()
        .get(0)
        .asMap() shouldBe mapOf("sourceId" to "node-1", "name" to "Alice")
  }

  @Test
  fun `should create node with labels`() {
    task.put(
        listOf(
            newChangeEventMessage(
                    NodeEvent(
                        "node-1",
                        EntityOperation.CREATE,
                        listOf("Person", "Employee"),
                        emptyMap(),
                        null,
                        NodeState(
                            listOf("Person", "Employee"),
                            mapOf("name" to "Alice", "department" to "Engineering"),
                        ),
                    ),
                    1,
                    0,
                )
                .record
        )
    )

    val result =
        session
            .run(
                "MATCH (n:SourceEvent:Person:Employee {sourceId: 'node-1'}) RETURN labels(n), n{.*}"
            )
            .single()
    result.get(0).asList { it.asString() }.sorted() shouldBe
        listOf("Employee", "Person", "SourceEvent")
    result.get(1).asMap() shouldBe
        mapOf("sourceId" to "node-1", "name" to "Alice", "department" to "Engineering")
  }

  @Test
  fun `should create multiple nodes in a single batch`() {
    task.put(
        listOf(
            newChangeEventMessage(
                    NodeEvent(
                        "node-1",
                        EntityOperation.CREATE,
                        emptyList(),
                        emptyMap(),
                        null,
                        NodeState(emptyList(), mapOf("name" to "Alice")),
                    ),
                    1,
                    0,
                )
                .record,
            newChangeEventMessage(
                    NodeEvent(
                        "node-2",
                        EntityOperation.CREATE,
                        emptyList(),
                        emptyMap(),
                        null,
                        NodeState(emptyList(), mapOf("name" to "Bob")),
                    ),
                    1,
                    1,
                )
                .record,
            newChangeEventMessage(
                    NodeEvent(
                        "node-3",
                        EntityOperation.CREATE,
                        emptyList(),
                        emptyMap(),
                        null,
                        NodeState(emptyList(), mapOf("name" to "Charlie")),
                    ),
                    1,
                    2,
                )
                .record,
        )
    )

    session.run("MATCH (n:SourceEvent) RETURN count(n)").single().get(0).asInt() shouldBe 3
  }

  @Test
  fun `should add label to existing node`() {
    task.put(
        listOf(
            newChangeEventMessage(
                    NodeEvent(
                        "node-1",
                        EntityOperation.CREATE,
                        listOf("Person"),
                        emptyMap(),
                        null,
                        NodeState(listOf("Person"), mapOf("name" to "Alice")),
                    ),
                    1,
                    0,
                )
                .record
        )
    )

    task.put(
        listOf(
            newChangeEventMessage(
                    NodeEvent(
                        "node-1",
                        EntityOperation.UPDATE,
                        listOf("Person", "Admin"),
                        emptyMap(),
                        NodeState(listOf("Person"), mapOf("name" to "Alice")),
                        NodeState(listOf("Person", "Admin"), mapOf("name" to "Alice")),
                    ),
                    2,
                    0,
                )
                .record
        )
    )

    session
        .run("MATCH (n:SourceEvent {sourceId: 'node-1'}) RETURN labels(n)")
        .single()
        .get(0)
        .asList { it.asString() }
        .sorted() shouldBe listOf("Admin", "Person", "SourceEvent")
  }

  @Test
  fun `should remove label from existing node`() {
    task.put(
        listOf(
            newChangeEventMessage(
                    NodeEvent(
                        "node-1",
                        EntityOperation.CREATE,
                        listOf("Person", "Admin"),
                        emptyMap(),
                        null,
                        NodeState(listOf("Person", "Admin"), mapOf("name" to "Alice")),
                    ),
                    1,
                    0,
                )
                .record
        )
    )

    task.put(
        listOf(
            newChangeEventMessage(
                    NodeEvent(
                        "node-1",
                        EntityOperation.UPDATE,
                        listOf("Person"),
                        emptyMap(),
                        NodeState(listOf("Person", "Admin"), mapOf("name" to "Alice")),
                        NodeState(listOf("Person"), mapOf("name" to "Alice")),
                    ),
                    2,
                    0,
                )
                .record
        )
    )

    session
        .run("MATCH (n:SourceEvent {sourceId: 'node-1'}) RETURN labels(n)")
        .single()
        .get(0)
        .asList { it.asString() }
        .sorted() shouldBe listOf("Person", "SourceEvent")
  }

  @Test
  fun `should update node properties`() {
    task.put(
        listOf(
            newChangeEventMessage(
                    NodeEvent(
                        "node-1",
                        EntityOperation.CREATE,
                        emptyList(),
                        emptyMap(),
                        null,
                        NodeState(emptyList(), mapOf("name" to "Alice", "age" to 30)),
                    ),
                    1,
                    0,
                )
                .record
        )
    )

    task.put(
        listOf(
            newChangeEventMessage(
                    NodeEvent(
                        "node-1",
                        EntityOperation.UPDATE,
                        emptyList(),
                        emptyMap(),
                        NodeState(emptyList(), mapOf("name" to "Alice", "age" to 30)),
                        NodeState(
                            emptyList(),
                            mapOf("name" to "Alice", "age" to 31, "city" to "NYC"),
                        ),
                    ),
                    2,
                    0,
                )
                .record
        )
    )

    session
        .run("MATCH (n:SourceEvent {sourceId: 'node-1'}) RETURN n{.*}")
        .single()
        .get(0)
        .asMap() shouldBe
        mapOf("sourceId" to "node-1", "name" to "Alice", "age" to 31L, "city" to "NYC")
  }

  @Test
  fun `should remove property from node`() {
    task.put(
        listOf(
            newChangeEventMessage(
                    NodeEvent(
                        "node-1",
                        EntityOperation.CREATE,
                        emptyList(),
                        emptyMap(),
                        null,
                        NodeState(emptyList(), mapOf("name" to "Alice", "age" to 30)),
                    ),
                    1,
                    0,
                )
                .record
        )
    )

    task.put(
        listOf(
            newChangeEventMessage(
                    NodeEvent(
                        "node-1",
                        EntityOperation.UPDATE,
                        emptyList(),
                        emptyMap(),
                        NodeState(emptyList(), mapOf("name" to "Alice", "age" to 30)),
                        NodeState(emptyList(), mapOf("name" to "Alice")),
                    ),
                    2,
                    0,
                )
                .record
        )
    )

    session
        .run("MATCH (n:SourceEvent {sourceId: 'node-1'}) RETURN n{.*}")
        .single()
        .get(0)
        .asMap() shouldBe mapOf("sourceId" to "node-1", "name" to "Alice")
  }

  @Test
  fun `should delete a node`() {
    task.put(
        listOf(
            newChangeEventMessage(
                    NodeEvent(
                        "node-1",
                        EntityOperation.CREATE,
                        emptyList(),
                        emptyMap(),
                        null,
                        NodeState(emptyList(), mapOf("name" to "Alice")),
                    ),
                    1,
                    0,
                )
                .record
        )
    )

    task.put(
        listOf(
            newChangeEventMessage(
                    NodeEvent(
                        "node-1",
                        EntityOperation.DELETE,
                        emptyList(),
                        emptyMap(),
                        NodeState(emptyList(), mapOf("name" to "Alice")),
                        null,
                    ),
                    2,
                    0,
                )
                .record
        )
    )

    session
        .run("MATCH (n:SourceEvent {sourceId: 'node-1'}) RETURN count(n)")
        .single()
        .get(0)
        .asInt() shouldBe 0
  }

  @Test
  fun `should create, update and delete a node within the same batch`() {
    task.put(
        listOf(
            newChangeEventMessage(
                    NodeEvent(
                        "node-1",
                        EntityOperation.CREATE,
                        emptyList(),
                        emptyMap(),
                        null,
                        NodeState(emptyList(), mapOf("name" to "Alice")),
                    ),
                    1,
                    0,
                )
                .record,
            newChangeEventMessage(
                    NodeEvent(
                        "node-1",
                        EntityOperation.UPDATE,
                        emptyList(),
                        emptyMap(),
                        NodeState(emptyList(), mapOf("name" to "Alice")),
                        NodeState(emptyList(), mapOf("name" to "Alice", "surname" to "Smith")),
                    ),
                    2,
                    0,
                )
                .record,
            newChangeEventMessage(
                    NodeEvent(
                        "node-1",
                        EntityOperation.DELETE,
                        emptyList(),
                        emptyMap(),
                        NodeState(emptyList(), mapOf("name" to "Alice", "surname" to "Smith")),
                        null,
                    ),
                    3,
                    0,
                )
                .record,
        )
    )

    session
        .run("MATCH (n:SourceEvent {sourceId: 'node-1'}) RETURN count(n)")
        .single()
        .get(0)
        .asInt() shouldBe 0
  }

  @Test
  fun `should create a simple relationship between existing nodes`() {
    task.put(
        listOf(
            newChangeEventMessage(
                    NodeEvent(
                        "node-1",
                        EntityOperation.CREATE,
                        emptyList(),
                        emptyMap(),
                        null,
                        NodeState(emptyList(), mapOf("name" to "Alice")),
                    ),
                    1,
                    0,
                )
                .record,
            newChangeEventMessage(
                    NodeEvent(
                        "node-2",
                        EntityOperation.CREATE,
                        emptyList(),
                        emptyMap(),
                        null,
                        NodeState(emptyList(), mapOf("name" to "Bob")),
                    ),
                    1,
                    1,
                )
                .record,
        )
    )

    task.put(
        listOf(
            newChangeEventMessage(
                    RelationshipEvent(
                        "rel-1",
                        "FOLLOWS",
                        Node("node-1", emptyList(), emptyMap()),
                        Node("node-2", emptyList(), emptyMap()),
                        emptyList(),
                        EntityOperation.CREATE,
                        null,
                        RelationshipState(mapOf("since" to LocalDate.of(2023, 6, 15))),
                    ),
                    2,
                    0,
                )
                .record
        )
    )

    session
        .run(
            "MATCH (:SourceEvent {sourceId: 'node-1'})-[r:FOLLOWS]->(:SourceEvent {sourceId: 'node-2'}) RETURN r{.*}"
        )
        .single()
        .get(0)
        .asMap() shouldBe mapOf("sourceId" to "rel-1", "since" to LocalDate.of(2023, 6, 15))
  }

  @Test
  fun `should create relationship between nodes created in same batch`() {
    task.put(
        listOf(
            newChangeEventMessage(
                    NodeEvent(
                        "node-1",
                        EntityOperation.CREATE,
                        emptyList(),
                        emptyMap(),
                        null,
                        NodeState(emptyList(), mapOf("name" to "Alice")),
                    ),
                    1,
                    0,
                )
                .record,
            newChangeEventMessage(
                    NodeEvent(
                        "node-2",
                        EntityOperation.CREATE,
                        emptyList(),
                        emptyMap(),
                        null,
                        NodeState(emptyList(), mapOf("name" to "Bob")),
                    ),
                    1,
                    1,
                )
                .record,
            newChangeEventMessage(
                    RelationshipEvent(
                        "rel-1",
                        "FOLLOWS",
                        Node("node-1", emptyList(), emptyMap()),
                        Node("node-2", emptyList(), emptyMap()),
                        emptyList(),
                        EntityOperation.CREATE,
                        null,
                        RelationshipState(mapOf("since" to LocalDate.of(2023, 6, 15))),
                    ),
                    1,
                    2,
                )
                .record,
        )
    )

    session.run("MATCH (n:SourceEvent) RETURN count(n)").single().get(0).asInt() shouldBe 2
    session
        .run(
            "MATCH (:SourceEvent {sourceId: 'node-1'})-[r:FOLLOWS]->(:SourceEvent {sourceId: 'node-2'}) RETURN r{.*}"
        )
        .single()
        .get(0)
        .asMap() shouldBe mapOf("sourceId" to "rel-1", "since" to LocalDate.of(2023, 6, 15))
  }

  @Test
  fun `should create multiple relationships between same nodes`() {
    task.put(
        listOf(
            newChangeEventMessage(
                    NodeEvent(
                        "node-1",
                        EntityOperation.CREATE,
                        emptyList(),
                        emptyMap(),
                        null,
                        NodeState(emptyList(), mapOf("name" to "Alice")),
                    ),
                    1,
                    0,
                )
                .record,
            newChangeEventMessage(
                    NodeEvent(
                        "node-2",
                        EntityOperation.CREATE,
                        emptyList(),
                        emptyMap(),
                        null,
                        NodeState(emptyList(), mapOf("name" to "Bob")),
                    ),
                    1,
                    1,
                )
                .record,
            newChangeEventMessage(
                    RelationshipEvent(
                        "rel-1",
                        "FOLLOWS",
                        Node("node-1", emptyList(), emptyMap()),
                        Node("node-2", emptyList(), emptyMap()),
                        emptyList(),
                        EntityOperation.CREATE,
                        null,
                        RelationshipState(emptyMap()),
                    ),
                    1,
                    2,
                )
                .record,
            newChangeEventMessage(
                    RelationshipEvent(
                        "rel-2",
                        "LIKES",
                        Node("node-1", emptyList(), emptyMap()),
                        Node("node-2", emptyList(), emptyMap()),
                        emptyList(),
                        EntityOperation.CREATE,
                        null,
                        RelationshipState(emptyMap()),
                    ),
                    1,
                    3,
                )
                .record,
            newChangeEventMessage(
                    RelationshipEvent(
                        "rel-3",
                        "KNOWS",
                        Node("node-1", emptyList(), emptyMap()),
                        Node("node-2", emptyList(), emptyMap()),
                        emptyList(),
                        EntityOperation.CREATE,
                        null,
                        RelationshipState(mapOf("since" to LocalDate.of(2020, 1, 1))),
                    ),
                    1,
                    4,
                )
                .record,
        )
    )

    session
        .run(
            "MATCH (:SourceEvent {sourceId: 'node-1'})-[r]->(:SourceEvent {sourceId: 'node-2'}) RETURN count(r)"
        )
        .single()
        .get(0)
        .asInt() shouldBe 3
  }

  @Test
  fun `should update a relationship`() {
    task.put(
        listOf(
            newChangeEventMessage(
                    NodeEvent(
                        "node-1",
                        EntityOperation.CREATE,
                        emptyList(),
                        emptyMap(),
                        null,
                        NodeState(emptyList(), mapOf("name" to "Alice")),
                    ),
                    1,
                    0,
                )
                .record,
            newChangeEventMessage(
                    NodeEvent(
                        "node-2",
                        EntityOperation.CREATE,
                        emptyList(),
                        emptyMap(),
                        null,
                        NodeState(emptyList(), mapOf("name" to "Bob")),
                    ),
                    1,
                    1,
                )
                .record,
            newChangeEventMessage(
                    RelationshipEvent(
                        "rel-1",
                        "FOLLOWS",
                        Node("node-1", emptyList(), emptyMap()),
                        Node("node-2", emptyList(), emptyMap()),
                        emptyList(),
                        EntityOperation.CREATE,
                        null,
                        RelationshipState(mapOf("since" to LocalDate.of(2020, 1, 1))),
                    ),
                    1,
                    2,
                )
                .record,
        )
    )

    task.put(
        listOf(
            newChangeEventMessage(
                    RelationshipEvent(
                        "rel-1",
                        "FOLLOWS",
                        Node("node-1", emptyList(), emptyMap()),
                        Node("node-2", emptyList(), emptyMap()),
                        emptyList(),
                        EntityOperation.UPDATE,
                        RelationshipState(mapOf("since" to LocalDate.of(2020, 1, 1))),
                        RelationshipState(
                            mapOf("since" to LocalDate.of(2020, 1, 1), "strength" to 5)
                        ),
                    ),
                    2,
                    0,
                )
                .record
        )
    )

    session
        .run(
            "MATCH (:SourceEvent {sourceId: 'node-1'})-[r:FOLLOWS]->(:SourceEvent {sourceId: 'node-2'}) RETURN r{.*}"
        )
        .single()
        .get(0)
        .asMap() shouldBe
        mapOf("sourceId" to "rel-1", "since" to LocalDate.of(2020, 1, 1), "strength" to 5L)
  }

  @Test
  fun `should remove property from relationship`() {
    task.put(
        listOf(
            newChangeEventMessage(
                    NodeEvent(
                        "node-1",
                        EntityOperation.CREATE,
                        emptyList(),
                        emptyMap(),
                        null,
                        NodeState(emptyList(), mapOf("name" to "Alice")),
                    ),
                    1,
                    0,
                )
                .record,
            newChangeEventMessage(
                    NodeEvent(
                        "node-2",
                        EntityOperation.CREATE,
                        emptyList(),
                        emptyMap(),
                        null,
                        NodeState(emptyList(), mapOf("name" to "Bob")),
                    ),
                    1,
                    1,
                )
                .record,
            newChangeEventMessage(
                    RelationshipEvent(
                        "rel-1",
                        "FOLLOWS",
                        Node("node-1", emptyList(), emptyMap()),
                        Node("node-2", emptyList(), emptyMap()),
                        emptyList(),
                        EntityOperation.CREATE,
                        null,
                        RelationshipState(
                            mapOf("since" to LocalDate.of(2020, 1, 1), "strength" to 5)
                        ),
                    ),
                    1,
                    2,
                )
                .record,
        )
    )

    task.put(
        listOf(
            newChangeEventMessage(
                    RelationshipEvent(
                        "rel-1",
                        "FOLLOWS",
                        Node("node-1", emptyList(), emptyMap()),
                        Node("node-2", emptyList(), emptyMap()),
                        emptyList(),
                        EntityOperation.UPDATE,
                        RelationshipState(
                            mapOf("since" to LocalDate.of(2020, 1, 1), "strength" to 5)
                        ),
                        RelationshipState(mapOf("since" to LocalDate.of(2020, 1, 1))),
                    ),
                    2,
                    0,
                )
                .record
        )
    )

    session
        .run(
            "MATCH (:SourceEvent {sourceId: 'node-1'})-[r:FOLLOWS]->(:SourceEvent {sourceId: 'node-2'}) RETURN r{.*}"
        )
        .single()
        .get(0)
        .asMap() shouldBe mapOf("sourceId" to "rel-1", "since" to LocalDate.of(2020, 1, 1))
  }

  @Test
  fun `should delete a relationship`() {
    task.put(
        listOf(
            newChangeEventMessage(
                    NodeEvent(
                        "node-1",
                        EntityOperation.CREATE,
                        emptyList(),
                        emptyMap(),
                        null,
                        NodeState(emptyList(), mapOf("name" to "Alice")),
                    ),
                    1,
                    0,
                )
                .record,
            newChangeEventMessage(
                    NodeEvent(
                        "node-2",
                        EntityOperation.CREATE,
                        emptyList(),
                        emptyMap(),
                        null,
                        NodeState(emptyList(), mapOf("name" to "Bob")),
                    ),
                    1,
                    1,
                )
                .record,
            newChangeEventMessage(
                    RelationshipEvent(
                        "rel-1",
                        "FOLLOWS",
                        Node("node-1", emptyList(), emptyMap()),
                        Node("node-2", emptyList(), emptyMap()),
                        emptyList(),
                        EntityOperation.CREATE,
                        null,
                        RelationshipState(mapOf("since" to LocalDate.of(2023, 1, 1))),
                    ),
                    1,
                    2,
                )
                .record,
        )
    )

    task.put(
        listOf(
            newChangeEventMessage(
                    RelationshipEvent(
                        "rel-1",
                        "FOLLOWS",
                        Node("node-1", emptyList(), emptyMap()),
                        Node("node-2", emptyList(), emptyMap()),
                        emptyList(),
                        EntityOperation.DELETE,
                        RelationshipState(mapOf("since" to LocalDate.of(2023, 1, 1))),
                        null,
                    ),
                    2,
                    0,
                )
                .record
        )
    )

    session.run("MATCH (n:SourceEvent) RETURN count(n)").single().get(0).asInt() shouldBe 2
    session
        .run(
            "MATCH (:SourceEvent {sourceId: 'node-1'})-[r:FOLLOWS]->(:SourceEvent {sourceId: 'node-2'}) RETURN count(r)"
        )
        .single()
        .get(0)
        .asInt() shouldBe 0
  }

  @Test
  fun `should delete relationship after an update event within the same batch`() {
    task.put(
        listOf(
            newChangeEventMessage(
                    NodeEvent(
                        "node-1",
                        EntityOperation.CREATE,
                        emptyList(),
                        emptyMap(),
                        null,
                        NodeState(emptyList(), mapOf("name" to "Alice")),
                    ),
                    1,
                    0,
                )
                .record,
            newChangeEventMessage(
                    NodeEvent(
                        "node-2",
                        EntityOperation.CREATE,
                        emptyList(),
                        emptyMap(),
                        null,
                        NodeState(emptyList(), mapOf("name" to "Bob")),
                    ),
                    1,
                    1,
                )
                .record,
            newChangeEventMessage(
                    RelationshipEvent(
                        "rel-1",
                        "FOLLOWS",
                        Node("node-1", emptyList(), emptyMap()),
                        Node("node-2", emptyList(), emptyMap()),
                        emptyList(),
                        EntityOperation.CREATE,
                        null,
                        RelationshipState(emptyMap()),
                    ),
                    1,
                    2,
                )
                .record,
        )
    )

    task.put(
        listOf(
            newChangeEventMessage(
                    RelationshipEvent(
                        "rel-1",
                        "FOLLOWS",
                        Node("node-1", emptyList(), emptyMap()),
                        Node("node-2", emptyList(), emptyMap()),
                        emptyList(),
                        EntityOperation.UPDATE,
                        RelationshipState(emptyMap()),
                        RelationshipState(mapOf("since" to LocalDate.of(2023, 1, 1))),
                    ),
                    2,
                    0,
                )
                .record,
            newChangeEventMessage(
                    RelationshipEvent(
                        "rel-1",
                        "FOLLOWS",
                        Node("node-1", emptyList(), emptyMap()),
                        Node("node-2", emptyList(), emptyMap()),
                        emptyList(),
                        EntityOperation.DELETE,
                        RelationshipState(mapOf("since" to LocalDate.of(2023, 1, 1))),
                        null,
                    ),
                    3,
                    0,
                )
                .record,
        )
    )

    session
        .run(
            "MATCH (:SourceEvent {sourceId: 'node-1'})-[r:FOLLOWS]->(:SourceEvent {sourceId: 'node-2'}) RETURN count(r)"
        )
        .single()
        .get(0)
        .asInt() shouldBe 0
  }

  @Test
  fun `should handle interleaved node and relationship operations`() {
    task.put(
        listOf(
            newChangeEventMessage(
                    NodeEvent(
                        "node-1",
                        EntityOperation.CREATE,
                        emptyList(),
                        emptyMap(),
                        null,
                        NodeState(emptyList(), mapOf("name" to "Alice")),
                    ),
                    1,
                    0,
                )
                .record,
            newChangeEventMessage(
                    NodeEvent(
                        "node-2",
                        EntityOperation.CREATE,
                        emptyList(),
                        emptyMap(),
                        null,
                        NodeState(emptyList(), mapOf("name" to "Bob")),
                    ),
                    1,
                    1,
                )
                .record,
            newChangeEventMessage(
                    RelationshipEvent(
                        "rel-1",
                        "FOLLOWS",
                        Node("node-1", emptyList(), emptyMap()),
                        Node("node-2", emptyList(), emptyMap()),
                        emptyList(),
                        EntityOperation.CREATE,
                        null,
                        RelationshipState(emptyMap()),
                    ),
                    1,
                    2,
                )
                .record,
            newChangeEventMessage(
                    NodeEvent(
                        "node-1",
                        EntityOperation.UPDATE,
                        emptyList(),
                        emptyMap(),
                        NodeState(emptyList(), mapOf("name" to "Alice")),
                        NodeState(emptyList(), mapOf("name" to "Alice", "age" to 30)),
                    ),
                    2,
                    0,
                )
                .record,
        )
    )

    session
        .run("MATCH (n:SourceEvent {sourceId: 'node-1'}) RETURN n{.*}")
        .single()
        .get(0)
        .asMap() shouldBe mapOf("sourceId" to "node-1", "name" to "Alice", "age" to 30L)
    session
        .run(
            "MATCH (:SourceEvent {sourceId: 'node-1'})-[r:FOLLOWS]->(:SourceEvent {sourceId: 'node-2'}) RETURN count(r)"
        )
        .single()
        .get(0)
        .asInt() shouldBe 1
  }

  @Test
  fun `should handle updates to multiple nodes in interleaved fashion`() {
    task.put(
        listOf(
            newChangeEventMessage(
                    NodeEvent(
                        "node-1",
                        EntityOperation.CREATE,
                        emptyList(),
                        emptyMap(),
                        null,
                        NodeState(emptyList(), mapOf("name" to "Alice")),
                    ),
                    1,
                    0,
                )
                .record,
            newChangeEventMessage(
                    NodeEvent(
                        "node-2",
                        EntityOperation.CREATE,
                        emptyList(),
                        emptyMap(),
                        null,
                        NodeState(emptyList(), mapOf("name" to "Bob")),
                    ),
                    1,
                    1,
                )
                .record,
        )
    )

    task.put(
        listOf(
            newChangeEventMessage(
                    NodeEvent(
                        "node-1",
                        EntityOperation.UPDATE,
                        emptyList(),
                        emptyMap(),
                        NodeState(emptyList(), mapOf("name" to "Alice")),
                        NodeState(emptyList(), mapOf("name" to "Alice", "score" to 10)),
                    ),
                    2,
                    0,
                )
                .record,
            newChangeEventMessage(
                    NodeEvent(
                        "node-2",
                        EntityOperation.UPDATE,
                        emptyList(),
                        emptyMap(),
                        NodeState(emptyList(), mapOf("name" to "Bob")),
                        NodeState(emptyList(), mapOf("name" to "Bob", "score" to 20)),
                    ),
                    2,
                    1,
                )
                .record,
            newChangeEventMessage(
                    NodeEvent(
                        "node-1",
                        EntityOperation.UPDATE,
                        emptyList(),
                        emptyMap(),
                        NodeState(emptyList(), mapOf("name" to "Alice", "score" to 10)),
                        NodeState(emptyList(), mapOf("name" to "Alice", "score" to 15)),
                    ),
                    3,
                    0,
                )
                .record,
            newChangeEventMessage(
                    NodeEvent(
                        "node-2",
                        EntityOperation.UPDATE,
                        emptyList(),
                        emptyMap(),
                        NodeState(emptyList(), mapOf("name" to "Bob", "score" to 20)),
                        NodeState(emptyList(), mapOf("name" to "Bob", "score" to 25)),
                    ),
                    3,
                    1,
                )
                .record,
        )
    )

    session
        .run("MATCH (n:SourceEvent {sourceId: 'node-1'}) RETURN n{.*}")
        .single()
        .get(0)
        .asMap() shouldBe mapOf("sourceId" to "node-1", "name" to "Alice", "score" to 15L)
    session
        .run("MATCH (n:SourceEvent {sourceId: 'node-2'}) RETURN n{.*}")
        .single()
        .get(0)
        .asMap() shouldBe mapOf("sourceId" to "node-2", "name" to "Bob", "score" to 25L)
  }

  private fun newTaskContext(): SinkTaskContext {
    return mock<SinkTaskContext> {
      on { errantRecordReporter() } doReturn ErrantRecordReporter { _, error -> throw error }
    }
  }
}
