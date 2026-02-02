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
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
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
class ApocCdcSchemaHandlerTaskIT {
  companion object {
    @Container
    val container: Neo4jContainer<*> =
        Neo4jContainer(neo4jImage())
            .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
            .withPlugins("apoc")
            .withExposedPorts(7687)
            .withoutAuthentication()

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
    if (this::session.isInitialized) session.close()
    if (this::task.isInitialized) task.stop()
  }

  @BeforeEach
  fun before() {
    db = "test-${UUID.randomUUID()}"
    driver.session(SessionConfig.forDatabase("system")).use {
      it.run("CREATE OR REPLACE DATABASE \$db WAIT", mapOf("db" to db)).consume()
    }
    session = driver.session(SessionConfig.forDatabase(db))

    task = Neo4jSinkTask()
    task.initialize(newTaskContext())
    task.start(
        mapOf(
            "topics" to "my-topic",
            "neo4j.database" to db,
            "neo4j.uri" to container.boltUrl,
            "neo4j.authentication.type" to "NONE",
            "neo4j.cdc.schema.topics" to "my-topic",
        )
    )

    task.config.topicHandlers["my-topic"] shouldBe instanceOf(ApocCdcSchemaHandler::class)
  }

  @Test
  fun `should create a simple node`() {
    session.run("CREATE CONSTRAINT FOR (n:User) REQUIRE n.userId IS KEY").consume()

    task.put(
        listOf(
            newChangeEventMessage(
                    userNodeEvent(
                        "node-1",
                        EntityOperation.CREATE,
                        "user1",
                        afterProps = mapOf("userId" to "user1", "name" to "Alice"),
                    ),
                    1,
                    0,
                )
                .record
        )
    )

    session.run("MATCH (a:User {userId: 'user1'}) RETURN a{.*}").single().get(0).asMap() shouldBe
        mapOf("userId" to "user1", "name" to "Alice")
  }

  @Test
  fun `should create node with multiple labels`() {
    session.run("CREATE CONSTRAINT FOR (n:Person) REQUIRE n.personId IS KEY").consume()

    task.put(
        listOf(
            newChangeEventMessage(
                    NodeEvent(
                        "node-1",
                        EntityOperation.CREATE,
                        listOf("Person", "Employee", "Manager"),
                        mapOf("Person" to listOf(mapOf("personId" to "p1"))),
                        null,
                        NodeState(
                            listOf("Person", "Employee", "Manager"),
                            mapOf(
                                "personId" to "p1",
                                "name" to "Alice",
                                "department" to "Engineering",
                            ),
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
            .run("MATCH (n:Person:Employee:Manager {personId: 'p1'}) RETURN n{.*}, labels(n)")
            .single()
    result.get(0).asMap() shouldBe
        mapOf("personId" to "p1", "name" to "Alice", "department" to "Engineering")
    result.get(1).asList { it.asString() }.sorted() shouldBe listOf("Employee", "Manager", "Person")
  }

  @Test
  fun `should create multiple nodes in a single batch`() {
    session.run("CREATE CONSTRAINT FOR (n:User) REQUIRE n.userId IS KEY").consume()

    task.put(
        listOf(
            newChangeEventMessage(
                    userNodeEvent(
                        "node-1",
                        EntityOperation.CREATE,
                        "user1",
                        afterProps = mapOf("userId" to "user1", "name" to "Alice"),
                    ),
                    1,
                    0,
                )
                .record,
            newChangeEventMessage(
                    userNodeEvent(
                        "node-2",
                        EntityOperation.CREATE,
                        "user2",
                        afterProps = mapOf("userId" to "user2", "name" to "Bob"),
                    ),
                    1,
                    1,
                )
                .record,
            newChangeEventMessage(
                    userNodeEvent(
                        "node-3",
                        EntityOperation.CREATE,
                        "user3",
                        afterProps = mapOf("userId" to "user3", "name" to "Charlie"),
                    ),
                    1,
                    2,
                )
                .record,
        )
    )

    session.run("MATCH (a:User) RETURN count(a)").single().get(0).asInt() shouldBe 3
  }

  @Test
  fun `should add label to existing node`() {
    session.run("CREATE CONSTRAINT FOR (n:User) REQUIRE n.userId IS KEY").consume()
    session.run("CREATE (:User {userId: 'user1', name: 'Alice'})").consume()

    task.put(
        listOf(
            newChangeEventMessage(
                    NodeEvent(
                        "node-1",
                        EntityOperation.UPDATE,
                        listOf("User", "Admin"),
                        mapOf("User" to listOf(mapOf("userId" to "user1"))),
                        NodeState(listOf("User"), mapOf("userId" to "user1", "name" to "Alice")),
                        NodeState(
                            listOf("User", "Admin"),
                            mapOf("userId" to "user1", "name" to "Alice"),
                        ),
                    ),
                    1,
                    0,
                )
                .record
        )
    )

    val result = session.run("MATCH (n:User {userId: 'user1'}) RETURN labels(n)").single()
    result.get(0).asList { it.asString() }.sorted() shouldBe listOf("Admin", "User")
  }

  @Test
  fun `should remove label from existing node`() {
    session.run("CREATE CONSTRAINT FOR (n:User) REQUIRE n.userId IS KEY").consume()
    session.run("CREATE (:User:Admin {userId: 'user1', name: 'Alice'})").consume()

    task.put(
        listOf(
            newChangeEventMessage(
                    NodeEvent(
                        "node-1",
                        EntityOperation.UPDATE,
                        listOf("User"),
                        mapOf("User" to listOf(mapOf("userId" to "user1"))),
                        NodeState(
                            listOf("User", "Admin"),
                            mapOf("userId" to "user1", "name" to "Alice"),
                        ),
                        NodeState(listOf("User"), mapOf("userId" to "user1", "name" to "Alice")),
                    ),
                    1,
                    0,
                )
                .record
        )
    )

    val result = session.run("MATCH (n:User {userId: 'user1'}) RETURN labels(n)").single()
    result.get(0).asList { it.asString() } shouldBe listOf("User")
  }

  @Test
  fun `should remove property from node`() {
    session.run("CREATE CONSTRAINT FOR (n:User) REQUIRE n.userId IS KEY").consume()
    session.run("CREATE (:User {userId: 'user1', name: 'Alice', age: 30})").consume()

    task.put(
        listOf(
            newChangeEventMessage(
                    userNodeEvent(
                        "node-1",
                        EntityOperation.UPDATE,
                        "user1",
                        beforeProps = mapOf("userId" to "user1", "name" to "Alice", "age" to 30),
                        afterProps = mapOf("userId" to "user1", "name" to "Alice"),
                    ),
                    1,
                    0,
                )
                .record
        )
    )

    val result = session.run("MATCH (n:User {userId: 'user1'}) RETURN n{.*}").single()
    result.get(0).asMap() shouldBe mapOf("userId" to "user1", "name" to "Alice")
  }

  @Test
  fun `should delete a node`() {
    session.run("CREATE CONSTRAINT FOR (n:User) REQUIRE n.userId IS KEY").consume()
    session.run("CREATE (:User {userId: 'user1', name: 'Alice'})").consume()

    task.put(
        listOf(
            newChangeEventMessage(
                    userNodeEvent(
                        "node-1",
                        EntityOperation.DELETE,
                        "user1",
                        beforeProps = mapOf("userId" to "user1", "name" to "Alice"),
                    ),
                    1,
                    0,
                )
                .record
        )
    )

    session.run("MATCH (a:User {userId: 'user1'}) RETURN count(a)").single().get(0).asInt() shouldBe
        0
  }

  @Test
  fun `should create, update and delete a node within the same batch`() {
    session.run("CREATE CONSTRAINT FOR (n:User) REQUIRE n.userId IS KEY").consume()

    task.put(
        listOf(
            newChangeEventMessage(
                    userNodeEvent(
                        "node-1",
                        EntityOperation.CREATE,
                        "user1",
                        afterProps = mapOf("userId" to "user1", "name" to "Alice"),
                    ),
                    1,
                    0,
                )
                .record,
            newChangeEventMessage(
                    userNodeEvent(
                        "node-1",
                        EntityOperation.UPDATE,
                        "user1",
                        beforeProps = mapOf("userId" to "user1", "name" to "Alice"),
                        afterProps =
                            mapOf("userId" to "user1", "name" to "Alice", "surname" to "Smith"),
                    ),
                    2,
                    0,
                )
                .record,
            newChangeEventMessage(
                    userNodeEvent(
                        "node-1",
                        EntityOperation.DELETE,
                        "user1",
                        beforeProps =
                            mapOf("userId" to "user1", "name" to "Alice", "surname" to "Smith"),
                    ),
                    3,
                    0,
                )
                .record,
        )
    )

    session.run("MATCH (a:User {userId: 'user1'}) RETURN count(a)").single().get(0).asInt() shouldBe
        0
  }

  @Test
  fun `should create node with composite key`() {
    session
        .run("CREATE CONSTRAINT FOR (n:Order) REQUIRE (n.customerId, n.orderId) IS NODE KEY")
        .consume()

    task.put(
        listOf(
            newChangeEventMessage(
                    NodeEvent(
                        "node-1",
                        EntityOperation.CREATE,
                        listOf("Order"),
                        mapOf("Order" to listOf(mapOf("customerId" to "c1", "orderId" to "o1"))),
                        null,
                        NodeState(
                            listOf("Order"),
                            mapOf("customerId" to "c1", "orderId" to "o1", "total" to 100.00),
                        ),
                    ),
                    1,
                    0,
                )
                .record
        )
    )

    session
        .run("MATCH (n:Order {customerId: 'c1', orderId: 'o1'}) RETURN n{.*}")
        .single()
        .get(0)
        .asMap() shouldBe mapOf("customerId" to "c1", "orderId" to "o1", "total" to 100.00)
  }

  @Test
  fun `should update node with composite key`() {
    session
        .run("CREATE CONSTRAINT FOR (n:Order) REQUIRE (n.customerId, n.orderId) IS NODE KEY")
        .consume()
    session.run("CREATE (:Order {customerId: 'c1', orderId: 'o1', total: 100.00})").consume()

    task.put(
        listOf(
            newChangeEventMessage(
                    NodeEvent(
                        "node-1",
                        EntityOperation.UPDATE,
                        listOf("Order"),
                        mapOf("Order" to listOf(mapOf("customerId" to "c1", "orderId" to "o1"))),
                        NodeState(
                            listOf("Order"),
                            mapOf("customerId" to "c1", "orderId" to "o1", "total" to 100.00),
                        ),
                        NodeState(
                            listOf("Order"),
                            mapOf(
                                "customerId" to "c1",
                                "orderId" to "o1",
                                "total" to 150.00,
                                "status" to "shipped",
                            ),
                        ),
                    ),
                    1,
                    0,
                )
                .record
        )
    )

    session
        .run("MATCH (n:Order {customerId: 'c1', orderId: 'o1'}) RETURN n{.*}")
        .single()
        .get(0)
        .asMap() shouldBe
        mapOf("customerId" to "c1", "orderId" to "o1", "total" to 150.00, "status" to "shipped")
  }

  @Test
  fun `should create a simple relationship between existing nodes`() {
    session.run("CREATE CONSTRAINT FOR (n:User) REQUIRE n.userId IS KEY").consume()
    session
        .run(
            "CREATE (:User {userId: 'user1', name: 'Alice'}), (:User {userId: 'user2', name: 'Bob'})"
        )
        .consume()

    task.put(
        listOf(
            newChangeEventMessage(
                    userRelEvent(
                        "rel-1",
                        "FOLLOWS",
                        userNode("node-1", "user1"),
                        userNode("node-2", "user2"),
                        EntityOperation.CREATE,
                        afterProps = mapOf("since" to LocalDate.of(2023, 6, 15)),
                    ),
                    1,
                    0,
                )
                .record
        )
    )

    session
        .run(
            "MATCH (a:User {userId: 'user1'})-[r:FOLLOWS]->(b:User {userId: 'user2'}) RETURN r{.*}"
        )
        .single()
        .get(0)
        .asMap() shouldBe mapOf("since" to LocalDate.of(2023, 6, 15))
  }

  @Test
  fun `should create relationship between nodes created in same batch`() {
    session.run("CREATE CONSTRAINT FOR (n:User) REQUIRE n.userId IS KEY").consume()

    task.put(
        listOf(
            newChangeEventMessage(
                    userNodeEvent(
                        "node-1",
                        EntityOperation.CREATE,
                        "user1",
                        afterProps = mapOf("userId" to "user1", "name" to "Alice"),
                    ),
                    1,
                    0,
                )
                .record,
            newChangeEventMessage(
                    userNodeEvent(
                        "node-2",
                        EntityOperation.CREATE,
                        "user2",
                        afterProps = mapOf("userId" to "user2", "name" to "Bob"),
                    ),
                    1,
                    1,
                )
                .record,
            newChangeEventMessage(
                    userRelEvent(
                        "rel-1",
                        "FOLLOWS",
                        userNode("node-1", "user1"),
                        userNode("node-2", "user2"),
                        EntityOperation.CREATE,
                        afterProps = mapOf("since" to LocalDate.of(2023, 6, 15)),
                    ),
                    1,
                    2,
                )
                .record,
        )
    )

    session.run("MATCH (a:User) RETURN count(a)").single().get(0).asInt() shouldBe 2
    session
        .run(
            "MATCH (a:User {userId: 'user1'})-[r:FOLLOWS]->(b:User {userId: 'user2'}) RETURN r{.*}"
        )
        .single()
        .get(0)
        .asMap() shouldBe mapOf("since" to LocalDate.of(2023, 6, 15))
  }

  @Test
  fun `should create multiple relationships between same nodes`() {
    session.run("CREATE CONSTRAINT FOR (n:User) REQUIRE n.userId IS KEY").consume()
    session.run("CREATE (:User {userId: 'user1'}), (:User {userId: 'user2'})").consume()

    task.put(
        listOf(
            newChangeEventMessage(
                    userRelEvent(
                        "rel-1",
                        "FOLLOWS",
                        userNode("node-1", "user1"),
                        userNode("node-2", "user2"),
                        EntityOperation.CREATE,
                        afterProps = emptyMap(),
                    ),
                    1,
                    0,
                )
                .record,
            newChangeEventMessage(
                    userRelEvent(
                        "rel-2",
                        "LIKES",
                        userNode("node-1", "user1"),
                        userNode("node-2", "user2"),
                        EntityOperation.CREATE,
                        afterProps = emptyMap(),
                    ),
                    1,
                    1,
                )
                .record,
            newChangeEventMessage(
                    userRelEvent(
                        "rel-3",
                        "KNOWS",
                        userNode("node-1", "user1"),
                        userNode("node-2", "user2"),
                        EntityOperation.CREATE,
                        afterProps = mapOf("since" to LocalDate.of(2020, 1, 1)),
                    ),
                    1,
                    2,
                )
                .record,
        )
    )

    session
        .run("MATCH (a:User {userId: 'user1'})-[r]->(b:User {userId: 'user2'}) RETURN count(r)")
        .single()
        .get(0)
        .asInt() shouldBe 3
    session
        .run("MATCH (a:User {userId: 'user1'})-[r:KNOWS]->(b:User {userId: 'user2'}) RETURN r{.*}")
        .single()
        .get(0)
        .asMap() shouldBe mapOf("since" to LocalDate.of(2020, 1, 1))
    session
        .run("MATCH (a:User {userId: 'user1'})-[r:LIKES]->(b:User {userId: 'user2'}) RETURN r{.*}")
        .single()
        .get(0)
        .asMap() shouldBe emptyMap()
    session
        .run(
            "MATCH (a:User {userId: 'user1'})-[r:FOLLOWS]->(b:User {userId: 'user2'}) RETURN r{.*}"
        )
        .single()
        .get(0)
        .asMap() shouldBe emptyMap()
  }

  @Test
  fun `should update a relationship`() {
    session.run("CREATE CONSTRAINT FOR (n:User) REQUIRE n.userId IS KEY").consume()
    session
        .run(
            "CREATE (:User {userId: 'user1'})-[:FOLLOWS {since: date('2020-01-01')}]->(:User {userId: 'user2'})"
        )
        .consume()

    task.put(
        listOf(
            newChangeEventMessage(
                    userRelEvent(
                        "rel-1",
                        "FOLLOWS",
                        userNode("node-1", "user1"),
                        userNode("node-2", "user2"),
                        EntityOperation.UPDATE,
                        beforeProps = mapOf("since" to LocalDate.of(2020, 1, 1)),
                        afterProps = mapOf("since" to LocalDate.of(2020, 1, 1), "strength" to 5),
                    ),
                    1,
                    0,
                )
                .record
        )
    )

    session
        .run("MATCH (:User {userId: 'user1'})-[r:FOLLOWS]->(:User {userId: 'user2'}) RETURN r{.*}")
        .single()
        .get(0)
        .asMap() shouldBe mapOf("since" to LocalDate.of(2020, 1, 1), "strength" to 5)
  }

  @Test
  fun `should remove property from relationship`() {
    session.run("CREATE CONSTRAINT FOR (n:User) REQUIRE n.userId IS KEY").consume()
    session
        .run(
            "CREATE (:User {userId: 'user1'})-[:FOLLOWS {since: date('2020-01-01'), strength: 5}]->(:User {userId: 'user2'})"
        )
        .consume()

    task.put(
        listOf(
            newChangeEventMessage(
                    userRelEvent(
                        "rel-1",
                        "FOLLOWS",
                        userNode("node-1", "user1"),
                        userNode("node-2", "user2"),
                        EntityOperation.UPDATE,
                        beforeProps = mapOf("since" to LocalDate.of(2020, 1, 1), "strength" to 5),
                        afterProps = mapOf("since" to LocalDate.of(2020, 1, 1)),
                    ),
                    1,
                    0,
                )
                .record
        )
    )

    session
        .run("MATCH (:User {userId: 'user1'})-[r:FOLLOWS]->(:User {userId: 'user2'}) RETURN r{.*}")
        .single()
        .get(0)
        .asMap() shouldBe mapOf("since" to LocalDate.of(2020, 1, 1))
  }

  @Test
  fun `should delete a relationship`() {
    session.run("CREATE CONSTRAINT FOR (n:User) REQUIRE n.userId IS KEY").consume()
    session
        .run(
            "CREATE (:User {userId: 'user1'})-[:FOLLOWS {since: date('2023-01-01')}]->(:User {userId: 'user2'})"
        )
        .consume()

    task.put(
        listOf(
            newChangeEventMessage(
                    userRelEvent(
                        "rel-1",
                        "FOLLOWS",
                        userNode("node-1", "user1"),
                        userNode("node-2", "user2"),
                        EntityOperation.DELETE,
                        beforeProps = mapOf("since" to LocalDate.of(2023, 1, 1)),
                    ),
                    1,
                    0,
                )
                .record
        )
    )

    session
        .run(
            "MATCH (a:User {userId: 'user1'})-[r:FOLLOWS]->(b:User {userId: 'user2'}) RETURN count(r)"
        )
        .single()
        .get(0)
        .asInt() shouldBe 0
    // Nodes should still exist
    session.run("MATCH (a:User) RETURN count(a)").single().get(0).asInt() shouldBe 2
  }

  @Test
  fun `should delete relationship after an update event within the same batch`() {
    session.run("CREATE CONSTRAINT FOR (n:User) REQUIRE n.userId IS KEY").consume()
    session.run("MERGE (:User {userId: 'user1'})-[:FOLLOWS]->(:User {userId: 'user2'})").consume()

    task.put(
        listOf(
            newChangeEventMessage(
                    userRelEvent(
                        "rel-1",
                        "FOLLOWS",
                        userNode("node-1", "user1"),
                        userNode("node-2", "user2"),
                        EntityOperation.UPDATE,
                        beforeProps = emptyMap(),
                        afterProps = mapOf("since" to LocalDate.of(2023, 1, 1)),
                    ),
                    1,
                    0,
                )
                .record,
            newChangeEventMessage(
                    userRelEvent(
                        "rel-1",
                        "FOLLOWS",
                        userNode("node-1", "user1"),
                        userNode("node-2", "user2"),
                        EntityOperation.DELETE,
                        beforeProps = mapOf("since" to LocalDate.of(2023, 1, 1)),
                    ),
                    2,
                    0,
                )
                .record,
        )
    )

    session
        .run(
            "MATCH (a:User {userId: 'user1'})-[r:FOLLOWS]->(b:User {userId: 'user2'}) RETURN count(r)"
        )
        .single()
        .get(0)
        .asInt() shouldBe 0
  }

  @Test
  fun `should create relationship with key`() {
    session.run("CREATE CONSTRAINT FOR (n:User) REQUIRE n.userId IS KEY").consume()
    session.run("CREATE CONSTRAINT FOR (n:Product) REQUIRE n.productId IS KEY").consume()
    session
        .run("CREATE CONSTRAINT FOR ()-[r:PURCHASED]-() REQUIRE r.orderId IS RELATIONSHIP KEY")
        .consume()
    session.run("CREATE (:User {userId: 'user1'}), (:Product {productId: 'product1'})").consume()

    task.put(
        listOf(
            newChangeEventMessage(
                    RelationshipEvent(
                        "rel-1",
                        "PURCHASED",
                        userNode("node-1", "user1"),
                        productNode("node-2", "product1"),
                        listOf(mapOf("orderId" to "order-123")),
                        EntityOperation.CREATE,
                        null,
                        RelationshipState(mapOf("orderId" to "order-123", "amount" to 99.99)),
                    ),
                    1,
                    0,
                )
                .record
        )
    )

    session
        .run(
            "MATCH (:User {userId: 'user1'})-[r:PURCHASED]->(:Product {productId: 'product1'}) RETURN r{.*}"
        )
        .single()
        .get(0)
        .asMap() shouldBe mapOf("orderId" to "order-123", "amount" to 99.99)
  }

  @Test
  fun `should update relationship with key`() {
    session.run("CREATE CONSTRAINT FOR (n:User) REQUIRE n.userId IS KEY").consume()
    session.run("CREATE CONSTRAINT FOR (n:Product) REQUIRE n.productId IS KEY").consume()
    session
        .run("CREATE CONSTRAINT FOR ()-[r:PURCHASED]-() REQUIRE r.orderId IS RELATIONSHIP KEY")
        .consume()
    session
        .run(
            "CREATE (:User {userId: 'user1'})-[:PURCHASED {orderId: 'order-123', amount: 50.00}]->(:Product {productId: 'product1'})"
        )
        .consume()

    task.put(
        listOf(
            newChangeEventMessage(
                    RelationshipEvent(
                        "rel-1",
                        "PURCHASED",
                        userNode("node-1", "user1"),
                        productNode("node-2", "product1"),
                        listOf(mapOf("orderId" to "order-123")),
                        EntityOperation.UPDATE,
                        RelationshipState(mapOf("orderId" to "order-123", "amount" to 50.00)),
                        RelationshipState(
                            mapOf(
                                "orderId" to "order-123",
                                "amount" to 75.00,
                                "status" to "updated",
                            )
                        ),
                    ),
                    1,
                    0,
                )
                .record
        )
    )

    session
        .run(
            "MATCH (:User {userId: 'user1'})-[r:PURCHASED]->(:Product {productId: 'product1'}) RETURN r{.*}"
        )
        .single()
        .get(0)
        .asMap() shouldBe mapOf("orderId" to "order-123", "amount" to 75.00, "status" to "updated")
  }

  @Test
  fun `should delete relationship with key`() {
    session.run("CREATE CONSTRAINT FOR (n:User) REQUIRE n.userId IS KEY").consume()
    session.run("CREATE CONSTRAINT FOR (n:Product) REQUIRE n.productId IS KEY").consume()
    session
        .run("CREATE CONSTRAINT FOR ()-[r:PURCHASED]-() REQUIRE r.orderId IS RELATIONSHIP KEY")
        .consume()
    session
        .run(
            "CREATE (:User {userId: 'user1'})-[:PURCHASED {orderId: 'order-123', amount: 50.00}]->(:Product {productId: 'product1'})"
        )
        .consume()

    task.put(
        listOf(
            newChangeEventMessage(
                    RelationshipEvent(
                        "rel-1",
                        "PURCHASED",
                        userNode("node-1", "user1"),
                        productNode("node-2", "product1"),
                        listOf(mapOf("orderId" to "order-123")),
                        EntityOperation.DELETE,
                        RelationshipState(mapOf("orderId" to "order-123", "amount" to 50.00)),
                        null,
                    ),
                    1,
                    0,
                )
                .record
        )
    )

    session
        .run(
            "MATCH (:User {userId: 'user1'})-[r:PURCHASED]->(:Product {productId: 'product1'}) RETURN count(r)"
        )
        .single()
        .get(0)
        .asInt() shouldBe 0
  }

  @Test
  fun `should handle multiple relationships with different keys between same nodes`() {
    session.run("CREATE CONSTRAINT FOR (n:User) REQUIRE n.userId IS KEY").consume()
    session.run("CREATE CONSTRAINT FOR (n:Product) REQUIRE n.productId IS KEY").consume()
    session
        .run("CREATE CONSTRAINT FOR ()-[r:PURCHASED]-() REQUIRE r.orderId IS RELATIONSHIP KEY")
        .consume()
    session.run("CREATE (:User {userId: 'user1'}), (:Product {productId: 'product1'})").consume()

    task.put(
        listOf(
            newChangeEventMessage(
                    RelationshipEvent(
                        "rel-1",
                        "PURCHASED",
                        userNode("node-1", "user1"),
                        productNode("node-2", "product1"),
                        listOf(mapOf("orderId" to "order-001")),
                        EntityOperation.CREATE,
                        null,
                        RelationshipState(mapOf("orderId" to "order-001", "amount" to 10.00)),
                    ),
                    1,
                    0,
                )
                .record,
            newChangeEventMessage(
                    RelationshipEvent(
                        "rel-2",
                        "PURCHASED",
                        userNode("node-1", "user1"),
                        productNode("node-2", "product1"),
                        listOf(mapOf("orderId" to "order-002")),
                        EntityOperation.CREATE,
                        null,
                        RelationshipState(mapOf("orderId" to "order-002", "amount" to 20.00)),
                    ),
                    1,
                    1,
                )
                .record,
            newChangeEventMessage(
                    RelationshipEvent(
                        "rel-3",
                        "PURCHASED",
                        userNode("node-1", "user1"),
                        productNode("node-2", "product1"),
                        listOf(mapOf("orderId" to "order-003")),
                        EntityOperation.CREATE,
                        null,
                        RelationshipState(mapOf("orderId" to "order-003", "amount" to 30.00)),
                    ),
                    1,
                    2,
                )
                .record,
        )
    )

    session
        .run(
            "MATCH (:User {userId: 'user1'})-[r:PURCHASED]->(:Product {productId: 'product1'}) RETURN count(r)"
        )
        .single()
        .get(0)
        .asInt() shouldBe 3

    session
        .run(
            "MATCH (:User {userId: 'user1'})-[r:PURCHASED {orderId: 'order-002'}]->(:Product {productId: 'product1'}) RETURN r.amount"
        )
        .single()
        .get(0)
        .asDouble() shouldBe 20.00
  }

  @Test
  fun `should update specific relationship by key when multiple exist`() {
    session.run("CREATE CONSTRAINT FOR (n:User) REQUIRE n.userId IS KEY").consume()
    session.run("CREATE CONSTRAINT FOR (n:Product) REQUIRE n.productId IS KEY").consume()
    session
        .run("CREATE CONSTRAINT FOR ()-[r:PURCHASED]-() REQUIRE r.orderId IS RELATIONSHIP KEY")
        .consume()
    session
        .run(
            """
            CREATE (u1:User {userId: 'user1'}), (p1:Product {productId: 'product1'})
            CREATE (u1)-[:PURCHASED {orderId: 'order-001', amount: 10.00}]->(p1)
            CREATE (u1)-[:PURCHASED {orderId: 'order-002', amount: 20.00}]->(p1)
            """
                .trimIndent()
        )
        .consume()

    task.put(
        listOf(
            newChangeEventMessage(
                    RelationshipEvent(
                        "rel-2",
                        "PURCHASED",
                        userNode("node-1", "user1"),
                        productNode("node-2", "product1"),
                        listOf(mapOf("orderId" to "order-002")),
                        EntityOperation.UPDATE,
                        RelationshipState(mapOf("orderId" to "order-002", "amount" to 20.00)),
                        RelationshipState(mapOf("orderId" to "order-002", "amount" to 25.00)),
                    ),
                    1,
                    0,
                )
                .record
        )
    )

    // order-001 should be unchanged
    session
        .run("MATCH (:User)-[r:PURCHASED {orderId: 'order-001'}]->(:Product) RETURN r.amount")
        .single()
        .get(0)
        .asDouble() shouldBe 10.00

    // order-002 should be updated
    session
        .run("MATCH (:User)-[r:PURCHASED {orderId: 'order-002'}]->(:Product) RETURN r.amount")
        .single()
        .get(0)
        .asDouble() shouldBe 25.00
  }

  @Test
  fun `should create relationship with composite key`() {
    session.run("CREATE CONSTRAINT FOR (n:User) REQUIRE n.userId IS KEY").consume()
    session.run("CREATE CONSTRAINT FOR (n:Product) REQUIRE n.productId IS KEY").consume()
    session
        .run(
            "CREATE CONSTRAINT FOR ()-[r:REVIEWED]-() REQUIRE (r.reviewId, r.version) IS RELATIONSHIP KEY"
        )
        .consume()
    session.run("CREATE (:User {userId: 'user1'}), (:Product {productId: 'product1'})").consume()

    task.put(
        listOf(
            newChangeEventMessage(
                    RelationshipEvent(
                        "rel-1",
                        "REVIEWED",
                        userNode("node-1", "user1"),
                        productNode("node-2", "product1"),
                        listOf(mapOf("reviewId" to "r1", "version" to 1)),
                        EntityOperation.CREATE,
                        null,
                        RelationshipState(
                            mapOf(
                                "reviewId" to "r1",
                                "version" to 1,
                                "rating" to 5,
                                "comment" to "Great!",
                            )
                        ),
                    ),
                    1,
                    0,
                )
                .record
        )
    )

    session
        .run("MATCH (:User)-[r:REVIEWED {reviewId: 'r1', version: 1}]->(:Product) RETURN r{.*}")
        .single()
        .get(0)
        .asMap() shouldBe
        mapOf("reviewId" to "r1", "version" to 1L, "rating" to 5L, "comment" to "Great!")
  }

  @Test
  fun `should update relationship with composite key`() {
    session.run("CREATE CONSTRAINT FOR (n:User) REQUIRE n.userId IS KEY").consume()
    session.run("CREATE CONSTRAINT FOR (n:Product) REQUIRE n.productId IS KEY").consume()
    session
        .run(
            "CREATE CONSTRAINT FOR ()-[r:REVIEWED]-() REQUIRE (r.reviewId, r.version) IS RELATIONSHIP KEY"
        )
        .consume()
    session
        .run(
            "CREATE (:User {userId: 'user1'})-[:REVIEWED {reviewId: 'r1', version: 1, rating: 4, comment: 'Good'}]->(:Product {productId: 'product1'})"
        )
        .consume()

    task.put(
        listOf(
            newChangeEventMessage(
                    RelationshipEvent(
                        "rel-1",
                        "REVIEWED",
                        userNode("node-1", "user1"),
                        productNode("node-2", "product1"),
                        listOf(mapOf("reviewId" to "r1", "version" to 1)),
                        EntityOperation.UPDATE,
                        RelationshipState(
                            mapOf(
                                "reviewId" to "r1",
                                "version" to 1,
                                "rating" to 4,
                                "comment" to "Good",
                            )
                        ),
                        RelationshipState(
                            mapOf(
                                "reviewId" to "r1",
                                "version" to 1,
                                "rating" to 5,
                                "comment" to "Great!",
                                "verified" to true,
                            )
                        ),
                    ),
                    1,
                    0,
                )
                .record
        )
    )

    session
        .run("MATCH (:User)-[r:REVIEWED {reviewId: 'r1', version: 1}]->(:Product) RETURN r{.*}")
        .single()
        .get(0)
        .asMap() shouldBe
        mapOf(
            "reviewId" to "r1",
            "version" to 1L,
            "rating" to 5L,
            "comment" to "Great!",
            "verified" to true,
        )
  }

  @Test
  fun `should distinguish relationships by composite key`() {
    session.run("CREATE CONSTRAINT FOR (n:User) REQUIRE n.userId IS KEY").consume()
    session.run("CREATE CONSTRAINT FOR (n:Product) REQUIRE n.productId IS KEY").consume()
    session
        .run(
            "CREATE CONSTRAINT FOR ()-[r:REVIEWED]-() REQUIRE (r.reviewId, r.version) IS RELATIONSHIP KEY"
        )
        .consume()
    session
        .run(
            """
            CREATE (u:User {userId: 'user1'}), (p:Product {productId: 'product1'})
            CREATE (u)-[:REVIEWED {reviewId: 'r1', version: 1, rating: 4}]->(p)
            CREATE (u)-[:REVIEWED {reviewId: 'r1', version: 2, rating: 5}]->(p)
            """
                .trimIndent()
        )
        .consume()

    task.put(
        listOf(
            newChangeEventMessage(
                    RelationshipEvent(
                        "rel-1",
                        "REVIEWED",
                        userNode("node-1", "user1"),
                        productNode("node-2", "product1"),
                        listOf(mapOf("reviewId" to "r1", "version" to 1)),
                        EntityOperation.UPDATE,
                        RelationshipState(mapOf("reviewId" to "r1", "version" to 1, "rating" to 4)),
                        RelationshipState(
                            mapOf(
                                "reviewId" to "r1",
                                "version" to 1,
                                "rating" to 3,
                                "edited" to true,
                            )
                        ),
                    ),
                    1,
                    0,
                )
                .record
        )
    )

    // version 1 should be updated
    session
        .run(
            "MATCH (:User)-[r:REVIEWED {reviewId: 'r1', version: 1}]->(:Product) RETURN r.rating, r.edited"
        )
        .single()
        .let {
          it.get(0).asInt() shouldBe 3
          it.get(1).asBoolean() shouldBe true
        }

    // version 2 should be unchanged
    session
        .run(
            "MATCH (:User)-[r:REVIEWED {reviewId: 'r1', version: 2}]->(:Product) RETURN r.rating, r.edited"
        )
        .single()
        .let {
          it.get(0).asInt() shouldBe 5
          it.get(1).isNull shouldBe true
        }
  }

  @Test
  fun `should handle interleaved node and relationship operations`() {
    session.run("CREATE CONSTRAINT FOR (n:User) REQUIRE n.userId IS KEY").consume()

    task.put(
        listOf(
            newChangeEventMessage(
                    userNodeEvent(
                        "node-1",
                        EntityOperation.CREATE,
                        "user1",
                        afterProps = mapOf("userId" to "user1", "name" to "Alice"),
                    ),
                    1,
                    0,
                )
                .record,
            newChangeEventMessage(
                    userNodeEvent(
                        "node-2",
                        EntityOperation.CREATE,
                        "user2",
                        afterProps = mapOf("userId" to "user2", "name" to "Bob"),
                    ),
                    1,
                    1,
                )
                .record,
            newChangeEventMessage(
                    userRelEvent(
                        "rel-1",
                        "FOLLOWS",
                        userNode("node-1", "user1"),
                        userNode("node-2", "user2"),
                        EntityOperation.CREATE,
                        afterProps = emptyMap(),
                    ),
                    1,
                    2,
                )
                .record,
            newChangeEventMessage(
                    userNodeEvent(
                        "node-1",
                        EntityOperation.UPDATE,
                        "user1",
                        beforeProps = mapOf("userId" to "user1", "name" to "Alice"),
                        afterProps = mapOf("userId" to "user1", "name" to "Alice", "age" to 30),
                    ),
                    2,
                    0,
                )
                .record,
        )
    )

    session.run("MATCH (a:User {userId: 'user1'}) RETURN a{.*}").single().get(0).asMap() shouldBe
        mapOf("userId" to "user1", "name" to "Alice", "age" to 30)
    session
        .run(
            "MATCH (:User {userId: 'user1'})-[r:FOLLOWS]->(:User {userId: 'user2'}) RETURN count(r)"
        )
        .single()
        .get(0)
        .asInt() shouldBe 1
  }

  @Test
  fun `should handle updates to multiple nodes in interleaved fashion`() {
    session.run("CREATE CONSTRAINT FOR (n:User) REQUIRE n.userId IS KEY").consume()
    session
        .run(
            "CREATE (:User {userId: 'user1', name: 'Alice'}), (:User {userId: 'user2', name: 'Bob'})"
        )
        .consume()

    task.put(
        listOf(
            newChangeEventMessage(
                    userNodeEvent(
                        "node-1",
                        EntityOperation.UPDATE,
                        "user1",
                        beforeProps = mapOf("userId" to "user1", "name" to "Alice"),
                        afterProps = mapOf("userId" to "user1", "name" to "Alice", "score" to 10),
                    ),
                    1,
                    0,
                )
                .record,
            newChangeEventMessage(
                    userNodeEvent(
                        "node-2",
                        EntityOperation.UPDATE,
                        "user2",
                        beforeProps = mapOf("userId" to "user2", "name" to "Bob"),
                        afterProps = mapOf("userId" to "user2", "name" to "Bob", "score" to 20),
                    ),
                    1,
                    1,
                )
                .record,
            newChangeEventMessage(
                    userNodeEvent(
                        "node-1",
                        EntityOperation.UPDATE,
                        "user1",
                        beforeProps = mapOf("userId" to "user1", "name" to "Alice", "score" to 10),
                        afterProps = mapOf("userId" to "user1", "name" to "Alice", "score" to 15),
                    ),
                    2,
                    0,
                )
                .record,
            newChangeEventMessage(
                    userNodeEvent(
                        "node-2",
                        EntityOperation.UPDATE,
                        "user2",
                        beforeProps = mapOf("userId" to "user2", "name" to "Bob", "score" to 20),
                        afterProps = mapOf("userId" to "user2", "name" to "Bob", "score" to 25),
                    ),
                    2,
                    1,
                )
                .record,
        )
    )

    session.run("MATCH (a:User {userId: 'user1'}) RETURN a{.*}").single().get(0).asMap() shouldBe
        mapOf("userId" to "user1", "name" to "Alice", "score" to 15)
    session.run("MATCH (a:User {userId: 'user2'}) RETURN a{.*}").single().get(0).asMap() shouldBe
        mapOf("userId" to "user2", "name" to "Bob", "score" to 25)
  }

  private fun newTaskContext(): SinkTaskContext {
    return mock<SinkTaskContext> {
      on { errantRecordReporter() } doReturn ErrantRecordReporter { _, error -> throw error }
    }
  }

  private fun userNode(elementId: String, userId: String) =
      Node(elementId, listOf("User"), mapOf("User" to listOf(mapOf("userId" to userId))))

  private fun productNode(elementId: String, productId: String) =
      Node(
          elementId,
          listOf("Product"),
          mapOf("Product" to listOf(mapOf("productId" to productId))),
      )

  private fun userNodeEvent(
      elementId: String,
      operation: EntityOperation,
      userId: String,
      beforeProps: Map<String, Any>? = null,
      afterProps: Map<String, Any>? = null,
  ) =
      NodeEvent(
          elementId,
          operation,
          listOf("User"),
          mapOf("User" to listOf(mapOf("userId" to userId))),
          beforeProps?.let { NodeState(listOf("User"), it) },
          afterProps?.let { NodeState(listOf("User"), it) },
      )

  private fun userRelEvent(
      elementId: String,
      type: String,
      startNode: Node,
      endNode: Node,
      operation: EntityOperation,
      beforeProps: Map<String, Any>? = null,
      afterProps: Map<String, Any>? = null,
      keys: List<Map<String, Any>> = emptyList(),
  ) =
      RelationshipEvent(
          elementId,
          type,
          startNode,
          endNode,
          keys,
          operation,
          beforeProps?.let { RelationshipState(it) },
          afterProps?.let { RelationshipState(it) },
      )
}
