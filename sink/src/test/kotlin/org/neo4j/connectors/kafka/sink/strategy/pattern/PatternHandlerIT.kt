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
package org.neo4j.connectors.kafka.sink.strategy.pattern

import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.instanceOf
import java.util.UUID
import kotlin.reflect.KClass
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.ErrantRecordReporter
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTaskContext
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.neo4j.caniuse.Neo4j
import org.neo4j.connectors.kafka.metrics.Metrics
import org.neo4j.connectors.kafka.sink.Neo4jSinkTask
import org.neo4j.connectors.kafka.sink.SinkStrategy
import org.neo4j.connectors.kafka.sink.SinkStrategyHandler
import org.neo4j.connectors.kafka.sink.strategy.SinkBatchStrategy
import org.neo4j.connectors.kafka.sink.strategy.SinkHandler
import org.neo4j.connectors.kafka.sink.strategy.TestUtils.verifyEosOffsetIfEnabled
import org.neo4j.connectors.kafka.testing.DatabaseSupport.createDatabase
import org.neo4j.connectors.kafka.testing.DatabaseSupport.dropDatabase
import org.neo4j.connectors.kafka.testing.createNodeKeyConstraint
import org.neo4j.driver.Driver
import org.neo4j.driver.Session
import org.neo4j.driver.SessionConfig
import org.testcontainers.containers.Neo4jContainer

abstract class PatternHandlerIT(
    val eosOffsetLabel: String,
    val expectedBatchStrategy: KClass<out SinkBatchStrategy>,
) {
  abstract fun container(): Neo4jContainer<*>

  abstract fun neo4j(): Neo4j

  abstract fun driver(): Driver

  private lateinit var task: Neo4jSinkTask
  protected lateinit var db: String
  private lateinit var session: Session

  @BeforeEach
  fun setup() {
    db = "test-${UUID.randomUUID()}"
    driver().createDatabase(db)
    session = driver().session(SessionConfig.forDatabase(db))
  }

  @AfterEach
  fun tearDown() {
    if (this::task.isInitialized) task.stop()
    if (this::db.isInitialized) driver().dropDatabase(db)
    if (this::session.isInitialized) session.close()
  }

  private fun startTask(
      pattern: String,
      mergeNodeProperties: Boolean = true,
      mergeRelationshipProperties: Boolean = true,
  ) {
    if (eosOffsetLabel.isNotEmpty()) {
      session.createNodeKeyConstraint(
          neo4j(),
          "eos_offset_key",
          eosOffsetLabel,
          "strategy",
          "topic",
          "partition",
      )
    }

    task = Neo4jSinkTask()
    task.initialize(newTaskContext())
    task.start(
        buildMap {
          this["topics"] = "my-topic"
          this["neo4j.database"] = db
          this["neo4j.uri"] = container().boltUrl
          this["neo4j.authentication.type"] = "NONE"
          this["neo4j.pattern.topic.my-topic"] = pattern
          this["neo4j.pattern.merge-node-properties"] = mergeNodeProperties.toString()
          this["neo4j.pattern.merge-relationship-properties"] =
              mergeRelationshipProperties.toString()
          if (eosOffsetLabel.isNotEmpty()) {
            this["neo4j.eos-offset-label"] = eosOffsetLabel
          }
        }
    )

    val metricsMock: Metrics = mock()
    val handler = SinkStrategyHandler.createFrom(task.config, metricsMock)["my-topic"]
    handler shouldBe instanceOf<SinkHandler>()

    val sinkHandler = handler as SinkHandler
    sinkHandler.batchStrategy shouldBe instanceOf(expectedBatchStrategy)
    when (Pattern.parse(pattern)) {
      is NodePattern ->
          sinkHandler.eventTransformer shouldBe instanceOf<NodePatternEventTransformer>()

      is RelationshipPattern ->
          sinkHandler.eventTransformer shouldBe instanceOf<RelationshipPatternEventTransformer>()
    }
  }

  // Node Pattern Tests

  @Test
  fun `should create a node via node pattern`() {
    session.createNodeKeyConstraint(neo4j(), "user_key", "User", "userId")
    startTask("(:User{!userId, name})")

    task.put(listOf(newMessage(mapOf("userId" to "user1", "name" to "Alice"), 0)))

    val node = session.run("MATCH (a:User {userId: 'user1'}) RETURN a").single().get(0).asNode()
    node.labels().toSet() shouldBe setOf("User")
    node.asMap() shouldBe mapOf("userId" to "user1", "name" to "Alice")

    verifyEosOffsetIfEnabled(session, SinkStrategy.NODE_PATTERN, eosOffsetLabel, 0)
  }

  @Test
  fun `should create node with multiple labels`() {
    session.createNodeKeyConstraint(neo4j(), "person_key", "Person", "personId")
    startTask("(:Person:Employee{!personId, name, department})")

    task.put(
        listOf(
            newMessage(
                mapOf("personId" to "p1", "name" to "Alice", "department" to "Engineering"),
                0,
            )
        )
    )

    val node =
        session.run("MATCH (n:Person:Employee {personId: 'p1'}) RETURN n").single().get(0).asNode()
    node.asMap() shouldBe
        mapOf("personId" to "p1", "name" to "Alice", "department" to "Engineering")
    node.labels().toSet() shouldBe setOf("Employee", "Person")

    verifyEosOffsetIfEnabled(session, SinkStrategy.NODE_PATTERN, eosOffsetLabel, 0)
  }

  @Test
  fun `should create multiple nodes in a single batch`() {
    session.createNodeKeyConstraint(neo4j(), "user_key", "User", "userId")
    startTask("(:User{!userId, name})")

    task.put(
        listOf(
            newMessage(mapOf("userId" to "user1", "name" to "Alice"), 0),
            newMessage(mapOf("userId" to "user2", "name" to "Bob"), 1),
            newMessage(mapOf("userId" to "user3", "name" to "Charlie"), 2),
        )
    )

    session.run("MATCH (a:User) RETURN count(a)").single().get(0).asInt() shouldBe 3

    verifyEosOffsetIfEnabled(session, SinkStrategy.NODE_PATTERN, eosOffsetLabel, 2)
  }

  @Test
  fun `should merge a node with merge-node-properties enabled`() {
    session.createNodeKeyConstraint(neo4j(), "user_key", "User", "userId")
    session.run("CREATE (:User {userId: 'user1', name: 'Alice', age: 25})").consume()
    startTask("(:User{!userId, name})", mergeNodeProperties = true)

    task.put(listOf(newMessage(mapOf("userId" to "user1", "name" to "Alice Updated"), 0)))

    // age should be preserved since merge-node-properties=true uses SET +=
    val node = session.run("MATCH (a:User {userId: 'user1'}) RETURN a").single().get(0).asNode()
    node.asMap() shouldBe mapOf("userId" to "user1", "name" to "Alice Updated", "age" to 25L)
    node.labels().toSet() shouldBe setOf("User")

    verifyEosOffsetIfEnabled(session, SinkStrategy.NODE_PATTERN, eosOffsetLabel, 0)
  }

  @Test
  fun `should replace node properties with merge-node-properties disabled`() {
    session.createNodeKeyConstraint(neo4j(), "user_key", "User", "userId")
    session.run("CREATE (:User {userId: 'user1', name: 'Alice', age: 25})").consume()
    startTask("(:User{!userId, name})", mergeNodeProperties = false)

    task.put(listOf(newMessage(mapOf("userId" to "user1", "name" to "Alice Updated"), 0)))

    // age should be removed since merge-node-properties=false uses SET =
    val node = session.run("MATCH (a:User {userId: 'user1'}) RETURN a").single().get(0).asNode()
    node.asMap() shouldBe mapOf("userId" to "user1", "name" to "Alice Updated")
    node.labels().toSet() shouldBe setOf("User")

    verifyEosOffsetIfEnabled(session, SinkStrategy.NODE_PATTERN, eosOffsetLabel, 0)
  }

  @Test
  fun `should delete a node via tombstone message`() {
    session.createNodeKeyConstraint(neo4j(), "user_key", "User", "userId")
    session.run("CREATE (:User {userId: 'user1', name: 'Alice'})").consume()
    startTask("(:User{!userId, name})")

    val keySchema = SchemaBuilder.struct().field("userId", Schema.STRING_SCHEMA).build()
    val key = Struct(keySchema).put("userId", "user1")

    task.put(
        listOf(
            SinkRecord(
                "my-topic",
                0,
                keySchema,
                key,
                null,
                null,
                0,
                System.currentTimeMillis(),
                TimestampType.CREATE_TIME,
            )
        )
    )

    session.run("MATCH (a:User {userId: 'user1'}) RETURN count(a)").single().get(0).asInt() shouldBe
        0

    verifyEosOffsetIfEnabled(session, SinkStrategy.NODE_PATTERN, eosOffsetLabel, 0)
  }

  @Test
  fun `should handle node pattern with composite key`() {
    session.createNodeKeyConstraint(neo4j(), "order_key", "Order", "orderId", "lineItem")
    startTask("(:Order{!orderId, !lineItem, product, quantity})")

    task.put(
        listOf(
            newMessage(
                mapOf(
                    "orderId" to "order1",
                    "lineItem" to 1,
                    "product" to "Widget",
                    "quantity" to 10,
                ),
                0,
            ),
            newMessage(
                mapOf(
                    "orderId" to "order1",
                    "lineItem" to 2,
                    "product" to "Gadget",
                    "quantity" to 5,
                ),
                1,
            ),
        )
    )

    val orders =
        session.run("MATCH (o:Order) RETURN o ORDER BY o.orderId, o.lineItem").list {
          it.get(0).asNode()
        }

    orders shouldHaveSize 2
    orders.map { it.asMap() } shouldBe
        listOf(
            mapOf(
                "orderId" to "order1",
                "lineItem" to 1L,
                "product" to "Widget",
                "quantity" to 10L,
            ),
            mapOf("orderId" to "order1", "lineItem" to 2L, "product" to "Gadget", "quantity" to 5L),
        )
    orders.map { it.labels().toSet() } shouldBe listOf(setOf("Order"), setOf("Order"))

    verifyEosOffsetIfEnabled(session, SinkStrategy.NODE_PATTERN, eosOffsetLabel, 1)
  }

  // Relationship Pattern Tests

  @Test
  fun `should create a relationship between existing nodes`() {
    session.createNodeKeyConstraint(neo4j(), "user_key", "User", "userId")
    session.run("CREATE (:User {userId: 'user1'}), (:User {userId: 'user2'})").consume()
    startTask("(:User{!userId: start.userId})-[:FOLLOWS{since}]->(:User{!userId: end.userId})")

    task.put(
        listOf(
            newMessage(
                mapOf("start.userId" to "user1", "end.userId" to "user2", "since" to "2023-06-15"),
                0,
            )
        )
    )

    val rel =
        session
            .run(
                "MATCH (a:User {userId: 'user1'})-[r:FOLLOWS]->(b:User {userId: 'user2'}) RETURN r"
            )
            .single()
            .get(0)
            .asRelationship()
    rel.asMap() shouldBe mapOf("since" to "2023-06-15")

    verifyEosOffsetIfEnabled(session, SinkStrategy.RELATIONSHIP_PATTERN, eosOffsetLabel, 0)
  }

  @Test
  fun `should create relationship and merge endpoint nodes`() {
    session.createNodeKeyConstraint(neo4j(), "user_key", "User", "userId")
    startTask("(:User{!userId: start.userId})-[:FOLLOWS{since}]->(:User{!userId: end.userId})")

    task.put(
        listOf(
            newMessage(
                mapOf("start.userId" to "user1", "end.userId" to "user2", "since" to "2023-01-01"),
                0,
            )
        )
    )

    val nodes = session.run("MATCH (a:User) RETURN a ORDER BY a.userId").list { it.get(0).asNode() }
    nodes shouldHaveSize 2
    nodes.map { it.asMap() } shouldBe listOf(mapOf("userId" to "user1"), mapOf("userId" to "user2"))

    val rel =
        session
            .run(
                "MATCH (a:User {userId: 'user1'})-[r:FOLLOWS]->(b:User {userId: 'user2'}) RETURN r"
            )
            .single()
            .get(0)
            .asRelationship()
    rel.asMap() shouldBe mapOf("since" to "2023-01-01")

    verifyEosOffsetIfEnabled(session, SinkStrategy.RELATIONSHIP_PATTERN, eosOffsetLabel, 0)
  }

  @Test
  fun `should merge relationship with merge-relationship-properties enabled`() {
    session.createNodeKeyConstraint(neo4j(), "user_key", "User", "userId")
    session
        .run(
            "CREATE (:User {userId: 'user1'})-[:FOLLOWS {since: '2020-01-01', note: 'original'}]->(:User {userId: 'user2'})"
        )
        .consume()
    startTask(
        "(:User{!userId: start.userId})-[:FOLLOWS{since}]->(:User{!userId: end.userId})",
        mergeRelationshipProperties = true,
    )

    task.put(
        listOf(
            newMessage(
                mapOf("start.userId" to "user1", "end.userId" to "user2", "since" to "2023-01-01"),
                0,
            )
        )
    )

    // note should be preserved since merge-relationship-properties=true uses SET +=
    val rel =
        session
            .run("MATCH (:User {userId: 'user1'})-[r:FOLLOWS]->(:User {userId: 'user2'}) RETURN r")
            .single()
            .get(0)
            .asRelationship()
    rel.asMap() shouldBe mapOf("since" to "2023-01-01", "note" to "original")

    verifyEosOffsetIfEnabled(session, SinkStrategy.RELATIONSHIP_PATTERN, eosOffsetLabel, 0)
  }

  @Test
  fun `should replace relationship properties with merge-relationship-properties disabled`() {
    session.createNodeKeyConstraint(neo4j(), "user_key", "User", "userId")
    session
        .run(
            "CREATE (:User {userId: 'user1'})-[:FOLLOWS {since: '2020-01-01', note: 'original'}]->(:User {userId: 'user2'})"
        )
        .consume()
    startTask(
        "(:User{!userId: start.userId})-[:FOLLOWS{since}]->(:User{!userId: end.userId})",
        mergeRelationshipProperties = false,
    )

    task.put(
        listOf(
            newMessage(
                mapOf("start.userId" to "user1", "end.userId" to "user2", "since" to "2023-01-01"),
                0,
            )
        )
    )

    // note should be removed since merge-relationship-properties=false uses SET =
    val rel =
        session
            .run("MATCH (:User {userId: 'user1'})-[r:FOLLOWS]->(:User {userId: 'user2'}) RETURN r")
            .single()
            .get(0)
            .asRelationship()
    rel.asMap() shouldBe mapOf("since" to "2023-01-01")

    verifyEosOffsetIfEnabled(session, SinkStrategy.RELATIONSHIP_PATTERN, eosOffsetLabel, 0)
  }

  @Test
  fun `should merge endpoint node properties with merge-node-properties enabled`() {
    session.createNodeKeyConstraint(neo4j(), "user_key", "User", "userId")
    session
        .run("CREATE (:User {userId: 'user1', age: 25}), (:User {userId: 'user2', age: 30})")
        .consume()
    startTask(
        "(:User{!userId: start.userId, name: start.name})-[:FOLLOWS{since}]->(:User{!userId: end.userId, name: end.name})",
        mergeNodeProperties = true,
    )

    task.put(
        listOf(
            newMessage(
                mapOf(
                    "start.userId" to "user1",
                    "start.name" to "Alice",
                    "end.userId" to "user2",
                    "end.name" to "Bob",
                    "since" to "2023-01-01",
                ),
                0,
            )
        )
    )

    // age should be preserved on both nodes
    val nodes = session.run("MATCH (a:User) RETURN a ORDER BY a.userId").list { it.get(0).asNode() }
    nodes.map { it.asMap() } shouldBe
        listOf(
            mapOf("userId" to "user1", "name" to "Alice", "age" to 25L),
            mapOf("userId" to "user2", "name" to "Bob", "age" to 30L),
        )

    verifyEosOffsetIfEnabled(session, SinkStrategy.RELATIONSHIP_PATTERN, eosOffsetLabel, 0)
  }

  @Test
  fun `should replace endpoint node properties with merge-node-properties disabled`() {
    session.createNodeKeyConstraint(neo4j(), "user_key", "User", "userId")
    session
        .run("CREATE (:User {userId: 'user1', age: 25}), (:User {userId: 'user2', age: 30})")
        .consume()
    startTask(
        "(:User{!userId: start.userId, name: start.name})-[:FOLLOWS{since}]->(:User{!userId: end.userId, name: end.name})",
        mergeNodeProperties = false,
    )

    task.put(
        listOf(
            newMessage(
                mapOf(
                    "start.userId" to "user1",
                    "start.name" to "Alice",
                    "end.userId" to "user2",
                    "end.name" to "Bob",
                    "since" to "2023-01-01",
                ),
                0,
            )
        )
    )

    // age should be removed from both nodes
    val nodes = session.run("MATCH (a:User) RETURN a ORDER BY a.userId").list { it.get(0).asNode() }
    nodes.map { it.asMap() } shouldBe
        listOf(
            mapOf("userId" to "user1", "name" to "Alice"),
            mapOf("userId" to "user2", "name" to "Bob"),
        )

    verifyEosOffsetIfEnabled(session, SinkStrategy.RELATIONSHIP_PATTERN, eosOffsetLabel, 0)
  }

  @Test
  fun `should create multiple relationships in a batch`() {
    session.createNodeKeyConstraint(neo4j(), "user_key", "User", "userId")
    session
        .run(
            "CREATE (:User {userId: 'user1'}), (:User {userId: 'user2'}), (:User {userId: 'user3'})"
        )
        .consume()
    startTask("(:User{!userId: start.userId})-[:FOLLOWS{since}]->(:User{!userId: end.userId})")

    task.put(
        listOf(
            newMessage(
                mapOf("start.userId" to "user1", "end.userId" to "user2", "since" to "2023-01-01"),
                0,
            ),
            newMessage(
                mapOf("start.userId" to "user2", "end.userId" to "user3", "since" to "2023-02-01"),
                1,
            ),
            newMessage(
                mapOf("start.userId" to "user1", "end.userId" to "user3", "since" to "2023-03-01"),
                2,
            ),
        )
    )

    val rels = session.run("MATCH (s)-[r:FOLLOWS]->(e) RETURN s, r, e ORDER BY r.since").list()
    rels shouldHaveSize 3
    rels.map {
      it.get("s").asNode().asMap() to
          it.get("r").asRelationship().asMap() to
          it.get("e").asNode().asMap()
    } shouldBe
        listOf(
            mapOf("userId" to "user1") to
                mapOf("since" to "2023-01-01") to
                mapOf("userId" to "user2"),
            mapOf("userId" to "user2") to
                mapOf("since" to "2023-02-01") to
                mapOf("userId" to "user3"),
            mapOf("userId" to "user1") to
                mapOf("since" to "2023-03-01") to
                mapOf("userId" to "user3"),
        )

    verifyEosOffsetIfEnabled(session, SinkStrategy.RELATIONSHIP_PATTERN, eosOffsetLabel, 2)
  }

  @Test
  fun `should handle relationship pattern with different start and end labels`() {
    session.createNodeKeyConstraint(neo4j(), "person_key", "Person", "personId")
    session.createNodeKeyConstraint(neo4j(), "company_key", "Company", "companyId")
    session
        .run(
            "CREATE (:Person {personId: 'p1', name: 'Alice'}), (:Company {companyId: 'c1', name: 'Acme'})"
        )
        .consume()
    startTask(
        "(:Person{!personId: person.id})-[:WORKS_AT{role, startDate}]->(:Company{!companyId: company.id})"
    )

    task.put(
        listOf(
            newMessage(
                mapOf(
                    "person.id" to "p1",
                    "company.id" to "c1",
                    "role" to "Engineer",
                    "startDate" to "2022-03-01",
                ),
                0,
            )
        )
    )

    val rel =
        session
            .run(
                "MATCH (:Person {personId: 'p1'})-[r:WORKS_AT]->(:Company {companyId: 'c1'}) RETURN r"
            )
            .single()
            .get(0)
            .asRelationship()
    rel.asMap() shouldBe mapOf("role" to "Engineer", "startDate" to "2022-03-01")

    verifyEosOffsetIfEnabled(session, SinkStrategy.RELATIONSHIP_PATTERN, eosOffsetLabel, 0)
  }

  @Test
  fun `should delete a relationship via tombstone message`() {
    session.createNodeKeyConstraint(neo4j(), "user_key", "User", "userId")
    session
        .run(
            "CREATE (:User {userId: 'user1'})-[:FOLLOWS {since: '2023-01-01'}]->(:User {userId: 'user2'})"
        )
        .consume()
    startTask("(:User{!userId: start.userId})-[:FOLLOWS]->(:User{!userId: end.userId})")

    val keySchema =
        SchemaBuilder.struct()
            .field("start.userId", Schema.STRING_SCHEMA)
            .field("end.userId", Schema.STRING_SCHEMA)
            .build()
    val key = Struct(keySchema).put("start.userId", "user1").put("end.userId", "user2")

    task.put(
        listOf(
            SinkRecord(
                "my-topic",
                0,
                keySchema,
                key,
                null,
                null,
                0,
                System.currentTimeMillis(),
                TimestampType.CREATE_TIME,
            )
        )
    )

    session
        .run(
            "MATCH (:User {userId: 'user1'})-[r:FOLLOWS]->(:User {userId: 'user2'}) RETURN count(r)"
        )
        .single()
        .get(0)
        .asInt() shouldBe 0
    // Nodes should still exist
    session.run("MATCH (a:User) RETURN count(a)").single().get(0).asInt() shouldBe 2

    verifyEosOffsetIfEnabled(session, SinkStrategy.RELATIONSHIP_PATTERN, eosOffsetLabel, 0)
  }

  // Exactly Once Semantics Tests

  @Test
  fun `should handle exactly once semantics when retrying same batch for node pattern`() {
    assumeTrue { eosOffsetLabel.isNotEmpty() }

    session.createNodeKeyConstraint(neo4j(), "user_key", "User", "userId")
    startTask("(:User{!userId, name})")

    val batch =
        listOf(
            newMessage(mapOf("userId" to "user1", "name" to "Alice"), 0),
            newMessage(mapOf("userId" to "user2", "name" to "Bob"), 1),
            newMessage(mapOf("userId" to "user3", "name" to "Charlie"), 2),
        )

    task.put(batch)

    session.run("MATCH (a:User) RETURN count(a)").single().get(0).asInt() shouldBe 3
    verifyEosOffsetIfEnabled(session, SinkStrategy.NODE_PATTERN, eosOffsetLabel, 2)

    // Retry same batch with an additional message, only the new one should be processed
    task.put(batch + listOf(newMessage(mapOf("userId" to "user4", "name" to "David"), 3)))

    session.run("MATCH (a:User) RETURN count(a)").single().get(0).asInt() shouldBe 4
    session
        .run("MATCH (a:User {userId: 'user4'}) RETURN a.name")
        .single()
        .get(0)
        .asString() shouldBe "David"

    verifyEosOffsetIfEnabled(session, SinkStrategy.NODE_PATTERN, eosOffsetLabel, 3)
  }

  @Test
  fun `should handle exactly once semantics when retrying same batch for relationship pattern`() {
    assumeTrue { eosOffsetLabel.isNotEmpty() }

    session.createNodeKeyConstraint(neo4j(), "user_key", "User", "userId")
    session.run("CREATE (:User {userId: 'user1'}), (:User {userId: 'user2'})").consume()
    startTask("(:User{!userId: start.userId})-[:FOLLOWS{since}]->(:User{!userId: end.userId})")

    val batch =
        listOf(
            newMessage(
                mapOf("start.userId" to "user1", "end.userId" to "user2", "since" to "2023-01-01"),
                0,
            )
        )

    task.put(batch)

    session
        .run(
            "MATCH (a:User {userId: 'user1'})-[r:FOLLOWS]->(b:User {userId: 'user2'}) RETURN r{.*}"
        )
        .single()
        .get(0)
        .asMap() shouldBe mapOf("since" to "2023-01-01")

    verifyEosOffsetIfEnabled(session, SinkStrategy.RELATIONSHIP_PATTERN, eosOffsetLabel, 0)

    // Retry same batch with an additional message, only the new one should be processed
    task.put(
        batch +
            listOf(
                newMessage(
                    mapOf(
                        "start.userId" to "user1",
                        "end.userId" to "user3",
                        "since" to "2025-01-01",
                    ),
                    1,
                )
            )
    )

    session
        .run("MATCH (a:User {userId: 'user1'})-[r:FOLLOWS]->(b:User {userId: 'user3'}) RETURN r")
        .single()
        .get(0)
        .asMap() shouldBe mapOf("since" to "2025-01-01")

    verifyEosOffsetIfEnabled(session, SinkStrategy.RELATIONSHIP_PATTERN, eosOffsetLabel, 1)
  }

  private fun newTaskContext(): SinkTaskContext {
    return mock<SinkTaskContext> {
      on { errantRecordReporter() } doReturn ErrantRecordReporter { _, error -> throw error }
    }
  }

  private fun newMessage(value: Map<String, Any?>, offset: Long): SinkRecord {
    return SinkRecord(
        "my-topic",
        0,
        null,
        null,
        null,
        value,
        offset,
        System.currentTimeMillis(),
        TimestampType.CREATE_TIME,
    )
  }
}
