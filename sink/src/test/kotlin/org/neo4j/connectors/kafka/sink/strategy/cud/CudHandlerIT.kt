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
package org.neo4j.connectors.kafka.sink.strategy.cud

import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.instanceOf
import java.util.UUID
import kotlin.reflect.KClass
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.connect.data.Schema
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
import org.neo4j.connectors.kafka.sink.SinkStrategy.CUD
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

abstract class CudHandlerIT(
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
  fun setupTask() {
    db = "test-${UUID.randomUUID()}"
    driver().createDatabase(db)
    session = driver().session(SessionConfig.forDatabase(db))

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
          this["neo4j.cud.topics"] = "my-topic"
          if (eosOffsetLabel.isNotEmpty()) {
            this["neo4j.eos-offset-label"] = eosOffsetLabel
          }
        }
    )

    val metricsMock: Metrics = mock()
    val handler = SinkStrategyHandler.createFrom(task.config, metricsMock)["my-topic"]
    handler shouldBe instanceOf<SinkHandler>()

    val sinkHandler = handler as SinkHandler
    sinkHandler.eventTransformer shouldBe instanceOf<CudEventTransformer>()
    sinkHandler.batchStrategy shouldBe instanceOf(expectedBatchStrategy)
  }

  @AfterEach
  fun stopTask() {
    if (this::task.isInitialized) task.stop()
    if (this::db.isInitialized) driver().dropDatabase(db)
    if (this::session.isInitialized) session.close()
  }

  @Test
  fun `should create a simple node`() {
    session.createNodeKeyConstraint(neo4j(), "user_key", "User", "userId")

    task.put(
        listOf(
            newCudMessage(
                """
                {
                  "type": "node",
                  "op": "create",
                  "labels": ["User"],
                  "properties": {
                    "userId": "user1",
                    "name": "Alice"
                  }
                }
                """,
                0,
            )
        )
    )

    session.run("MATCH (a:User {userId: 'user1'}) RETURN a{.*}").single().get(0).asMap() shouldBe
        mapOf("userId" to "user1", "name" to "Alice")

    verifyEosOffsetIfEnabled(session, CUD, eosOffsetLabel, 0)
  }

  @Test
  fun `should create node with multiple labels`() {
    session.createNodeKeyConstraint(neo4j(), "person_key", "Person", "personId")

    task.put(
        listOf(
            newCudMessage(
                """
                {
                  "type": "node",
                  "op": "create",
                  "labels": ["Person", "Employee", "Manager"],
                  "properties": {
                    "personId": "p1",
                    "name": "Alice",
                    "department": "Engineering"
                  }
                }
                """,
                0,
            )
        )
    )

    val result =
        session
            .run("MATCH (n:Person:Employee:Manager {personId: 'p1'}) RETURN n{.*}, labels(n)")
            .single()
    result.get(0).asMap() shouldBe
        mapOf("personId" to "p1", "name" to "Alice", "department" to "Engineering")
    result.get(1).asList { it.asString() }.sorted() shouldBe listOf("Employee", "Manager", "Person")

    verifyEosOffsetIfEnabled(session, CUD, eosOffsetLabel, 0)
  }

  @Test
  fun `should create multiple nodes in a single batch`() {
    session.createNodeKeyConstraint(neo4j(), "user_key", "User", "userId")

    task.put(
        listOf(
            newCudMessage(
                """
                {
                  "type": "node",
                  "op": "create",
                  "labels": ["User"],
                  "properties": {
                    "userId": "user1",
                    "name": "Alice"
                  }
                }
                """,
                0,
            ),
            newCudMessage(
                """
                {
                  "type": "node",
                  "op": "create",
                  "labels": ["User"],
                  "properties": {
                    "userId": "user2",
                    "name": "Bob"
                  }
                }
                """,
                1,
            ),
            newCudMessage(
                """
                {
                  "type": "node",
                  "op": "create",
                  "labels": ["User"],
                  "properties": {
                    "userId": "user3",
                    "name": "Charlie"
                  }
                }
                """,
                2,
            ),
        )
    )

    session.run("MATCH (a:User) RETURN count(a)").single().get(0).asInt() shouldBe 3

    verifyEosOffsetIfEnabled(session, CUD, eosOffsetLabel, 2)
  }

  @Test
  fun `should update a node`() {
    session.createNodeKeyConstraint(neo4j(), "user_key", "User", "userId")
    session.run("CREATE (:User {userId: 'user1', name: 'Alice'})").consume()

    task.put(
        listOf(
            newCudMessage(
                """
                {
                  "type": "node",
                  "op": "update",
                  "labels": ["User"],
                  "ids": {
                    "userId": "user1"
                  },
                  "properties": {
                    "name": "Alice Updated",
                    "age": 30
                  }
                }
                """,
                0,
            )
        )
    )

    session.run("MATCH (a:User {userId: 'user1'}) RETURN a{.*}").single().get(0).asMap() shouldBe
        mapOf("userId" to "user1", "name" to "Alice Updated", "age" to 30L)

    verifyEosOffsetIfEnabled(session, CUD, eosOffsetLabel, 0)
  }

  @Test
  fun `should merge a node`() {
    session.createNodeKeyConstraint(neo4j(), "user_key", "User", "userId")

    task.put(
        listOf(
            newCudMessage(
                """
                {
                  "type": "node",
                  "op": "merge",
                  "labels": ["User"],
                  "ids": {
                    "userId": "user1"
                  },
                  "properties": {
                    "name": "Alice",
                    "age": 25
                  }
                }
                """,
                0,
            )
        )
    )

    session.run("MATCH (a:User {userId: 'user1'}) RETURN a{.*}").single().get(0).asMap() shouldBe
        mapOf("userId" to "user1", "name" to "Alice", "age" to 25L)

    verifyEosOffsetIfEnabled(session, CUD, eosOffsetLabel, 0)
  }

  @Test
  fun `should merge existing node`() {
    session.createNodeKeyConstraint(neo4j(), "user_key", "User", "userId")
    session.run("CREATE (:User {userId: 'user1', name: 'Alice'})").consume()

    task.put(
        listOf(
            newCudMessage(
                """
                {
                  "type": "node",
                  "op": "merge",
                  "labels": ["User"],
                  "ids": {
                    "userId": "user1"
                  },
                  "properties": {
                    "age": 30
                  }
                }
                """,
                0,
            )
        )
    )

    session.run("MATCH (a:User {userId: 'user1'}) RETURN a{.*}").single().get(0).asMap() shouldBe
        mapOf("userId" to "user1", "name" to "Alice", "age" to 30L)

    verifyEosOffsetIfEnabled(session, CUD, eosOffsetLabel, 0)
  }

  @Test
  fun `should delete a node`() {
    session.createNodeKeyConstraint(neo4j(), "user_key", "User", "userId")
    session.run("CREATE (:User {userId: 'user1', name: 'Alice'})").consume()

    task.put(
        listOf(
            newCudMessage(
                """
                {
                  "type": "node",
                  "op": "delete",
                  "labels": ["User"],
                  "ids": {
                    "userId": "user1"
                  }
                }
                """,
                0,
            )
        )
    )

    session.run("MATCH (a:User {userId: 'user1'}) RETURN count(a)").single().get(0).asInt() shouldBe
        0

    verifyEosOffsetIfEnabled(session, CUD, eosOffsetLabel, 0)
  }

  @Test
  fun `should detach delete a node`() {
    session.createNodeKeyConstraint(neo4j(), "user_key", "User", "userId")
    session.createNodeKeyConstraint(neo4j(), "product_key", "Product", "productId")
    session
        .run(
            """
            CREATE (u:User {userId: 'user1', name: 'Alice'})
            CREATE (p:Product {productId: 'prod1', name: 'Widget'})
            CREATE (u)-[:PURCHASED]->(p)
            """
        )
        .consume()

    task.put(
        listOf(
            newCudMessage(
                """
                {
                  "type": "node",
                  "op": "delete",
                  "labels": ["User"],
                  "ids": {
                    "userId": "user1"
                  },
                  "detach": true
                }
                """,
                0,
            )
        )
    )

    session.run("MATCH (a:User {userId: 'user1'}) RETURN count(a)").single().get(0).asInt() shouldBe
        0
    session.run("MATCH (p:Product) RETURN count(p)").single().get(0).asInt() shouldBe 1
    session.run("MATCH ()-[r:PURCHASED]->() RETURN count(r)").single().get(0).asInt() shouldBe 0

    verifyEosOffsetIfEnabled(session, CUD, eosOffsetLabel, 0)
  }

  @Test
  fun `should create, update and delete a node within the same batch`() {
    session.createNodeKeyConstraint(neo4j(), "user_key", "User", "userId")

    task.put(
        listOf(
            newCudMessage(
                """
                {
                  "type": "node",
                  "op": "create",
                  "labels": ["User"],
                  "properties": {
                    "userId": "user1",
                    "name": "Alice"
                  }
                }
                """,
                0,
            ),
            newCudMessage(
                """
                {
                  "type": "node",
                  "op": "update",
                  "labels": ["User"],
                  "ids": {
                    "userId": "user1"
                  },
                  "properties": {
                    "name": "Alice Updated",
                    "surname": "Smith"
                  }
                }
                """,
                1,
            ),
            newCudMessage(
                """
                {
                  "type": "node",
                  "op": "delete",
                  "labels": ["User"],
                  "ids": {
                    "userId": "user1"
                  }
                }
                """,
                2,
            ),
        )
    )

    session.run("MATCH (a:User {userId: 'user1'}) RETURN count(a)").single().get(0).asInt() shouldBe
        0

    verifyEosOffsetIfEnabled(session, CUD, eosOffsetLabel, 2)
  }

  @Test
  fun `should create a simple relationship between existing nodes`() {
    session.createNodeKeyConstraint(neo4j(), "user_key", "User", "userId")
    session
        .run(
            "CREATE (:User {userId: 'user1', name: 'Alice'}), (:User {userId: 'user2', name: 'Bob'})"
        )
        .consume()

    task.put(
        listOf(
            newCudMessage(
                """
                {
                  "type": "relationship",
                  "op": "create",
                  "rel_type": "FOLLOWS",
                  "from": {
                    "labels": ["User"],
                    "ids": {
                      "userId": "user1"
                    }
                  },
                  "to": {
                    "labels": ["User"],
                    "ids": {
                      "userId": "user2"
                    }
                  },
                  "properties": {
                    "since": "2023-06-15"
                  }
                }
                """,
                0,
            )
        )
    )

    session
        .run(
            "MATCH (a:User {userId: 'user1'})-[r:FOLLOWS]->(b:User {userId: 'user2'}) RETURN r{.*}"
        )
        .single()
        .get(0)
        .asMap() shouldBe mapOf("since" to "2023-06-15")

    verifyEosOffsetIfEnabled(session, CUD, eosOffsetLabel, 0)
  }

  @Test
  fun `should create relationship by merging nodes`() {
    session.createNodeKeyConstraint(neo4j(), "user_key", "User", "userId")

    task.put(
        listOf(
            newCudMessage(
                """
                {
                  "type": "relationship",
                  "op": "create",
                  "rel_type": "FOLLOWS",
                  "from": {
                    "labels": ["User"],
                    "ids": {
                      "userId": "user1"
                    },
                    "op": "merge"
                  },
                  "to": {
                    "labels": ["User"],
                    "ids": {
                      "userId": "user2"
                    },
                    "op": "merge"
                  },
                  "properties": {
                    "since": "2023-06-15"
                  }
                }
                """,
                0,
            )
        )
    )

    session.run("MATCH (a:User) RETURN count(a)").single().get(0).asInt() shouldBe 2
    session
        .run(
            "MATCH (a:User {userId: 'user1'})-[r:FOLLOWS]->(b:User {userId: 'user2'}) RETURN r{.*}"
        )
        .single()
        .get(0)
        .asMap() shouldBe mapOf("since" to "2023-06-15")

    verifyEosOffsetIfEnabled(session, CUD, eosOffsetLabel, 0)
  }

  @Test
  fun `should create multiple relationships between same nodes`() {
    session.createNodeKeyConstraint(neo4j(), "user_key", "User", "userId")
    session.run("CREATE (:User {userId: 'user1'}), (:User {userId: 'user2'})").consume()

    task.put(
        listOf(
            newCudMessage(
                """
                {
                  "type": "relationship",
                  "op": "create",
                  "rel_type": "FOLLOWS",
                  "from": {
                    "labels": ["User"],
                    "ids": { "userId": "user1" }
                  },
                  "to": {
                    "labels": ["User"],
                    "ids": { "userId": "user2" }
                  },
                  "properties": {}
                }
                """,
                0,
            ),
            newCudMessage(
                """
                {
                  "type": "relationship",
                  "op": "create",
                  "rel_type": "LIKES",
                  "from": {
                    "labels": ["User"],
                    "ids": { "userId": "user1" }
                  },
                  "to": {
                    "labels": ["User"],
                    "ids": { "userId": "user2" }
                  },
                  "properties": {}
                }
                """,
                1,
            ),
            newCudMessage(
                """
                {
                  "type": "relationship",
                  "op": "create",
                  "rel_type": "KNOWS",
                  "from": {
                    "labels": ["User"],
                    "ids": { "userId": "user1" }
                  },
                  "to": {
                    "labels": ["User"],
                    "ids": { "userId": "user2" }
                  },
                  "properties": {
                    "since": "2020-01-01"
                  }
                }
                """,
                2,
            ),
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
        .asMap() shouldBe mapOf("since" to "2020-01-01")

    verifyEosOffsetIfEnabled(session, CUD, eosOffsetLabel, 2)
  }

  @Test
  fun `should update a relationship`() {
    session.createNodeKeyConstraint(neo4j(), "user_key", "User", "userId")
    session
        .run(
            "CREATE (:User {userId: 'user1'})-[:FOLLOWS {since: '2020-01-01'}]->(:User {userId: 'user2'})"
        )
        .consume()

    task.put(
        listOf(
            newCudMessage(
                """
                {
                  "type": "relationship",
                  "op": "update",
                  "rel_type": "FOLLOWS",
                  "from": {
                    "labels": ["User"],
                    "ids": { "userId": "user1" }
                  },
                  "to": {
                    "labels": ["User"],
                    "ids": { "userId": "user2" }
                  },
                  "properties": {
                    "since": "2020-01-01",
                    "strength": 5
                  }
                }
                """,
                0,
            )
        )
    )

    session
        .run("MATCH (:User {userId: 'user1'})-[r:FOLLOWS]->(:User {userId: 'user2'}) RETURN r{.*}")
        .single()
        .get(0)
        .asMap() shouldBe mapOf("since" to "2020-01-01", "strength" to 5L)

    verifyEosOffsetIfEnabled(session, CUD, eosOffsetLabel, 0)
  }

  @Test
  fun `should merge a relationship`() {
    session.createNodeKeyConstraint(neo4j(), "user_key", "User", "userId")
    session.run("CREATE (:User {userId: 'user1'}), (:User {userId: 'user2'})").consume()

    task.put(
        listOf(
            newCudMessage(
                """
                {
                  "type": "relationship",
                  "op": "merge",
                  "rel_type": "FOLLOWS",
                  "from": {
                    "labels": ["User"],
                    "ids": { "userId": "user1" }
                  },
                  "to": {
                    "labels": ["User"],
                    "ids": { "userId": "user2" }
                  },
                  "properties": {
                    "since": "2023-01-01"
                  }
                }
                """,
                0,
            )
        )
    )

    session
        .run("MATCH (:User {userId: 'user1'})-[r:FOLLOWS]->(:User {userId: 'user2'}) RETURN r{.*}")
        .single()
        .get(0)
        .asMap() shouldBe mapOf("since" to "2023-01-01")

    verifyEosOffsetIfEnabled(session, CUD, eosOffsetLabel, 0)
  }

  @Test
  fun `should delete a relationship`() {
    session.createNodeKeyConstraint(neo4j(), "user_key", "User", "userId")
    session
        .run(
            "CREATE (:User {userId: 'user1'})-[:FOLLOWS {since: '2023-01-01'}]->(:User {userId: 'user2'})"
        )
        .consume()

    task.put(
        listOf(
            newCudMessage(
                """
                {
                  "type": "relationship",
                  "op": "delete",
                  "rel_type": "FOLLOWS",
                  "from": {
                    "labels": ["User"],
                    "ids": { "userId": "user1" }
                  },
                  "to": {
                    "labels": ["User"],
                    "ids": { "userId": "user2" }
                  }
                }
                """,
                0,
            )
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

    verifyEosOffsetIfEnabled(session, CUD, eosOffsetLabel, 0)
  }

  @Test
  fun `should create nodes and relationships in same batch`() {
    session.createNodeKeyConstraint(neo4j(), "user_key", "User", "userId")

    task.put(
        listOf(
            newCudMessage(
                """
                {
                  "type": "node",
                  "op": "create",
                  "labels": ["User"],
                  "properties": {
                    "userId": "user1",
                    "name": "Alice"
                  }
                }
                """,
                0,
            ),
            newCudMessage(
                """
                {
                  "type": "node",
                  "op": "create",
                  "labels": ["User"],
                  "properties": {
                    "userId": "user2",
                    "name": "Bob"
                  }
                }
                """,
                1,
            ),
            newCudMessage(
                """
                {
                  "type": "relationship",
                  "op": "create",
                  "rel_type": "FOLLOWS",
                  "from": {
                    "labels": ["User"],
                    "ids": { "userId": "user1" }
                  },
                  "to": {
                    "labels": ["User"],
                    "ids": { "userId": "user2" }
                  },
                  "properties": {
                    "since": "2023-06-15"
                  }
                }
                """,
                2,
            ),
        )
    )

    session.run("MATCH (a:User) RETURN count(a)").single().get(0).asInt() shouldBe 2
    session
        .run(
            "MATCH (a:User {userId: 'user1'})-[r:FOLLOWS]->(b:User {userId: 'user2'}) RETURN r{.*}"
        )
        .single()
        .get(0)
        .asMap() shouldBe mapOf("since" to "2023-06-15")

    verifyEosOffsetIfEnabled(session, CUD, eosOffsetLabel, 2)
  }

  @Test
  fun `should handle mixed node operations in batch`() {
    session.createNodeKeyConstraint(neo4j(), "user_key", "User", "userId")
    session.run("CREATE (:User {userId: 'existing', name: 'Existing User'})").consume()

    task.put(
        listOf(
            newCudMessage(
                """
                {
                  "type": "node",
                  "op": "create",
                  "labels": ["User"],
                  "properties": {
                    "userId": "new1",
                    "name": "New User 1"
                  }
                }
                """,
                0,
            ),
            newCudMessage(
                """
                {
                  "type": "node",
                  "op": "update",
                  "labels": ["User"],
                  "ids": { "userId": "existing" },
                  "properties": {
                    "name": "Updated Existing"
                  }
                }
                """,
                1,
            ),
            newCudMessage(
                """
                {
                  "type": "node",
                  "op": "merge",
                  "labels": ["User"],
                  "ids": { "userId": "merged" },
                  "properties": {
                    "name": "Merged User"
                  }
                }
                """,
                2,
            ),
        )
    )

    session.run("MATCH (a:User) RETURN count(a)").single().get(0).asInt() shouldBe 3
    session
        .run("MATCH (a:User {userId: 'existing'}) RETURN a.name")
        .single()
        .get(0)
        .asString() shouldBe "Updated Existing"
    session
        .run("MATCH (a:User {userId: 'merged'}) RETURN a.name")
        .single()
        .get(0)
        .asString() shouldBe "Merged User"

    verifyEosOffsetIfEnabled(session, CUD, eosOffsetLabel, 2)
  }

  @Test
  fun `should handle exactly once semantics when retrying same batch`() {
    // This cannot work when not using EOS offset tracking, as the change queries would be
    // re-executed and fail due to key changes
    assumeTrue { eosOffsetLabel.isNotEmpty() }

    session.createNodeKeyConstraint(neo4j(), "user_key", "User", "userId")

    val batch =
        listOf(
            newCudMessage(
                """
                {
                  "type": "node",
                  "op": "create",
                  "labels": ["User"],
                  "properties": {
                    "userId": "user1",
                    "name": "Alice"
                  }
                }
                """,
                0,
            ),
            newCudMessage(
                """
                {
                  "type": "node",
                  "op": "update",
                  "labels": ["User"],
                  "ids": { "userId": "user1" },
                  "properties": {
                    "name": "Alice Updated"
                  }
                }
                """,
                1,
            ),
            newCudMessage(
                """
                {
                  "type": "node",
                  "op": "merge",
                  "labels": ["User"],
                  "ids": { "userId": "user2" },
                  "properties": {
                    "name": "Bob"
                  }
                }
                """,
                2,
            ),
        )

    // First execution
    task.put(batch)

    session.run("MATCH (a:User) RETURN count(a)").single().get(0).asInt() shouldBe 2
    session
        .run("MATCH (a:User {userId: 'user1'}) RETURN a.name")
        .single()
        .get(0)
        .asString() shouldBe "Alice Updated"
    session
        .run("MATCH (a:User {userId: 'user2'}) RETURN a.name")
        .single()
        .get(0)
        .asString() shouldBe "Bob"

    verifyEosOffsetIfEnabled(session, CUD, eosOffsetLabel, 2)

    // Retry same batch - with EOS enabled, this should be a no-op
    task.put(batch)

    // Should still have exactly 2 users (no duplicates)
    session.run("MATCH (a:User) RETURN count(a)").single().get(0).asInt() shouldBe 2
    session
        .run("MATCH (a:User {userId: 'user1'}) RETURN a.name")
        .single()
        .get(0)
        .asString() shouldBe "Alice Updated"
    session
        .run("MATCH (a:User {userId: 'user2'}) RETURN a.name")
        .single()
        .get(0)
        .asString() shouldBe "Bob"

    verifyEosOffsetIfEnabled(session, CUD, eosOffsetLabel, 2)
  }

  protected fun newTaskContext(): SinkTaskContext {
    return mock<SinkTaskContext> {
      on { errantRecordReporter() } doReturn ErrantRecordReporter { _, error -> throw error }
    }
  }

  private fun newCudMessage(json: String, offset: Long): SinkRecord {
    return SinkRecord(
        "my-topic",
        0,
        null,
        null,
        Schema.STRING_SCHEMA,
        json.trimIndent(),
        offset,
        System.currentTimeMillis(),
        TimestampType.CREATE_TIME,
    )
  }
}
