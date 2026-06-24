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
package org.neo4j.connectors.kafka.sink.strategy.cypher

import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.instanceOf
import java.util.UUID
import kotlin.reflect.KClass
import org.apache.kafka.common.record.TimestampType
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

abstract class CypherHandlerIT(
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

  private fun startTask(query: String) {
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
          this["neo4j.cypher.topic.my-topic"] = query
          if (eosOffsetLabel.isNotEmpty()) {
            this["neo4j.eos-offset-label"] = eosOffsetLabel
          }
        }
    )

    val metricsMock: Metrics = mock()
    val handler = SinkStrategyHandler.createFrom(task.config, metricsMock)["my-topic"]
    handler shouldBe instanceOf<SinkHandler>()

    val sinkHandler = handler as SinkHandler
    sinkHandler.eventTransformer shouldBe instanceOf<CypherEventTransformer>()
    sinkHandler.batchStrategy shouldBe instanceOf(expectedBatchStrategy)
  }

  @Test
  fun `should create a node`() {
    session.createNodeKeyConstraint(neo4j(), "user_key", "User", "userId")
    startTask("MERGE (u:User {userId: event.userId}) SET u.name = event.name")

    task.put(listOf(newMessage(mapOf("userId" to "user1", "name" to "Alice"), 0)))

    session.run("MATCH (a:User {userId: 'user1'}) RETURN a{.*}").single().get(0).asMap() shouldBe
        mapOf("userId" to "user1", "name" to "Alice")

    verifyEosOffsetIfEnabled(session, SinkStrategy.CYPHER, eosOffsetLabel, 0)
  }

  @Test
  fun `should process multiple records in a single batch`() {
    session.createNodeKeyConstraint(neo4j(), "user_key", "User", "userId")
    startTask("MERGE (u:User {userId: event.userId}) SET u.name = event.name")

    task.put(
        listOf(
            newMessage(mapOf("userId" to "user1", "name" to "Alice"), 0),
            newMessage(mapOf("userId" to "user2", "name" to "Bob"), 1),
            newMessage(mapOf("userId" to "user3", "name" to "Charlie"), 2),
        )
    )

    session.run("MATCH (a:User) RETURN count(a)").single().get(0).asInt() shouldBe 3

    verifyEosOffsetIfEnabled(session, SinkStrategy.CYPHER, eosOffsetLabel, 2)
  }

  @Test
  fun `should update an existing node`() {
    session.createNodeKeyConstraint(neo4j(), "user_key", "User", "userId")
    session.run("CREATE (:User {userId: 'user1', name: 'Alice', age: 25})").consume()
    startTask("MERGE (u:User {userId: event.userId}) SET u.name = event.name")

    task.put(listOf(newMessage(mapOf("userId" to "user1", "name" to "Alice Updated"), 0)))

    session.run("MATCH (a:User) RETURN count(a)").single().get(0).asInt() shouldBe 1
    session.run("MATCH (a:User {userId: 'user1'}) RETURN a{.*}").single().get(0).asMap() shouldBe
        mapOf("userId" to "user1", "name" to "Alice Updated", "age" to 25L)

    verifyEosOffsetIfEnabled(session, SinkStrategy.CYPHER, eosOffsetLabel, 0)
  }

  @Test
  fun `should handle exactly once semantics when retrying same batch`() {
    assumeTrue { eosOffsetLabel.isNotEmpty() }

    startTask("CREATE (u:User) SET u = event")

    val batch =
        listOf(
            newMessage(mapOf("userId" to "user1", "name" to "Alice"), 0),
            newMessage(mapOf("userId" to "user2", "name" to "Bob"), 1),
            newMessage(mapOf("userId" to "user3", "name" to "Charlie"), 2),
        )

    task.put(batch)

    session.run("MATCH (a:User) RETURN count(a)").single().get(0).asInt() shouldBe 3
    verifyEosOffsetIfEnabled(session, SinkStrategy.CYPHER, eosOffsetLabel, 2)

    // Retry same batch with an additional message, only the new one should be processed
    task.put(batch + listOf(newMessage(mapOf("userId" to "user4", "name" to "David"), 3)))

    session.run("MATCH (a:User) RETURN count(a)").single().get(0).asInt() shouldBe 4
    session
        .run("MATCH (a:User {userId: 'user4'}) RETURN a.name")
        .single()
        .get(0)
        .asString() shouldBe "David"

    verifyEosOffsetIfEnabled(session, SinkStrategy.CYPHER, eosOffsetLabel, 3)
  }

  protected fun newTaskContext(): SinkTaskContext {
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
