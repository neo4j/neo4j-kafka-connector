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

import io.kotest.assertions.assertSoftly
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.assertions.withClue
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.instanceOf
import kotlin.reflect.KClass
import org.apache.kafka.connect.sink.ErrantRecordReporter
import org.apache.kafka.connect.sink.SinkTaskContext
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.neo4j.cdc.client.model.EntityOperation
import org.neo4j.cdc.client.model.Event
import org.neo4j.cdc.client.model.NodeEvent
import org.neo4j.cdc.client.model.NodeState
import org.neo4j.connectors.kafka.metrics.Metrics
import org.neo4j.connectors.kafka.sink.strategy.SinkBatchStrategy
import org.neo4j.connectors.kafka.sink.strategy.SinkHandler
import org.neo4j.connectors.kafka.sink.strategy.TestUtils.createKnowsRelationshipEvent
import org.neo4j.connectors.kafka.sink.strategy.TestUtils.newChangeEventMessage
import org.neo4j.driver.Driver
import org.testcontainers.containers.Neo4jContainer

/**
 * End to end checks of CDC schema strict mode, driven through the real [Neo4jSinkTask] against a
 * real server. Subclasses pick the batching path: with APOC, or without it.
 *
 * Strict mode turns "the event found nothing to act on" into a task failure: the batch is rolled
 * back together with its exactly-once offset tracker, and the failure is raised rather than sent to
 * the dead letter queue. The reporter handed to the task here rethrows, so any record that is
 * wrongly diverted to the DLQ fails the test too.
 */
abstract class Neo4jSinkStrictModeIT(
    private val expectedBatchStrategy: KClass<out SinkBatchStrategy>
) {
  abstract fun container(): Neo4jContainer<*>

  abstract fun driver(): Driver

  companion object {
    const val TOPIC = "my-topic"
    const val EOS_LABEL = "__KafkaOffset"
  }

  private lateinit var task: Neo4jSinkTask

  @BeforeEach
  fun before() {
    execute("MATCH (n) DETACH DELETE n")
    execute("DROP CONSTRAINT person_id IF EXISTS")

    task = Neo4jSinkTask()
    task.initialize(
        mock<SinkTaskContext> {
          on { errantRecordReporter() } doReturn ErrantRecordReporter { _, error -> throw error }
        }
    )
    task.start(
        mapOf(
            "topics" to TOPIC,
            "neo4j.uri" to container().boltUrl,
            "neo4j.authentication.type" to "NONE",
            "neo4j.cdc.schema.topics" to TOPIC,
            "neo4j.cdc.strict-mode" to "true",
            "neo4j.eos-offset-label" to EOS_LABEL,
            "neo4j.eos-offset-auto-constraint" to "true",
        )
    )

    val handler = SinkStrategyHandler.createFrom(task.config, mock<Metrics>())[TOPIC]
    withClue("the batching strategy under test") {
      (handler as SinkHandler).batchStrategy shouldBe instanceOf(expectedBatchStrategy)
    }
  }

  @AfterEach
  fun after() {
    task.stop()
  }

  @Test
  fun `should apply events that find their targets`() {
    execute("CREATE CONSTRAINT person_id IF NOT EXISTS FOR (n:Person) REQUIRE n.id IS UNIQUE")

    task.put(
        listOf(
            message(
                personEvent(EntityOperation.CREATE, 1, after = mapOf("id" to 1, "name" to "Ann")),
                1,
            ),
            message(
                personEvent(
                    EntityOperation.UPDATE,
                    1,
                    before = mapOf("id" to 1, "name" to "Ann"),
                    after = mapOf("id" to 1, "name" to "Anna"),
                ),
                2,
            ),
        )
    )

    assertSoftly {
      withClue("create then update applied") { personName(1) shouldBe "Anna" }
      withClue("the tracker advanced to the last offset") { trackerOffset() shouldBe 2L }
    }
  }

  @Test
  fun `should fail when an update targets a node that does not exist`() {
    val failure =
        shouldThrow<StrictModeViolationException> {
          task.put(
              listOf(
                  message(
                      personEvent(
                          EntityOperation.UPDATE,
                          9,
                          before = mapOf("id" to 9, "name" to "Zed"),
                          after = mapOf("id" to 9, "name" to "Zoe"),
                      ),
                      5,
                  )
              )
          )
        }

    assertSoftly {
      withClue("the failure names the offset") { failure.offsets shouldBe listOf(5L) }
      withClue("an update must not invent a node") { personCount() shouldBe 0L }
      withClue("nothing was committed, so there is no tracker") { trackerOffset() shouldBe null }
    }
  }

  @Test
  fun `should fail when a delete targets a node that does not exist`() {
    val failure =
        shouldThrow<StrictModeViolationException> {
          task.put(
              listOf(message(personEvent(EntityOperation.DELETE, 9, before = mapOf("id" to 9)), 7))
          )
        }

    failure.offsets shouldBe listOf(7L)
  }

  @Test
  fun `should fail when a relationship create is missing an end node`() {
    execute("CREATE (:Person {id: 1})")

    val failure =
        shouldThrow<StrictModeViolationException> {
          task.put(listOf(message(createKnowsRelationshipEvent(1, 9, 100), 3)))
        }

    assertSoftly {
      failure.offsets shouldBe listOf(3L)
      withClue("no relationship was invented") { relationshipCount() shouldBe 0L }
    }
  }

  @Test
  fun `should fail a duplicate create when a uniqueness constraint exists`() {
    execute("CREATE CONSTRAINT person_id IF NOT EXISTS FOR (n:Person) REQUIRE n.id IS UNIQUE")
    execute("CREATE (:Person {id: 1, name: 'Ann'})")

    // not a StrictModeViolationException: the database rejects it, so the exact type is the
    // driver's. What matters is that it is raised and nothing is absorbed.
    shouldThrow<Throwable> {
      task.put(
          listOf(
              message(
                  personEvent(EntityOperation.CREATE, 1, after = mapOf("id" to 1, "name" to "Bob")),
                  4,
              )
          )
      )
    }

    assertSoftly {
      withClue("the existing node is untouched") { personName(1) shouldBe "Ann" }
      withClue("no duplicate was created") { personCount() shouldBe 1L }
    }
  }

  @Test
  fun `should roll back the whole batch and the tracker when one event fails`() {
    execute("CREATE (:Person {id: 1, name: 'Ann'})")

    shouldThrow<StrictModeViolationException> {
      task.put(
          listOf(
              message(
                  personEvent(
                      EntityOperation.UPDATE,
                      1,
                      before = mapOf("id" to 1, "name" to "Ann"),
                      after = mapOf("id" to 1, "name" to "Anna"),
                  ),
                  10,
              ),
              message(
                  personEvent(
                      EntityOperation.UPDATE,
                      9,
                      before = mapOf("id" to 9, "name" to "Zed"),
                      after = mapOf("id" to 9, "name" to "Zoe"),
                  ),
                  11,
              ),
          )
      )
    }

    assertSoftly {
      withClue("the good update in the same batch was undone") { personName(1) shouldBe "Ann" }
      withClue("the tracker did not move") { trackerOffset() shouldBe null }
    }
  }

  @Test
  fun `should not fail when an already applied offset is replayed`() {
    execute("CREATE CONSTRAINT person_id IF NOT EXISTS FOR (n:Person) REQUIRE n.id IS UNIQUE")
    val create = message(personEvent(EntityOperation.CREATE, 1, after = mapOf("id" to 1)), 1)

    task.put(listOf(create))
    // exactly-once skips it, so a plain CREATE must not hit the constraint a second time
    task.put(listOf(create))

    assertSoftly {
      personCount() shouldBe 1L
      trackerOffset() shouldBe 1L
    }
  }

  private fun message(event: Event, offset: Long) =
      newChangeEventMessage(event, offset, 0, offset).record

  private fun personEvent(
      operation: EntityOperation,
      id: Int,
      before: Map<String, Any>? = null,
      after: Map<String, Any>? = null,
  ) =
      NodeEvent(
          "4:cafe:$id",
          operation,
          listOf("Person"),
          mapOf("Person" to listOf(mapOf("id" to id))),
          before?.let { NodeState(listOf("Person"), it) },
          after?.let { NodeState(listOf("Person"), it) },
      )

  private fun personName(id: Int): String? =
      single("MATCH (n:Person {id: $id}) RETURN n.name AS value") { it.asString() }

  private fun personCount(): Long? =
      single("MATCH (n:Person) RETURN count(n) AS value") { it.asLong() }

  private fun relationshipCount(): Long? =
      single("MATCH ()-[r]->() RETURN count(r) AS value") { it.asLong() }

  private fun trackerOffset(): Long? =
      single("MATCH (k:$EOS_LABEL) RETURN k.offset AS value") { it.asLong() }

  private fun <T> single(cypher: String, extract: (org.neo4j.driver.Value) -> T): T? =
      driver().session().use { session ->
        session.run(cypher).list().singleOrNull()?.get("value")?.takeIf { !it.isNull }?.let(extract)
      }

  private fun execute(cypher: String) {
    driver().session().use { it.run(cypher).consume() }
  }
}
