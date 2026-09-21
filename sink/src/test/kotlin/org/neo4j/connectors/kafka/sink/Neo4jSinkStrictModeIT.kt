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
import io.kotest.assertions.withClue
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.neo4j.cdc.client.model.EntityOperation
import org.neo4j.cdc.client.model.Node
import org.neo4j.cdc.client.model.NodeEvent
import org.neo4j.cdc.client.model.NodeState
import org.neo4j.cdc.client.model.RelationshipEvent
import org.neo4j.cdc.client.model.RelationshipState
import org.neo4j.connectors.kafka.metrics.Metrics
import org.neo4j.connectors.kafka.metrics.MetricsFactory
import org.neo4j.connectors.kafka.sink.strategy.TestUtils
import org.neo4j.connectors.kafka.testing.createNeo4jContainer
import org.neo4j.driver.summary.ResultSummary
import org.testcontainers.containers.Neo4jContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

/**
 * Cypher treats "matched zero rows" as an ordinary outcome, so a CDC update aimed at a node the
 * target database does not have succeeds while changing nothing. No exception reaches the task, so
 * neither the dead letter queue routing nor the driver's retry can notice, and the sink carries on
 * widening the gap between source and target.
 *
 * Before any strict mode is built, we need to know whether the server gives us a signal at all.
 * `ResultSummary.counters()` is the only candidate, and [Neo4jSinkTask] currently discards it. The
 * tests here run the real generated statements against a real database and keep the summary, to
 * establish what that signal actually looks like.
 *
 * Spike: `docs/SPIKE-cdc-sink-strict-mode.md`, specs in `docs/SPIKE-strict-mode-test-specs.md`.
 * This class covers T1 and spec A-1.
 *
 * Note this container has no APOC, so
 * [org.neo4j.connectors.kafka.sink.strategy.NativeBatchStrategy] is the path under test. Option C's
 * prototype will need an APOC-enabled container.
 */
@Testcontainers
class Neo4jSinkStrictModeIT {
  companion object {
    @Container val container: Neo4jContainer<*> = createNeo4jContainer()

    private const val TOPIC = "my-topic"
  }

  private lateinit var config: SinkConfiguration
  private lateinit var metrics: Metrics
  private lateinit var handler: SinkStrategyHandler

  @BeforeEach
  fun before() {
    config =
        SinkConfiguration(
            mapOf(
                "topics" to TOPIC,
                "neo4j.uri" to container.boltUrl,
                "neo4j.authentication.type" to "NONE",
                "neo4j.cdc.schema.topics" to TOPIC,
            )
        )
    metrics = MetricsFactory().createMetrics(config)
    handler = SinkStrategyHandler.createFrom(config, metrics).getValue(TOPIC)

    execute("MATCH (n) DETACH DELETE n")
  }

  @AfterEach
  fun after() {
    metrics.close()
    config.close()
  }

  @Test
  fun `harness applies a cdc create to the database`() {
    val summaries =
        applyAsSinkTaskWould(
            TestUtils.newChangeEventMessage(
                nodeEvent(EntityOperation.CREATE, before = null, after = personState("John")),
                txId = 1,
                seq = 0,
                offset = 0,
            )
        )

    assertSoftly {
      withClue("the batch must produce exactly one statement") { summaries shouldHaveSize 1 }
      withClue("creating a node must move the counters") {
        summaries.single().counters().containsUpdates() shouldBe true
      }
      withClue("the person must actually be in the database") { personName(id = 1) shouldBe "John" }
    }
  }

  @Test
  fun `update for a missing node invents a partial node instead of doing nothing`() {
    withClue("the node must be absent, or this test proves nothing") { nodeCount() shouldBe 0 }

    val summaries =
        applyAsSinkTaskWould(
            TestUtils.newChangeEventMessage(
                nodeEvent(
                    EntityOperation.UPDATE,
                    before = personState("John"),
                    after = personState("Jane"),
                ),
                txId = 2,
                seq = 0,
                offset = 1,
            )
        )

    assertSoftly {
      withClue("MERGE creates the node, so the counters move and this looks like success") {
        summaries.single().counters().containsUpdates() shouldBe true
      }
      withClue("a node the source never had now exists in the target") { nodeCount() shouldBe 1 }
      withClue("and it holds only the key plus what the diff changed -- age is lost") {
        personProperties(id = 1) shouldBe mapOf("id" to 1L, "name" to "Jane")
      }
    }
  }

  @Test
  fun `update for an existing node keeps properties the diff did not mention`() {
    execute("CREATE (:Person {id: 1, name: 'John', age: 30})")

    val summaries =
        applyAsSinkTaskWould(
            TestUtils.newChangeEventMessage(
                nodeEvent(
                    EntityOperation.UPDATE,
                    before = personState("John"),
                    after = personState("Jane"),
                ),
                txId = 4,
                seq = 0,
                offset = 3,
            )
        )

    assertSoftly {
      withClue("the update must land") {
        summaries.single().counters().containsUpdates() shouldBe true
      }
      withClue("merging must not duplicate the node") { nodeCount() shouldBe 1L }
      withClue("age was not in the diff, but += must leave it alone") {
        personProperties(id = 1) shouldBe mapOf("id" to 1L, "name" to "Jane", "age" to 30L)
      }
    }
  }

  @Test
  fun `relationship whose end nodes are missing commits without changing anything`() {
    withClue("the end nodes must be absent, or this test proves nothing") { nodeCount() shouldBe 0 }

    val summaries =
        applyAsSinkTaskWould(
            TestUtils.newChangeEventMessage(
                TestUtils.createKnowsRelationshipEvent(startId = 1, endId = 2, id = 3),
                txId = 3,
                seq = 0,
                offset = 2,
            )
        )

    assertSoftly {
      withClue("the write must succeed, which is why no error handler can see this") {
        summaries shouldHaveSize 1
      }
      withClue("containsUpdates is the signal strict mode would rely on") {
        summaries.single().counters().containsUpdates() shouldBe false
      }
      withClue("no relationship was created, because the end nodes were not found") {
        summaries.single().counters().relationshipsCreated() shouldBe 0
      }
      withClue("MATCH must not invent the nodes it failed to find") { nodeCount() shouldBe 0 }
    }
  }

  @Test
  fun `deleting an already deleted relationship reports the same as a real failure`() {
    execute("CREATE (:Person {id: 1}), (:Person {id: 2})")

    val summaries =
        applyAsSinkTaskWould(
            TestUtils.newChangeEventMessage(
                knowsEvent(EntityOperation.DELETE, before = RelationshipState(emptyMap())),
                txId = 5,
                seq = 0,
                offset = 4,
            )
        )

    assertSoftly {
      withClue("there was nothing to delete, so nothing was deleted") {
        summaries.single().counters().relationshipsDeleted() shouldBe 0
      }
      withClue("indistinguishable from a genuine failure by the summary alone") {
        summaries.single().counters().containsUpdates() shouldBe false
      }
      withClue("the end nodes are still here -- this is the signal counters do not give us") {
        nodeCount() shouldBe 2L
      }
    }
  }

  @Test
  fun `creating an existing relationship reports the same as a real failure`() {
    execute("CREATE (a:Person {id: 1}), (b:Person {id: 2}) CREATE (a)-[:KNOWS {id: 3}]->(b)")

    val summaries =
        applyAsSinkTaskWould(
            TestUtils.newChangeEventMessage(
                knowsEvent(EntityOperation.CREATE, after = RelationshipState(emptyMap())),
                txId = 6,
                seq = 0,
                offset = 5,
            )
        )

    assertSoftly {
      withClue("the relationship was already there, so the merge matched it") {
        summaries.single().counters().relationshipsCreated() shouldBe 0
      }
      withClue("indistinguishable from a genuine failure by the summary alone") {
        summaries.single().counters().containsUpdates() shouldBe false
      }
      withClue("and it must not have been duplicated") { relationshipCount() shouldBe 1L }
    }
  }

  /**
   * Mirrors [Neo4jSinkTask.processMessages], except that the summaries are kept rather than
   * discarded.
   */
  private fun applyAsSinkTaskWould(vararg messages: SinkMessage): List<ResultSummary> =
      handler.handle(messages.toList()).flatMap { group ->
        config.driver.session(config.sessionConfig()).use { session ->
          session.executeWrite(
              { tx -> group.map { tx.run(it.query).consume() } },
              config.txConfig(),
          )
        }
      }

  private fun nodeEvent(
      operation: EntityOperation,
      before: NodeState?,
      after: NodeState?,
  ): NodeEvent =
      NodeEvent(
          "4:abcd:1",
          operation,
          listOf("Person"),
          mapOf("Person" to listOf(mapOf("id" to 1L))),
          before,
          after,
      )

  private fun knowsEvent(
      operation: EntityOperation,
      before: RelationshipState? = null,
      after: RelationshipState? = null,
  ): RelationshipEvent =
      RelationshipEvent(
          "5:abcd:3",
          "KNOWS",
          Node("4:abcd:1", listOf("Person"), mapOf("Person" to listOf(mapOf("id" to 1L)))),
          Node("4:abcd:2", listOf("Person"), mapOf("Person" to listOf(mapOf("id" to 2L)))),
          listOf(mapOf("id" to 3L)),
          operation,
          before,
          after,
      )

  private fun personState(name: String, age: Long = 30L): NodeState =
      NodeState(listOf("Person"), mapOf("id" to 1L, "name" to name, "age" to age))

  private fun nodeCount(): Long? = query("MATCH (n) RETURN count(n) AS value") { it.asLong() }

  private fun relationshipCount(): Long? =
      query("MATCH ()-[r]->() RETURN count(r) AS value") { it.asLong() }

  private fun personProperties(id: Long): Map<String, Any>? =
      query("MATCH (p:Person {id: $id}) RETURN properties(p) AS value") { it.asMap() }

  private fun personName(id: Long): String? =
      query("MATCH (p:Person {id: $id}) RETURN p.name AS value") { it.asString(null) }

  private fun <T> query(cypher: String, extract: (org.neo4j.driver.Value) -> T): T? =
      config.driver.session(config.sessionConfig()).use { session ->
        session.run(cypher).list().singleOrNull()?.get("value")?.let(extract)
      }

  private fun execute(cypher: String) {
    config.driver.session(config.sessionConfig()).use { it.run(cypher).consume() }
  }
}
