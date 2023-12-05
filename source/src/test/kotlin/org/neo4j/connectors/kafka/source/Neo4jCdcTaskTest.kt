/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.connectors.kafka.source

import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.comparables.shouldBeGreaterThan
import io.kotest.matchers.comparables.shouldBeLessThan
import kotlin.time.Duration.Companion.seconds
import kotlin.time.measureTime
import org.apache.kafka.connect.source.SourceTask
import org.apache.kafka.connect.source.SourceTaskContext
import org.apache.kafka.connect.storage.OffsetStorageReader
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.mockito.ArgumentMatchers
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.neo4j.connectors.kafka.configuration.AuthenticationType
import org.neo4j.connectors.kafka.configuration.Neo4jConfiguration
import org.neo4j.driver.Session
import org.testcontainers.containers.Neo4jContainer
import org.testcontainers.junit.jupiter.Testcontainers

@Testcontainers
abstract class Neo4jCdcTaskTest {

  private lateinit var task: SourceTask

  abstract var session: Session
  abstract var neo4j: Neo4jContainer<*>

  @AfterEach
  fun after() {
    task.stop()
  }

  @BeforeEach
  fun before() {
    session
        .run(
            "CREATE OR REPLACE DATABASE \$db OPTIONS { txLogEnrichment: \$mode } WAIT",
            mapOf("db" to "neo4j", "mode" to "FULL"))
        .consume()

    task = Neo4jCdcTask()
    task.initialize(newTaskContextWithOffset())
  }

  @Test
  fun `should use correct offset when startFrom=earliest`() {
    // create data, 200 nodes + 100 relationships
    session.run("UNWIND RANGE(1, 100) AS x CREATE (n), (m), (n)-[:RELATED_TO]->(m)").consume()

    // start task with EARLIEST, previous changes should be visible
    task.start(
        mapOf(
            Neo4jConfiguration.URI to neo4j.boltUrl,
            Neo4jConfiguration.AUTHENTICATION_TYPE to AuthenticationType.NONE.toString(),
            SourceConfiguration.STRATEGY to SourceType.CDC.toString(),
            SourceConfiguration.START_FROM to StartFrom.EARLIEST.toString(),
            "neo4j.cdc.topic.nodes.patterns" to "()",
            "neo4j.cdc.topic.relationships.patterns" to "()-[]-()"))

    // poll for changes
    val changes = task.poll()

    // expect to see previously created data
    changes shouldHaveSize 100 * 2 + 100
  }

  @Test
  fun `should use correct offset when startFrom=now`() {
    // create data (1), 200 nodes + 100 relationships
    session.run("UNWIND RANGE(1, 100) AS x CREATE (n), (m), (n)-[:RELATED_TO]->(m)").consume()

    // start task with NOW, previous changes should NOT be visible
    task.start(
        mapOf(
            Neo4jConfiguration.URI to neo4j.boltUrl,
            Neo4jConfiguration.AUTHENTICATION_TYPE to AuthenticationType.NONE.toString(),
            SourceConfiguration.STRATEGY to SourceType.CDC.toString(),
            SourceConfiguration.START_FROM to StartFrom.NOW.toString(),
            "neo4j.cdc.topic.nodes.patterns" to "()",
            "neo4j.cdc.topic.relationships.patterns" to "()-[]-()"))

    // create data (2)
    session.run("UNWIND RANGE(1, 75) AS x CREATE (n), (m), (n)-[:RELATED_TO]->(m)").consume()

    // poll for changes
    val changes = task.poll()

    // expected to see created data (2)
    changes shouldHaveSize 75 * 2 + 75
  }

  @Test
  fun `should use correct offset when startFrom=user provided`() {
    // create data (1), 200 nodes + 100 relationships
    session.run("UNWIND RANGE(1, 100) AS x CREATE (n), (m), (n)-[:RELATED_TO]->(m)").consume()

    // capture change identifier
    val changeId = currentChangeId()

    // create data (2), 150 nodes + 75 relationships
    session.run("UNWIND RANGE(1, 75) AS x CREATE (n), (m), (n)-[:RELATED_TO]->(m)").consume()

    // start task with USER_PROVIDED, with value set as captured change identifier
    task.start(
        mapOf(
            Neo4jConfiguration.URI to neo4j.boltUrl,
            Neo4jConfiguration.AUTHENTICATION_TYPE to AuthenticationType.NONE.toString(),
            SourceConfiguration.STRATEGY to SourceType.CDC.toString(),
            SourceConfiguration.START_FROM to StartFrom.USER_PROVIDED.toString(),
            SourceConfiguration.START_FROM_VALUE to changeId,
            "neo4j.cdc.topic.nodes.patterns" to "()",
            "neo4j.cdc.topic.relationships.patterns" to "()-[]-()"))

    // poll for changes
    val changes = task.poll()

    // expected to see created data (2)
    changes shouldHaveSize 75 * 2 + 75
  }

  @ParameterizedTest
  @EnumSource(StartFrom::class)
  fun `should use stored offset regardless of provided startFrom`(startFrom: StartFrom) {
    // create data (1), 200 nodes + 100 relationships
    session.run("UNWIND RANGE(1, 100) AS x CREATE (n), (m), (n)-[:RELATED_TO]->(m)").consume()

    // capture change identifier and set it as stored offset
    task.initialize(newTaskContextWithCurrentChangeId())

    // create data (2), 150 nodes + 75 relationships
    session.run("UNWIND RANGE(1, 75) AS x CREATE (n), (m), (n)-[:RELATED_TO]->(m)").consume()

    // start task with provided START_FROM, with the mocked task context
    task.start(
        buildMap {
          put(Neo4jConfiguration.URI, neo4j.boltUrl)
          put(Neo4jConfiguration.AUTHENTICATION_TYPE, AuthenticationType.NONE.toString())
          put(SourceConfiguration.STRATEGY, SourceType.CDC.toString())
          put(SourceConfiguration.START_FROM, startFrom.toString())
          if (startFrom == StartFrom.USER_PROVIDED) {
            put(SourceConfiguration.START_FROM_VALUE, earliestChangeId())
          }
          put("neo4j.cdc.topic.nodes.patterns", "()")
          put("neo4j.cdc.topic.relationships.patterns", "()-[]-()")
        })

    // poll for changes
    val changes = task.poll()

    // expected to see create data (2) because of the stored offset value
    changes shouldHaveSize 75 * 2 + 75
  }

  @Test
  fun `should ignore stored offset when startFrom=earliest`() {
    // create data (1), 100 nodes + 50 relationships
    session.run("UNWIND RANGE(1, 50) AS x CREATE (n), (m), (n)-[:RELATED_TO]->(m)").consume()

    // capture change identifier and set it as stored offset
    task.initialize(newTaskContextWithCurrentChangeId())

    // create data (2), 100 nodes + 50 relationships
    session.run("UNWIND RANGE(1, 50) AS x CREATE (n), (m), (n)-[:RELATED_TO]->(m)").consume()

    // start task with EARLIEST, previous changes should be visible
    task.start(
        mapOf(
            Neo4jConfiguration.URI to neo4j.boltUrl,
            Neo4jConfiguration.AUTHENTICATION_TYPE to AuthenticationType.NONE.toString(),
            SourceConfiguration.STRATEGY to SourceType.CDC.toString(),
            SourceConfiguration.START_FROM to StartFrom.EARLIEST.toString(),
            SourceConfiguration.IGNORE_STORED_OFFSET to "true",
            "neo4j.cdc.topic.nodes.patterns" to "()",
            "neo4j.cdc.topic.relationships.patterns" to "()-[]-()"))

    // poll for changes
    val changes = task.poll()

    // expect to see previously created data
    changes shouldHaveSize 100 * 2 + 100
  }

  @Test
  fun `should ignore stored offset when startFrom=now`() {
    // capture change identifier and set it as stored offset
    task.initialize(newTaskContextWithCurrentChangeId())

    // create data (1), 100 nodes + 50 relationships
    session.run("UNWIND RANGE(1, 50) AS x CREATE (n), (m), (n)-[:RELATED_TO]->(m)").consume()

    // start task with NOW, previous changes should NOT be visible
    task.start(
        mapOf(
            Neo4jConfiguration.URI to neo4j.boltUrl,
            Neo4jConfiguration.AUTHENTICATION_TYPE to AuthenticationType.NONE.toString(),
            SourceConfiguration.STRATEGY to SourceType.CDC.toString(),
            SourceConfiguration.START_FROM to StartFrom.NOW.toString(),
            SourceConfiguration.IGNORE_STORED_OFFSET to "true",
            "neo4j.cdc.topic.nodes.patterns" to "()",
            "neo4j.cdc.topic.relationships.patterns" to "()-[]-()"))

    // create data (2), 100 nodes + 50 relationships
    session.run("UNWIND RANGE(1, 50) AS x CREATE (n), (m), (n)-[:RELATED_TO]->(m)").consume()

    // poll for changes
    val changes = task.poll()

    // expect to see previously created data
    changes shouldHaveSize 50 * 2 + 50
  }

  @Test
  fun `should ignore stored offset when startFrom=user provided`() {
    // capture change identifier and set it as stored offset
    task.initialize(newTaskContextWithCurrentChangeId())

    // create data (1), 100 nodes + 50 relationships
    session.run("UNWIND RANGE(1, 50) AS x CREATE (n), (m), (n)-[:RELATED_TO]->(m)").consume()

    // start task with USER_PROVIDED, previous changes should NOT be visible
    task.start(
        mapOf(
            Neo4jConfiguration.URI to neo4j.boltUrl,
            Neo4jConfiguration.AUTHENTICATION_TYPE to AuthenticationType.NONE.toString(),
            SourceConfiguration.STRATEGY to SourceType.CDC.toString(),
            SourceConfiguration.START_FROM to StartFrom.USER_PROVIDED.toString(),
            SourceConfiguration.START_FROM_VALUE to currentChangeId(),
            SourceConfiguration.IGNORE_STORED_OFFSET to "true",
            "neo4j.cdc.topic.nodes.patterns" to "()",
            "neo4j.cdc.topic.relationships.patterns" to "()-[]-()"))

    // create data (2), 100 nodes + 50 relationships
    session.run("UNWIND RANGE(1, 50) AS x CREATE (n), (m), (n)-[:RELATED_TO]->(m)").consume()

    // poll for changes
    val changes = task.poll()

    // expect to see previously created data
    changes shouldHaveSize 50 * 2 + 50
  }

  @Test
  fun `should route change events based on matched selectors`() {
    session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (n:Person) REQUIRE n.id IS KEY").consume()
    session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (n:Company) REQUIRE n.id IS KEY").consume()
    session
        .run("CREATE CONSTRAINT IF NOT EXISTS FOR ()-[r:WORKS_FOR]->() REQUIRE r.id IS KEY")
        .consume()

    session
        .run("UNWIND RANGE(1, 100) AS n CREATE (p:Person) SET p.id = n, p.name = 'name ' + n")
        .consume()
    session
        .run("UNWIND RANGE(1, 25) AS n CREATE (p:Company) SET p.id = n, p.name = 'company ' + n")
        .consume()
    session
        .run(
            """
              UNWIND RANGE(1, 100, 2) AS n 
              MATCH (p:Person {id: n})
              MATCH (c:Company {id: n%25+1})
              CREATE (p)-[:WORKS_FOR {id: n, since: date()}]->(c)
            """
                .trimIndent())
        .consume()

    task.start(
        mapOf(
            Neo4jConfiguration.URI to neo4j.boltUrl,
            Neo4jConfiguration.AUTHENTICATION_TYPE to AuthenticationType.NONE.toString(),
            SourceConfiguration.STRATEGY to SourceType.CDC.toString(),
            SourceConfiguration.START_FROM to StartFrom.EARLIEST.toString(),
            "neo4j.cdc.topic.nodes.patterns" to "()",
            "neo4j.cdc.topic.relationships.patterns" to "()-[]-()",
            "neo4j.cdc.topic.people.patterns" to "(:Person)",
            "neo4j.cdc.topic.people-no-id.patterns" to "(:Person {-id})",
            "neo4j.cdc.topic.people-key.patterns" to "(:Person {id:5})",
            "neo4j.cdc.topic.company.patterns" to "(:Company)",
            "neo4j.cdc.topic.works_for.patterns" to "(:Person)-[:WORKS_FOR]->(:Company)",
            "neo4j.cdc.topic.works_for-no-since.patterns" to
                "(:Person)-[:WORKS_FOR{-since}]->(:Company)",
            "neo4j.cdc.topic.works_for-key.patterns" to
                "(:Person)-[:WORKS_FOR{id: 11}]->(:Company)",
            "neo4j.cdc.topic.none.patterns" to "(:People),()-[:KNOWS]-()",
        ))

    val changes = task.poll().toList()

    changes.filter { it.topic() == "nodes" } shouldHaveSize 125
    changes.filter { it.topic() == "relationships" } shouldHaveSize 50
    changes.filter { it.topic() == "people" } shouldHaveSize 100
    changes.filter { it.topic() == "people-no-id" } shouldHaveSize 100
    changes.filter { it.topic() == "people-key" } shouldHaveSize 1
    changes.filter { it.topic() == "company" } shouldHaveSize 25
    changes.filter { it.topic() == "works_for" } shouldHaveSize 50
    changes.filter { it.topic() == "works_for-no-since" } shouldHaveSize 50
    changes.filter { it.topic() == "works_for-key" } shouldHaveSize 1
    changes.filter { it.topic() == "none" } shouldHaveSize 0
  }

  @Test
  fun `batch size should be respected`() {
    task.start(
        mapOf(
            Neo4jConfiguration.URI to neo4j.boltUrl,
            Neo4jConfiguration.AUTHENTICATION_TYPE to AuthenticationType.NONE.toString(),
            SourceConfiguration.STRATEGY to SourceType.CDC.toString(),
            SourceConfiguration.START_FROM to StartFrom.EARLIEST.toString(),
            SourceConfiguration.BATCH_SIZE to "5",
            "neo4j.cdc.topic.nodes.patterns" to "()"))

    session.run("UNWIND RANGE(1, 100) AS n CREATE ()").consume()

    // should return records in batches of `5` as configured
    for (i in 1..20) {
      val changes = task.poll().toList()

      changes shouldHaveSize 5
    }

    measureTime {
      val changes = task.poll().toList()

      changes shouldHaveSize 0
    } shouldBeGreaterThan 5.seconds
  }

  @Test
  fun `poll duration should be respected`() {
    task.start(
        mapOf(
            Neo4jConfiguration.URI to neo4j.boltUrl,
            Neo4jConfiguration.AUTHENTICATION_TYPE to AuthenticationType.NONE.toString(),
            SourceConfiguration.STRATEGY to SourceType.CDC.toString(),
            SourceConfiguration.START_FROM to StartFrom.EARLIEST.toString(),
            SourceConfiguration.CDC_POLL_DURATION to "5s",
            "neo4j.cdc.topic.nodes.patterns" to "()"))

    // should block at most CDC_POLL_DURATION waiting for an event
    measureTime {
      val changes = task.poll().toList()

      changes shouldHaveSize 0
    } shouldBeGreaterThan 5.seconds

    session.run("UNWIND RANGE(1, 100) AS n CREATE ()").consume()

    // should return immediately when changes are returned
    measureTime {
      val changes = task.poll().toList()

      changes shouldHaveSize 100
    } shouldBeLessThan 5.seconds
  }

  private fun newTaskContextWithCurrentChangeId(): SourceTaskContext {
    return newTaskContextWithOffset(mapOf("value" to currentChangeId()))
  }

  private fun newTaskContextWithOffset(
      offsetMap: Map<String, Any> = emptyMap()
  ): SourceTaskContext {
    val offsetStorageReader =
        mock<OffsetStorageReader> {
          on { offset(ArgumentMatchers.anyMap<String, Any>()) } doReturn offsetMap
        }

    return mock<SourceTaskContext> { on { offsetStorageReader() } doReturn offsetStorageReader }
  }

  private fun currentChangeId(): String {
    return session.run("CALL cdc.current").single().get(0).asString()
  }

  private fun earliestChangeId(): String {
    return session.run("CALL cdc.earliest").single().get(0).asString()
  }
}
