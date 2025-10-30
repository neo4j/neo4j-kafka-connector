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
package org.neo4j.connectors.kafka.source

import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.comparables.shouldBeGreaterThan
import io.kotest.matchers.comparables.shouldBeLessThan
import io.kotest.matchers.equals.shouldNotBeEqual
import io.kotest.matchers.shouldBe
import java.util.UUID
import kotlin.collections.get
import kotlin.run
import kotlin.time.Duration.Companion.seconds
import kotlin.time.measureTime
import org.apache.kafka.connect.source.SourceTaskContext
import org.apache.kafka.connect.storage.OffsetStorageReader
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assumptions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.mockito.ArgumentMatchers
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.neo4j.caniuse.CanIUse.canIUse
import org.neo4j.caniuse.Dbms
import org.neo4j.caniuse.Neo4j
import org.neo4j.caniuse.Neo4jDetector
import org.neo4j.connectors.kafka.configuration.AuthenticationType
import org.neo4j.connectors.kafka.configuration.Neo4jConfiguration
import org.neo4j.connectors.kafka.testing.createNodeKeyConstraint
import org.neo4j.connectors.kafka.testing.createRelationshipKeyConstraint
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
class Neo4jCdcTaskTest {
  companion object {
    @Container
    val container: Neo4jContainer<*> =
        Neo4jContainer(neo4jImage())
            .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
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

  private lateinit var task: Neo4jCdcTask
  private lateinit var db: String
  private lateinit var session: Session

  @AfterEach
  fun after() {
    if (this::session.isInitialized) session.close()
    if (this::task.isInitialized) task.stop()
  }

  @BeforeEach
  fun before() {
    Assumptions.assumeTrue(canIUse(Dbms.changeDataCapture()).withNeo4j(neo4j))

    db = "test-${UUID.randomUUID()}"
    driver.session(SessionConfig.forDatabase("system")).use {
      it.run(
              "CREATE OR REPLACE DATABASE \$db OPTIONS { txLogEnrichment: \$mode } WAIT",
              mapOf("db" to db, "mode" to "FULL"),
          )
          .consume()
    }
    session = driver.session(SessionConfig.forDatabase(db))

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
            Neo4jConfiguration.URI to container.boltUrl,
            Neo4jConfiguration.AUTHENTICATION_TYPE to AuthenticationType.NONE.toString(),
            Neo4jConfiguration.DATABASE to db,
            SourceConfiguration.STRATEGY to SourceType.CDC.toString(),
            SourceConfiguration.START_FROM to StartFrom.EARLIEST.toString(),
            "neo4j.cdc.topic.nodes.patterns" to "()",
            "neo4j.cdc.topic.relationships.patterns" to "()-[]-()",
        )
    )

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
            Neo4jConfiguration.URI to container.boltUrl,
            Neo4jConfiguration.AUTHENTICATION_TYPE to AuthenticationType.NONE.toString(),
            Neo4jConfiguration.DATABASE to db,
            SourceConfiguration.STRATEGY to SourceType.CDC.toString(),
            SourceConfiguration.START_FROM to StartFrom.NOW.toString(),
            "neo4j.cdc.topic.nodes.patterns" to "()",
            "neo4j.cdc.topic.relationships.patterns" to "()-[]-()",
        )
    )

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
            Neo4jConfiguration.URI to container.boltUrl,
            Neo4jConfiguration.AUTHENTICATION_TYPE to AuthenticationType.NONE.toString(),
            Neo4jConfiguration.DATABASE to db,
            SourceConfiguration.STRATEGY to SourceType.CDC.toString(),
            SourceConfiguration.START_FROM to StartFrom.USER_PROVIDED.toString(),
            SourceConfiguration.START_FROM_VALUE to changeId,
            "neo4j.cdc.topic.nodes.patterns" to "()",
            "neo4j.cdc.topic.relationships.patterns" to "()-[]-()",
        )
    )

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
          put(Neo4jConfiguration.URI, container.boltUrl)
          put(Neo4jConfiguration.AUTHENTICATION_TYPE, AuthenticationType.NONE.toString())
          put(Neo4jConfiguration.DATABASE, db)
          put(SourceConfiguration.STRATEGY, SourceType.CDC.toString())
          put(SourceConfiguration.START_FROM, startFrom.toString())
          if (startFrom == StartFrom.USER_PROVIDED) {
            put(SourceConfiguration.START_FROM_VALUE, earliestChangeId())
          }
          put("neo4j.cdc.topic.nodes.patterns", "()")
          put("neo4j.cdc.topic.relationships.patterns", "()-[]-()")
        }
    )

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
            Neo4jConfiguration.URI to container.boltUrl,
            Neo4jConfiguration.AUTHENTICATION_TYPE to AuthenticationType.NONE.toString(),
            Neo4jConfiguration.DATABASE to db,
            SourceConfiguration.STRATEGY to SourceType.CDC.toString(),
            SourceConfiguration.START_FROM to StartFrom.EARLIEST.toString(),
            SourceConfiguration.IGNORE_STORED_OFFSET to "true",
            "neo4j.cdc.topic.nodes.patterns" to "()",
            "neo4j.cdc.topic.relationships.patterns" to "()-[]-()",
        )
    )

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
            Neo4jConfiguration.URI to container.boltUrl,
            Neo4jConfiguration.AUTHENTICATION_TYPE to AuthenticationType.NONE.toString(),
            Neo4jConfiguration.DATABASE to db,
            SourceConfiguration.STRATEGY to SourceType.CDC.toString(),
            SourceConfiguration.START_FROM to StartFrom.NOW.toString(),
            SourceConfiguration.IGNORE_STORED_OFFSET to "true",
            "neo4j.cdc.topic.nodes.patterns" to "()",
            "neo4j.cdc.topic.relationships.patterns" to "()-[]-()",
        )
    )

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
            Neo4jConfiguration.URI to container.boltUrl,
            Neo4jConfiguration.AUTHENTICATION_TYPE to AuthenticationType.NONE.toString(),
            Neo4jConfiguration.DATABASE to db,
            SourceConfiguration.STRATEGY to SourceType.CDC.toString(),
            SourceConfiguration.START_FROM to StartFrom.USER_PROVIDED.toString(),
            SourceConfiguration.START_FROM_VALUE to currentChangeId(),
            SourceConfiguration.IGNORE_STORED_OFFSET to "true",
            "neo4j.cdc.topic.nodes.patterns" to "()",
            "neo4j.cdc.topic.relationships.patterns" to "()-[]-()",
        )
    )

    // create data (2), 100 nodes + 50 relationships
    session.run("UNWIND RANGE(1, 50) AS x CREATE (n), (m), (n)-[:RELATED_TO]->(m)").consume()

    // poll for changes
    val changes = task.poll()

    // expect to see previously created data
    changes shouldHaveSize 50 * 2 + 50
  }

  @Test
  fun `should route change events based on matched selectors`() {
    session.createNodeKeyConstraint(neo4j, "person_id", "Person", "id")
    session.createNodeKeyConstraint(neo4j, "company_id", "Company", "id")
    session.createRelationshipKeyConstraint(neo4j, "works_for_id", "WORKS_FOR", "id")

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
                .trimIndent()
        )
        .consume()

    task.start(
        mapOf(
            Neo4jConfiguration.URI to container.boltUrl,
            Neo4jConfiguration.AUTHENTICATION_TYPE to AuthenticationType.NONE.toString(),
            Neo4jConfiguration.DATABASE to db,
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
        )
    )

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
            Neo4jConfiguration.URI to container.boltUrl,
            Neo4jConfiguration.AUTHENTICATION_TYPE to AuthenticationType.NONE.toString(),
            Neo4jConfiguration.DATABASE to db,
            SourceConfiguration.STRATEGY to SourceType.CDC.toString(),
            SourceConfiguration.START_FROM to StartFrom.EARLIEST.toString(),
            SourceConfiguration.BATCH_SIZE to "5",
            "neo4j.cdc.topic.nodes.patterns" to "()",
        )
    )

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
            Neo4jConfiguration.URI to container.boltUrl,
            Neo4jConfiguration.AUTHENTICATION_TYPE to AuthenticationType.NONE.toString(),
            Neo4jConfiguration.DATABASE to db,
            SourceConfiguration.STRATEGY to SourceType.CDC.toString(),
            SourceConfiguration.START_FROM to StartFrom.EARLIEST.toString(),
            SourceConfiguration.CDC_POLL_DURATION to "5s",
            "neo4j.cdc.topic.nodes.patterns" to "()",
        )
    )

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

  @Test
  fun `should update in-memory offset even if no changes are polled`() {
    task.start(
        mapOf(
            Neo4jConfiguration.URI to container.boltUrl,
            Neo4jConfiguration.AUTHENTICATION_TYPE to AuthenticationType.NONE.toString(),
            Neo4jConfiguration.DATABASE to db,
            SourceConfiguration.STRATEGY to SourceType.CDC.toString(),
            SourceConfiguration.START_FROM to StartFrom.EARLIEST.toString(),
            SourceConfiguration.CDC_POLL_DURATION to "1s",
            SourceConfiguration.CDC_POLL_INTERVAL to "250ms",
            "neo4j.cdc.topic.nodes.patterns" to "(:Company)",
        )
    )

    val startedWith = task.latestOffset()

    session.run("UNWIND RANGE(1, 10) AS n CREATE (:Person {id: n, name: 'person ' + n})").consume()

    task.poll() shouldBe emptyList()
    val afterFirstPoll = task.latestOffset()
    startedWith shouldNotBeEqual afterFirstPoll

    session
        .run("UNWIND RANGE(1, 10) AS n CREATE (:Employee {id: n, name: 'employee ' + n})")
        .consume()

    task.poll() shouldBe emptyList()
    val finishedWith = task.latestOffset()
    finishedWith shouldNotBeEqual afterFirstPoll
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

  fun currentChangeId(): String {
    return session.run("CALL cdc.current").single().get(0).asString()
  }

  fun earliestChangeId(): String {
    return session.run("CALL cdc.earliest").single().get(0).asString()
  }
}
