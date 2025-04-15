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

import io.kotest.assertions.nondeterministic.eventually
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.util.*
import kotlin.test.assertFailsWith
import kotlin.time.Duration.Companion.seconds
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.source.SourceTask
import org.apache.kafka.connect.source.SourceTaskContext
import org.apache.kafka.connect.storage.OffsetStorageReader
import org.junit.jupiter.api.*
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.mockito.ArgumentMatchers
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.neo4j.connectors.kafka.configuration.AuthenticationType
import org.neo4j.connectors.kafka.configuration.Neo4jConfiguration
import org.neo4j.connectors.kafka.data.DynamicTypes
import org.neo4j.connectors.kafka.testing.TestSupport.runTest
import org.neo4j.connectors.kafka.testing.neo4jImage
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.Session
import org.neo4j.driver.SessionConfig
import org.neo4j.driver.exceptions.ClientException
import org.neo4j.driver.types.Node
import org.testcontainers.containers.Neo4jContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

@Testcontainers
class Neo4jQueryTaskTest {

  companion object {
    @Container
    val neo4j: Neo4jContainer<*> =
        Neo4jContainer(neo4jImage())
            .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
            .withExposedPorts(7687)
            .withoutAuthentication()

    private lateinit var driver: Driver
    private lateinit var session: Session

    @BeforeAll
    @JvmStatic
    fun setUpContainer() {
      driver = GraphDatabase.driver(neo4j.boltUrl, AuthTokens.none())
      session = driver.session()
    }

    @AfterAll
    @JvmStatic
    fun tearDownContainer() {
      session.close()
      driver.close()
    }
  }

  private lateinit var task: SourceTask

  @AfterEach
  fun after() {
    driver.session(SessionConfig.forDatabase("system")).use {
      it.run("CREATE OR REPLACE DATABASE neo4j WAIT").consume()
    }
    task.stop()
  }

  @BeforeEach
  fun before() {
    task = Neo4jQueryTask()
    task.initialize(newTaskContextWithOffset(emptyMap()))
  }

  private val sourceQuery =
      """
        |MATCH (n:Test)
        |WHERE n.timestamp > ${'$'}lastCheck
        |RETURN n.name AS name,
        |   n.timestamp AS timestamp,
        |   n.point AS point,
        |   n.array AS array,
        |   n.datetime AS datetime,
        |   n.boolean AS boolean,
        |   {
        |       key1: "value1",
        |       key2: "value2"
        |   } AS map,
        |   n AS node
        |ORDER BY n.timestamp
      """
          .trimMargin()

  @Test
  fun `should source data correctly when startFrom=earliest`() = runTest {
    val expected = mutableListOf<Map<String, Any>>()

    // create data with timestamp set as 0
    expected.addAll(insertRecords(50, Clock.fixed(Instant.EPOCH, ZoneId.systemDefault())))

    // create data with timestamp set as NOW + 5m
    expected.addAll(
        insertRecords(
            100, Clock.fixed(Instant.now().plus(Duration.ofMinutes(5)), ZoneId.systemDefault())))

    // start task with EARLIEST, previous changes should be visible
    task.start(
        mapOf(
            Neo4jConfiguration.URI to neo4j.boltUrl,
            Neo4jConfiguration.AUTHENTICATION_TYPE to AuthenticationType.NONE.toString(),
            SourceConfiguration.STRATEGY to SourceType.QUERY.toString(),
            SourceConfiguration.START_FROM to StartFrom.EARLIEST.toString(),
            SourceConfiguration.QUERY_TOPIC to UUID.randomUUID().toString(),
            SourceConfiguration.QUERY to sourceQuery))

    // expect to see all data
    pollAndAssertReceived(expected)
  }

  @Test
  fun `should source data correctly when startFrom=now`() = runTest {
    // create data with timestamp set as NOW - 5m
    insertRecords(
        100, Clock.fixed(Instant.now().minus(Duration.ofMinutes(5)), ZoneId.systemDefault()))

    // create data with timestamp set as NOW + 5m
    val expected =
        insertRecords(
            75, Clock.fixed(Instant.now().plus(Duration.ofMinutes(5)), ZoneId.systemDefault()))

    // start task with NOW, previous changes should NOT be visible
    task.start(
        mapOf(
            Neo4jConfiguration.URI to neo4j.boltUrl,
            Neo4jConfiguration.AUTHENTICATION_TYPE to AuthenticationType.NONE.toString(),
            SourceConfiguration.STRATEGY to SourceType.QUERY.toString(),
            SourceConfiguration.START_FROM to StartFrom.NOW.toString(),
            SourceConfiguration.QUERY_TOPIC to UUID.randomUUID().toString(),
            SourceConfiguration.QUERY to sourceQuery))

    // expect to see only the data created after task is started
    pollAndAssertReceived(expected)
  }

  @Test
  fun `should source data correctly when startFrom=user provided`() = runTest {
    // create data with timestamp set as NOW - 5m
    insertRecords(
        10, Clock.fixed(Instant.now().minus(Duration.ofMinutes(5)), ZoneId.systemDefault()))

    // create data with timestamp set as NOW + 5m
    insertRecords(
        25, Clock.fixed(Instant.now().plus(Duration.ofMinutes(5)), ZoneId.systemDefault()))

    // create data with timestamp set as NOW + 10m
    val expected =
        insertRecords(
            75, Clock.fixed(Instant.now().plus(Duration.ofMinutes(10)), ZoneId.systemDefault()))

    // start task with NOW + 7m, previous changes should NOT be visible
    task.start(
        mapOf(
            Neo4jConfiguration.URI to neo4j.boltUrl,
            Neo4jConfiguration.AUTHENTICATION_TYPE to AuthenticationType.NONE.toString(),
            SourceConfiguration.STRATEGY to SourceType.QUERY.toString(),
            SourceConfiguration.START_FROM to StartFrom.USER_PROVIDED.toString(),
            SourceConfiguration.START_FROM_VALUE to
                Instant.now().plus(Duration.ofMinutes(7)).toEpochMilli().toString(),
            SourceConfiguration.QUERY_TOPIC to UUID.randomUUID().toString(),
            SourceConfiguration.QUERY to sourceQuery))

    // expect to see only the data created after the provided timestamp
    pollAndAssertReceived(expected)
  }

  @ParameterizedTest
  @EnumSource(StartFrom::class)
  fun `should use stored offset regardless of provided startFrom`(startFrom: StartFrom) = runTest {
    // create data with timestamp set as NOW - 5m
    insertRecords(
        25, Clock.fixed(Instant.now().minus(Duration.ofMinutes(5)), ZoneId.systemDefault()))

    val expected =
        // create data with timestamp set as NOW - 2m and NOW + 2m
        insertRecords(
            25, Clock.fixed(Instant.now().minus(Duration.ofMinutes(2)), ZoneId.systemDefault())) +
            insertRecords(
                25, Clock.fixed(Instant.now().plus(Duration.ofMinutes(2)), ZoneId.systemDefault()))

    // set an offset of NOW - 3m
    task.initialize(
        newTaskContextWithOffset(
            "timestamp", Instant.now().minus(Duration.ofMinutes(3)).toEpochMilli()))

    // start task with provided START_FROM, with the mocked task context
    task.start(
        buildMap {
          put(Neo4jConfiguration.URI, neo4j.boltUrl)
          put(Neo4jConfiguration.AUTHENTICATION_TYPE, AuthenticationType.NONE.toString())
          put(SourceConfiguration.STRATEGY, SourceType.QUERY.toString())
          put(SourceConfiguration.QUERY_TOPIC, UUID.randomUUID().toString())
          put(SourceConfiguration.QUERY, sourceQuery)
          put(SourceConfiguration.QUERY_STREAMING_PROPERTY, "timestamp")

          put(SourceConfiguration.START_FROM, startFrom.toString())
          if (startFrom == StartFrom.USER_PROVIDED) {
            put(SourceConfiguration.START_FROM_VALUE, "-1")
          }
        })

    // expect to see only the data created after the stored offset
    pollAndAssertReceived(expected)
  }

  @Test
  fun `should ignore stored offset when startFrom=earliest and ignore-stored-offset=true`() =
      runTest {
        // create data with timestamp set as 0
        insertRecords(50, Clock.fixed(Instant.EPOCH, ZoneId.systemDefault()))

        // create data with timestamp set as NOW + 5m
        insertRecords(
            100, Clock.fixed(Instant.now().plus(Duration.ofMinutes(5)), ZoneId.systemDefault()))

        task.initialize(newTaskContextWithOffset("", Instant.now().toEpochMilli()))

        // start task with EARLIEST, previous changes should be visible
        task.start(
            mapOf(
                Neo4jConfiguration.URI to neo4j.boltUrl,
                Neo4jConfiguration.AUTHENTICATION_TYPE to AuthenticationType.NONE.toString(),
                SourceConfiguration.STRATEGY to SourceType.QUERY.toString(),
                SourceConfiguration.START_FROM to StartFrom.EARLIEST.toString(),
                SourceConfiguration.QUERY_TOPIC to UUID.randomUUID().toString(),
                SourceConfiguration.QUERY to sourceQuery,
                SourceConfiguration.IGNORE_STORED_OFFSET to "true"))

        // expect to see all data
        pollAndAssertReceivedSize(150)
      }

  @Test
  fun `should ignore stored offset when startFrom=now and ignore-stored-offset=true`() = runTest {
    // create data with timestamp set as NOW - 5m
    insertRecords(
        100, Clock.fixed(Instant.now().minus(Duration.ofMinutes(5)), ZoneId.systemDefault()))

    // create data with timestamp set as NOW + 5m
    insertRecords(
        75, Clock.fixed(Instant.now().plus(Duration.ofMinutes(5)), ZoneId.systemDefault()))

    task.initialize(newTaskContextWithOffset("", Instant.EPOCH.toEpochMilli()))

    // start task with NOW, previous changes should NOT be visible
    task.start(
        mapOf(
            Neo4jConfiguration.URI to neo4j.boltUrl,
            Neo4jConfiguration.AUTHENTICATION_TYPE to AuthenticationType.NONE.toString(),
            SourceConfiguration.STRATEGY to SourceType.QUERY.toString(),
            SourceConfiguration.START_FROM to StartFrom.NOW.toString(),
            SourceConfiguration.QUERY_TOPIC to UUID.randomUUID().toString(),
            SourceConfiguration.QUERY to sourceQuery,
            SourceConfiguration.IGNORE_STORED_OFFSET to "true"))

    // expect to see only the data created after task is started
    pollAndAssertReceivedSize(75)
  }

  @Test
  fun `should ignore stored offset when startFrom=user provided and ignore-stored-offset=true`() =
      runTest {
        // create data with timestamp set as NOW - 5m
        insertRecords(
            10, Clock.fixed(Instant.now().minus(Duration.ofMinutes(5)), ZoneId.systemDefault()))

        // create data with timestamp set as NOW + 5m
        insertRecords(
            25, Clock.fixed(Instant.now().plus(Duration.ofMinutes(5)), ZoneId.systemDefault()))

        // create data with timestamp set as NOW + 10m
        insertRecords(
            75, Clock.fixed(Instant.now().plus(Duration.ofMinutes(10)), ZoneId.systemDefault()))

        task.initialize(newTaskContextWithOffset("", Instant.EPOCH.toEpochMilli()))

        // start task with NOW, previous changes should NOT be visible
        task.start(
            mapOf(
                Neo4jConfiguration.URI to neo4j.boltUrl,
                Neo4jConfiguration.AUTHENTICATION_TYPE to AuthenticationType.NONE.toString(),
                SourceConfiguration.STRATEGY to SourceType.QUERY.toString(),
                SourceConfiguration.START_FROM to StartFrom.USER_PROVIDED.toString(),
                SourceConfiguration.START_FROM_VALUE to
                    Instant.now().plus(Duration.ofMinutes(7)).toEpochMilli().toString(),
                SourceConfiguration.QUERY_TOPIC to UUID.randomUUID().toString(),
                SourceConfiguration.QUERY to sourceQuery,
                SourceConfiguration.IGNORE_STORED_OFFSET to "true"))

        // expect to see only the data created after the provided timestamp
        pollAndAssertReceivedSize(75)
      }

  @Test
  fun `should support null values returned from query`() = runTest {
    val props = mutableMapOf<String, String>()
    props[Neo4jConfiguration.URI] = neo4j.boltUrl
    props[SourceConfiguration.QUERY_TOPIC] = UUID.randomUUID().toString()
    props[SourceConfiguration.QUERY_POLL_INTERVAL] = "1s"
    props[SourceConfiguration.QUERY] =
        """
                |RETURN {
                |   prop1: 1,
                |   prop2: "string",
                |   prop3: true,
                |   prop4: null,
                |   prop5: {
                |       prop: null
                |   },
                |   prop6: [1],
                |   prop7: [null]
                |} AS data, 1717773205 AS timestamp
            """
            .trimMargin()
    props[Neo4jConfiguration.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()

    task.start(props)

    val expected =
        mapOf(
            "data" to
                mapOf(
                    "prop1" to 1L,
                    "prop2" to "string",
                    "prop3" to true,
                    "prop4" to null,
                    "prop5" to mapOf("prop" to null),
                    "prop6" to listOf(1L),
                    "prop7" to listOf<Any?>(null)),
            "timestamp" to 1717773205L)

    eventually(30.seconds) {
      val first =
          task.poll()?.map { DynamicTypes.fromConnectValue(it.valueSchema(), it.value()) }?.first()

      first!! shouldBe expected
    }
  }

  @Test
  fun `should source data with complex type with custom QUERY`() = runTest {
    val props = mutableMapOf<String, String>()
    props[Neo4jConfiguration.URI] = neo4j.boltUrl
    props[SourceConfiguration.QUERY_TOPIC] = UUID.randomUUID().toString()
    props[SourceConfiguration.QUERY_POLL_INTERVAL] = "10ms"
    props[SourceConfiguration.QUERY] =
        """
                |WITH
                |{
                |   id: 'ROOT_ID',
                |   root: [
                |       { children: [] },
                |       { children: [{ name: "child" }] }
                |   ],
                |   arr: [null, {foo: "bar"}]
                |} AS data
                |RETURN data, data.id AS id, 123456789 as timestamp
            """
            .trimMargin()
    props[Neo4jConfiguration.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()

    task.start(props)
    val totalRecords = 10
    insertRecords(totalRecords)

    val expected =
        mapOf(
            "timestamp" to 123456789L,
            "id" to "ROOT_ID",
            "data" to
                mapOf(
                    "id" to "ROOT_ID",
                    "arr" to listOf(null, mapOf("foo" to "bar")),
                    "root" to
                        listOf(
                            mapOf("children" to emptyList<Map<String, Any>>()),
                            mapOf("children" to listOf(mapOf("name" to "child"))))))

    val list = mutableListOf<SourceRecord>()

    eventually(30.seconds) {
      task.poll()?.let { list.addAll(it) }
      val actualList = list.map { DynamicTypes.fromConnectValue(it.valueSchema(), it.value()) }

      actualList.firstOrNull()!! shouldBe expected
    }
  }

  @Test
  fun `should throw exception if provided with an invalid query`() {
    assertFailsWith(ClientException::class) {
      val props = mutableMapOf<String, String>()
      props[Neo4jConfiguration.URI] = neo4j.boltUrl
      props[SourceConfiguration.QUERY_TOPIC] = UUID.randomUUID().toString()
      props[SourceConfiguration.QUERY_POLL_INTERVAL] = "10ms"
      props[SourceConfiguration.QUERY] = "WRONG QUERY".trimMargin()
      props[Neo4jConfiguration.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()

      task.start(props)
      val totalRecords = 10
      insertRecords(totalRecords)

      task.poll()
    }
  }

  private fun insertRecords(
      totalRecords: Int,
      clock: Clock = Clock.systemDefaultZone(),
  ): List<Map<String, Any>> =
      session.beginTransaction().use { tx ->
        val ts = clock.millis()
        val elements =
            (1..totalRecords).map {
              val result =
                  tx.run(
                      """
                                |CREATE (n:Test{
                                |   name: 'Name ' + $it,
                                |   timestamp: ${'$'}timestamp,
                                |   point: point({longitude: 56.7, latitude: 12.78, height: 8}),
                                |   array: [1,2,3],
                                |   datetime: localdatetime(),
                                |   boolean: true
                                |})
                                |RETURN n.name AS name, n.timestamp AS timestamp,
                                |   n.point AS point,
                                |   n.array AS array,
                                |   n.datetime AS datetime,
                                |   n.boolean AS boolean,
                                |   {
                                |       key1: "value1",
                                |       key2: "value2"
                                |   } AS map,
                                |   n AS node
                            """
                          .trimMargin(),
                      mapOf("timestamp" to ts + it))
              result.next().asMap().mapValues { (_, v) ->
                when (v) {
                  is Node ->
                      buildMap {
                        this["<id>"] = v.id()
                        this["<labels>"] = v.labels()
                        this.putAll(v.asMap())
                      }
                  else -> v
                }
              }
            }
        tx.commit()
        elements
      }

  private suspend fun pollAndAssertReceivedSize(expected: Int) {
    val changes = mutableListOf<SourceRecord>()

    eventually(30.seconds) {
      task.poll()?.let { changes.addAll(it) }

      changes shouldHaveSize expected
    }
  }

  private suspend fun pollAndAssertReceived(expected: List<Map<String, Any>>) {
    val list = mutableListOf<SourceRecord>()

    eventually(30.seconds) {
      task.poll()?.let { list.addAll(it) }

      list.map { DynamicTypes.fromConnectValue(it.valueSchema(), it.value()) } shouldBe expected
    }
  }

  private fun newTaskContextWithOffset(property: String, offset: Long): SourceTaskContext {
    return newTaskContextWithOffset(mapOf("property" to property, "value" to offset))
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
}
