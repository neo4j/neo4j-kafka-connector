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

import io.kotest.assertions.nondeterministic.until
import io.kotest.matchers.collections.shouldContainExactly
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.util.*
import kotlin.test.assertFailsWith
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.runBlocking
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.ConnectException
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
import org.neo4j.connectors.kafka.utils.JSONUtils
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.Session
import org.testcontainers.containers.Neo4jContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

@Testcontainers
class Neo4jQueryTaskTest {

  companion object {
    @Container
    val neo4j: Neo4jContainer<*> =
        Neo4jContainer("neo4j:5-enterprise")
            .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
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
    session.run("MATCH (n) DETACH DELETE n")
    task.stop()
  }

  @BeforeEach
  fun before() {
    task = Neo4jQueryTask()
    task.initialize(newTaskContextWithOffset(emptyMap()))
  }

  private fun structToMap(struct: Struct): Map<String, Any?> =
      struct
          .schema()
          .fields()
          .map {
            it.name() to
                when (val value = struct[it.name()]) {
                  is Struct -> structToMap(value)
                  else -> value
                }
          }
          .toMap()

  fun Struct.toMap() = structToMap(this)

  @Test
  fun `should use correct offset when startFrom=earliest`() {
    // create data with timestamp set as 0
    insertRecords(50, Clock.fixed(Instant.EPOCH, ZoneId.systemDefault()))
    // create data with timestamp set as NOW + 5m
    insertRecords(
        100, Clock.fixed(Instant.now().plus(Duration.ofMinutes(5)), ZoneId.systemDefault()))

    // start task with EARLIEST, previous changes should be visible
    task.start(
        mapOf(
            Neo4jConfiguration.URI to neo4j.boltUrl,
            Neo4jConfiguration.AUTHENTICATION_TYPE to AuthenticationType.NONE.toString(),
            SourceConfiguration.STRATEGY to SourceType.QUERY.toString(),
            SourceConfiguration.START_FROM to StartFrom.EARLIEST.toString(),
            SourceConfiguration.TOPIC to UUID.randomUUID().toString(),
            SourceConfiguration.QUERY to getSourceQuery()))

    // expect to see all data
    pollAndAssertReceivedSize(150)
  }

  @Test
  fun `should use correct offset when startFrom=now`() {
    // create data with timestamp set as NOW - 5m
    insertRecords(
        100, Clock.fixed(Instant.now().minus(Duration.ofMinutes(5)), ZoneId.systemDefault()))
    // create data with timestamp set as NOW + 5m
    insertRecords(
        75, Clock.fixed(Instant.now().plus(Duration.ofMinutes(5)), ZoneId.systemDefault()))

    // start task with NOW, previous changes should NOT be visible
    task.start(
        mapOf(
            Neo4jConfiguration.URI to neo4j.boltUrl,
            Neo4jConfiguration.AUTHENTICATION_TYPE to AuthenticationType.NONE.toString(),
            SourceConfiguration.STRATEGY to SourceType.QUERY.toString(),
            SourceConfiguration.START_FROM to StartFrom.NOW.toString(),
            SourceConfiguration.TOPIC to UUID.randomUUID().toString(),
            SourceConfiguration.QUERY to getSourceQuery()))

    // expect to see only the data created after task is started
    pollAndAssertReceivedSize(75)
  }

  @Test
  fun `should use correct offset when startFrom=user provided`() {
    // create data with timestamp set as NOW - 5m
    insertRecords(
        10, Clock.fixed(Instant.now().minus(Duration.ofMinutes(5)), ZoneId.systemDefault()))
    // create data with timestamp set as NOW + 5m
    insertRecords(
        25, Clock.fixed(Instant.now().plus(Duration.ofMinutes(5)), ZoneId.systemDefault()))
    // create data with timestamp set as NOW + 10m
    insertRecords(
        75, Clock.fixed(Instant.now().plus(Duration.ofMinutes(10)), ZoneId.systemDefault()))

    // start task with NOW, previous changes should NOT be visible
    task.start(
        mapOf(
            Neo4jConfiguration.URI to neo4j.boltUrl,
            Neo4jConfiguration.AUTHENTICATION_TYPE to AuthenticationType.NONE.toString(),
            SourceConfiguration.STRATEGY to SourceType.QUERY.toString(),
            SourceConfiguration.START_FROM to StartFrom.USER_PROVIDED.toString(),
            SourceConfiguration.START_FROM_VALUE to
                Instant.now().plus(Duration.ofMinutes(7)).toEpochMilli().toString(),
            SourceConfiguration.TOPIC to UUID.randomUUID().toString(),
            SourceConfiguration.QUERY to getSourceQuery()))

    // expect to see only the data created after the provided timestamp
    pollAndAssertReceivedSize(75)
  }

  @ParameterizedTest
  @EnumSource(StartFrom::class)
  fun `should use stored offset regardless of provided startFrom`(startFrom: StartFrom) {
    // create data with timestamp set as NOW - 5m
    insertRecords(
        25, Clock.fixed(Instant.now().minus(Duration.ofMinutes(5)), ZoneId.systemDefault()))
    // create data with timestamp set as NOW - 2m
    insertRecords(
        25, Clock.fixed(Instant.now().minus(Duration.ofMinutes(2)), ZoneId.systemDefault()))
    // create data with timestamp set as NOW + 2m
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
          put(SourceConfiguration.TOPIC, UUID.randomUUID().toString())
          put(SourceConfiguration.QUERY, getSourceQuery())
          put(SourceConfiguration.QUERY_STREAMING_PROPERTY, "timestamp")

          put(SourceConfiguration.START_FROM, startFrom.toString())
          if (startFrom == StartFrom.USER_PROVIDED) {
            put(SourceConfiguration.START_FROM_VALUE, "-1")
          }
        })

    // expect to see only the data created after the stored offset
    pollAndAssertReceivedSize(50)
  }

  @Test
  fun `should ignore stored offset when startFrom=earliest`() {
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
            SourceConfiguration.TOPIC to UUID.randomUUID().toString(),
            SourceConfiguration.QUERY to getSourceQuery(),
            SourceConfiguration.IGNORE_STORED_OFFSET to "true"))

    // expect to see all data
    pollAndAssertReceivedSize(150)
  }

  @Test
  fun `should ignore stored offset when startFrom=now`() {
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
            SourceConfiguration.TOPIC to UUID.randomUUID().toString(),
            SourceConfiguration.QUERY to getSourceQuery(),
            SourceConfiguration.IGNORE_STORED_OFFSET to "true"))

    // expect to see only the data created after task is started
    pollAndAssertReceivedSize(75)
  }

  @Test
  fun `should ignore stored offset when startFrom=user provided`() {
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
            SourceConfiguration.TOPIC to UUID.randomUUID().toString(),
            SourceConfiguration.QUERY to getSourceQuery(),
            SourceConfiguration.IGNORE_STORED_OFFSET to "true"))

    // expect to see only the data created after the provided timestamp
    pollAndAssertReceivedSize(75)
  }

  @Test
  fun `should source data from Neo4j with custom QUERY from NOW`() {
    val props = mutableMapOf<String, String>()
    props[Neo4jConfiguration.URI] = neo4j.boltUrl
    props[SourceConfiguration.TOPIC] = UUID.randomUUID().toString()
    props[SourceConfiguration.QUERY_POLL_INTERVAL] = "10ms"
    props[SourceConfiguration.QUERY_STREAMING_PROPERTY] = "timestamp"
    props[SourceConfiguration.QUERY] = getSourceQuery()
    props[Neo4jConfiguration.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()

    val expected =
        insertRecords(
            10,
            Clock.fixed(Instant.now().plus(Duration.ofMinutes(5)), ZoneId.systemDefault()),
            true)

    task.start(props)

    pollAndAssertReceived(expected)
  }

  @Test
  fun `should source data from Neo4j with custom QUERY from NOW with Schema`() {
    val props = mutableMapOf<String, String>()
    props[Neo4jConfiguration.URI] = neo4j.boltUrl
    props[SourceConfiguration.TOPIC] = UUID.randomUUID().toString()
    props[SourceConfiguration.QUERY_POLL_INTERVAL] = "1s"
    props[SourceConfiguration.ENFORCE_SCHEMA] = "true"
    props[SourceConfiguration.QUERY_STREAMING_PROPERTY] = "timestamp"
    props[SourceConfiguration.QUERY] = getSourceQuery()
    props[Neo4jConfiguration.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()

    val expected = insertRecords(10, Clock.offset(Clock.systemDefaultZone(), Duration.ofSeconds(5)))

    task.start(props)

    pollAndAssertReceived(expected)
  }

  @Test
  fun `should source data from Neo4j with custom QUERY from ALL`() {
    val props = mutableMapOf<String, String>()
    props[Neo4jConfiguration.URI] = neo4j.boltUrl
    props[SourceConfiguration.TOPIC] = UUID.randomUUID().toString()
    props[SourceConfiguration.START_FROM] = "EARLIEST"
    props[SourceConfiguration.QUERY_POLL_INTERVAL] = "10ms"
    props[SourceConfiguration.QUERY_STREAMING_PROPERTY] = "timestamp"
    props[SourceConfiguration.QUERY] = getSourceQuery()
    props[Neo4jConfiguration.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()

    val expected = mutableListOf<Map<String, Any>>()
    expected.addAll(
        insertRecords(10, Clock.offset(Clock.systemDefaultZone(), Duration.ofMinutes(-5)), true))
    expected.addAll(
        insertRecords(10, Clock.offset(Clock.systemDefaultZone(), Duration.ofMinutes(5)), true))

    task.start(props)

    pollAndAssertReceived(expected)
  }

  @Test
  fun `should source data from Neo4j with custom QUERY from ALL with Schema`() {
    val props = mutableMapOf<String, String>()
    props[Neo4jConfiguration.URI] = neo4j.boltUrl
    props[SourceConfiguration.TOPIC] = UUID.randomUUID().toString()
    props[SourceConfiguration.START_FROM] = "EARLIEST"
    props[SourceConfiguration.QUERY_POLL_INTERVAL] = "10ms"
    props[SourceConfiguration.ENFORCE_SCHEMA] = "true"
    props[SourceConfiguration.QUERY_STREAMING_PROPERTY] = "timestamp"
    props[SourceConfiguration.QUERY] = getSourceQuery()
    props[Neo4jConfiguration.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()

    val expected = mutableListOf<Map<String, Any>>()
    expected.addAll(
        insertRecords(
            10, Clock.fixed(Instant.now().minus(Duration.ofMinutes(5)), ZoneId.systemDefault())))
    expected.addAll(
        insertRecords(
            10, Clock.fixed(Instant.now().plus(Duration.ofMinutes(5)), ZoneId.systemDefault())))

    task.start(props)

    pollAndAssertReceived(expected)
  }

  @Test
  fun `should source data from Neo4j with custom QUERY from USER_PROVIDED`() {
    val props = mutableMapOf<String, String>()
    props[Neo4jConfiguration.URI] = neo4j.boltUrl
    props[SourceConfiguration.TOPIC] = UUID.randomUUID().toString()
    props[SourceConfiguration.QUERY_POLL_INTERVAL] = "10ms"
    props[SourceConfiguration.QUERY_STREAMING_PROPERTY] = "timestamp"
    props[SourceConfiguration.QUERY] = getSourceQuery()
    props[SourceConfiguration.START_FROM] = StartFrom.USER_PROVIDED.toString()
    props[SourceConfiguration.START_FROM_VALUE] =
        Instant.now().minus(Duration.ofMinutes(7)).toEpochMilli().toString()
    props[Neo4jConfiguration.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()

    insertRecords(10, Clock.offset(Clock.systemDefaultZone(), Duration.ofMinutes(-10)))

    val expected = mutableListOf<Map<String, Any>>()
    expected.addAll(
        insertRecords(10, Clock.offset(Clock.systemDefaultZone(), Duration.ofMinutes(-5)), true))
    expected.addAll(
        insertRecords(10, Clock.offset(Clock.systemDefaultZone(), Duration.ofMinutes(5)), true))

    task.start(props)

    pollAndAssertReceived(expected)
  }

  @Test
  fun `should source data from Neo4j with custom QUERY from USER_PROVIDED with Schema`() {
    val props = mutableMapOf<String, String>()
    props[Neo4jConfiguration.URI] = neo4j.boltUrl
    props[SourceConfiguration.TOPIC] = UUID.randomUUID().toString()
    props[SourceConfiguration.QUERY_POLL_INTERVAL] = "10ms"
    props[SourceConfiguration.ENFORCE_SCHEMA] = "true"
    props[SourceConfiguration.QUERY_STREAMING_PROPERTY] = "timestamp"
    props[SourceConfiguration.QUERY] = getSourceQuery()
    props[SourceConfiguration.START_FROM] = StartFrom.USER_PROVIDED.toString()
    props[SourceConfiguration.START_FROM_VALUE] =
        Instant.now().minus(Duration.ofMinutes(7)).toEpochMilli().toString()
    props[Neo4jConfiguration.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()

    insertRecords(10, Clock.offset(Clock.systemDefaultZone(), Duration.ofMinutes(-10)))

    val expected = mutableListOf<Map<String, Any>>()
    expected.addAll(
        insertRecords(10, Clock.offset(Clock.systemDefaultZone(), Duration.ofMinutes(-5))))
    expected.addAll(
        insertRecords(10, Clock.offset(Clock.systemDefaultZone(), Duration.ofMinutes(5))))

    task.start(props)

    pollAndAssertReceived(expected)
  }

  private fun insertRecords(
      totalRecords: Int,
      clock: Clock = Clock.systemDefaultZone(),
      longToInt: Boolean = false
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
              val next = result.next()
              val map = next.asMap().toMutableMap()
              map["array"] =
                  next["array"].asList().map { if (longToInt) (it as Long).toInt() else it }
              map["point"] = JSONUtils.readValue<Map<String, Any>>(map["point"]!!)
              map["datetime"] = next["datetime"].asLocalDateTime().toString()
              val node = next["node"].asNode()
              val nodeMap = node.asMap().toMutableMap()
              nodeMap["<id>"] = if (longToInt) node.id().toInt() else node.id()
              nodeMap["<labels>"] = node.labels()
              // are the same value as above
              nodeMap["array"] = map["array"]
              nodeMap["point"] = map["point"]
              nodeMap["datetime"] = map["datetime"]
              map["node"] = nodeMap
              map
            }
        tx.commit()
        elements
      }

  @Test
  fun `should source data from Neo4j with custom QUERY without streaming property`() {
    val props = mutableMapOf<String, String>()
    props[Neo4jConfiguration.URI] = neo4j.boltUrl
    props[SourceConfiguration.TOPIC] = UUID.randomUUID().toString()
    props[SourceConfiguration.QUERY_POLL_INTERVAL] = "5s"
    props[SourceConfiguration.QUERY] = getSourceQuery()
    props[Neo4jConfiguration.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()

    task.start(props)

    val expected =
        insertRecords(10, Clock.offset(Clock.systemDefaultZone(), Duration.ofMillis(10)), true)

    pollAndAssertReceived(expected)
  }

  @Test
  fun `should source data from Neo4j with custom QUERY without streaming property with Schema`() {
    val props = mutableMapOf<String, String>()
    props[Neo4jConfiguration.URI] = neo4j.boltUrl
    props[SourceConfiguration.TOPIC] = UUID.randomUUID().toString()
    props[SourceConfiguration.QUERY_POLL_INTERVAL] = "5s"
    props[SourceConfiguration.ENFORCE_SCHEMA] = "true"
    props[SourceConfiguration.QUERY] = getSourceQuery()
    props[Neo4jConfiguration.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()

    task.start(props)

    val expected = insertRecords(10, Clock.offset(Clock.systemDefaultZone(), Duration.ofMillis(10)))

    pollAndAssertReceived(expected)
  }

  private fun getSourceQuery() =
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
  fun `should throw exception`() {
    assertFailsWith(ConnectException::class) {
      val props = mutableMapOf<String, String>()
      props[Neo4jConfiguration.URI] = neo4j.boltUrl
      props[SourceConfiguration.TOPIC] = UUID.randomUUID().toString()
      props[SourceConfiguration.QUERY_POLL_INTERVAL] = "10ms"
      props[SourceConfiguration.QUERY] = "WRONG QUERY".trimMargin()
      props[Neo4jConfiguration.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()

      task.start(props)
      val totalRecords = 10
      insertRecords(totalRecords)

      task.poll()
    }
  }

  @Test
  fun `should source data from mock with custom QUERY without streaming property with Schema`() {
    val props = mutableMapOf<String, String>()
    props[Neo4jConfiguration.URI] = neo4j.boltUrl
    props[SourceConfiguration.TOPIC] = UUID.randomUUID().toString()
    props[SourceConfiguration.QUERY_POLL_INTERVAL] = "10ms"
    props[SourceConfiguration.ENFORCE_SCHEMA] = "true"
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
                |RETURN data, data.id AS id
            """
            .trimMargin()
    props[Neo4jConfiguration.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()

    task.start(props)
    val totalRecords = 10
    insertRecords(totalRecords)

    val expected =
        mapOf(
            "id" to "ROOT_ID",
            "data" to
                mapOf(
                    "id" to "ROOT_ID",
                    "arr" to listOf(null, mapOf("foo" to "bar")),
                    "root" to
                        listOf(
                            mapOf("children" to emptyList<Map<String, Any>>()),
                            mapOf("children" to listOf(mapOf("name" to "child"))))))

    runBlocking {
      val list = mutableListOf<SourceRecord>()

      until(30.seconds) {
        task.poll()?.let { list.addAll(it) }
        val actualList = list.map { (it.value() as Struct).toMap() }
        actualList.firstOrNull() == expected
      }
    }
  }

  private fun pollAndAssertReceivedSize(expected: Int) {
    val changes = mutableListOf<SourceRecord>()

    runBlocking {
      until(30.seconds) {
        task.poll()?.let { changes.addAll(it) }

        changes.size == expected
      }
    }
  }

  private fun pollAndAssertReceived(expected: List<Map<String, Any>>) {
    val list = mutableListOf<SourceRecord>()

    runBlocking {
      until(30.seconds) {
        task.poll()?.let { list.addAll(it) }
        list.size == expected.size
      }
    }

    list.map {
      when (val value = it.value()) {
        is Struct -> value.toMap()
        else -> JSONUtils.readValue<Map<String, Any?>>(value)
      }
    } shouldContainExactly expected
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
