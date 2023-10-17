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

import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.test.assertFailsWith
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.source.SourceTask
import org.apache.kafka.connect.source.SourceTaskContext
import org.apache.kafka.connect.storage.OffsetStorageReader
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.*
import org.mockito.Mockito
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
class Neo4jSourceTaskTest {

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
    task = Neo4jSourceTask()
    val sourceTaskContextMock = Mockito.mock(SourceTaskContext::class.java)
    val offsetStorageReader = Mockito.mock(OffsetStorageReader::class.java)
    Mockito.`when`(sourceTaskContextMock.offsetStorageReader()).thenReturn(offsetStorageReader)
    Mockito.`when`(offsetStorageReader.offset(Mockito.anyMap<String, Any>())).thenReturn(emptyMap())
    task.initialize(sourceTaskContextMock)
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
  fun `should source data from Neo4j with custom QUERY from NOW`() {
    val props = mutableMapOf<String, String>()
    props[Neo4jConfiguration.URI] = neo4j.boltUrl
    props[SourceConfiguration.TOPIC] = UUID.randomUUID().toString()
    props[SourceConfiguration.QUERY_POLL_INTERVAL] = "10ms"
    props[SourceConfiguration.QUERY_STREAMING_PROPERTY] = "timestamp"
    props[SourceConfiguration.QUERY] = getSourceQuery()
    props[Neo4jConfiguration.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()

    task.start(props)
    val totalRecords = 10
    val expected = insertRecords(totalRecords, true)

    val list = mutableListOf<SourceRecord>()
    await().atMost(60, TimeUnit.SECONDS).until {
      task.poll()?.let { list.addAll(it) }
      val actualList = list.map { JSONUtils.readValue<Map<String, Any?>>(it.value()) }
      expected.containsAll(actualList)
    }
  }

  @Test
  fun `should source data from Neo4j with custom QUERY from NOW with Schema`() {
    val props = mutableMapOf<String, String>()
    props[Neo4jConfiguration.URI] = neo4j.boltUrl
    props[SourceConfiguration.TOPIC] = UUID.randomUUID().toString()
    props[SourceConfiguration.QUERY_POLL_INTERVAL] = "10ms"
    props[SourceConfiguration.ENFORCE_SCHEMA] = "true"
    props[SourceConfiguration.QUERY_STREAMING_PROPERTY] = "timestamp"
    props[SourceConfiguration.QUERY] = getSourceQuery()
    props[Neo4jConfiguration.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()

    task.start(props)
    val totalRecords = 10
    val expected = insertRecords(totalRecords)

    val list = mutableListOf<SourceRecord>()
    await().atMost(60, TimeUnit.SECONDS).until {
      task.poll()?.let { list.addAll(it) }
      val actualList = list.map { (it.value() as Struct).toMap() }
      expected.containsAll(actualList)
    }
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

    val totalRecords = 10
    val expected = insertRecords(totalRecords, true)

    task.start(props)

    val list = mutableListOf<SourceRecord>()
    await().atMost(60, TimeUnit.SECONDS).until {
      task.poll()?.let { list.addAll(it) }
      val actualList = list.map { JSONUtils.readValue<Map<String, Any?>>(it.value()) }
      expected == actualList
    }
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

    val totalRecords = 10
    val expected = insertRecords(totalRecords)

    task.start(props)

    val list = mutableListOf<SourceRecord>()
    await().atMost(60, TimeUnit.SECONDS).until {
      task.poll()?.let { list.addAll(it) }
      val actualList = list.map { (it.value() as Struct).toMap() }
      expected == actualList
    }
  }

  private fun insertRecords(totalRecords: Int, longToInt: Boolean = false) =
      session.beginTransaction().use { tx ->
        val elements =
            (1..totalRecords).map {
              val result =
                  tx.run(
                      """
                                |CREATE (n:Test{
                                |   name: 'Name ' + $it,
                                |   timestamp: timestamp(),
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
                          .trimMargin())
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
    props[SourceConfiguration.QUERY_POLL_INTERVAL] = "10ms"
    props[SourceConfiguration.QUERY] = getSourceQuery()
    props[Neo4jConfiguration.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()

    task.start(props)
    val totalRecords = 10
    insertRecords(totalRecords)

    val list = mutableListOf<SourceRecord>()
    await().atMost(60, TimeUnit.SECONDS).until {
      task.poll()?.let { list.addAll(it) }
      val actualList = list.map { JSONUtils.readValue<Map<String, Any?>>(it.value()) }
      actualList.size >= 2
    }
  }

  @Test
  fun `should source data from Neo4j with custom QUERY without streaming property with Schema`() {
    val props = mutableMapOf<String, String>()
    props[Neo4jConfiguration.URI] = neo4j.boltUrl
    props[SourceConfiguration.TOPIC] = UUID.randomUUID().toString()
    props[SourceConfiguration.QUERY_POLL_INTERVAL] = "10ms"
    props[SourceConfiguration.ENFORCE_SCHEMA] = "true"
    props[SourceConfiguration.QUERY] = getSourceQuery()
    props[Neo4jConfiguration.AUTHENTICATION_TYPE] = AuthenticationType.NONE.toString()

    task.start(props)
    val totalRecords = 10
    insertRecords(totalRecords)

    val list = mutableListOf<SourceRecord>()
    await().atMost(60, TimeUnit.SECONDS).until {
      task.poll()?.let { list.addAll(it) }
      val actualList = list.map { (it.value() as Struct).toMap() }
      actualList.size >= 2
    }
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

    val list = mutableListOf<SourceRecord>()

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

    await().atMost(60, TimeUnit.SECONDS).until {
      task.poll()?.let { list.addAll(it) }
      val actualList = list.map { (it.value() as Struct).toMap() }
      actualList.first() == expected
    }
  }
}
