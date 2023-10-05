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
package org.neo4j.connectors.kafka.sink

import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask
import org.apache.kafka.connect.sink.SinkTaskContext
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assumptions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.neo4j.connectors.kafka.configuration.AuthenticationType
import org.neo4j.connectors.kafka.configuration.DeprecatedNeo4jConfiguration
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.Session
import streams.events.Meta
import streams.events.NodeChange
import streams.events.NodePayload
import streams.events.OperationType
import streams.events.RelationshipChange
import streams.events.RelationshipNodeChange
import streams.events.RelationshipPayload
import streams.events.StreamsTransactionEvent
import streams.kafka.connect.sink.DeprecatedNeo4jSinkConfiguration
import streams.service.sink.strategy.CUDNode
import streams.service.sink.strategy.CUDOperations
import streams.utils.JSONUtils

class Neo4jSinkTaskAuraTest {

  companion object {

    private val SIMPLE_SCHEMA =
        SchemaBuilder.struct()
            .name("com.example.Person")
            .field("name", Schema.STRING_SCHEMA)
            .build()

    private val user: String? = System.getenv("AURA_USER")
    private val password: String? = System.getenv("AURA_PASSWORD")
    private val uri: String? = System.getenv("AURA_URI")
    private var driver: Driver? = null

    private const val NAME_TOPIC = "neotopic"
    private const val LABEL_SINK_AURA = "SinkAuraTest"
    private const val COUNT_NODES_SINK_AURA = "MATCH (s:$LABEL_SINK_AURA) RETURN count(s) as count"

    @BeforeAll
    @JvmStatic
    fun setUp() {
      Assumptions.assumeTrue(user != null)
      Assumptions.assumeTrue(password != null)
      driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password))
    }

    fun getMapSinkConnectorConfig() =
        mutableMapOf(
            DeprecatedNeo4jConfiguration.AUTHENTICATION_BASIC_USERNAME to user!!,
            DeprecatedNeo4jConfiguration.AUTHENTICATION_BASIC_PASSWORD to password!!,
            DeprecatedNeo4jConfiguration.SERVER_URI to uri!!,
            DeprecatedNeo4jConfiguration.AUTHENTICATION_TYPE to AuthenticationType.BASIC.toString(),
            DeprecatedNeo4jSinkConfiguration.TOPIC_CDC_SOURCE_ID_LABEL_NAME to LABEL_SINK_AURA,
        )

    fun countEntitiesSinkAura(
        session: Session?,
        number: Int,
        query: String = COUNT_NODES_SINK_AURA
    ) =
        session!!.run(query).let {
          assertTrue { it!!.hasNext() }
          assertEquals(number, it!!.next()["count"].asInt())
          assertFalse { it.hasNext() }
        }
  }

  @AfterEach
  fun clearNodesAura() {
    driver?.session()?.run("MATCH (n:$LABEL_SINK_AURA) DETACH DELETE n")
  }

  @Test
  fun `test with struct in Aura`() {
    driver?.session().use { countEntitiesSinkAura(it, 0) }
    val props = getMapSinkConnectorConfig()
    props["${DeprecatedNeo4jSinkConfiguration.TOPIC_CYPHER_PREFIX}$NAME_TOPIC"] =
        " CREATE (b:$LABEL_SINK_AURA)"
    props[DeprecatedNeo4jConfiguration.BATCH_SIZE] = 2.toString()
    props[SinkTask.TOPICS_CONFIG] = NAME_TOPIC

    val task = Neo4jSinkTask()
    task.initialize(Mockito.mock(SinkTaskContext::class.java))
    task.start(props)
    val input =
        listOf(
            SinkRecord(
                NAME_TOPIC,
                1,
                null,
                null,
                SIMPLE_SCHEMA,
                Struct(SIMPLE_SCHEMA).put("name", "Baz"),
                42))
    task.put(input)

    driver?.session().use { countEntitiesSinkAura(it, 1) }
  }

  @Test
  fun `should insert data into Neo4j from CDC events in Aura`() {

    val props = getMapSinkConnectorConfig()
    props[SinkTask.TOPICS_CONFIG] = NAME_TOPIC
    props[DeprecatedNeo4jSinkConfiguration.TOPIC_CDC_SOURCE_ID] = NAME_TOPIC

    val cdcDataStart =
        StreamsTransactionEvent(
            meta =
                Meta(
                    timestamp = System.currentTimeMillis(),
                    username = "user",
                    txId = 1,
                    txEventId = 0,
                    txEventsCount = 3,
                    operation = OperationType.created),
            payload =
                NodePayload(
                    id = "0",
                    before = null,
                    after =
                        NodeChange(properties = mapOf("name" to "Pippo"), labels = listOf("User"))),
            schema = streams.events.Schema())
    val cdcDataEnd =
        StreamsTransactionEvent(
            meta =
                Meta(
                    timestamp = System.currentTimeMillis(),
                    username = "user",
                    txId = 1,
                    txEventId = 1,
                    txEventsCount = 3,
                    operation = OperationType.created),
            payload =
                NodePayload(
                    id = "1",
                    before = null,
                    after =
                        NodeChange(
                            properties = mapOf("name" to "Pluto"), labels = listOf("User Ext"))),
            schema = streams.events.Schema())
    val cdcDataRelationship =
        StreamsTransactionEvent(
            meta =
                Meta(
                    timestamp = System.currentTimeMillis(),
                    username = "user",
                    txId = 1,
                    txEventId = 2,
                    txEventsCount = 3,
                    operation = OperationType.created),
            payload =
                RelationshipPayload(
                    id = "2",
                    start =
                        RelationshipNodeChange(id = "0", labels = listOf("User"), ids = emptyMap()),
                    end =
                        RelationshipNodeChange(
                            id = "1", labels = listOf("User Ext"), ids = emptyMap()),
                    after = RelationshipChange(properties = mapOf("since" to 2014)),
                    before = null,
                    label = "HAS_REL"),
            schema = streams.events.Schema())

    val task = Neo4jSinkTask()
    task.initialize(Mockito.mock(SinkTaskContext::class.java))
    task.start(props)
    val input =
        listOf(
            SinkRecord(NAME_TOPIC, 1, null, null, null, cdcDataStart, 42),
            SinkRecord(NAME_TOPIC, 1, null, null, null, cdcDataEnd, 43),
            SinkRecord(NAME_TOPIC, 1, null, null, null, cdcDataRelationship, 44))
    task.put(input)

    driver?.session().use {
      countEntitiesSinkAura(
          it,
          1,
          "MATCH (:$LABEL_SINK_AURA)-[r:HAS_REL]->(:$LABEL_SINK_AURA) RETURN COUNT(r) as count")
    }
  }

  @Test
  fun `should update data into Neo4j from CDC events in Aura`() {
    driver
        ?.session()
        ?.run(
            """
                CREATE (s:User:OldLabel:$LABEL_SINK_AURA{name:'Pippo', sourceId:'0'})
                    -[r:`KNOWS WHO`{since:2014, sourceId:'2'}]->
                    (e:`User Ext`:$LABEL_SINK_AURA{name:'Pluto', sourceId:'1'})
            """
                .trimIndent())

    val props = getMapSinkConnectorConfig()
    props[DeprecatedNeo4jSinkConfiguration.TOPIC_CDC_SOURCE_ID] = NAME_TOPIC
    props[SinkTask.TOPICS_CONFIG] = NAME_TOPIC

    val cdcDataStart =
        StreamsTransactionEvent(
            meta =
                Meta(
                    timestamp = System.currentTimeMillis(),
                    username = "user",
                    txId = 1,
                    txEventId = 0,
                    txEventsCount = 3,
                    operation = OperationType.updated),
            payload =
                NodePayload(
                    id = "0",
                    before =
                        NodeChange(
                            properties = mapOf("name" to "Pippo"),
                            labels = listOf("User", "OldLabel")),
                    after =
                        NodeChange(
                            properties = mapOf("name" to "Pippo", "age" to 99),
                            labels = listOf("User"))),
            schema = streams.events.Schema())
    val cdcDataRelationship =
        StreamsTransactionEvent(
            meta =
                Meta(
                    timestamp = System.currentTimeMillis(),
                    username = "user",
                    txId = 1,
                    txEventId = 2,
                    txEventsCount = 3,
                    operation = OperationType.updated),
            payload =
                RelationshipPayload(
                    id = "2",
                    start =
                        RelationshipNodeChange(id = "0", labels = listOf("User"), ids = emptyMap()),
                    end =
                        RelationshipNodeChange(
                            id = "1", labels = listOf("User Ext"), ids = emptyMap()),
                    after = RelationshipChange(properties = mapOf("since" to 1999, "foo" to "bar")),
                    before = RelationshipChange(properties = mapOf("since" to 2014)),
                    label = "KNOWS WHO"),
            schema = streams.events.Schema())

    val task = Neo4jSinkTask()
    task.initialize(Mockito.mock(SinkTaskContext::class.java))
    task.start(props)
    val input =
        listOf(
            SinkRecord(NAME_TOPIC, 1, null, null, null, cdcDataStart, 42),
            SinkRecord(NAME_TOPIC, 1, null, null, null, cdcDataRelationship, 43))
    task.put(input)

    driver?.session().use {
      countEntitiesSinkAura(
          it,
          1,
          "MATCH (:User {age:99})-[r:`KNOWS WHO`{since:1999, sourceId:'2', foo:'bar'}]->(:`User Ext`) RETURN COUNT(r) as count")
    }
  }

  @Test
  fun `should delete data into Neo4j from CDC events in Aura`() {

    driver?.session().use {
      it?.run(
          "CREATE (s:User:OldLabel:$LABEL_SINK_AURA{name:'Andrea', `comp@ny`:'LARUS-BA', sourceId:'0'})")
      it?.run(
          "CREATE (s:User:OldLabel:$LABEL_SINK_AURA{name:'Andrea', `comp@ny`:'LARUS-BA', sourceId:'1'})")
    }

    driver?.session().use { countEntitiesSinkAura(it, 2) }

    val props = getMapSinkConnectorConfig()
    props[SinkTask.TOPICS_CONFIG] = NAME_TOPIC
    props[DeprecatedNeo4jSinkConfiguration.TOPIC_CDC_SOURCE_ID] = NAME_TOPIC

    val cdcDataStart =
        StreamsTransactionEvent(
            meta =
                Meta(
                    timestamp = System.currentTimeMillis(),
                    username = "user",
                    txId = 1,
                    txEventId = 0,
                    txEventsCount = 3,
                    operation = OperationType.deleted),
            payload =
                NodePayload(
                    id = "0",
                    before =
                        NodeChange(
                            properties = mapOf("name" to "Andrea", "comp@ny" to "LARUS-BA"),
                            labels = listOf("User", "OldLabel")),
                    after = null),
            schema = streams.events.Schema())
    val task = Neo4jSinkTask()
    task.initialize(Mockito.mock(SinkTaskContext::class.java))
    task.start(props)
    val input = listOf(SinkRecord(NAME_TOPIC, 1, null, null, null, cdcDataStart, 42))
    task.put(input)

    driver?.session().use { countEntitiesSinkAura(it, 1) }
  }

  @Test
  fun `should work with node pattern topic in Aura`() {
    val props = getMapSinkConnectorConfig()
    props["${DeprecatedNeo4jSinkConfiguration.TOPIC_PATTERN_NODE_PREFIX}$NAME_TOPIC"] =
        "$LABEL_SINK_AURA{!userId,name,surname,address.city}"
    props[SinkTask.TOPICS_CONFIG] = NAME_TOPIC

    val data =
        mapOf(
            "userId" to 1,
            "name" to "Pippo",
            "surname" to "Pluto",
            "address" to mapOf("city" to "Cerignola", "CAP" to "12345"))

    val task = Neo4jSinkTask()
    task.initialize(Mockito.mock(SinkTaskContext::class.java))
    task.start(props)
    val input = listOf(SinkRecord(NAME_TOPIC, 1, null, null, null, data, 42))
    task.put(input)

    driver?.session().use {
      countEntitiesSinkAura(
          it,
          1,
          "MATCH (n:$LABEL_SINK_AURA{name: 'Pippo', surname: 'Pluto', userId: 1, `address.city`: 'Cerignola'}) RETURN count(n) AS count")
    }
  }

  @Test
  fun `should work with relationship pattern topic in Aura`() {
    val props = getMapSinkConnectorConfig()
    props["${DeprecatedNeo4jSinkConfiguration.TOPIC_PATTERN_RELATIONSHIP_PREFIX}$NAME_TOPIC"] =
        "(:$LABEL_SINK_AURA{!sourceId,sourceName,sourceSurname})-[:HAS_REL]->(:$LABEL_SINK_AURA{!targetId,targetName,targetSurname})"
    props[SinkTask.TOPICS_CONFIG] = NAME_TOPIC

    val data =
        mapOf(
            "sourceId" to 1,
            "sourceName" to "Pippo",
            "sourceSurname" to "Pluto",
            "targetId" to 1,
            "targetName" to "Foo",
            "targetSurname" to "Bar")

    val task = Neo4jSinkTask()
    task.initialize(Mockito.mock(SinkTaskContext::class.java))
    task.start(props)
    val input = listOf(SinkRecord(NAME_TOPIC, 1, null, null, null, data, 42))
    task.put(input)
    driver?.session().use {
      countEntitiesSinkAura(
          it,
          1,
          "MATCH (:$LABEL_SINK_AURA{sourceId: 1})-[r:HAS_REL]->(:$LABEL_SINK_AURA{targetId: 1}) RETURN COUNT(r) as count")
    }
  }

  @Test
  fun `should ingest node data from CUD Events in Aura`() {
    val mergeMarkers = listOf(2, 5, 7)
    val key = "key"
    val topic = UUID.randomUUID().toString()
    val data =
        (1..10).map {
          val labels =
              if (it % 2 == 0) listOf(LABEL_SINK_AURA, "Bar")
              else listOf(LABEL_SINK_AURA, "Bar", "Label")
          val properties = mapOf("foo" to "foo-value-$it", "id" to it)
          val (op, ids) =
              when (it) {
                in mergeMarkers -> CUDOperations.merge to mapOf(key to it)
                else -> CUDOperations.create to emptyMap()
              }
          val cudNode = CUDNode(op = op, labels = labels, ids = ids, properties = properties)
          SinkRecord(topic, 1, null, null, null, JSONUtils.asMap(cudNode), it.toLong())
        }

    val props = getMapSinkConnectorConfig()
    props[DeprecatedNeo4jSinkConfiguration.TOPIC_CUD] = topic
    props[SinkTask.TOPICS_CONFIG] = topic

    val task = Neo4jSinkTask()
    task.initialize(Mockito.mock(SinkTaskContext::class.java))
    task.start(props)
    task.put(data)

    driver?.session().use {
      countEntitiesSinkAura(it, 5, "MATCH (n:$LABEL_SINK_AURA:Bar:Label) RETURN count(n) AS count")
      countEntitiesSinkAura(it, 10, "MATCH (n:$LABEL_SINK_AURA:Bar) RETURN count(n) AS count")
    }
  }
}
