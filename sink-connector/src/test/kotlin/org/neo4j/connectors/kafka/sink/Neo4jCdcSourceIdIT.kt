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

import io.kotest.assertions.nondeterministic.eventually
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import java.time.LocalDate
import java.time.ZonedDateTime
import kotlin.time.Duration.Companion.seconds
import org.junit.jupiter.api.Test
import org.neo4j.cdc.client.model.CaptureMode
import org.neo4j.cdc.client.model.ChangeEvent
import org.neo4j.cdc.client.model.ChangeIdentifier
import org.neo4j.cdc.client.model.EntityOperation
import org.neo4j.cdc.client.model.Event
import org.neo4j.cdc.client.model.Metadata
import org.neo4j.cdc.client.model.Node
import org.neo4j.cdc.client.model.NodeEvent
import org.neo4j.cdc.client.model.NodeState
import org.neo4j.cdc.client.model.RelationshipEvent
import org.neo4j.cdc.client.model.RelationshipState
import org.neo4j.connectors.kafka.testing.TestSupport.runTest
import org.neo4j.connectors.kafka.testing.format.KafkaConverter
import org.neo4j.connectors.kafka.testing.format.KeyValueConverter
import org.neo4j.connectors.kafka.testing.kafka.ConvertingKafkaProducer
import org.neo4j.connectors.kafka.testing.sink.CdcSourceIdStrategy
import org.neo4j.connectors.kafka.testing.sink.Neo4jSink
import org.neo4j.connectors.kafka.testing.sink.SchemaCompatibilityMode
import org.neo4j.connectors.kafka.testing.sink.TopicProducer
import org.neo4j.driver.Session

abstract class Neo4jCdcSourceIdIT {
  companion object {
    private const val TOPIC = "source-id"
  }

  @Neo4jSink(cdcSourceId = [CdcSourceIdStrategy(TOPIC, "SourceEvent", "sourceId")])
  @Test
  fun `should create node`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    producer.publish(
        newEvent(
            0,
            0,
            NodeEvent(
                "person1",
                EntityOperation.CREATE,
                emptyList(),
                emptyMap(),
                null,
                NodeState(emptyList(), mapOf("id" to 5L)))))

    eventually(30.seconds) {
      val result =
          session
              .run(
                  "MATCH (n:SourceEvent {sourceId: ${'$'}sourceId}) RETURN n",
                  mapOf("sourceId" to "person1"))
              .single()

      result.get("n").asNode() should
          {
            it.labels() shouldBe listOf("SourceEvent")
            it.asMap() shouldBe mapOf("sourceId" to "person1", "id" to 5L)
          }
    }
  }

  @Neo4jSink(cdcSourceId = [CdcSourceIdStrategy(TOPIC, "SourceEvent", "sourceId")])
  @Test
  fun `should update node`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session
        .run(
            "CREATE (n:SourceEvent:Person) SET n = ${'$'}props",
            mapOf("props" to mapOf("sourceId" to "person1", "id" to 1L, "name" to "john")))
        .consume()

    producer.publish(
        newEvent(
            0,
            0,
            NodeEvent(
                "person1",
                EntityOperation.UPDATE,
                listOf("Person"),
                mapOf("Person" to listOf(mapOf("id" to 1L))),
                NodeState(listOf("Person"), mapOf("id" to 1L, "name" to "john")),
                NodeState(
                    listOf("Person", "Employee"),
                    mapOf(
                        "id" to 5L,
                        "name" to "john",
                        "surname" to "doe",
                        "dob" to LocalDate.of(2000, 1, 1))))))

    eventually(30.seconds) {
      val result =
          session
              .run(
                  "MATCH (n:SourceEvent {sourceId: ${'$'}sourceId}) RETURN n",
                  mapOf("sourceId" to "person1"))
              .single()

      result.get("n").asNode() should
          {
            it.labels() shouldBe listOf("SourceEvent", "Person", "Employee")
            it.asMap() shouldBe
                mapOf(
                    "sourceId" to "person1",
                    "id" to 5L,
                    "name" to "john",
                    "surname" to "doe",
                    "dob" to LocalDate.of(2000, 1, 1))
          }
    }
  }

  @Neo4jSink(cdcSourceId = [CdcSourceIdStrategy(TOPIC, "SourceEvent", "sourceId")])
  @Test
  fun `should delete node`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session
        .run(
            "CREATE (n:SourceEvent) SET n = ${'$'}props",
            mapOf("props" to mapOf("sourceId" to "person1", "id" to 1L, "name" to "john")))
        .consume()

    producer.publish(
        newEvent(
            0,
            0,
            NodeEvent(
                "person1",
                EntityOperation.DELETE,
                emptyList(),
                emptyMap(),
                NodeState(emptyList(), mapOf("id" to 1L, "name" to "john")),
                null)))

    eventually(30.seconds) {
      val result =
          session
              .run(
                  "MATCH (n:SourceEvent {sourceId: ${'$'}sourceId}) RETURN n",
                  mapOf("sourceId" to "person1"))
              .list()

      result shouldHaveSize 0
    }
  }

  @Neo4jSink(cdcSourceId = [CdcSourceIdStrategy(TOPIC, "SourceEvent", "sourceId")])
  @Test
  fun `should create relationship`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session
        .run(
            """
            CREATE (n1:SourceEvent:Person) SET n1 = ${'$'}person1
            CREATE (n2:SourceEvent:Person) SET n2 = ${'$'}person2
            """,
            mapOf(
                "person1" to mapOf("sourceId" to "person1", "name" to "john", "surname" to "doe"),
                "person2" to mapOf("sourceId" to "person2", "name" to "mary", "surname" to "doe")))
        .consume()

    producer.publish(
        newEvent(
            0,
            0,
            RelationshipEvent(
                "knows1",
                "KNOWS",
                Node("person1", listOf("Person"), emptyMap()),
                Node("person2", listOf("Person"), emptyMap()),
                emptyList(),
                EntityOperation.CREATE,
                null,
                RelationshipState(mapOf("since" to LocalDate.of(2000, 1, 1))))))

    eventually(30.seconds) {
      val result =
          session
              .run(
                  "MATCH (start)-[r:KNOWS {sourceId: ${'$'}sourceId}]->(end) RETURN start, r, end",
                  mapOf("sourceId" to "knows1"))
              .single()

      result.get("r").asRelationship() should
          {
            it.asMap() shouldBe mapOf("sourceId" to "knows1", "since" to LocalDate.of(2000, 1, 1))
          }
      result.get("start").asNode() should
          {
            it.labels() shouldBe listOf("SourceEvent", "Person")
            it.asMap() shouldBe mapOf("sourceId" to "person1", "name" to "john", "surname" to "doe")
          }
      result.get("end").asNode() should
          {
            it.labels() shouldBe listOf("SourceEvent", "Person")
            it.asMap() shouldBe mapOf("sourceId" to "person2", "name" to "mary", "surname" to "doe")
          }
    }
  }

  @Neo4jSink(cdcSourceId = [CdcSourceIdStrategy(TOPIC, "SourceEvent", "sourceId")])
  @Test
  fun `should update relationship`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session
        .run(
            """
            CREATE (n1:SourceEvent:Person) SET n1 = ${'$'}person1
            CREATE (n2:SourceEvent:Person) SET n2 = ${'$'}person2
            CREATE (n1)-[r:KNOWS]->(n2) SET r = ${'$'}knows
            """,
            mapOf(
                "person1" to mapOf("sourceId" to "person1", "name" to "john", "surname" to "doe"),
                "person2" to mapOf("sourceId" to "person2", "name" to "mary", "surname" to "doe"),
                "knows" to
                    mapOf(
                        "sourceId" to "knows1",
                        "since" to LocalDate.of(2000, 1, 1),
                        "type" to "friend")))
        .consume()

    producer.publish(
        newEvent(
            0,
            0,
            RelationshipEvent(
                "knows1",
                "KNOWS",
                Node("person1", listOf("Person"), emptyMap()),
                Node("person2", listOf("Person"), emptyMap()),
                emptyList(),
                EntityOperation.UPDATE,
                RelationshipState(mapOf("since" to LocalDate.of(2000, 1, 1), "type" to "friend")),
                RelationshipState(mapOf("since" to LocalDate.of(1999, 1, 1))))))

    eventually(30.seconds) {
      val result =
          session
              .run(
                  "MATCH (start)-[r:KNOWS {sourceId: ${'$'}sourceId}]->(end) RETURN start, r, end",
                  mapOf("sourceId" to "knows1"))
              .single()

      result.get("r").asRelationship() should
          {
            it.asMap() shouldBe mapOf("sourceId" to "knows1", "since" to LocalDate.of(1999, 1, 1))
          }
      result.get("start").asNode() should
          {
            it.labels() shouldBe listOf("SourceEvent", "Person")
            it.asMap() shouldBe mapOf("sourceId" to "person1", "name" to "john", "surname" to "doe")
          }
      result.get("end").asNode() should
          {
            it.labels() shouldBe listOf("SourceEvent", "Person")
            it.asMap() shouldBe mapOf("sourceId" to "person2", "name" to "mary", "surname" to "doe")
          }
    }
  }

  @Neo4jSink(cdcSourceId = [CdcSourceIdStrategy(TOPIC, "SourceEvent", "sourceId")])
  @Test
  fun `should delete relationship`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session
        .run(
            "CREATE (n1:SourceEvent:Person) SET n1 = ${'$'}person1 " +
                "CREATE (n2:SourceEvent:Person) SET n2 = ${'$'}person2 " +
                "CREATE (n1)-[r:KNOWS]->(n2) SET r = ${'$'}knows",
            mapOf(
                "person1" to mapOf("sourceId" to "person1", "name" to "john", "surname" to "doe"),
                "person2" to mapOf("sourceId" to "person2", "name" to "mary", "surname" to "doe"),
                "knows" to
                    mapOf(
                        "sourceId" to "knows1",
                        "since" to LocalDate.of(2000, 1, 1),
                        "type" to "friend")))
        .consume()

    producer.publish(
        newEvent(
            0,
            0,
            RelationshipEvent(
                "knows1",
                "KNOWS",
                Node("person1", listOf("Person"), emptyMap()),
                Node("person2", listOf("Person"), emptyMap()),
                emptyList(),
                EntityOperation.DELETE,
                RelationshipState(mapOf("since" to LocalDate.of(1999, 1, 1))),
                null)))

    eventually(30.seconds) {
      val result =
          session
              .run(
                  "MATCH ()-[r:KNOWS {sourceId: ${'$'}sourceId}]->() RETURN count(r)",
                  mapOf("sourceId" to "knows1"))
              .single()

      result.get(0).asInt() shouldBe 0
    }
  }

  @Neo4jSink(
      schemaControlKeyCompatibility = SchemaCompatibilityMode.NONE,
      schemaControlValueCompatibility = SchemaCompatibilityMode.NONE,
      cdcSourceId = [CdcSourceIdStrategy(TOPIC, "SourceEvent", "sourceId")])
  @Test
  fun `should sync continuous changes`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    producer.publish(
        newEvent(
            0,
            0,
            NodeEvent(
                "person1",
                EntityOperation.CREATE,
                listOf("Person"),
                mapOf("Person" to listOf(mapOf("id" to 1L))),
                null,
                NodeState(
                    listOf("Person"), mapOf("id" to 1L, "name" to "john", "surname" to "doe")))))

    producer.publish(
        newEvent(
            1,
            0,
            NodeEvent(
                "person2",
                EntityOperation.CREATE,
                listOf("Person"),
                mapOf("Person" to listOf(mapOf("id" to 2L))),
                null,
                NodeState(
                    listOf("Person"), mapOf("id" to 2L, "name" to "mary", "surname" to "doe")))))

    producer.publish(
        newEvent(
            2,
            0,
            RelationshipEvent(
                "knows1",
                "KNOWS",
                Node("person1", listOf("Person"), mapOf("Person" to listOf(mapOf("id" to 1L)))),
                Node("person2", listOf("Person"), mapOf("Person" to listOf(mapOf("id" to 2L)))),
                emptyList(),
                EntityOperation.CREATE,
                null,
                RelationshipState(emptyMap()))))

    eventually(30.seconds) {
      val result =
          session
              .run(
                  """
            MATCH (start:SourceEvent {sourceId: ${'$'}startId})
            MATCH (end:SourceEvent {sourceId: ${'$'}endId})
            MATCH (start)-[r:KNOWS {sourceId: ${'$'}rId}]->(end)
            RETURN start, r, end
          """,
                  mapOf("startId" to "person1", "rId" to "knows1", "endId" to "person2"))
              .single()

      result.get("r").asRelationship() should { it.asMap() shouldBe mapOf("sourceId" to "knows1") }
      result.get("start").asNode() should
          {
            it.labels() shouldBe listOf("SourceEvent", "Person")
            it.asMap() shouldBe
                mapOf("sourceId" to "person1", "id" to 1L, "name" to "john", "surname" to "doe")
          }
      result.get("end").asNode() should
          {
            it.labels() shouldBe listOf("SourceEvent", "Person")
            it.asMap() shouldBe
                mapOf("sourceId" to "person2", "id" to 2L, "name" to "mary", "surname" to "doe")
          }
    }

    producer.publish(
        newEvent(
            3,
            0,
            RelationshipEvent(
                "knows1",
                "KNOWS",
                Node("person1", listOf("Person"), mapOf("Person" to listOf(mapOf("id" to 1L)))),
                Node("person2", listOf("Person"), mapOf("Person" to listOf(mapOf("id" to 2L)))),
                emptyList(),
                EntityOperation.UPDATE,
                RelationshipState(emptyMap()),
                RelationshipState(mapOf("since" to LocalDate.of(2000, 1, 1))))))

    eventually(30.seconds) {
      val result =
          session
              .run(
                  """
            MATCH (start:SourceEvent {sourceId: ${'$'}startId})
            MATCH (end:SourceEvent {sourceId: ${'$'}endId})
            MATCH (start)-[r:KNOWS {sourceId: ${'$'}rId}]->(end)
            RETURN r
          """,
                  mapOf("startId" to "person1", "rId" to "knows1", "endId" to "person2"))
              .single()

      result.get("r").asRelationship() should
          {
            it.asMap() shouldBe mapOf("sourceId" to "knows1", "since" to LocalDate.of(2000, 1, 1))
          }
    }

    producer.publish(
        newEvent(
            4,
            0,
            RelationshipEvent(
                "knows1",
                "KNOWS",
                Node("person1", listOf("Person"), mapOf("Person" to listOf(mapOf("id" to 1L)))),
                Node("person2", listOf("Person"), mapOf("Person" to listOf(mapOf("id" to 2L)))),
                emptyList(),
                EntityOperation.DELETE,
                RelationshipState(mapOf("since" to LocalDate.of(2000, 1, 1))),
                null)))
    producer.publish(
        newEvent(
            4,
            1,
            NodeEvent(
                "person1",
                EntityOperation.DELETE,
                listOf("Person"),
                mapOf("Person" to listOf(mapOf("id" to 1L))),
                NodeState(
                    listOf("Person"), mapOf("id" to 1L, "name" to "john", "surname" to "doe")),
                null)))
    producer.publish(
        newEvent(
            4,
            2,
            NodeEvent(
                "person2",
                EntityOperation.DELETE,
                listOf("Person"),
                mapOf("Person" to listOf(mapOf("id" to 2L))),
                NodeState(
                    listOf("Person"), mapOf("id" to 2L, "name" to "mary", "surname" to "doe")),
                null)))

    eventually(30.seconds) {
      val result = session.run("MATCH (n) RETURN count(n)").single()

      result.get(0).asInt() shouldBe 0
    }
  }

  private fun newEvent(txId: Long, seq: Int, event: Event): ChangeEvent =
      ChangeEvent(
          ChangeIdentifier("$txId:$seq"),
          txId,
          seq,
          Metadata(
              "neo4j",
              "neo4j",
              "server-id",
              CaptureMode.DIFF,
              "bolt",
              "localhost:32000",
              "localhost:7687",
              ZonedDateTime.now().minusSeconds(5),
              ZonedDateTime.now(),
              emptyMap(),
              emptyMap()),
          event)
}

@KeyValueConverter(key = KafkaConverter.AVRO, value = KafkaConverter.AVRO)
class Neo4jCdcSourceIdAvroIT : Neo4jCdcSourceIdIT()

@KeyValueConverter(key = KafkaConverter.JSON_SCHEMA, value = KafkaConverter.JSON_SCHEMA)
class Neo4jCdcSourceIdJsonIT : Neo4jCdcSourceIdIT()

@KeyValueConverter(key = KafkaConverter.PROTOBUF, value = KafkaConverter.PROTOBUF)
class Neo4jCdcSourceIdProtobufIT : Neo4jCdcSourceIdIT()
