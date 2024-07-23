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
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.kotest.matchers.string.shouldContain
import java.time.Duration
import java.time.ZonedDateTime
import kotlin.time.Duration.Companion.seconds
import org.apache.kafka.common.header.Headers
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.storage.SimpleHeaderConverter
import org.junit.jupiter.api.Test
import org.neo4j.cdc.client.model.CaptureMode
import org.neo4j.cdc.client.model.ChangeEvent
import org.neo4j.cdc.client.model.ChangeIdentifier
import org.neo4j.cdc.client.model.EntityOperation
import org.neo4j.cdc.client.model.Event
import org.neo4j.cdc.client.model.Metadata
import org.neo4j.cdc.client.model.NodeEvent
import org.neo4j.cdc.client.model.NodeState
import org.neo4j.connectors.kafka.testing.TestSupport.runTest
import org.neo4j.connectors.kafka.testing.assertions.TopicVerifier
import org.neo4j.connectors.kafka.testing.format.KafkaConverter.AVRO
import org.neo4j.connectors.kafka.testing.format.KafkaConverter.JSON_SCHEMA
import org.neo4j.connectors.kafka.testing.format.KafkaConverter.PROTOBUF
import org.neo4j.connectors.kafka.testing.format.KeyValueConverter
import org.neo4j.connectors.kafka.testing.kafka.ConvertingKafkaConsumer
import org.neo4j.connectors.kafka.testing.kafka.ConvertingKafkaProducer
import org.neo4j.connectors.kafka.testing.kafka.KafkaMessage
import org.neo4j.connectors.kafka.testing.sink.CdcSchemaStrategy
import org.neo4j.connectors.kafka.testing.sink.CdcSourceIdStrategy
import org.neo4j.connectors.kafka.testing.sink.CudStrategy
import org.neo4j.connectors.kafka.testing.sink.CypherStrategy
import org.neo4j.connectors.kafka.testing.sink.Neo4jSink
import org.neo4j.connectors.kafka.testing.sink.NodePatternStrategy
import org.neo4j.connectors.kafka.testing.sink.RelationshipPatternStrategy
import org.neo4j.connectors.kafka.testing.sink.TopicProducer
import org.neo4j.connectors.kafka.testing.source.TopicConsumer
import org.neo4j.driver.Session

class ErrorHeaders(private val headers: Headers) {
  companion object {
    const val TOPIC = "__connect.errors.topic"
    const val PARTITION = "__connect.errors.partition"
    const val OFFSET = "__connect.errors.offset"
    const val CONNECTOR_NAME = "__connect.errors.connector.name"
    const val TASK_ID = "__connect.errors.task.id"
    const val STAGE = "__connect.errors.stage"
    const val CLASS_NAME = "__connect.errors.class.name"
    const val EXCEPTION_CLASS_NAME = "__connect.errors.exception.class.name"
    const val EXCEPTION_MESSAGE = "__connect.errors.exception.message"
    const val EXCEPTION_STACKTRACE = "__connect.errors.exception.stacktrace"
  }

  fun getValue(key: String): Any? {
    return headers
        .find { it.key() == key }
        ?.let {
          val schemaAndValue = SimpleHeaderConverter().toConnectHeader("", it.key(), it.value())
          when (key) {
            TOPIC -> schemaAndValue.value() as String
            PARTITION -> (schemaAndValue.value() as Byte).toInt()
            OFFSET -> (schemaAndValue.value() as Byte).toInt()
            CONNECTOR_NAME -> schemaAndValue.value() as String
            TASK_ID -> (schemaAndValue.value() as Byte).toInt()
            STAGE -> schemaAndValue.value() as String
            CLASS_NAME -> schemaAndValue.value() as String
            EXCEPTION_CLASS_NAME -> schemaAndValue.value() as String
            EXCEPTION_MESSAGE -> schemaAndValue.value() as String?
            EXCEPTION_STACKTRACE -> schemaAndValue.value() as String
            else -> throw IllegalArgumentException("Unknown error key $key")
          }
        }
  }
}

abstract class Neo4jSinkErrorIT {
  companion object {
    private const val TOPIC = "topic"
    private const val TOPIC_1 = "topic-1"
    private const val TOPIC_2 = "topic-2"
    private const val TOPIC_3 = "topic-3"
    private const val DLQ_TOPIC = "dlq-topic"
  }

  @Neo4jSink(
      cypher =
          [
              CypherStrategy(
                  TOPIC,
                  "MERGE (p:Person {id: event.id, name: event.name, surname: event.surname})")],
      errorDlqTopic = DLQ_TOPIC,
      enableErrorHeaders = true)
  @Test
  fun `should report an error with all error headers when headers are enabled`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      @TopicConsumer(topic = DLQ_TOPIC, offset = "earliest", isDlq = true)
      errorConsumer: ConvertingKafkaConsumer,
      session: Session
  ) {
    session.run("CREATE CONSTRAINT FOR (n:Person) REQUIRE n.id IS KEY").consume()

    val schemaWithMissingSurname =
        SchemaBuilder.struct()
            .field("id", Schema.INT64_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .build()
    val struct = Struct(schemaWithMissingSurname)
    struct.put("id", 1L)
    struct.put("name", "John")
    producer.publish(valueSchema = schemaWithMissingSurname, value = struct)

    TopicVerifier.createForMap(errorConsumer)
        .assertMessage {
          val errorHeaders = ErrorHeaders(it.raw.headers())
          errorHeaders.getValue(ErrorHeaders.TOPIC) shouldBe producer.topic
          errorHeaders.getValue(ErrorHeaders.PARTITION) shouldBe 0
          errorHeaders.getValue(ErrorHeaders.OFFSET) shouldBe 0
          (errorHeaders.getValue(ErrorHeaders.CONNECTOR_NAME) as String) shouldContain
              "Neo4jSinkConnector"
          errorHeaders.getValue(ErrorHeaders.TASK_ID) shouldBe 0
          errorHeaders.getValue(ErrorHeaders.STAGE) shouldBe "TASK_PUT"
          errorHeaders.getValue(ErrorHeaders.CLASS_NAME) shouldBe
              "org.apache.kafka.connect.sink.SinkTask"
          errorHeaders.getValue(ErrorHeaders.EXCEPTION_CLASS_NAME) shouldBe
              "org.neo4j.driver.exceptions.ClientException"
          errorHeaders.getValue(ErrorHeaders.EXCEPTION_MESSAGE) shouldBe
              """Cannot merge the following node because of null property value for 'surname': (:Person {surname: null})"""
          errorHeaders.getValue(ErrorHeaders.EXCEPTION_STACKTRACE) shouldNotBe null

          it.value shouldBe mapOf("id" to 1L, "name" to "John")
        }
        .verifyWithin(Duration.ofSeconds(30))
  }

  @Neo4jSink(
      cypher =
          [
              CypherStrategy(
                  TOPIC,
                  "MERGE (p:Person {id: event.id, name: event.name, surname: event.surname})")],
      errorDlqTopic = DLQ_TOPIC,
      enableErrorHeaders = true)
  @Test
  fun `should report failed events with cypher strategy`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      @TopicConsumer(topic = DLQ_TOPIC, offset = "earliest", isDlq = true)
      errorConsumer: ConvertingKafkaConsumer,
      session: Session,
  ) = runTest {
    session.run("CREATE CONSTRAINT FOR (n:Person) REQUIRE n.id IS KEY").consume()

    val schema =
        SchemaBuilder.struct()
            .field("id", Schema.INT64_SCHEMA)
            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
            .field("surname", Schema.OPTIONAL_STRING_SCHEMA)
            .build()

    val struct1 = Struct(schema)
    struct1.put("id", 1L)
    struct1.put("name", "John")
    struct1.put("surname", "Doe")
    val struct2ToFail = Struct(schema)
    struct2ToFail.put("id", 2L)
    struct2ToFail.put("surname", "Doe")
    val struct3 = Struct(schema)
    struct3.put("id", 3L)
    struct3.put("name", "Mary")
    struct3.put("surname", "Doe")
    val struct4ToFail = Struct(schema)
    struct4ToFail.put("id", 4L)
    struct4ToFail.put("name", "Martin")
    val struct5 = Struct(schema)
    struct5.put("id", 5L)
    struct5.put("name", "Sue")
    struct5.put("surname", "Doe")

    producer.publish(
        KafkaMessage(valueSchema = schema, value = struct1),
        KafkaMessage(valueSchema = schema, value = struct2ToFail),
        KafkaMessage(valueSchema = schema, value = struct3),
        KafkaMessage(valueSchema = schema, value = struct4ToFail),
        KafkaMessage(valueSchema = schema, value = struct5))

    eventually(30.seconds) {
      session.run("MATCH (n) RETURN n", emptyMap()).list().map {
        it.get("n").asNode().let { n -> (n.labels() to n.asMap()) }
      } shouldContainExactlyInAnyOrder
          listOf(
              (listOf("Person") to mapOf("id" to 1L, "name" to "John", "surname" to "Doe")),
              (listOf("Person") to mapOf("id" to 3L, "name" to "Mary", "surname" to "Doe")),
              (listOf("Person") to mapOf("id" to 5L, "name" to "Sue", "surname" to "Doe")))
    }

    TopicVerifier.createForMap(errorConsumer)
        .assertMessage {
          val errorHeaders = ErrorHeaders(it.raw.headers())
          errorHeaders.getValue(ErrorHeaders.OFFSET) shouldBe 1
          errorHeaders.getValue(ErrorHeaders.EXCEPTION_CLASS_NAME) shouldBe
              "org.neo4j.driver.exceptions.ClientException"
          errorHeaders.getValue(ErrorHeaders.EXCEPTION_MESSAGE) shouldBe
              "Cannot merge the following node because of null property value for 'name': (:Person {name: null})"

          it.value shouldBe mapOf("id" to 2L, "surname" to "Doe")
        }
        .assertMessage {
          val errorHeaders = ErrorHeaders(it.raw.headers())
          errorHeaders.getValue(ErrorHeaders.OFFSET) shouldBe 3
          errorHeaders.getValue(ErrorHeaders.EXCEPTION_CLASS_NAME) shouldBe
              "org.neo4j.driver.exceptions.ClientException"
          errorHeaders.getValue(ErrorHeaders.EXCEPTION_MESSAGE) shouldBe
              "Cannot merge the following node because of null property value for 'surname': (:Person {surname: null})"

          it.value shouldBe mapOf("id" to 4L, "name" to "Martin")
        }
        .verifyWithin(Duration.ofSeconds(30))
  }

  @Neo4jSink(
      nodePattern =
          [
              NodePatternStrategy(
                  TOPIC, "(:Person{!id, name, surname})", mergeNodeProperties = false)],
      errorDlqTopic = DLQ_TOPIC,
      enableErrorHeaders = true)
  @Test
  fun `should report failed events with node pattern strategy`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      @TopicConsumer(topic = DLQ_TOPIC, offset = "earliest", isDlq = true)
      errorConsumer: ConvertingKafkaConsumer,
      session: Session,
  ) = runTest {
    session.run("CREATE CONSTRAINT FOR (n:Person) REQUIRE n.id IS KEY").consume()
    val message1 =
        KafkaMessage(
            valueSchema = Schema.STRING_SCHEMA,
            value = """{"id": 1, "name": "John", "surname": "Doe"}""")
    val message2ToFail =
        KafkaMessage(
            valueSchema = Schema.STRING_SCHEMA,
            value = """{"id": 2, name: "Jane", "surname": "Doe"}""")
    val message3 =
        KafkaMessage(
            valueSchema = Schema.STRING_SCHEMA,
            value = """{"id": 3, "name": "Mary", "surname": "Doe"}""")
    val message4ToFail =
        KafkaMessage(
            valueSchema = Schema.STRING_SCHEMA, value = """{"name": "Martin", "surname": "Doe"}""")
    val message5 =
        KafkaMessage(
            valueSchema = Schema.STRING_SCHEMA,
            value = """{"id": 5, "name": "Sue", "surname": "Doe"}""")

    producer.publish(message1, message2ToFail, message3, message4ToFail, message5)

    eventually(30.seconds) {
      session.run("MATCH (n) RETURN n", emptyMap()).list().map {
        it.get("n").asNode().let { n -> (n.labels() to n.asMap()) }
      } shouldContainExactlyInAnyOrder
          listOf(
              (listOf("Person") to mapOf("id" to 1L, "name" to "John", "surname" to "Doe")),
              (listOf("Person") to mapOf("id" to 3L, "name" to "Mary", "surname" to "Doe")),
              (listOf("Person") to mapOf("id" to 5L, "name" to "Sue", "surname" to "Doe")))
    }

    TopicVerifier.create<String, String>(errorConsumer)
        .assertMessage {
          val errorHeaders = ErrorHeaders(it.raw.headers())
          errorHeaders.getValue(ErrorHeaders.OFFSET) shouldBe 1
          errorHeaders.getValue(ErrorHeaders.EXCEPTION_CLASS_NAME) shouldBe
              "org.apache.kafka.connect.errors.ConnectException"
          errorHeaders.getValue(ErrorHeaders.EXCEPTION_MESSAGE) shouldBe
              "Message value must be convertible to a Map."

          it.value shouldBe message2ToFail.value
        }
        .assertMessage {
          val errorHeaders = ErrorHeaders(it.raw.headers())
          errorHeaders.getValue(ErrorHeaders.OFFSET) shouldBe 3
          errorHeaders.getValue(ErrorHeaders.EXCEPTION_CLASS_NAME) shouldBe
              "org.neo4j.connectors.kafka.exceptions.InvalidDataException"
          errorHeaders.getValue(ErrorHeaders.EXCEPTION_MESSAGE) shouldBe
              "Key 'id' could not be located in the message."

          it.value shouldBe message4ToFail.value
        }
        .verifyWithin(Duration.ofSeconds(30))
  }

  @Neo4jSink(
      relationshipPattern =
          [
              RelationshipPatternStrategy(
                  TOPIC,
                  "(:Person{!id: personId})-[:OWNS]->(:Item{!id: itemId})",
                  mergeNodeProperties = false,
                  mergeRelationshipProperties = false)],
      errorDlqTopic = DLQ_TOPIC,
      enableErrorHeaders = true)
  @Test
  fun `should report failed events with relationship pattern strategy`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      @TopicConsumer(topic = DLQ_TOPIC, offset = "earliest", isDlq = true)
      errorConsumer: ConvertingKafkaConsumer,
      session: Session,
  ) = runTest {
    session.run("CREATE CONSTRAINT FOR (n:Person) REQUIRE n.id IS KEY").consume()
    session.run("CREATE CONSTRAINT FOR (n:Item) REQUIRE n.id IS KEY").consume()

    val message1 =
        KafkaMessage(valueSchema = Schema.STRING_SCHEMA, value = """{"personId": 1, "itemId": 1}""")
    val message2 =
        KafkaMessage(valueSchema = Schema.STRING_SCHEMA, value = """{"personId": 2, "itemId": 2}""")
    val message3ToFail =
        KafkaMessage(valueSchema = Schema.STRING_SCHEMA, value = """{personId: 3, "itemId": 3}""")
    val message4 =
        KafkaMessage(valueSchema = Schema.STRING_SCHEMA, value = """{"personId": 4, "itemId": 4}""")
    val message5ToFail =
        KafkaMessage(valueSchema = Schema.STRING_SCHEMA, value = """{"personId": 5}""")

    producer.publish(message1, message2, message3ToFail, message4, message5ToFail)

    eventually(30.seconds) {
      val result1 =
          session
              .run(
                  "MATCH (p:Person {id: ${'$'}productId})-[r:OWNS]->(i:Item {id: ${'$'}itemId}) RETURN p,r,i",
                  mapOf("productId" to 1L, "itemId" to 1L))
              .single()

      result1.get("p").asNode() should
          {
            it.labels() shouldBe listOf("Person")
            it.asMap() shouldBe mapOf("id" to 1L)
          }

      result1.get("r").asRelationship() should { it.type() shouldBe "OWNS" }

      result1.get("i").asNode() should
          {
            it.labels() shouldBe listOf("Item")
            it.asMap() shouldBe mapOf("id" to 1L)
          }

      val result2 =
          session
              .run(
                  "MATCH (p:Person {id: ${'$'}productId})-[r:OWNS]->(i:Item {id: ${'$'}itemId}) RETURN p,r,i",
                  mapOf("productId" to 2L, "itemId" to 2L))
              .single()

      result2.get("p").asNode() should
          {
            it.labels() shouldBe listOf("Person")
            it.asMap() shouldBe mapOf("id" to 2L)
          }

      result2.get("r").asRelationship() should { it.type() shouldBe "OWNS" }

      result2.get("i").asNode() should
          {
            it.labels() shouldBe listOf("Item")
            it.asMap() shouldBe mapOf("id" to 2L)
          }

      val result3 =
          session
              .run(
                  "MATCH (p:Person {id: ${'$'}productId})-[r:OWNS]->(i:Item {id: ${'$'}itemId}) RETURN p,r,i",
                  mapOf("productId" to 4L, "itemId" to 4L))
              .single()

      result3.get("p").asNode() should
          {
            it.labels() shouldBe listOf("Person")
            it.asMap() shouldBe mapOf("id" to 4L)
          }

      result3.get("r").asRelationship() should { it.type() shouldBe "OWNS" }

      result3.get("i").asNode() should
          {
            it.labels() shouldBe listOf("Item")
            it.asMap() shouldBe mapOf("id" to 4L)
          }
    }

    TopicVerifier.create<String, String>(errorConsumer)
        .assertMessage {
          val errorHeaders = ErrorHeaders(it.raw.headers())
          errorHeaders.getValue(ErrorHeaders.OFFSET) shouldBe 2
          errorHeaders.getValue(ErrorHeaders.EXCEPTION_CLASS_NAME) shouldBe
              "org.apache.kafka.connect.errors.ConnectException"
          errorHeaders.getValue(ErrorHeaders.EXCEPTION_MESSAGE) shouldBe
              "Message value must be convertible to a Map."

          it.value shouldBe message3ToFail.value
        }
        .assertMessage {
          val errorHeaders = ErrorHeaders(it.raw.headers())
          errorHeaders.getValue(ErrorHeaders.OFFSET) shouldBe 4
          errorHeaders.getValue(ErrorHeaders.EXCEPTION_CLASS_NAME) shouldBe
              "org.neo4j.connectors.kafka.exceptions.InvalidDataException"
          errorHeaders.getValue(ErrorHeaders.EXCEPTION_MESSAGE) shouldBe
              "Key 'itemId' could not be located in the message."

          it.value shouldBe message5ToFail.value
        }
        .verifyWithin(Duration.ofSeconds(30))
  }

  @Neo4jSink(cud = [CudStrategy(TOPIC)], errorDlqTopic = DLQ_TOPIC, enableErrorHeaders = true)
  @Test
  fun `should report failed events with cud strategy`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      @TopicConsumer(topic = DLQ_TOPIC, offset = "earliest", isDlq = true)
      errorConsumer: ConvertingKafkaConsumer,
      session: Session,
  ) = runTest {
    session.run("CREATE CONSTRAINT FOR (n:Person) REQUIRE n.id IS KEY").consume()

    val message1ToFail =
        KafkaMessage(
            valueSchema = Schema.STRING_SCHEMA,
            value =
                """{
                  "type": "node",
                  "labels": ["Person"],
                  "properties": {
                    "id": 1,
                    "name": "John",
                    "surname": "Doe"
                  }
                }""")
    val message2 =
        KafkaMessage(
            valueSchema = Schema.STRING_SCHEMA,
            value =
                """{
                  "type": "node",
                  "op": "create",
                  "labels": ["Person"],
                  "properties": {
                    "id": 2,
                    "name": "Jane",
                    "surname": "Doe"
                  }
                }""")
    val message3ToFail =
        KafkaMessage(
            valueSchema = Schema.STRING_SCHEMA,
            value =
                """{
                  "type": "node",
                  "op": "create",
                  "labels": ["Person"],
                  "properties": {
                    "id": 3,
                    "name": "Mary"
                    "surname": "Doe"
                  }
                }""")
    val message4 =
        KafkaMessage(
            valueSchema = Schema.STRING_SCHEMA,
            value =
                """{
                  "type": "node",
                  "op": "create",
                  "labels": ["Person"],
                  "properties": {
                    "id": 4,
                    "name": "Martin",
                    "surname": "Doe"
                  }
                }""")
    val message5 =
        KafkaMessage(
            valueSchema = Schema.STRING_SCHEMA,
            value =
                """{
                  "type": "node",
                  "op": "create",
                  "labels": ["Person"],
                  "properties": {
                    "id": 5,
                    "name": "Sue",
                    "surname": "Doe"
                  }
                }""")

    producer.publish(message1ToFail, message2, message3ToFail, message4, message5)

    eventually(30.seconds) {
      session.run("MATCH (n) RETURN n", emptyMap()).list().map {
        it.get("n").asNode().let { n -> (n.labels() to n.asMap()) }
      } shouldContainExactlyInAnyOrder
          listOf(
              (listOf("Person") to mapOf("id" to 2L, "name" to "Jane", "surname" to "Doe")),
              (listOf("Person") to mapOf("id" to 4L, "name" to "Martin", "surname" to "Doe")),
              (listOf("Person") to mapOf("id" to 5L, "name" to "Sue", "surname" to "Doe")))
    }

    TopicVerifier.create<String, String>(errorConsumer)
        .assertMessage {
          val errorHeaders = ErrorHeaders(it.raw.headers())
          errorHeaders.getValue(ErrorHeaders.OFFSET) shouldBe 0
          errorHeaders.getValue(ErrorHeaders.EXCEPTION_CLASS_NAME) shouldBe
              "java.lang.IllegalArgumentException"
          errorHeaders.getValue(ErrorHeaders.EXCEPTION_MESSAGE) shouldBe
              "Unsupported data type ('null') for CUD file operation"

          it.value shouldBe message1ToFail.value
        }
        .assertMessage {
          val errorHeaders = ErrorHeaders(it.raw.headers())
          errorHeaders.getValue(ErrorHeaders.OFFSET) shouldBe 2
          errorHeaders.getValue(ErrorHeaders.EXCEPTION_CLASS_NAME) shouldBe
              "org.apache.kafka.connect.errors.ConnectException"
          errorHeaders.getValue(ErrorHeaders.EXCEPTION_MESSAGE) shouldBe
              "Message value must be convertible to a Map."

          it.value shouldBe message3ToFail.value
        }
        .verifyWithin(Duration.ofSeconds(30))
  }

  @Neo4jSink(
      cdcSchema = [CdcSchemaStrategy(TOPIC)], errorDlqTopic = DLQ_TOPIC, enableErrorHeaders = true)
  @Test
  fun `should report failed events with cdc schema strategy`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      @TopicConsumer(DLQ_TOPIC, offset = "earliest", isDlq = true)
      errorConsumer: ConvertingKafkaConsumer,
      session: Session
  ) = runTest {
    session.run("CREATE CONSTRAINT FOR (n:Person) REQUIRE n.id IS KEY").consume()

    val event1ToFail =
        newEvent(
            1,
            1,
            NodeEvent(
                "person1",
                EntityOperation.UPDATE,
                listOf("Person"),
                mapOf("Person" to listOf(mapOf("id" to 1L))),
                null,
                NodeState(
                    listOf("Person"), mapOf("id" to 1L, "name" to "John", "surname" to "Doe"))))

    val event2 =
        newEvent(
            1,
            2,
            NodeEvent(
                "person1",
                EntityOperation.CREATE,
                listOf("Person"),
                mapOf("Person" to listOf(mapOf("id" to 2L))),
                null,
                NodeState(
                    listOf("Person"), mapOf("id" to 2L, "name" to "Jane", "surname" to "Doe"))))

    val event3ToFail =
        newEvent(
            1,
            3,
            NodeEvent(
                "person1",
                EntityOperation.UPDATE,
                listOf("Person"),
                mapOf("Person" to listOf(mapOf("id" to 3L))),
                null,
                NodeState(
                    listOf("Person"), mapOf("id" to 3L, "name" to "Mary", "surname" to "Doe"))))

    val event4 =
        newEvent(
            1,
            4,
            NodeEvent(
                "person1",
                EntityOperation.CREATE,
                listOf("Person"),
                mapOf("Person" to listOf(mapOf("id" to 4L))),
                null,
                NodeState(
                    listOf("Person"), mapOf("id" to 4L, "name" to "Martin", "surname" to "Doe"))))

    val event5 =
        newEvent(
            1,
            5,
            NodeEvent(
                "person1",
                EntityOperation.CREATE,
                listOf("Person"),
                mapOf("Person" to listOf(mapOf("id" to 5L))),
                null,
                NodeState(
                    listOf("Person"), mapOf("id" to 5L, "name" to "Sue", "surname" to "Doe"))))

    producer.publish(event1ToFail, event2, event3ToFail, event4, event5)

    eventually(30.seconds) {
      session.run("MATCH (n) RETURN n", emptyMap()).list().map {
        it.get("n").asNode().let { n -> (n.labels() to n.asMap()) }
      } shouldContainExactlyInAnyOrder
          listOf(
              (listOf("Person") to mapOf("id" to 2L, "name" to "Jane", "surname" to "Doe")),
              (listOf("Person") to mapOf("id" to 4L, "name" to "Martin", "surname" to "Doe")),
              (listOf("Person") to mapOf("id" to 5L, "name" to "Sue", "surname" to "Doe")))
    }

    TopicVerifier.create<ChangeEvent, ChangeEvent>(errorConsumer)
        .assertMessage {
          val errorHeaders = ErrorHeaders(it.raw.headers())
          errorHeaders.getValue(ErrorHeaders.OFFSET) shouldBe 0
          errorHeaders.getValue(ErrorHeaders.EXCEPTION_CLASS_NAME) shouldBe
              "org.neo4j.connectors.kafka.exceptions.InvalidDataException"
          errorHeaders.getValue(ErrorHeaders.EXCEPTION_MESSAGE) shouldBe
              "update operation requires 'before' field in the event object"

          it.value shouldBe event1ToFail
        }
        .assertMessage {
          val errorHeaders = ErrorHeaders(it.raw.headers())
          errorHeaders.getValue(ErrorHeaders.OFFSET) shouldBe 2
          errorHeaders.getValue(ErrorHeaders.EXCEPTION_CLASS_NAME) shouldBe
              "org.neo4j.connectors.kafka.exceptions.InvalidDataException"
          errorHeaders.getValue(ErrorHeaders.EXCEPTION_MESSAGE) shouldBe
              "update operation requires 'before' field in the event object"

          it.value shouldBe event3ToFail
        }
        .verifyWithin(Duration.ofSeconds(30))
  }

  @Neo4jSink(
      cdcSourceId = [CdcSourceIdStrategy(TOPIC, "SourceEvent", "sourceId")],
      errorDlqTopic = DLQ_TOPIC,
      enableErrorHeaders = true)
  @Test
  fun `should report failed events with cdc source id strategy`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      @TopicConsumer(DLQ_TOPIC, offset = "earliest", isDlq = true)
      errorConsumer: ConvertingKafkaConsumer,
      session: Session
  ) = runTest {
    session.run("CREATE CONSTRAINT FOR (n:Person) REQUIRE n.id IS KEY").consume()

    val event1 =
        newEvent(
            1,
            1,
            NodeEvent(
                "person1",
                EntityOperation.CREATE,
                listOf("SourceEvent"),
                emptyMap(),
                null,
                NodeState(
                    listOf("SourceEvent"),
                    mapOf("id" to 1L, "name" to "John", "surname" to "Doe"))))

    val event2ToFail =
        newEvent(
            1,
            2,
            NodeEvent(
                "person2",
                EntityOperation.UPDATE,
                listOf("SourceEvent"),
                emptyMap(),
                null,
                NodeState(
                    listOf("SourceEvent"),
                    mapOf("id" to 2L, "name" to "Jane", "surname" to "Doe"))))

    val event3 =
        newEvent(
            1,
            3,
            NodeEvent(
                "person3",
                EntityOperation.CREATE,
                listOf("SourceEvent"),
                emptyMap(),
                null,
                NodeState(
                    listOf("SourceEvent"), mapOf("id" to 3, "name" to "Mary", "surname" to "Doe"))))

    val event4 =
        newEvent(
            1,
            4,
            NodeEvent(
                "person4",
                EntityOperation.CREATE,
                listOf("SourceEvent"),
                emptyMap(),
                null,
                NodeState(
                    listOf("SourceEvent"),
                    mapOf("id" to 4L, "name" to "Martin", "surname" to "Doe"))))

    val event5ToFail =
        newEvent(
            1,
            5,
            NodeEvent(
                "person5",
                EntityOperation.UPDATE,
                listOf("SourceEvent"),
                emptyMap(),
                null,
                NodeState(
                    listOf("SourceEvent"), mapOf("id" to 5L, "name" to "Sue", "surname" to "*"))))

    producer.publish(event1, event2ToFail, event3, event4, event5ToFail)

    eventually(30.seconds) {
      session.run("MATCH (n) RETURN n", emptyMap()).list().map {
        it.get("n").asNode().let { n -> (n.labels() to n.asMap()) }
      } shouldContainExactlyInAnyOrder
          listOf(
              (listOf("SourceEvent") to
                  mapOf("sourceId" to "person1", "id" to 1L, "name" to "John", "surname" to "Doe")),
              (listOf("SourceEvent") to
                  mapOf("sourceId" to "person3", "id" to 3L, "name" to "Mary", "surname" to "Doe")),
              (listOf("SourceEvent") to
                  mapOf(
                      "sourceId" to "person4", "id" to 4L, "name" to "Martin", "surname" to "Doe")))
    }

    TopicVerifier.create<ChangeEvent, ChangeEvent>(errorConsumer)
        .assertMessage {
          val errorHeaders = ErrorHeaders(it.raw.headers())
          errorHeaders.getValue(ErrorHeaders.OFFSET) shouldBe 1
          errorHeaders.getValue(ErrorHeaders.EXCEPTION_CLASS_NAME) shouldBe
              "org.neo4j.connectors.kafka.exceptions.InvalidDataException"
          errorHeaders.getValue(ErrorHeaders.EXCEPTION_MESSAGE) shouldBe
              "update operation requires 'before' field in the event object"

          it.value shouldBe event2ToFail
        }
        .assertMessage {
          val errorHeaders = ErrorHeaders(it.raw.headers())
          errorHeaders.getValue(ErrorHeaders.OFFSET) shouldBe 4
          errorHeaders.getValue(ErrorHeaders.EXCEPTION_CLASS_NAME) shouldBe
              "org.neo4j.connectors.kafka.exceptions.InvalidDataException"
          errorHeaders.getValue(ErrorHeaders.EXCEPTION_MESSAGE) shouldBe
              "update operation requires 'before' field in the event object"

          it.value shouldBe event5ToFail
        }
        .verifyWithin(Duration.ofSeconds(30))
  }

  @Neo4jSink(
      nodePattern =
          [NodePatternStrategy(TOPIC, "(:User{!id, name, surname})", mergeNodeProperties = false)],
      errorTolerance = "none",
      errorDlqTopic = DLQ_TOPIC)
  @Test
  fun `should stop the process and only report first failed event when error tolerance is none`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      @TopicConsumer(topic = DLQ_TOPIC, offset = "earliest", isDlq = true)
      errorConsumer: ConvertingKafkaConsumer,
      session: Session,
  ) = runTest {
    val message1 =
        KafkaMessage(
            valueSchema = Schema.STRING_SCHEMA,
            value = """{"id": 1, "name": "John", "surname": "Doe"}""")
    val message2ToFail =
        KafkaMessage(
            valueSchema = Schema.STRING_SCHEMA,
            value = """{"id": 2, name: "Jane", "surname": "Doe"}""")
    val message3 =
        KafkaMessage(
            valueSchema = Schema.STRING_SCHEMA,
            value = """{"id": 3, name: "Mary", "surname": "Doe"}""")

    producer.publish(message1, message2ToFail, message3)

    eventually(30.seconds) {
      session.run("MATCH (n) RETURN n", emptyMap()).single().get("n").asNode() should
          {
            it.labels() shouldBe listOf("User")
            it.asMap() shouldBe mapOf("id" to 1L, "name" to "John", "surname" to "Doe")
          }
    }

    TopicVerifier.create<String, String>(errorConsumer)
        .assertMessageValue { it shouldBe message2ToFail.value }
        .verifyWithin(Duration.ofSeconds(30))
  }

  @Neo4jSink(
      cud = [CudStrategy(TOPIC_1)],
      cypher =
          [
              CypherStrategy(
                  TOPIC_2,
                  "MERGE (p:Person {id: event.id, name: event.name, surname: event.surname})")],
      nodePattern =
          [
              NodePatternStrategy(
                  TOPIC_3, "(:Person{!id, name, surname})", mergeNodeProperties = false)],
      errorDlqTopic = DLQ_TOPIC,
      enableErrorHeaders = true)
  @Test
  fun `should report failed events from different topics`(
      @TopicProducer(TOPIC_1) producer1: ConvertingKafkaProducer,
      @TopicProducer(TOPIC_2) producer2: ConvertingKafkaProducer,
      @TopicProducer(TOPIC_3) producer3: ConvertingKafkaProducer,
      @TopicConsumer(topic = DLQ_TOPIC, offset = "earliest", isDlq = true)
      consumer: ConvertingKafkaConsumer,
      session: Session,
  ) = runTest {
    val cudMessageToFail =
        KafkaMessage(
            valueSchema = Schema.STRING_SCHEMA,
            value =
                """{
                  "type": "node",
                  "labels": ["User"],
                  "properties": {
                    "id": 1,
                    "name": "John",
                    "surname": "Doe"
                  }
                }""")

    val cypherMessage =
        KafkaMessage(
            valueSchema = Schema.STRING_SCHEMA,
            value = """{"id": 1, "name": "John", "surname": "Doe"}""")

    val nodePatternMessageToFail =
        KafkaMessage(valueSchema = Schema.STRING_SCHEMA, value = """{}""")

    producer1.publish(cudMessageToFail)
    producer2.publish(cypherMessage)
    producer3.publish(nodePatternMessageToFail)

    eventually(30.seconds) {
      session.run("MATCH (n) RETURN n", emptyMap()).single().get("n").asNode() should
          {
            it.labels() shouldBe listOf("Person")
            it.asMap() shouldBe mapOf("id" to 1L, "name" to "John", "surname" to "Doe")
          }
    }

    TopicVerifier.create<String, String>(consumer)
        .assertMessage {
          val errorHeaders = ErrorHeaders(it.raw.headers())
          errorHeaders.getValue(ErrorHeaders.TOPIC) shouldBe producer1.topic
          errorHeaders.getValue(ErrorHeaders.EXCEPTION_CLASS_NAME) shouldBe
              "java.lang.IllegalArgumentException"
          errorHeaders.getValue(ErrorHeaders.EXCEPTION_MESSAGE) shouldBe
              "Unsupported data type ('null') for CUD file operation"

          it.value shouldBe cudMessageToFail.value
        }
        .assertMessage {
          val errorHeaders = ErrorHeaders(it.raw.headers())
          errorHeaders.getValue(ErrorHeaders.TOPIC) shouldBe producer3.topic
          errorHeaders.getValue(ErrorHeaders.EXCEPTION_CLASS_NAME) shouldBe
              "org.neo4j.connectors.kafka.exceptions.InvalidDataException"
          errorHeaders.getValue(ErrorHeaders.EXCEPTION_MESSAGE) shouldBe
              "Key 'id' could not be located in the message."

          it.value shouldBe nodePatternMessageToFail.value
        }
        .verifyWithin(Duration.ofSeconds(30))
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

@KeyValueConverter(key = AVRO, value = AVRO) class Neo4jSinkErrorAvroIT : Neo4jSinkErrorIT()

@KeyValueConverter(key = JSON_SCHEMA, value = JSON_SCHEMA)
class Neo4jSinkErrorJsonIT : Neo4jSinkErrorIT()

@KeyValueConverter(key = PROTOBUF, value = PROTOBUF)
class Neo4jSinkErrorProtobufIT : Neo4jSinkErrorIT()
