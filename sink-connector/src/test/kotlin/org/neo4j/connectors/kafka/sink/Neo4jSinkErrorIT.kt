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
import java.time.Duration
import kotlin.time.Duration.Companion.seconds
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.storage.SimpleHeaderConverter
import org.junit.jupiter.api.Test
import org.neo4j.connectors.kafka.testing.TestSupport.runTest
import org.neo4j.connectors.kafka.testing.assertions.TopicVerifier
import org.neo4j.connectors.kafka.testing.format.KafkaConverter.AVRO
import org.neo4j.connectors.kafka.testing.format.KafkaConverter.JSON_SCHEMA
import org.neo4j.connectors.kafka.testing.format.KafkaConverter.PROTOBUF
import org.neo4j.connectors.kafka.testing.format.KeyValueConverter
import org.neo4j.connectors.kafka.testing.kafka.ConvertingKafkaConsumer
import org.neo4j.connectors.kafka.testing.kafka.ConvertingKafkaProducer
import org.neo4j.connectors.kafka.testing.kafka.GenericRecord
import org.neo4j.connectors.kafka.testing.kafka.KafkaMessage
import org.neo4j.connectors.kafka.testing.sink.CypherStrategy
import org.neo4j.connectors.kafka.testing.sink.Neo4jSink
import org.neo4j.connectors.kafka.testing.sink.NodePatternStrategy
import org.neo4j.connectors.kafka.testing.sink.TopicProducer
import org.neo4j.connectors.kafka.testing.source.TopicConsumer
import org.neo4j.driver.Session

object ErrorHeaders {
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

abstract class Neo4jSinkErrorIT {
  companion object {
    private const val TOPIC = "topic"
    private const val DLQ_TOPIC = "dlq-topic"
  }

  @Neo4jSink(
      cypher =
          [CypherStrategy(TOPIC, "MERGE (p:Person {name: event.name, surname: event.surname})")],
      errorDlqTopic = DLQ_TOPIC,
  )
  @Test
  fun `should report an error`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      @TopicConsumer(topic = DLQ_TOPIC, offset = "earliest") consumer: ConvertingKafkaConsumer,
      session: Session,
  ) {
    val schema = SchemaBuilder.struct().field("name", Schema.STRING_SCHEMA).build()
    val struct = Struct(schema)
    struct.put("name", "John")

    producer.publish(valueSchema = schema, value = struct)

    TopicVerifier.createForMap(consumer)
        .assertMessage { msg -> msg.value shouldBe mapOf("name" to "John") }
        .verifyWithin(Duration.ofSeconds(30))
  }

  @Neo4jSink(
      cypher =
          [CypherStrategy(TOPIC, "MERGE (p:Person {name: event.name, surname: event.surname})")],
      errorDlqTopic = DLQ_TOPIC,
      errorDlqContextHeadersEnable = true)
  @Test
  fun `should report an error with error headers`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      @TopicConsumer(topic = DLQ_TOPIC, offset = "earliest") consumer: ConvertingKafkaConsumer,
      session: Session,
  ) {
    val schema = SchemaBuilder.struct().field("name", Schema.STRING_SCHEMA).build()
    val struct = Struct(schema)
    struct.put("name", "John")
    producer.publish(valueSchema = schema, value = struct)

    TopicVerifier.createForMap(consumer)
        .assertMessage { msg ->
          val headersMap = getHeadersMap(msg)
          //todo: improve here ?
          headersMap[ErrorHeaders.EXCEPTION_CLASS_NAME] shouldNotBe null
          headersMap[ErrorHeaders.EXCEPTION_MESSAGE] shouldNotBe null
          headersMap[ErrorHeaders.EXCEPTION_STACKTRACE] shouldNotBe null

          msg.value shouldBe mapOf("name" to "John")
        }
        .verifyWithin(Duration.ofSeconds(30))
  }

  @Neo4jSink(
      nodePattern =
          [NodePatternStrategy(TOPIC, "(:User{!id, name, surname})", mergeNodeProperties = false)],
      errorDlqTopic = DLQ_TOPIC)
  @Test
  fun `should report an error with multiple events`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      @TopicConsumer(topic = DLQ_TOPIC, offset = "earliest") consumer: ConvertingKafkaConsumer,
      session: Session,
  ) = runTest {
    producer.publish(
        KafkaMessage(
            valueSchema = Schema.STRING_SCHEMA,
            value = """{"id": 1, "name": "John", "surname": "Doe"}"""),
        KafkaMessage(
            valueSchema = Schema.STRING_SCHEMA,
            value = """{"id": 2, name: "Jane", "surname": "Doe"}"""),
        KafkaMessage(
            valueSchema = Schema.STRING_SCHEMA,
            value = """{"id": 3, "name": "Mary", "surname": "Doe"}"""))

    eventually(30.seconds) {
      session.run("MATCH (n) RETURN n", emptyMap()).list().map {
        it.get("n").asNode().let { n -> (n.labels() to n.asMap()) }
      } shouldContainExactlyInAnyOrder
          listOf(
              (listOf("User") to mapOf("id" to 1L, "name" to "John", "surname" to "Doe")),
              (listOf("User") to mapOf("id" to 3L, "name" to "Mary", "surname" to "Doe")))
    }

    TopicVerifier.create<String, String>(consumer)
        .assertMessageValue { it shouldBe """{"id": 2, name: "Jane", "surname": "Doe"}""" }
        .verifyWithin(Duration.ofSeconds(30))
  }

  @Neo4jSink(
      nodePattern =
          [NodePatternStrategy(TOPIC, "(:User{!id, name, surname})", mergeNodeProperties = false)],
      errorTolerance = "none",
      errorDlqTopic = DLQ_TOPIC)
  @Test
  fun `should report an error when tolerance none with multiple events`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      @TopicConsumer(topic = DLQ_TOPIC, offset = "earliest") consumer: ConvertingKafkaConsumer,
      session: Session,
  ) = runTest {
    producer.publish(
        KafkaMessage(
            valueSchema = Schema.STRING_SCHEMA,
            value = """{"id": 1, "name": "John", "surname": "Doe"}"""),
        KafkaMessage(
            valueSchema = Schema.STRING_SCHEMA,
            value = """{"id": 2, name: "Jane", "surname": "Doe"}"""),
        KafkaMessage(
            valueSchema = Schema.STRING_SCHEMA,
            value = """{"id": 3, "name": "Mary", "surname": "Doe"}"""))

    eventually(30.seconds) {
      session.run("MATCH (n) RETURN n", emptyMap()).single().get("n").asNode() should
          {
            it.labels() shouldBe listOf("User")
            it.asMap() shouldBe mapOf("id" to 1L, "name" to "John", "surname" to "Doe")
          }
    }

    TopicVerifier.create<String, String>(consumer)
        .assertMessageValue { it shouldBe """{"id": 2, name: "Jane", "surname": "Doe"}""" }
        .verifyWithin(Duration.ofSeconds(30))
  }

  @Neo4jSink(
      nodePattern =
          [NodePatternStrategy(TOPIC, "(:User{!id, name, surname})", mergeNodeProperties = false)],
      errorDlqTopic = DLQ_TOPIC)
  @Test
  fun `should be able to report multiple errors`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      @TopicConsumer(topic = DLQ_TOPIC, offset = "earliest") consumer: ConvertingKafkaConsumer,
      session: Session,
  ) = runTest {
    producer.publish(
        KafkaMessage(
            valueSchema = Schema.STRING_SCHEMA,
            value = """{"id": 1, "name": "John", "surname": "Doe"}"""),
        KafkaMessage(
            valueSchema = Schema.STRING_SCHEMA,
            value = """{"id": 2, name: "Jane", "surname": "Doe"}"""),
        KafkaMessage(
            valueSchema = Schema.STRING_SCHEMA,
            value = """{"id": 3, "name": "Mary", "surname": "Doe"}"""),
        KafkaMessage(
            valueSchema = Schema.STRING_SCHEMA,
            value = """{"id": 4, "name": "Martin", "surname": "Doe"}"""),
        KafkaMessage(
            valueSchema = Schema.STRING_SCHEMA,
            value = """{"id": invalid-id, "name": "Sue", "surname": "Doe"}"""))

    eventually(30.seconds) {
      session.run("MATCH (n) RETURN n", emptyMap()).list().map {
        it.get("n").asNode().let { n -> (n.labels() to n.asMap()) }
      } shouldContainExactlyInAnyOrder
          listOf(
              (listOf("User") to mapOf("id" to 1L, "name" to "John", "surname" to "Doe")),
              (listOf("User") to mapOf("id" to 3L, "name" to "Mary", "surname" to "Doe")),
              (listOf("User") to mapOf("id" to 4L, "name" to "Martin", "surname" to "Doe")))
    }

    TopicVerifier.create<String, String>(consumer)
        .assertMessageValue { it shouldBe """{"id": 2, name: "Jane", "surname": "Doe"}""" }
        .assertMessageValue {
          it shouldBe """{"id": invalid-id, "name": "Sue", "surname": "Doe"}"""
        }
        .verifyWithin(Duration.ofSeconds(30))
  }

  private fun <K, V> getHeadersMap(msg: GenericRecord<K, V>): Map<String, Any> {
    return msg.raw.headers().associate {
      it.key() to SimpleHeaderConverter().toConnectHeader("", it.key(), it.value()).value()
    }
  }
}

@KeyValueConverter(key = AVRO, value = AVRO) class Neo4jSinkErrorAvroIT : Neo4jSinkErrorIT()

@KeyValueConverter(key = JSON_SCHEMA, value = JSON_SCHEMA)
class Neo4jSinkErrorJsonIT : Neo4jSinkErrorIT()

@KeyValueConverter(key = PROTOBUF, value = PROTOBUF)
class Neo4jSinkErrorProtobufIT : Neo4jSinkErrorIT()
