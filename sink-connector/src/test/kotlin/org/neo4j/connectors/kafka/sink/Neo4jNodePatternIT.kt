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

import com.fasterxml.jackson.databind.ObjectMapper
import io.kotest.assertions.nondeterministic.eventually
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import kotlin.time.Duration.Companion.seconds
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.junit.jupiter.api.Test
import org.neo4j.connectors.kafka.testing.TestSupport.runTest
import org.neo4j.connectors.kafka.testing.kafka.ConvertingKafkaProducer
import org.neo4j.connectors.kafka.testing.sink.Neo4jSink
import org.neo4j.connectors.kafka.testing.sink.NodePatternStrategy
import org.neo4j.connectors.kafka.testing.sink.TopicProducer
import org.neo4j.driver.Session

class Neo4jNodePatternIT {
  companion object {
    const val TOPIC = "test"
    const val TOPIC_1 = "test-1"
    const val TOPIC_2 = "test-2"
  }

  @Neo4jSink(nodePattern = [NodePatternStrategy(TOPIC, "(:User{!id,name,surname})", false)])
  @Test
  fun `should create node from json string`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    producer.publish(
        valueSchema = Schema.STRING_SCHEMA,
        value = """{"id": 1, "name": "john", "surname": "doe"}""")

    eventually(30.seconds) { session.run("MATCH (n:User) RETURN n", emptyMap()).single() }
        .get("n")
        .asNode() should
        {
          it.labels() shouldBe listOf("User")
          it.asMap() shouldBe mapOf("id" to 1L, "name" to "john", "surname" to "doe")
        }
  }

  @Neo4jSink(nodePattern = [NodePatternStrategy(TOPIC, "(:User{!id,name,surname})", false)])
  @Test
  fun `should create node from struct`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    SchemaBuilder.struct()
        .field("id", Schema.INT64_SCHEMA)
        .field("name", Schema.STRING_SCHEMA)
        .field("surname", Schema.STRING_SCHEMA)
        .build()
        .let { schema ->
          producer.publish(
              valueSchema = schema,
              value = Struct(schema).put("id", 1L).put("name", "john").put("surname", "doe"))
        }

    eventually(30.seconds) { session.run("MATCH (n:User) RETURN n", emptyMap()).single() }
        .get("n")
        .asNode() should
        {
          it.labels() shouldBe listOf("User")
          it.asMap() shouldBe mapOf("id" to 1L, "name" to "john", "surname" to "doe")
        }
  }

  @Neo4jSink(nodePattern = [NodePatternStrategy(TOPIC, "(:User{!id,name,surname})", false)])
  @Test
  fun `should create node from json byte array`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    producer.publish(
        valueSchema = Schema.BYTES_SCHEMA,
        value =
            ObjectMapper()
                .writeValueAsBytes(mapOf("id" to 1L, "name" to "john", "surname" to "doe")))

    eventually(30.seconds) { session.run("MATCH (n:User) RETURN n", emptyMap()).single() }
        .get("n")
        .asNode() should
        {
          it.labels() shouldBe listOf("User")
          it.asMap() shouldBe mapOf("id" to 1L, "name" to "john", "surname" to "doe")
        }
  }

  @Neo4jSink(
      nodePattern =
          [
              NodePatternStrategy(
                  TOPIC, "(:User{!id: old_id,name: first_name,surname: last_name})", false)])
  @Test
  fun `should create node with aliases`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    producer.publish(
        valueSchema = Schema.STRING_SCHEMA,
        value = """{"old_id": 1, "first_name": "john", "last_name": "doe"}""")

    eventually(30.seconds) { session.run("MATCH (n:User) RETURN n", emptyMap()).single() }
        .get("n")
        .asNode() should
        {
          it.labels() shouldBe listOf("User")
          it.asMap() shouldBe mapOf("id" to 1L, "name" to "john", "surname" to "doe")
        }
  }

  @Neo4jSink(
      nodePattern =
          [
              NodePatternStrategy(
                  TOPIC,
                  "(:User{!id: __value.old_id,name: __key.first_name,surname: last_name})",
                  false)])
  @Test
  fun `should create and delete node with aliases`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    producer.publish(
        keySchema = Schema.STRING_SCHEMA,
        key = """{"first_name": "john"}""",
        valueSchema = Schema.STRING_SCHEMA,
        value = """{"old_id": 1, "last_name": "doe"}""")
    eventually(30.seconds) { session.run("MATCH (n:User) RETURN n", emptyMap()).single() }
        .get("n")
        .asNode() should
        {
          it.labels() shouldBe listOf("User")
          it.asMap() shouldBe mapOf("id" to 1L, "name" to "john", "surname" to "doe")
        }

    producer.publish(
        keySchema = Schema.STRING_SCHEMA,
        key = """{"old_id": 1}""",
    )
    eventually(30.seconds) {
      session
          .run("MATCH (n:User) RETURN count(n) AS count", emptyMap())
          .single()
          .get("count")
          .asLong() shouldBe 0
    }
  }

  @Neo4jSink(nodePattern = [NodePatternStrategy(TOPIC, "(:User{!id,name,surname})", false)])
  @Test
  fun `should create, delete and recreate node`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    producer.publish(
        keySchema = Schema.STRING_SCHEMA,
        key = """{"id": 1}""",
        valueSchema = Schema.STRING_SCHEMA,
        value = """{"name": "john", "surname": "doe"}""")
    producer.publish(
        keySchema = Schema.STRING_SCHEMA,
        key = """{"id": 1}""",
    )
    producer.publish(
        keySchema = Schema.STRING_SCHEMA,
        key = """{"id": 1}""",
        valueSchema = Schema.STRING_SCHEMA,
        value = """{"name": "john-new", "surname": "doe-new"}""")

    eventually(30.seconds) {
      session.run("MATCH (n:User) RETURN n", emptyMap()).single().get("n").asNode() should
          {
            it.labels() shouldBe listOf("User")
            it.asMap() shouldBe mapOf("id" to 1L, "name" to "john-new", "surname" to "doe-new")
          }
    }
  }

  @Neo4jSink(nodePattern = [NodePatternStrategy(TOPIC, "(:User{!id,name,surname})", false)])
  @Test
  fun `should create multiple nodes`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    producer.publish(
        keySchema = Schema.STRING_SCHEMA,
        key = """{"id": 1}""",
        valueSchema = Schema.STRING_SCHEMA,
        value = """{"name": "john", "surname": "doe"}""")
    producer.publish(
        keySchema = Schema.STRING_SCHEMA,
        key = """{"id": 2}""",
        valueSchema = Schema.STRING_SCHEMA,
        value = """{"name": "mary", "surname": "doe"}""")

    eventually(30.seconds) {
      session.run("MATCH (n:User) RETURN n", emptyMap()).list { r ->
        r.get("n").asNode().let { mapOf("labels" to it.labels(), "properties" to it.asMap()) }
      } shouldBe
          listOf(
              mapOf(
                  "labels" to listOf("User"),
                  "properties" to mapOf("id" to 1, "name" to "john", "surname" to "doe")),
              mapOf(
                  "labels" to listOf("User"),
                  "properties" to mapOf("id" to 2, "name" to "mary", "surname" to "doe")))
    }
  }

  @Neo4jSink(nodePattern = [NodePatternStrategy(TOPIC, "(:User{!id,!name,surname})", false)])
  @Test
  fun `should create node with compositeKey`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    producer.publish(
        keySchema = Schema.STRING_SCHEMA,
        key = """{"id": 1, "name": "john"}""",
        valueSchema = Schema.STRING_SCHEMA,
        value = """{"surname": "doe"}""")
    eventually(30.seconds) { session.run("MATCH (n:User) RETURN n", emptyMap()).single() }
        .get("n")
        .asNode() should
        {
          it.labels() shouldBe listOf("User")
          it.asMap() shouldBe mapOf("id" to 1L, "name" to "john", "surname" to "doe")
        }
  }

  @Neo4jSink(nodePattern = [NodePatternStrategy(TOPIC, "(:User{!id})", false)])
  @Test
  fun `should create node with nested properties`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    producer.publish(
        valueSchema = Schema.STRING_SCHEMA,
        value =
            """{"id": 1, "name": "john", "surname": "doe", "address": {"city": "london", "country": "uk"}}""")
    eventually(30.seconds) { session.run("MATCH (n:User) RETURN n", emptyMap()).single() }
        .get("n")
        .asNode() should
        {
          it.labels() shouldBe listOf("User")
          it.asMap() shouldBe
              mapOf(
                  "id" to 1L,
                  "name" to "john",
                  "surname" to "doe",
                  "address.city" to "london",
                  "address.country" to "uk")
        }
  }

  @Neo4jSink(nodePattern = [NodePatternStrategy(TOPIC, "(:User{!id, -name, -surname})", false)])
  @Test
  fun `should create node with excluded properties`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    producer.publish(
        valueSchema = Schema.STRING_SCHEMA,
        value =
            """{"id": 1, "name": "john", "surname": "doe", "dob": "2000-01-01", "address": {"city": "london", "country": "uk"}}""")
    eventually(30.seconds) { session.run("MATCH (n:User) RETURN n", emptyMap()).single() }
        .get("n")
        .asNode() should
        {
          it.labels() shouldBe listOf("User")
          it.asMap() shouldBe
              mapOf(
                  "id" to 1L,
                  "dob" to "2000-01-01",
                  "address.city" to "london",
                  "address.country" to "uk")
        }
  }

  @Neo4jSink(
      nodePattern = [NodePatternStrategy(TOPIC, "(:User{!id, created_at: __timestamp})", false)])
  @Test
  fun `should create node with createdAt`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    producer.publish(
        valueSchema = Schema.STRING_SCHEMA,
        value = """{"id": 1, "name": "john", "surname": "doe"}""")
    eventually(30.seconds) { session.run("MATCH (n:User) RETURN n", emptyMap()).single() }
        .get("n")
        .asNode() should
        {
          it.labels() shouldBe listOf("User")
          it.asMap()["created_at"] shouldNotBe null
        }
  }

  @Neo4jSink(
      nodePattern =
          [
              NodePatternStrategy(
                  TOPIC,
                  "(:User{!id: __key.old_id, name: __key.first_name, surname: __key.last_name})",
                  false)])
  @Test
  fun `should create and delete node with all keys pattern`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    producer.publish(
        keySchema = Schema.STRING_SCHEMA,
        key = """{"old_id": 1, "first_name": "john", "last_name": "doe"}""",
        valueSchema = Schema.STRING_SCHEMA,
        value = """{}""")
    eventually(30.seconds) { session.run("MATCH (n:User) RETURN n", emptyMap()).single() }
        .get("n")
        .asNode() should
        {
          it.labels() shouldBe listOf("User")
          it.asMap() shouldBe mapOf("id" to 1L, "name" to "john", "surname" to "doe")
        }

    producer.publish(
        keySchema = Schema.STRING_SCHEMA,
        key = """{"old_id": 1}""",
    )
    eventually(30.seconds) {
      session
          .run("MATCH (n:User) RETURN count(n) AS count", emptyMap())
          .single()
          .get("count")
          .asLong() shouldBe 0
    }
  }

  @Neo4jSink(nodePattern = [NodePatternStrategy(TOPIC, "(:User{!id})", false)])
  @Test
  fun `should create and update node`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    producer.publish(
        keySchema = Schema.STRING_SCHEMA,
        key = """{"id": 1}""",
        valueSchema = Schema.STRING_SCHEMA,
        value = """{"name": "john", "surname": "doe"}""")

    eventually(30.seconds) { session.run("MATCH (n:User) RETURN n", emptyMap()).single() }
        .get("n")
        .asNode() should
        {
          it.labels() shouldBe listOf("User")
          it.asMap() shouldBe mapOf("id" to 1L, "name" to "john", "surname" to "doe")
        }

    producer.publish(
        keySchema = Schema.STRING_SCHEMA,
        key = """{"id": 1}""",
        valueSchema = Schema.STRING_SCHEMA,
        value = """{"name": "john-updated", "surname": "doe-updated"}""")

    eventually(30.seconds) {
      session.run("MATCH (n:User) RETURN n", emptyMap()).single().get("n").asNode() should
          {
            it.labels() shouldBe listOf("User")
            it.asMap() shouldBe
                mapOf("id" to 1L, "name" to "john-updated", "surname" to "doe-updated")
          }
    }
  }

  @Neo4jSink(
      nodePattern =
          [
              NodePatternStrategy(
                  TOPIC, "(:User{!id: old_id, name: first_name, surname: last_name})", false)])
  @Test
  fun `should create and update node with aliases`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    producer.publish(
        keySchema = Schema.STRING_SCHEMA,
        key = """{"old_id": 1}""",
        valueSchema = Schema.STRING_SCHEMA,
        value = """{"first_name": "john", "last_name": "doe"}""")

    eventually(30.seconds) { session.run("MATCH (n:User) RETURN n", emptyMap()).single() }
        .get("n")
        .asNode() should
        {
          it.labels() shouldBe listOf("User")
          it.asMap() shouldBe mapOf("id" to 1L, "name" to "john", "surname" to "doe")
        }

    producer.publish(
        keySchema = Schema.STRING_SCHEMA,
        key = """{"old_id": 1}""",
        valueSchema = Schema.STRING_SCHEMA,
        value = """{"first_name": "john-updated", "last_name": "doe-updated"}""")

    eventually(30.seconds) {
      session.run("MATCH (n:User) RETURN n", emptyMap()).single().get("n").asNode() should
          {
            it.labels() shouldBe listOf("User")
            it.asMap() shouldBe
                mapOf("id" to 1L, "name" to "john-updated", "surname" to "doe-updated")
          }
    }
  }

  @Neo4jSink(
      nodePattern =
          [
              NodePatternStrategy(TOPIC_1, "(:User{!id})", false),
              NodePatternStrategy(TOPIC_2, "(:Account{!id})", false)])
  @Test
  fun `should create nodes from multiple topics`(
      @TopicProducer(TOPIC_1) producer1: ConvertingKafkaProducer,
      @TopicProducer(TOPIC_2) producer2: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    producer1.publish(
        keySchema = Schema.STRING_SCHEMA,
        key = """{"id": 1}""",
        valueSchema = Schema.STRING_SCHEMA,
        value = """{"name": "john", "surname": "doe"}""")

    eventually(30.seconds) { session.run("MATCH (n:User) RETURN n", emptyMap()).single() }
        .get("n")
        .asNode() should
        {
          it.labels() shouldBe listOf("User")
          it.asMap() shouldBe mapOf("id" to 1L, "name" to "john", "surname" to "doe")
        }

    producer2.publish(
        keySchema = Schema.STRING_SCHEMA,
        key = """{"id": 1}""",
        valueSchema = Schema.STRING_SCHEMA,
        value = """{"email": "john@doe.com"}""")

    eventually(30.seconds) { session.run("MATCH (n:Account) RETURN n", emptyMap()).single() }
        .get("n")
        .asNode() should
        {
          it.labels() shouldBe listOf("Account")
          it.asMap() shouldBe mapOf("id" to 1L, "email" to "john@doe.com")
        }
  }
}
