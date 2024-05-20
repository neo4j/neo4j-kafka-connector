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
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import kotlin.time.Duration.Companion.seconds
import org.apache.kafka.connect.data.Schema
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
  }

  @Neo4jSink(nodePattern = [NodePatternStrategy(TOPIC, "(:User{!id,name,surname})", false)])
  @Test
  fun `should create node`(
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
}
