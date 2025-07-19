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
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import java.math.BigDecimal
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit
import kotlin.time.Duration.Companion.seconds
import org.apache.kafka.connect.data.Date
import org.apache.kafka.connect.data.Decimal
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.data.Time
import org.apache.kafka.connect.data.Timestamp
import org.junit.jupiter.api.Test
import org.neo4j.caniuse.Neo4j
import org.neo4j.connectors.kafka.data.DynamicTypes
import org.neo4j.connectors.kafka.data.PropertyType
import org.neo4j.connectors.kafka.data.PropertyType.schema
import org.neo4j.connectors.kafka.testing.DateSupport
import org.neo4j.connectors.kafka.testing.TestSupport.runTest
import org.neo4j.connectors.kafka.testing.createNodeKeyConstraint
import org.neo4j.connectors.kafka.testing.format.KafkaConverter
import org.neo4j.connectors.kafka.testing.format.KeyValueConverter
import org.neo4j.connectors.kafka.testing.kafka.ConvertingKafkaProducer
import org.neo4j.connectors.kafka.testing.kafka.KafkaMessage
import org.neo4j.connectors.kafka.testing.sink.Neo4jSink
import org.neo4j.connectors.kafka.testing.sink.NodePatternStrategy
import org.neo4j.connectors.kafka.testing.sink.TopicProducer
import org.neo4j.driver.Session
import org.neo4j.driver.Values

abstract class Neo4jNodePatternIT {
  companion object {
    const val TOPIC = "test"
    const val TOPIC_1 = "test-1"
    const val TOPIC_2 = "test-2"
  }

  @Neo4jSink(
      nodePattern =
          [NodePatternStrategy(TOPIC, "(:User{!id,name,surname})", mergeNodeProperties = false)]
  )
  @Test
  fun `should create node from json string`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
      neo4j: Neo4j,
  ) = runTest {
    session.createNodeKeyConstraint(neo4j, "user_id", "User", "id")

    producer.publish(
        valueSchema = Schema.STRING_SCHEMA,
        value = """{"id": 1, "name": "john", "surname": "doe"}""",
    )

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
                  "(:User{!id,name,surname,dob,place})",
                  mergeNodeProperties = false,
              )
          ]
  )
  @Test
  fun `should create node from struct`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
      neo4j: Neo4j,
  ) = runTest {
    session.createNodeKeyConstraint(neo4j, "user_id", "User", "id")

    SchemaBuilder.struct()
        .field("id", Schema.INT64_SCHEMA)
        .field("name", Schema.STRING_SCHEMA)
        .field("surname", Schema.STRING_SCHEMA)
        .field("dob", PropertyType.schema)
        .field("place", PropertyType.schema)
        .build()
        .let { schema ->
          producer.publish(
              valueSchema = schema,
              value =
                  Struct(schema)
                      .put("id", 1L)
                      .put("name", "john")
                      .put("surname", "doe")
                      .put(
                          "dob",
                          DynamicTypes.toConnectValue(PropertyType.schema, LocalDate.of(1995, 1, 1)),
                      )
                      .put(
                          "place",
                          DynamicTypes.toConnectValue(
                              PropertyType.schema,
                              Values.point(7203, 1.0, 2.5).asPoint(),
                          ),
                      ),
          )
        }

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
                  "dob" to LocalDate.of(1995, 1, 1),
                  "place" to Values.point(7203, 1.0, 2.5).asPoint(),
              )
        }
  }

  @Neo4jSink(
      nodePattern =
          [
              NodePatternStrategy(
                  TOPIC,
                  "(:User{!id,height,dob,tob,tsob})",
                  mergeNodeProperties = false,
              )
          ]
  )
  @Test
  fun `should create node from struct containing connect types`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
      neo4j: Neo4j,
  ) = runTest {
    session.createNodeKeyConstraint(neo4j, "user_id", "User", "id")

    SchemaBuilder.struct()
        .field("id", Schema.INT64_SCHEMA)
        .field("height", Decimal.schema(2))
        .field("dob", Date.SCHEMA)
        .field("tob", Time.SCHEMA)
        .field("tsob", Timestamp.SCHEMA)
        .build()
        .let { schema ->
          producer.publish(
              valueSchema = schema,
              value =
                  Struct(schema)
                      .put("id", 1L)
                      .put("height", BigDecimal.valueOf(185, 2))
                      .put("dob", DateSupport.date(1978, 1, 15))
                      .put("tob", DateSupport.time(7, 45, 12, 999))
                      .put("tsob", DateSupport.timestamp(1978, 1, 15, 7, 45, 12, 999)),
          )
        }

    eventually(30.seconds) { session.run("MATCH (n:User) RETURN n", emptyMap()).single() }
        .get("n")
        .asNode() should
        {
          it.labels() shouldBe listOf("User")
          it.asMap() shouldBe
              mapOf(
                  "id" to 1L,
                  "height" to "1.85",
                  "dob" to LocalDate.of(1978, 1, 15),
                  "tob" to LocalTime.of(7, 45, 12, 999000000),
                  "tsob" to LocalDateTime.of(1978, 1, 15, 7, 45, 12, 999000000),
              )
        }
  }

  @Neo4jSink(
      nodePattern =
          [NodePatternStrategy(TOPIC, "(:User{!id,name,surname})", mergeNodeProperties = false)]
  )
  @Test
  fun `should create node from json byte array`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
      neo4j: Neo4j,
  ) = runTest {
    session.createNodeKeyConstraint(neo4j, "user_id", "User", "id")

    producer.publish(
        valueSchema = Schema.BYTES_SCHEMA,
        value =
            ObjectMapper()
                .writeValueAsBytes(mapOf("id" to 1L, "name" to "john", "surname" to "doe")),
    )

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
                  "(:User:Person{!id,name,surname})",
                  mergeNodeProperties = false,
              )
          ]
  )
  @Test
  fun `should create node with multiple labels pattern`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
      neo4j: Neo4j,
  ) = runTest {
    session.createNodeKeyConstraint(neo4j, "user_id", "User", "id")
    session.createNodeKeyConstraint(neo4j, "person_id", "Person", "id")

    producer.publish(
        valueSchema = Schema.BYTES_SCHEMA,
        value =
            ObjectMapper()
                .writeValueAsBytes(mapOf("id" to 1L, "name" to "john", "surname" to "doe")),
    )

    eventually(30.seconds) { session.run("MATCH (n:User) RETURN n", emptyMap()).single() }
        .get("n")
        .asNode() should
        {
          it.labels().toList() shouldContainExactlyInAnyOrder listOf("User", "Person")
          it.asMap() shouldBe mapOf("id" to 1L, "name" to "john", "surname" to "doe")
        }
  }

  @Neo4jSink(
      nodePattern =
          [
              NodePatternStrategy(
                  TOPIC,
                  "(:User{!id: old_id,name: first_name,surname: last_name})",
                  mergeNodeProperties = false,
              )
          ]
  )
  @Test
  fun `should create node with aliases`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
      neo4j: Neo4j,
  ) = runTest {
    session.createNodeKeyConstraint(neo4j, "user_id", "User", "id")

    producer.publish(
        valueSchema = Schema.STRING_SCHEMA,
        value = """{"old_id": 1, "first_name": "john", "last_name": "doe"}""",
    )

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
                  mergeNodeProperties = false,
              )
          ]
  )
  @Test
  fun `should create and delete node with aliases`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
      neo4j: Neo4j,
  ) = runTest {
    session.createNodeKeyConstraint(neo4j, "user_id", "User", "id")

    producer.publish(
        keySchema = Schema.STRING_SCHEMA,
        key = """{"first_name": "john"}""",
        valueSchema = Schema.STRING_SCHEMA,
        value = """{"old_id": 1, "last_name": "doe"}""",
    )

    eventually(30.seconds) { session.run("MATCH (n:User) RETURN n", emptyMap()).single() }
        .get("n")
        .asNode() should
        {
          it.labels() shouldBe listOf("User")
          it.asMap() shouldBe mapOf("id" to 1L, "name" to "john", "surname" to "doe")
        }

    producer.publish(keySchema = Schema.STRING_SCHEMA, key = """{"old_id": 1}""")

    eventually(30.seconds) {
      session
          .run("MATCH (n:User) RETURN count(n) AS count", emptyMap())
          .single()
          .get("count")
          .asLong() shouldBe 0
    }
  }

  @Neo4jSink(
      nodePattern = [NodePatternStrategy(TOPIC, "(:User{!id})", mergeNodeProperties = false)]
  )
  @Test
  fun `should delete node`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
      neo4j: Neo4j,
  ) = runTest {
    session.createNodeKeyConstraint(neo4j, "user_id", "User", "id")

    session
        .run(
            "CREATE (n:User) SET n = ${'$'}props",
            mapOf("props" to mapOf("id" to 1, "name" to "john", "surname" to "doe")),
        )
        .consume()

    producer.publish(keySchema = Schema.STRING_SCHEMA, key = """{"id": 1}""")

    eventually(30.seconds) {
      session
          .run("MATCH (n:User) RETURN count(n) AS count", emptyMap())
          .single()
          .get("count")
          .asLong() shouldBe 0
    }
  }

  @Neo4jSink(
      nodePattern =
          [NodePatternStrategy(TOPIC, "(:User{!id,name,surname})", mergeNodeProperties = false)]
  )
  @Test
  fun `should create, delete and recreate node`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
      neo4j: Neo4j,
  ) = runTest {
    session.createNodeKeyConstraint(neo4j, "user_id", "User", "id")

    producer.publish(
        KafkaMessage(
            keySchema = Schema.STRING_SCHEMA,
            key = """{"id": 1}""",
            valueSchema = Schema.STRING_SCHEMA,
            value = """{"name": "john", "surname": "doe"}""",
        ),
        KafkaMessage(keySchema = Schema.STRING_SCHEMA, key = """{"id": 1}"""),
        KafkaMessage(
            keySchema = Schema.STRING_SCHEMA,
            key = """{"id": 1}""",
            valueSchema = Schema.STRING_SCHEMA,
            value = """{"name": "john-new", "surname": "doe-new"}""",
        ),
    )

    eventually(60.seconds) {
      session.run("MATCH (n:User) RETURN n", emptyMap()).single().get("n").asNode() should
          {
            it.labels() shouldBe listOf("User")
            it.asMap() shouldBe mapOf("id" to 1L, "name" to "john-new", "surname" to "doe-new")
          }
    }
  }

  @Neo4jSink(
      nodePattern =
          [NodePatternStrategy(TOPIC, "(:User{!id,name,surname})", mergeNodeProperties = false)]
  )
  @Test
  fun `should create multiple nodes`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
      neo4j: Neo4j,
  ) = runTest {
    session.createNodeKeyConstraint(neo4j, "user_id", "User", "id")

    producer.publish(
        KafkaMessage(
            keySchema = Schema.STRING_SCHEMA,
            key = """{"id": 1}""",
            valueSchema = Schema.STRING_SCHEMA,
            value = """{"name": "john", "surname": "doe"}""",
        ),
        KafkaMessage(
            keySchema = Schema.STRING_SCHEMA,
            key = """{"id": 2}""",
            valueSchema = Schema.STRING_SCHEMA,
            value = """{"name": "mary", "surname": "doe"}""",
        ),
    )

    eventually(30.seconds) {
      session.run("MATCH (n:User) RETURN n", emptyMap()).list { r ->
        r.get("n").asNode().let { mapOf("labels" to it.labels(), "properties" to it.asMap()) }
      } shouldBe
          listOf(
              mapOf(
                  "labels" to listOf("User"),
                  "properties" to mapOf("id" to 1, "name" to "john", "surname" to "doe"),
              ),
              mapOf(
                  "labels" to listOf("User"),
                  "properties" to mapOf("id" to 2, "name" to "mary", "surname" to "doe"),
              ),
          )
    }
  }

  @Neo4jSink(
      nodePattern =
          [NodePatternStrategy(TOPIC, "(:User{!id,!name,surname})", mergeNodeProperties = false)]
  )
  @Test
  fun `should create node with composite key`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
      neo4j: Neo4j,
  ) = runTest {
    session.createNodeKeyConstraint(neo4j, "user_id_name", "User", "id", "name")

    producer.publish(
        keySchema = Schema.STRING_SCHEMA,
        key = """{"id": 1, "name": "john"}""",
        valueSchema = Schema.STRING_SCHEMA,
        value = """{"surname": "doe"}""",
    )

    eventually(30.seconds) { session.run("MATCH (n:User) RETURN n", emptyMap()).single() }
        .get("n")
        .asNode() should
        {
          it.labels() shouldBe listOf("User")
          it.asMap() shouldBe mapOf("id" to 1L, "name" to "john", "surname" to "doe")
        }
  }

  @Neo4jSink(
      nodePattern = [NodePatternStrategy(TOPIC, "(:User{!id})", mergeNodeProperties = false)]
  )
  @Test
  fun `should create node with nested properties`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
      neo4j: Neo4j,
  ) = runTest {
    session.createNodeKeyConstraint(neo4j, "user_id", "User", "id")

    producer.publish(
        valueSchema = Schema.STRING_SCHEMA,
        value =
            """{"id": 1, "name": "john", "surname": "doe", "address": {"city": "london", "country": "uk"}}""",
    )

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
                  "address.country" to "uk",
              )
        }
  }

  @Neo4jSink(
      nodePattern =
          [NodePatternStrategy(TOPIC, "(:User{!id, -name, -surname})", mergeNodeProperties = false)]
  )
  @Test
  fun `should create node with excluded properties`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
      neo4j: Neo4j,
  ) = runTest {
    session.createNodeKeyConstraint(neo4j, "user_id", "User", "id")

    producer.publish(
        valueSchema = Schema.STRING_SCHEMA,
        value =
            """{"id": 1, "name": "john", "surname": "doe", "dob": "2000-01-01", "address": {"city": "london", "country": "uk"}}""",
    )

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
                  "address.country" to "uk",
              )
        }
  }

  @Neo4jSink(
      nodePattern =
          [
              NodePatternStrategy(
                  TOPIC,
                  "(:User{!id, created_at: __timestamp})",
                  mergeNodeProperties = false,
              )
          ]
  )
  @Test
  fun `should create node with timestamp`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
      neo4j: Neo4j,
  ) = runTest {
    val timestamp = Instant.now().truncatedTo(ChronoUnit.MILLIS)
    session.createNodeKeyConstraint(neo4j, "user_id", "User", "id")

    producer.publish(
        valueSchema = Schema.STRING_SCHEMA,
        value = """{"id": 1, "name": "john", "surname": "doe"}""",
        timestamp = timestamp,
    )

    eventually(30.seconds) { session.run("MATCH (n:User) RETURN n", emptyMap()).single() }
        .get("n")
        .asNode() should
        {
          println(it.asMap())
          it.labels() shouldBe listOf("User")
          it.asMap() shouldBe mapOf("id" to 1L, "created_at" to timestamp.atZone(ZoneOffset.UTC))
        }
  }

  @Neo4jSink(
      nodePattern =
          [
              NodePatternStrategy(
                  TOPIC,
                  "(:User{!id: __key.old_id, name: __key.first_name, surname: __key.last_name})",
                  mergeNodeProperties = false,
              )
          ]
  )
  @Test
  fun `should create and delete node with explicit properties from message key`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
      neo4j: Neo4j,
  ) = runTest {
    session.createNodeKeyConstraint(neo4j, "user_id", "User", "id")

    producer.publish(
        keySchema = Schema.STRING_SCHEMA,
        key = """{"old_id": 1, "first_name": "john", "last_name": "doe"}""",
        valueSchema = Schema.STRING_SCHEMA,
        value = """{}""",
    )

    eventually(30.seconds) { session.run("MATCH (n:User) RETURN n", emptyMap()).single() }
        .get("n")
        .asNode() should
        {
          it.labels() shouldBe listOf("User")
          it.asMap() shouldBe mapOf("id" to 1L, "name" to "john", "surname" to "doe")
        }

    producer.publish(keySchema = Schema.STRING_SCHEMA, key = """{"old_id": 1}""")

    eventually(30.seconds) {
      session
          .run("MATCH (n:User) RETURN count(n) AS count", emptyMap())
          .single()
          .get("count")
          .asLong() shouldBe 0
    }
  }

  @Neo4jSink(
      nodePattern = [NodePatternStrategy(TOPIC, "(:User{!id})", mergeNodeProperties = false)]
  )
  @Test
  fun `should update node`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
      neo4j: Neo4j,
  ) = runTest {
    session.createNodeKeyConstraint(neo4j, "user_id", "User", "id")

    session
        .run(
            "CREATE (n:User) SET n = ${'$'}props",
            mapOf("props" to mapOf("id" to 1L, "name" to "john", "surname" to "doe")),
        )
        .consume()

    producer.publish(
        keySchema = Schema.STRING_SCHEMA,
        key = """{"id": 1}""",
        valueSchema = Schema.STRING_SCHEMA,
        value = """{"name": "john-updated", "surname": "doe-updated"}""",
    )

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
                  TOPIC,
                  "(:User{!id: old_id, name: first_name, surname: last_name})",
                  mergeNodeProperties = false,
              )
          ]
  )
  @Test
  fun `should update node with aliases`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
      neo4j: Neo4j,
  ) = runTest {
    session.createNodeKeyConstraint(neo4j, "user_id", "User", "id")

    session.run(
        "CREATE (n:User) SET n = ${'$'}props",
        mapOf("props" to mapOf("id" to 1L, "name" to "john", "surname" to "doe")),
    )

    producer.publish(
        keySchema = Schema.STRING_SCHEMA,
        key = """{"old_id": 1}""",
        valueSchema = Schema.STRING_SCHEMA,
        value = """{"first_name": "john-updated", "last_name": "doe-updated"}""",
    )

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
              NodePatternStrategy(TOPIC_1, "(:User{!id})", mergeNodeProperties = false),
              NodePatternStrategy(TOPIC_2, "(:Account{!id})", mergeNodeProperties = false),
          ]
  )
  @Test
  fun `should create nodes from multiple topics`(
      @TopicProducer(TOPIC_1) producer1: ConvertingKafkaProducer,
      @TopicProducer(TOPIC_2) producer2: ConvertingKafkaProducer,
      session: Session,
      neo4j: Neo4j,
  ) = runTest {
    session.createNodeKeyConstraint(neo4j, "user_id", "User", "id")
    session.createNodeKeyConstraint(neo4j, "account_id", "Account", "id")

    producer1.publish(
        keySchema = Schema.STRING_SCHEMA,
        key = """{"id": 1}""",
        valueSchema = Schema.STRING_SCHEMA,
        value = """{"name": "john", "surname": "doe"}""",
    )

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
        value = """{"email": "john@doe.com"}""",
    )

    eventually(30.seconds) { session.run("MATCH (n:Account) RETURN n", emptyMap()).single() }
        .get("n")
        .asNode() should
        {
          it.labels() shouldBe listOf("Account")
          it.asMap() shouldBe mapOf("id" to 1L, "email" to "john@doe.com")
        }
  }

  @Neo4jSink(
      nodePattern = [NodePatternStrategy(TOPIC, "(:User{!id})", mergeNodeProperties = false)]
  )
  @Test
  fun `should create 1000 nodes`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
      neo4j: Neo4j,
  ) = runTest {
    session.createNodeKeyConstraint(neo4j, "user_id", "User", "id")

    val kafkaMessages = mutableListOf<KafkaMessage>()
    for (i in 1..1000) {
      kafkaMessages.add(
          KafkaMessage(
              keySchema = Schema.STRING_SCHEMA,
              key = """{"id": $i}""",
              valueSchema = Schema.STRING_SCHEMA,
              value = """{"name": "john-$i", "surname": "doe-$i"}""",
          )
      )
    }

    producer.publish(*kafkaMessages.toTypedArray())

    eventually(30.seconds) {
      session
          .run("MATCH (n:User) RETURN count(n) AS count", emptyMap())
          .single()
          .get("count")
          .asLong() shouldBe 1000
    }
  }

  @Neo4jSink(nodePattern = [NodePatternStrategy(TOPIC, "(:User{!id})", mergeNodeProperties = true)])
  @Test
  fun `should merge node properties`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
      neo4j: Neo4j,
  ) = runTest {
    session.createNodeKeyConstraint(neo4j, "user_id", "User", "id")
    session
        .run(
            "CREATE (n:User) SET n = ${'$'}props",
            mapOf("props" to mapOf("id" to 1, "name" to "john", "surname" to "doe")),
        )
        .consume()

    producer.publish(
        keySchema = Schema.STRING_SCHEMA,
        key = """{"id": 1}""",
        valueSchema = Schema.STRING_SCHEMA,
        value = """{"born": 1970}""",
    )

    eventually(30.seconds) {
      session.run("MATCH (n:User) RETURN n", emptyMap()).single().get("n").asNode() should
          {
            it.labels() shouldBe listOf("User")
            it.asMap() shouldBe
                mapOf("id" to 1L, "name" to "john", "surname" to "doe", "born" to 1970)
          }
    }
  }

  @Neo4jSink(
      nodePattern = [NodePatternStrategy(TOPIC, "(:User{!id})", mergeNodeProperties = false)]
  )
  @Test
  fun `should not merge node properties`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
      neo4j: Neo4j,
  ) = runTest {
    session.createNodeKeyConstraint(neo4j, "user_id", "User", "id")
    session
        .run(
            "CREATE (n:User) SET n = ${'$'}props",
            mapOf("props" to mapOf("id" to 1, "name" to "john", "surname" to "doe")),
        )
        .consume()

    producer.publish(
        keySchema = Schema.STRING_SCHEMA,
        key = """{"id": 1}""",
        valueSchema = Schema.STRING_SCHEMA,
        value = """{"born": 1970}""",
    )

    eventually(30.seconds) {
      session.run("MATCH (n:User) RETURN n", emptyMap()).single().get("n").asNode() should
          {
            it.labels() shouldBe listOf("User")
            it.asMap() shouldBe mapOf("id" to 1L, "born" to 1970)
          }
    }
  }

  @KeyValueConverter(key = KafkaConverter.AVRO, value = KafkaConverter.AVRO)
  class Neo4jNodePatternAvroIT : Neo4jNodePatternIT()

  @KeyValueConverter(key = KafkaConverter.JSON_SCHEMA, value = KafkaConverter.JSON_SCHEMA)
  class Neo4jNodePatternJsonSchemaIT : Neo4jNodePatternIT()

  @KeyValueConverter(key = KafkaConverter.JSON_EMBEDDED, value = KafkaConverter.JSON_EMBEDDED)
  class Neo4jNodePatternJsonEmbeddedIT : Neo4jNodePatternIT()

  @KeyValueConverter(key = KafkaConverter.PROTOBUF, value = KafkaConverter.PROTOBUF)
  class Neo4jNodePatternProtobufIT : Neo4jNodePatternIT()
}
