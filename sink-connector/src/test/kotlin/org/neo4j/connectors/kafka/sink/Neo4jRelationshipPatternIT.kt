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
import io.kotest.matchers.collections.shouldContainAll
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import java.math.BigDecimal
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import kotlin.time.Duration.Companion.seconds
import org.apache.kafka.connect.data.Date
import org.apache.kafka.connect.data.Decimal
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.data.Time
import org.apache.kafka.connect.data.Timestamp
import org.junit.jupiter.api.Test
import org.neo4j.connectors.kafka.data.DynamicTypes
import org.neo4j.connectors.kafka.data.PropertyType
import org.neo4j.connectors.kafka.data.PropertyType.schema
import org.neo4j.connectors.kafka.testing.DateSupport
import org.neo4j.connectors.kafka.testing.TestSupport.runTest
import org.neo4j.connectors.kafka.testing.format.KafkaConverter
import org.neo4j.connectors.kafka.testing.format.KeyValueConverter
import org.neo4j.connectors.kafka.testing.kafka.ConvertingKafkaProducer
import org.neo4j.connectors.kafka.testing.kafka.KafkaMessage
import org.neo4j.connectors.kafka.testing.sink.Neo4jSink
import org.neo4j.connectors.kafka.testing.sink.RelationshipPatternStrategy
import org.neo4j.connectors.kafka.testing.sink.TopicProducer
import org.neo4j.driver.Session
import org.neo4j.driver.Values

abstract class Neo4jRelationshipPatternIT {

  companion object {
    const val TOPIC = "test"
    const val TOPIC_1 = "test-1"
    const val TOPIC_2 = "test-2"
  }

  @Neo4jSink(
      relationshipPattern =
          [
              RelationshipPatternStrategy(
                  TOPIC,
                  "(:User{!userId})-[:BOUGHT]->(:Product{!productId})",
                  mergeNodeProperties = false,
                  mergeRelationshipProperties = false)])
  @Test
  fun `should create nodes and relationship from struct`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session.run("CREATE CONSTRAINT FOR (n:User) REQUIRE n.userId IS KEY").consume()
    session.run("CREATE CONSTRAINT FOR (n:Product) REQUIRE n.productId IS KEY").consume()

    SchemaBuilder.struct()
        .field("userId", Schema.INT64_SCHEMA)
        .field("productId", Schema.INT64_SCHEMA)
        .field("at", PropertyType.schema)
        .field("place", PropertyType.schema)
        .build()
        .let { schema ->
          producer.publish(
              valueSchema = schema,
              value =
                  Struct(schema)
                      .put("userId", 1L)
                      .put("productId", 2L)
                      .put(
                          "at",
                          DynamicTypes.toConnectValue(
                              PropertyType.schema, LocalDate.of(1995, 1, 1)))
                      .put(
                          "place",
                          DynamicTypes.toConnectValue(
                              PropertyType.schema, Values.point(7203, 1.0, 2.5).asPoint())))
        }

    eventually(30.seconds) {
      val result =
          session.run("MATCH (u:User)-[r:BOUGHT]->(p:Product) RETURN u, r, p", emptyMap()).single()

      result.get("u").asNode() should
          {
            it.labels() shouldBe listOf("User")
            it.asMap() shouldBe mapOf("userId" to 1L)
          }

      result.get("r").asRelationship() should
          {
            it.type() shouldBe "BOUGHT"
            it.asMap() shouldBe
                mapOf(
                    "at" to LocalDate.of(1995, 1, 1),
                    "place" to Values.point(7203, 1.0, 2.5).asPoint())
          }

      result.get("p").asNode() should
          {
            it.labels() shouldBe listOf("Product")
            it.asMap() shouldBe mapOf("productId" to 2L)
          }
    }
  }

  @Neo4jSink(
      relationshipPattern =
          [
              RelationshipPatternStrategy(
                  TOPIC,
                  "(:Person{!id: p1Id})-[:GAVE_BIRTH]->(:Person{!id: p2Id,height,dob,tob})",
                  mergeNodeProperties = false,
                  mergeRelationshipProperties = false)])
  @Test
  fun `should create nodes and relationship from struct containing connect types`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session.run("CREATE CONSTRAINT FOR (n:Person) REQUIRE n.id IS KEY").consume()

    SchemaBuilder.struct()
        .field("p1Id", Schema.INT64_SCHEMA)
        .field("p2Id", Schema.INT64_SCHEMA)
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
                      .put("p1Id", 1L)
                      .put("p2Id", 2L)
                      .put("height", BigDecimal.valueOf(185, 2))
                      .put("dob", DateSupport.date(1978, 1, 15))
                      .put("tob", DateSupport.time(7, 45, 12, 999))
                      .put("tsob", DateSupport.timestamp(1978, 1, 15, 7, 45, 12, 999)))
        }

    eventually(30.seconds) {
      val result =
          session
              .run("MATCH (p1:Person)-[b:GAVE_BIRTH]->(p2:Person) RETURN p1, b, p2", emptyMap())
              .single()

      result.get("p1").asNode() should
          {
            it.labels() shouldBe listOf("Person")
            it.asMap() shouldBe mapOf("id" to 1L)
          }

      result.get("b").asRelationship() should
          {
            it.type() shouldBe "GAVE_BIRTH"
            it.asMap() shouldBe mapOf("tsob" to LocalDateTime.of(1978, 1, 15, 7, 45, 12, 999000000))
          }

      result.get("p2").asNode() should
          {
            it.labels() shouldBe listOf("Person")
            it.asMap() shouldBe
                mapOf(
                    "id" to 2L,
                    "height" to "1.85",
                    "dob" to LocalDate.of(1978, 1, 15),
                    "tob" to LocalTime.of(7, 45, 12, 999000000),
                )
          }
    }
  }

  @Neo4jSink(
      relationshipPattern =
          [
              RelationshipPatternStrategy(
                  TOPIC,
                  "(:User{!userId})-[:BOUGHT]->(:Product{!productId})",
                  mergeNodeProperties = false,
                  mergeRelationshipProperties = false)])
  @Test
  fun `should create nodes and relationship from json string`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session.run("CREATE CONSTRAINT FOR (n:User) REQUIRE n.userId IS KEY").consume()
    session.run("CREATE CONSTRAINT FOR (n:Product) REQUIRE n.productId IS KEY").consume()

    producer.publish(
        valueSchema = Schema.STRING_SCHEMA, value = """{"userId": 1, "productId": 2}""")

    eventually(30.seconds) {
      val result =
          session.run("MATCH (u:User)-[r:BOUGHT]->(p:Product) RETURN u, r, p", emptyMap()).single()

      result.get("u").asNode() should
          {
            it.labels() shouldBe listOf("User")
            it.asMap() shouldBe mapOf("userId" to 1L)
          }

      result.get("r").asRelationship() should { it.type() shouldBe "BOUGHT" }

      result.get("p").asNode() should
          {
            it.labels() shouldBe listOf("Product")
            it.asMap() shouldBe mapOf("productId" to 2L)
          }
    }
  }

  @Neo4jSink(
      relationshipPattern =
          [
              RelationshipPatternStrategy(
                  TOPIC,
                  "(:User{!userId})-[:BOUGHT]->(:Product{!productId})",
                  mergeNodeProperties = false,
                  mergeRelationshipProperties = false)])
  @Test
  fun `should create nodes and relationship from byte array`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session.run("CREATE CONSTRAINT FOR (n:User) REQUIRE n.userId IS KEY").consume()
    session.run("CREATE CONSTRAINT FOR (n:Product) REQUIRE n.productId IS KEY").consume()

    producer.publish(
        valueSchema = Schema.BYTES_SCHEMA,
        value = ObjectMapper().writeValueAsBytes(mapOf("userId" to 1L, "productId" to 2L)))

    eventually(30.seconds) {
      val result =
          session.run("MATCH (u:User)-[r:BOUGHT]->(p:Product) RETURN u, r, p", emptyMap()).single()

      result.get("u").asNode() should
          {
            it.labels() shouldBe listOf("User")
            it.asMap() shouldBe mapOf("userId" to 1L)
          }

      result.get("r").asRelationship() should { it.type() shouldBe "BOUGHT" }

      result.get("p").asNode() should
          {
            it.labels() shouldBe listOf("Product")
            it.asMap() shouldBe mapOf("productId" to 2L)
          }
    }
  }

  @Neo4jSink(
      relationshipPattern =
          [
              RelationshipPatternStrategy(
                  TOPIC,
                  "(:User{!userId})-[:BOUGHT{-currency}]->(:Product{!productId})",
                  mergeNodeProperties = false,
                  mergeRelationshipProperties = false)])
  @Test
  fun `should create nodes and relationship with excluded properties`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session.run("CREATE CONSTRAINT FOR (n:User) REQUIRE n.userId IS KEY").consume()
    session.run("CREATE CONSTRAINT FOR (n:Product) REQUIRE n.productId IS KEY").consume()

    producer.publish(
        valueSchema = Schema.BYTES_SCHEMA,
        value =
            ObjectMapper()
                .writeValueAsBytes(
                    mapOf(
                        "userId" to 1L,
                        "productId" to 2L,
                        "amount" to 5,
                        "currency" to "EUR",
                    )))

    eventually(30.seconds) {
      val result =
          session.run("MATCH (u:User)-[r:BOUGHT]->(p:Product) RETURN u, r, p", emptyMap()).single()

      result.get("u").asNode() should
          {
            it.labels() shouldBe listOf("User")
            it.asMap() shouldBe
                mapOf(
                    "userId" to 1L,
                )
          }

      result.get("r").asRelationship() should
          {
            it.type() shouldBe "BOUGHT"
            it.asMap() shouldBe mapOf("amount" to 5)
          }

      result.get("p").asNode() should
          {
            it.labels() shouldBe listOf("Product")
            it.asMap() shouldBe
                mapOf(
                    "productId" to 2L,
                )
          }
    }
  }

  @Neo4jSink(
      relationshipPattern =
          [
              RelationshipPatternStrategy(
                  TOPIC,
                  "User{!userId} BOUGHT Product{!productId}",
                  mergeNodeProperties = false,
                  mergeRelationshipProperties = false)])
  @Test
  fun `should create nodes and relationship with simpler pattern`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session.run("CREATE CONSTRAINT FOR (n:User) REQUIRE n.userId IS KEY").consume()
    session.run("CREATE CONSTRAINT FOR (n:Product) REQUIRE n.productId IS KEY").consume()

    producer.publish(
        valueSchema = Schema.BYTES_SCHEMA,
        value = ObjectMapper().writeValueAsBytes(mapOf("userId" to 1L, "productId" to 2L)))

    eventually(30.seconds) {
      val result =
          session.run("MATCH (u:User)-[r:BOUGHT]->(p:Product) RETURN u, r, p", emptyMap()).single()

      result.get("u").asNode() should
          {
            it.labels() shouldBe listOf("User")
            it.asMap() shouldBe mapOf("userId" to 1L)
          }

      result.get("r").asRelationship() should { it.type() shouldBe "BOUGHT" }

      result.get("p").asNode() should
          {
            it.labels() shouldBe listOf("Product")
            it.asMap() shouldBe mapOf("productId" to 2L)
          }
    }
  }

  @Neo4jSink(
      relationshipPattern =
          [
              RelationshipPatternStrategy(
                  TOPIC,
                  "(:User{!id: userId})-[:BOUGHT {!id: transactionId}]->(:Product{!id: productId})",
                  mergeNodeProperties = false,
                  mergeRelationshipProperties = false)])
  @Test
  fun `should create nodes and relationship with aliased properties`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session.run("CREATE CONSTRAINT FOR (n:User) REQUIRE n.id IS KEY").consume()
    session.run("CREATE CONSTRAINT FOR (n:Product) REQUIRE n.id IS KEY").consume()

    producer.publish(
        valueSchema = Schema.STRING_SCHEMA,
        value = """{"userId": 1, "productId": 2, "transactionId": 3}""")

    eventually(30.seconds) {
      val result =
          session.run("MATCH (u:User)-[r:BOUGHT]->(p:Product) RETURN u, r, p", emptyMap()).single()

      result.get("u").asNode() should
          {
            it.labels() shouldBe listOf("User")
            it.asMap() shouldBe mapOf("id" to 1L)
          }

      result.get("r").asRelationship() should { it.type() shouldBe "BOUGHT" }

      result.get("p").asNode() should
          {
            it.labels() shouldBe listOf("Product")
            it.asMap() shouldBe mapOf("id" to 2L)
          }
    }
  }

  @Neo4jSink(
      relationshipPattern =
          [
              RelationshipPatternStrategy(
                  TOPIC,
                  "(:User{!id: userId})-[:BOUGHT{!id: transactionId, price, currency}]->(:Product{!id: productId})",
                  mergeNodeProperties = false,
                  mergeRelationshipProperties = false)])
  @Test
  fun `should create nodes and relationship with relationship key and properties`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session.run("CREATE CONSTRAINT FOR (n:User) REQUIRE n.id IS KEY").consume()
    session.run("CREATE CONSTRAINT FOR (n:Product) REQUIRE n.id IS KEY").consume()

    producer.publish(
        keySchema = Schema.STRING_SCHEMA,
        key = """{"userId": 1, "productId": 2, "transactionId": 3}""}""",
        valueSchema = Schema.STRING_SCHEMA,
        value = """{"price": 10.5, "currency": "EUR"}""")

    eventually(30.seconds) {
      val result =
          session.run("MATCH (u:User)-[r:BOUGHT]->(p:Product) RETURN u, r, p", emptyMap()).single()

      result.get("u").asNode() should
          {
            it.labels() shouldBe listOf("User")
            it.asMap() shouldBe mapOf("id" to 1L)
          }

      result.get("r").asRelationship() should
          {
            it.type() shouldBe "BOUGHT"
            it.asMap() shouldBe mapOf("id" to 3L, "price" to 10.5, "currency" to "EUR")
          }

      result.get("p").asNode() should
          {
            it.labels() shouldBe listOf("Product")
            it.asMap() shouldBe mapOf("id" to 2L)
          }
    }
  }

  @Neo4jSink(
      relationshipPattern =
          [
              RelationshipPatternStrategy(
                  TOPIC,
                  """(:User{!id: __key.user_id, name: __value.first_name})-[:BOUGHT{!id: __key.transaction_id, price: __value.paid_price, currency}]->(:Product{!id: __key.product_id, name: __value.product_name})""",
                  mergeNodeProperties = false,
                  mergeRelationshipProperties = false)])
  @Test
  fun `should create nodes and relationship with explicit properties`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session.run("CREATE CONSTRAINT FOR (n:User) REQUIRE n.id IS KEY").consume()
    session.run("CREATE CONSTRAINT FOR (n:Product) REQUIRE n.id IS KEY").consume()

    producer.publish(
        keySchema = Schema.STRING_SCHEMA,
        key = """{"user_id": 1, "product_id": 2, "transaction_id": 3}""}""",
        valueSchema = Schema.STRING_SCHEMA,
        value =
            """{"first_name": "John", "paid_price": 10.5, "currency": "EUR", "product_name": "computer"}""")

    eventually(30.seconds) {
      val result =
          session.run("MATCH (u:User)-[r:BOUGHT]->(p:Product) RETURN u, r, p", emptyMap()).single()

      result.get("u").asNode() should
          {
            it.labels() shouldBe listOf("User")
            it.asMap() shouldBe mapOf("id" to 1L, "name" to "John")
          }

      result.get("r").asRelationship() should
          {
            it.type() shouldBe "BOUGHT"
            it.asMap() shouldBe
                mapOf(
                    "id" to 3L,
                    "price" to 10.5,
                    "currency" to "EUR",
                )
          }

      result.get("p").asNode() should
          {
            it.labels() shouldBe listOf("Product")
            it.asMap() shouldBe mapOf("id" to 2L, "name" to "computer")
          }
    }
  }

  @Neo4jSink(
      relationshipPattern =
          [
              RelationshipPatternStrategy(
                  TOPIC,
                  "(:User:Person{!id: userId})-[:BOUGHT]->(:Product:Item{!id: productId})",
                  mergeNodeProperties = false,
                  mergeRelationshipProperties = false)])
  @Test
  fun `should create nodes and relationship with multiple labels pattern`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session.run("CREATE CONSTRAINT FOR (n:User) REQUIRE n.id IS KEY").consume()
    session.run("CREATE CONSTRAINT FOR (n:Person) REQUIRE n.id IS KEY").consume()
    session.run("CREATE CONSTRAINT FOR (n:Product) REQUIRE n.id IS KEY").consume()
    session.run("CREATE CONSTRAINT FOR (n:Item) REQUIRE n.id IS KEY").consume()

    producer.publish(
        valueSchema = Schema.STRING_SCHEMA, value = """{"userId": 1, "productId": 2}""")

    eventually(30.seconds) {
      val result =
          session.run("MATCH (u:User)-[r:BOUGHT]->(p:Product) RETURN u, r, p", emptyMap()).single()

      result.get("u").asNode() should
          {
            it.labels() shouldBe listOf("User", "Person")
            it.asMap() shouldBe mapOf("id" to 1L)
          }

      result.get("r").asRelationship() should { it.type() shouldBe "BOUGHT" }

      result.get("p").asNode() should
          {
            it.labels() shouldBe listOf("Product", "Item")
            it.asMap() shouldBe mapOf("id" to 2L)
          }
    }
  }

  @Neo4jSink(
      relationshipPattern =
          [
              RelationshipPatternStrategy(
                  TOPIC,
                  "(:User{!id: userId})-[:BOUGHT]->(:Product{!id: productId})",
                  mergeNodeProperties = false,
                  mergeRelationshipProperties = false)])
  @Test
  fun `should add non id values to relationship`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session.run("CREATE CONSTRAINT FOR (n:User) REQUIRE n.id IS KEY").consume()
    session.run("CREATE CONSTRAINT FOR (n:Product) REQUIRE n.id IS KEY").consume()

    producer.publish(
        valueSchema = Schema.STRING_SCHEMA,
        value = """{"userId": 1, "productId": 2, "price": 10, "currency": "EUR"}""")

    eventually(30.seconds) {
      val result =
          session.run("MATCH (u:User)-[r:BOUGHT]->(p:Product) RETURN u, r, p", emptyMap()).single()

      result.get("u").asNode() should
          {
            it.labels() shouldBe listOf("User")
            it.asMap() shouldBe mapOf("id" to 1L)
          }

      result.get("r").asRelationship() should
          {
            it.type() shouldBe "BOUGHT"
            it.asMap() shouldBe mapOf("price" to 10, "currency" to "EUR")
          }

      result.get("p").asNode() should
          {
            it.labels() shouldBe listOf("Product")
            it.asMap() shouldBe mapOf("id" to 2L)
          }
    }
  }

  @Neo4jSink(
      relationshipPattern =
          [
              RelationshipPatternStrategy(
                  TOPIC,
                  "(:User{!id: userId})-[:BOUGHT]->(:Product{!id: productId})",
                  mergeNodeProperties = false,
                  mergeRelationshipProperties = false)])
  @Test
  fun `should delete relationship`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session.run("CREATE CONSTRAINT FOR (n:User) REQUIRE n.id IS KEY").consume()
    session.run("CREATE CONSTRAINT FOR (n:Product) REQUIRE n.id IS KEY").consume()

    session
        .run(
            """CREATE (u:User) SET u.id = 1 
           CREATE (p:Product) SET p.id = 2 
           MERGE (u)-[:BOUGHT]->(p)""")
        .consume()

    producer.publish(keySchema = Schema.STRING_SCHEMA, key = """{"userId": 1, "productId": 2}""")

    eventually(30.seconds) {
      session
          .run(
              "MATCH (:User {id: 1})-[r:BOUGHT]->(:Product {id: 2}) RETURN count(r) as count",
              emptyMap())
          .single()
          .get("count")
          .asLong() shouldBe 0
    }
  }

  @Neo4jSink(
      relationshipPattern =
          [
              RelationshipPatternStrategy(
                  TOPIC,
                  "(:User{!id: userId})-[:BOUGHT{price, currency}]->(:Product{!id: productId})",
                  mergeNodeProperties = false,
                  mergeRelationshipProperties = false)])
  @Test
  fun `should add only relationship`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session.run("CREATE CONSTRAINT FOR (n:User) REQUIRE n.id IS KEY").consume()
    session.run("CREATE CONSTRAINT FOR (n:Product) REQUIRE n.id IS KEY").consume()

    session
        .run("""CREATE (u:User) SET u.id = 1 
           CREATE (p:Product) SET p.id = 2""")
        .consume()

    producer.publish(
        valueSchema = Schema.STRING_SCHEMA,
        value = """{"userId": 1, "productId": 2, "price": 10.5, "currency": "EUR"}""")

    eventually(30.seconds) {
          session
              .run("MATCH (:User {id: 1})-[r:BOUGHT]->(:Product {id: 2}) RETURN r ", emptyMap())
              .single()
        }
        .get("r")
        .asRelationship() should
        {
          it.type() shouldBe "BOUGHT"
          it.asMap() shouldBe mapOf("price" to 10.5, "currency" to "EUR")
        }
  }

  @Neo4jSink(
      relationshipPattern =
          [
              RelationshipPatternStrategy(
                  TOPIC,
                  "(:User{!id: user_id, name: user_name})-[:BOUGHT {amount}]->(:Product{!id: product_id, name: product_name})",
                  mergeNodeProperties = false,
                  mergeRelationshipProperties = false)])
  @Test
  fun `should create, delete and recreate relationship`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session.run("CREATE CONSTRAINT FOR (n:User) REQUIRE n.id IS KEY").consume()
    session.run("CREATE CONSTRAINT FOR (n:Product) REQUIRE n.id IS KEY").consume()

    producer.publish(
        KafkaMessage(
            keySchema = Schema.STRING_SCHEMA,
            key = """{"user_id": 1, "product_id": 2}""",
            valueSchema = Schema.STRING_SCHEMA,
            value = """{"user_name": "John", "amount": 1, "product_name": "computer"}"""),
        KafkaMessage(keySchema = Schema.STRING_SCHEMA, key = """{"user_id": 1, "product_id": 2}"""),
        KafkaMessage(
            keySchema = Schema.STRING_SCHEMA,
            key = """{"user_id": 1, "product_id": 2}""",
            valueSchema = Schema.STRING_SCHEMA,
            value = """{"user_name": "John-new", "amount": 5, "product_name": "computer-new"}"""))

    eventually(30.seconds) {
      val result =
          session
              .run(
                  "MATCH (u:User {id: 1})-[r:BOUGHT]->(p:Product {id: 2}) RETURN u, r, p",
                  emptyMap(),
              )
              .single()

      result.get("u").asNode() should
          {
            it.labels() shouldBe listOf("User")
            it.asMap() shouldBe mapOf("id" to 1L, "name" to "John-new")
          }

      result.get("r").asRelationship() should
          {
            it.type() shouldBe "BOUGHT"
            it.asMap() shouldBe mapOf("amount" to 5)
          }

      result.get("p").asNode() should
          {
            it.labels() shouldBe listOf("Product")
            it.asMap() shouldBe mapOf("id" to 2, "name" to "computer-new")
          }
    }
  }

  @Neo4jSink(
      relationshipPattern =
          [
              RelationshipPatternStrategy(
                  TOPIC_1,
                  "(:User{!id: userId})-[:BOUGHT]->(:Product{!id: productId})",
                  mergeNodeProperties = false,
                  mergeRelationshipProperties = false),
              RelationshipPatternStrategy(
                  TOPIC_2,
                  "(:User{!id: userId})-[:SOLD]->(:Product{!id: productId})",
                  mergeNodeProperties = false,
                  mergeRelationshipProperties = false)])
  @Test
  fun `should create multiple relationships from different topics`(
      @TopicProducer(TOPIC_1) producer1: ConvertingKafkaProducer,
      @TopicProducer(TOPIC_2) producer2: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session.run("CREATE CONSTRAINT FOR (n:User) REQUIRE n.id IS KEY").consume()
    session.run("CREATE CONSTRAINT FOR (n:Product) REQUIRE n.id IS KEY").consume()

    producer1.publish(
        valueSchema = Schema.STRING_SCHEMA, value = """{"userId": 1, "productId": 2}""")

    producer2.publish(
        valueSchema = Schema.STRING_SCHEMA, value = """{"userId": 1, "productId": 2}""")

    eventually(30.seconds) {
      session
          .run(
              "MATCH (u:User {id: 1})-[r]->(p:Product {id: 2}) RETURN r",
              emptyMap(),
          )
          .list()
          .map { it.get("r").asRelationship().type() } shouldContainAll listOf("BOUGHT", "SOLD")
    }
  }

  @Neo4jSink(
      relationshipPattern =
          [
              RelationshipPatternStrategy(
                  TOPIC,
                  "(:User{!id: userId})-[:BOUGHT {!id: transactionId}]->(:Product{!id: productId})",
                  mergeNodeProperties = false,
                  mergeRelationshipProperties = false)])
  @Test
  fun `should create 1000 relationships`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session.run("CREATE CONSTRAINT FOR (n:User) REQUIRE n.id IS KEY").consume()
    session.run("CREATE CONSTRAINT FOR (n:Product) REQUIRE n.id IS KEY").consume()

    val kafkaMessages = mutableListOf<KafkaMessage>()
    for (i in 1..1000) {
      kafkaMessages.add(
          KafkaMessage(
              valueSchema = Schema.STRING_SCHEMA,
              value = """{"userId": 1, "productId": 2, "transactionId": $i}"""))
    }

    producer.publish(*kafkaMessages.toTypedArray())

    eventually(30.seconds) {
      session
          .run(
              "MATCH (u:User {id: 1})-[r]->(p:Product {id: 2}) RETURN r",
              emptyMap(),
          )
          .list() shouldHaveSize 1000
    }
  }

  @Neo4jSink(
      relationshipPattern =
          [
              RelationshipPatternStrategy(
                  TOPIC,
                  "(:User{!id: userId, born})-[:BOUGHT]->(:Product{!id: productId, price})",
                  mergeNodeProperties = true,
                  mergeRelationshipProperties = false)])
  @Test
  fun `should merge node properties`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session.run("CREATE CONSTRAINT FOR (n:User) REQUIRE n.id IS KEY").consume()
    session.run("CREATE CONSTRAINT FOR (n:Product) REQUIRE n.id IS KEY").consume()

    session
        .run(
            """CREATE (:User {id: 1, name: "Joe", surname: "Doe"})-[:BOUGHT]->(:Product {id: 2, name: "computer"})""")
        .consume()

    producer.publish(
        KafkaMessage(
            valueSchema = Schema.STRING_SCHEMA,
            value = """{"userId": 1, "productId": 2, "born": 1970, "price": 10.5}"""))

    eventually(30.seconds) {
      val result =
          session
              .run(
                  "MATCH (u:User {id: 1})-[:BOUGHT]->(p:Product {id: 2}) RETURN u, p",
                  emptyMap(),
              )
              .single()

      result.get("u").asNode() should
          {
            it.labels() shouldBe listOf("User")
            it.asMap() shouldBe
                mapOf("id" to 1, "name" to "Joe", "surname" to "Doe", "born" to 1970)
          }

      result.get("p").asNode() should
          {
            it.labels() shouldBe listOf("Product")
            it.asMap() shouldBe mapOf("id" to 2, "name" to "computer", "price" to 10.5)
          }
    }
  }

  @Neo4jSink(
      relationshipPattern =
          [
              RelationshipPatternStrategy(
                  TOPIC,
                  "(:User{!id: userId, born})-[:BOUGHT]->(:Product{!id: productId, price})",
                  mergeNodeProperties = false,
                  mergeRelationshipProperties = false)])
  @Test
  fun `should not merge node properties`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session.run("CREATE CONSTRAINT FOR (n:User) REQUIRE n.id IS KEY").consume()
    session.run("CREATE CONSTRAINT FOR (n:Product) REQUIRE n.id IS KEY").consume()

    session
        .run(
            """CREATE (:User {id: 1, name: "Joe", surname: "Doe"})-[:BOUGHT]->(:Product {id: 2, name: "computer"})""")
        .consume()

    producer.publish(
        KafkaMessage(
            valueSchema = Schema.STRING_SCHEMA,
            value = """{"userId": 1, "productId": 2, "born": 1970, "price": 10.5}"""))

    eventually(30.seconds) {
      val result =
          session
              .run(
                  "MATCH (u:User {id: 1})-[:BOUGHT]->(p:Product {id: 2}) RETURN u, p",
                  emptyMap(),
              )
              .single()

      result.get("u").asNode() should
          {
            it.labels() shouldBe listOf("User")
            it.asMap() shouldBe mapOf("id" to 1, "born" to 1970)
          }

      result.get("p").asNode() should
          {
            it.labels() shouldBe listOf("Product")
            it.asMap() shouldBe mapOf("id" to 2, "price" to 10.5)
          }
    }
  }

  @Neo4jSink(
      relationshipPattern =
          [
              RelationshipPatternStrategy(
                  TOPIC,
                  "(:User{!id: userId})-[:BOUGHT]->(:Product{!id: productId})",
                  mergeNodeProperties = false,
                  mergeRelationshipProperties = true)])
  @Test
  fun `should merge relationship properties`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session.run("CREATE CONSTRAINT FOR (n:User) REQUIRE n.id IS KEY").consume()
    session.run("CREATE CONSTRAINT FOR (n:Product) REQUIRE n.id IS KEY").consume()

    session.run("""CREATE (:User {id: 1})-[:BOUGHT {amount: 10}]->(:Product {id: 2})""").consume()

    producer.publish(
        KafkaMessage(
            valueSchema = Schema.STRING_SCHEMA,
            value = """{"userId": 1, "productId": 2, "date": "2024-05-27"}"""))

    eventually(30.seconds) {
      val result =
          session
              .run(
                  "MATCH (:User {id: 1})-[r:BOUGHT]->(:Product {id: 2}) RETURN r",
                  emptyMap(),
              )
              .single()

      result.get("r").asRelationship() should
          {
            it.type() shouldBe "BOUGHT"
            it.asMap() shouldBe mapOf("amount" to 10, "date" to "2024-05-27")
          }
    }
  }

  @Neo4jSink(
      relationshipPattern =
          [
              RelationshipPatternStrategy(
                  TOPIC,
                  "(:User{!id: userId})-[:BOUGHT]->(:Product{!id: productId})",
                  mergeNodeProperties = false,
                  mergeRelationshipProperties = false)])
  @Test
  fun `should not merge relationship properties`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session.run("CREATE CONSTRAINT FOR (n:User) REQUIRE n.id IS KEY").consume()
    session.run("CREATE CONSTRAINT FOR (n:Product) REQUIRE n.id IS KEY").consume()

    session.run("""CREATE (:User {id: 1})-[:BOUGHT {amount: 10}]->(:Product {id: 2})""").consume()

    producer.publish(
        KafkaMessage(
            valueSchema = Schema.STRING_SCHEMA,
            value = """{"userId": 1, "productId": 2, "date": "2024-05-27"}"""))

    eventually(30.seconds) {
      val result =
          session
              .run(
                  "MATCH (:User {id: 1})-[r:BOUGHT]->(:Product {id: 2}) RETURN r",
                  emptyMap(),
              )
              .single()

      result.get("r").asRelationship() should
          {
            it.type() shouldBe "BOUGHT"
            it.asMap() shouldBe mapOf("date" to "2024-05-27")
          }
    }
  }

  @KeyValueConverter(key = KafkaConverter.AVRO, value = KafkaConverter.AVRO)
  class Neo4jRelationshipPatternAvroIT : Neo4jRelationshipPatternIT()

  @KeyValueConverter(key = KafkaConverter.JSON_SCHEMA, value = KafkaConverter.JSON_SCHEMA)
  class Neo4jRelationshipPatternJsonSchemaIT : Neo4jRelationshipPatternIT()

  @KeyValueConverter(key = KafkaConverter.JSON_EMBEDDED, value = KafkaConverter.JSON_EMBEDDED)
  class Neo4jRelationshipPatternJsonEmbeddedIT : Neo4jRelationshipPatternIT()

  @KeyValueConverter(key = KafkaConverter.PROTOBUF, value = KafkaConverter.PROTOBUF)
  class Neo4jRelationshipPatternProtobufIT : Neo4jRelationshipPatternIT()
}
