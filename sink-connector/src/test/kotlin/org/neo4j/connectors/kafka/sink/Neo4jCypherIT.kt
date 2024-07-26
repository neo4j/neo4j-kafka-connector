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
import io.kotest.assertions.nondeterministic.continually
import io.kotest.assertions.nondeterministic.eventually
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetTime
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit
import java.util.stream.Stream
import kotlin.time.Duration.Companion.seconds
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.ArgumentsProvider
import org.junit.jupiter.params.provider.ArgumentsSource
import org.neo4j.connectors.kafka.data.DynamicTypes
import org.neo4j.connectors.kafka.data.PropertyType
import org.neo4j.connectors.kafka.testing.TestSupport.runTest
import org.neo4j.connectors.kafka.testing.format.KafkaConverter
import org.neo4j.connectors.kafka.testing.format.KeyValueConverter
import org.neo4j.connectors.kafka.testing.kafka.ConvertingKafkaProducer
import org.neo4j.connectors.kafka.testing.sink.CypherStrategy
import org.neo4j.connectors.kafka.testing.sink.Neo4jSink
import org.neo4j.connectors.kafka.testing.sink.SchemaCompatibilityMode
import org.neo4j.connectors.kafka.testing.sink.TopicProducer
import org.neo4j.driver.Session
import org.neo4j.driver.Values

abstract class Neo4jCypherIT {
  companion object {
    private const val TOPIC = "cypher"
    private const val TOPIC_1 = "cypher-1"
    private const val TOPIC_2 = "cypher-2"
  }

  @Neo4jSink(
      cypher =
          [
              CypherStrategy(
                  TOPIC,
                  "CREATE (p:Person) SET p.name = event.firstName, p.surname = event.lastName")])
  @Test
  fun `should create node from struct`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    SchemaBuilder.struct()
        .field("firstName", Schema.STRING_SCHEMA)
        .field("lastName", Schema.STRING_SCHEMA)
        .build()
        .let { schema ->
          producer.publish(
              valueSchema = schema,
              value = Struct(schema).put("firstName", "john").put("lastName", "doe"))
        }

    eventually(30.seconds) { session.run("MATCH (n:Person) RETURN n", emptyMap()).single() }
        .get("n")
        .asNode() should
        {
          it.labels() shouldBe listOf("Person")
          it.asMap() shouldBe mapOf("name" to "john", "surname" to "doe")
        }
  }

  @Neo4jSink(
      cypher =
          [
              CypherStrategy(
                  TOPIC,
                  "CREATE (p:Person) SET p.name = event.firstName, p.surname = event.lastName")])
  @Test
  fun `should create node from json string`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    producer.publish(
        valueSchema = Schema.STRING_SCHEMA,
        value =
            ObjectMapper().writeValueAsString(mapOf("firstName" to "john", "lastName" to "doe")))

    eventually(30.seconds) { session.run("MATCH (n:Person) RETURN n", emptyMap()).single() }
        .get("n")
        .asNode() should
        {
          it.labels() shouldBe listOf("Person")
          it.asMap() shouldBe mapOf("name" to "john", "surname" to "doe")
        }
  }

  @Neo4jSink(
      cypher =
          [
              CypherStrategy(
                  TOPIC,
                  "CREATE (p:Person) SET p.name = event.firstName, p.surname = event.lastName")])
  @Test
  fun `should create node from json byte array`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    producer.publish(
        valueSchema = Schema.BYTES_SCHEMA,
        value = ObjectMapper().writeValueAsBytes(mapOf("firstName" to "john", "lastName" to "doe")))

    eventually(30.seconds) { session.run("MATCH (n:Person) RETURN n", emptyMap()).single() }
        .get("n")
        .asNode() should
        {
          it.labels() shouldBe listOf("Person")
          it.asMap() shouldBe mapOf("name" to "john", "surname" to "doe")
        }
  }

  @Neo4jSink(
      cypher =
          [
              CypherStrategy(
                  TOPIC_1,
                  "CREATE (p:Person) SET p.name = event.firstName, p.surname = event.lastName"),
              CypherStrategy(
                  TOPIC_2, "CREATE (p:Place) SET p.code = event.code, p.name = event.name")])
  @Test
  fun `should create nodes from multiple topics`(
      @TopicProducer(TOPIC_1) producer1: ConvertingKafkaProducer,
      @TopicProducer(TOPIC_2) producer2: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    SchemaBuilder.struct()
        .field("firstName", Schema.STRING_SCHEMA)
        .field("lastName", Schema.STRING_SCHEMA)
        .build()
        .let { schema ->
          producer1.publish(
              valueSchema = schema,
              value = Struct(schema).put("firstName", "john").put("lastName", "doe"))
          producer1.publish(
              valueSchema = schema,
              value = Struct(schema).put("firstName", "mary").put("lastName", "doe"))
          producer1.publish(
              valueSchema = schema,
              value = Struct(schema).put("firstName", "joe").put("lastName", "brown"))
        }

    SchemaBuilder.struct()
        .field("code", Schema.INT32_SCHEMA)
        .field("name", Schema.STRING_SCHEMA)
        .build()
        .let { schema ->
          producer2.publish(
              valueSchema = schema, value = Struct(schema).put("code", 34).put("name", "istanbul"))
          producer2.publish(
              valueSchema = schema, value = Struct(schema).put("code", 48).put("name", "mugla"))
        }

    eventually(30.seconds) {
      session.run("MATCH (n:Person) RETURN n", emptyMap()).list { r ->
        r.get("n").asNode().let { mapOf("labels" to it.labels(), "properties" to it.asMap()) }
      } shouldBe
          listOf(
              mapOf(
                  "labels" to listOf("Person"),
                  "properties" to mapOf("name" to "john", "surname" to "doe")),
              mapOf(
                  "labels" to listOf("Person"),
                  "properties" to mapOf("name" to "mary", "surname" to "doe")),
              mapOf(
                  "labels" to listOf("Person"),
                  "properties" to mapOf("name" to "joe", "surname" to "brown")),
          )

      session.run("MATCH (n:Place) RETURN n", emptyMap()).list { r ->
        r.get("n").asNode().let { mapOf("labels" to it.labels(), "properties" to it.asMap()) }
      } shouldBe
          listOf(
              mapOf(
                  "labels" to listOf("Place"),
                  "properties" to mapOf("code" to 34L, "name" to "istanbul")),
              mapOf(
                  "labels" to listOf("Place"),
                  "properties" to mapOf("code" to 48L, "name" to "mugla")),
          )
    }
  }

  @Neo4jSink(
      cypher = [CypherStrategy(TOPIC, "CREATE (p:Data) SET p.value = event")],
      schemaControlValueCompatibility = SchemaCompatibilityMode.NONE)
  @ParameterizedTest
  @ArgumentsSource(AvroSimpleTypes::class)
  fun `should support connect simple types`(
      schema: Schema,
      value: Any?,
      expected: Any?,
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    producer.publish(valueSchema = schema, value = value)

    eventually(30.seconds) { session.run("MATCH (n:Data) RETURN n", emptyMap()).single() }
        .get("n")
        .asNode() should
        {
          it.labels() shouldBe listOf("Data")
          it.asMap() shouldBe mapOf("value" to expected)
        }
  }

  object AvroSimpleTypes : ArgumentsProvider {
    override fun provideArguments(context: ExtensionContext?): Stream<out Arguments> {
      return Stream.of(
          Arguments.of(Schema.INT8_SCHEMA, Byte.MAX_VALUE, Byte.MAX_VALUE),
          Arguments.of(Schema.INT16_SCHEMA, Short.MAX_VALUE, Short.MAX_VALUE),
          Arguments.of(Schema.INT32_SCHEMA, Int.MAX_VALUE, Int.MAX_VALUE),
          Arguments.of(Schema.INT64_SCHEMA, Long.MAX_VALUE, Long.MAX_VALUE),
          Arguments.of(Schema.FLOAT32_SCHEMA, Float.MAX_VALUE, Float.MAX_VALUE),
          Arguments.of(Schema.FLOAT64_SCHEMA, Double.MAX_VALUE, Double.MAX_VALUE),
          Arguments.of(Schema.BOOLEAN_SCHEMA, true, true),
          Arguments.of(Schema.STRING_SCHEMA, "a string", "a string"),
          Arguments.of(
              Schema.BYTES_SCHEMA, "a string".encodeToByteArray(), "a string".encodeToByteArray()),
          Arguments.of(Schema.OPTIONAL_INT8_SCHEMA, Byte.MAX_VALUE, Byte.MAX_VALUE),
          Arguments.of(Schema.OPTIONAL_INT16_SCHEMA, Short.MAX_VALUE, Short.MAX_VALUE),
          Arguments.of(Schema.OPTIONAL_INT32_SCHEMA, Int.MAX_VALUE, Int.MAX_VALUE),
          Arguments.of(Schema.OPTIONAL_INT64_SCHEMA, Long.MAX_VALUE, Long.MAX_VALUE),
          Arguments.of(Schema.OPTIONAL_FLOAT32_SCHEMA, Float.MAX_VALUE, Float.MAX_VALUE),
          Arguments.of(Schema.OPTIONAL_FLOAT64_SCHEMA, Double.MAX_VALUE, Double.MAX_VALUE),
          Arguments.of(Schema.OPTIONAL_BOOLEAN_SCHEMA, true, true),
          Arguments.of(Schema.OPTIONAL_STRING_SCHEMA, "a string", "a string"),
          Arguments.of(
              Schema.OPTIONAL_BYTES_SCHEMA,
              "a string".encodeToByteArray(),
              "a string".encodeToByteArray()))
    }
  }

  @Neo4jSink(
      cypher = [CypherStrategy(TOPIC, "MATCH (n:Data) SET n.value = event")],
      schemaControlValueCompatibility = SchemaCompatibilityMode.NONE)
  @ParameterizedTest
  @ArgumentsSource(OptionalSchemas::class)
  fun `should support null values`(
      schema: Schema,
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session.run("MERGE (n:Data) SET n.value = 1").consume()

    producer.publish(valueSchema = schema, value = null)

    eventually(30.seconds) {
      session.run("MATCH (n:Data) RETURN n", emptyMap()).single().get("n").asNode() should
          {
            it.labels() shouldBe listOf("Data")
            it.asMap() shouldBe emptyMap()
          }
    }
  }

  object OptionalSchemas : ArgumentsProvider {
    override fun provideArguments(context: ExtensionContext?): Stream<out Arguments> {
      return Stream.of(
          Arguments.of(Schema.OPTIONAL_STRING_SCHEMA),
          Arguments.of(Schema.OPTIONAL_INT8_SCHEMA),
          Arguments.of(Schema.OPTIONAL_INT16_SCHEMA),
          Arguments.of(Schema.OPTIONAL_INT32_SCHEMA),
          Arguments.of(Schema.OPTIONAL_INT64_SCHEMA),
          Arguments.of(Schema.OPTIONAL_FLOAT32_SCHEMA),
          Arguments.of(Schema.OPTIONAL_FLOAT64_SCHEMA),
          Arguments.of(Schema.OPTIONAL_BOOLEAN_SCHEMA),
          Arguments.of(Schema.OPTIONAL_STRING_SCHEMA),
          Arguments.of(Schema.OPTIONAL_BYTES_SCHEMA),
          Arguments.of(PropertyType.schema))
    }
  }

  @Neo4jSink(
      cypher =
          [
              CypherStrategy(
                  TOPIC,
                  """
                  CREATE (n:Data) SET n.value = __value
                  """)])
  @ParameterizedTest
  @ArgumentsSource(KnownTypes::class)
  fun `should support cypher types`(
      schema: Schema,
      value: Any?,
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
  ) = runTest {
    producer.publish(valueSchema = schema, value = DynamicTypes.toConnectValue(schema, value))

    eventually(30.seconds) { session.run("MATCH (n:Data) RETURN n.value", emptyMap()).single() }
        .get(0)
        .asObject() shouldBe value
  }

  object KnownTypes : ArgumentsProvider {
    override fun provideArguments(context: ExtensionContext?): Stream<out Arguments> {
      return Stream.of(
          Arguments.of(PropertyType.schema, true),
          Arguments.of(PropertyType.schema, false),
          Arguments.of(PropertyType.schema, Long.MAX_VALUE),
          Arguments.of(PropertyType.schema, Long.MIN_VALUE),
          Arguments.of(PropertyType.schema, Double.MAX_VALUE),
          Arguments.of(PropertyType.schema, Double.MIN_VALUE),
          Arguments.of(PropertyType.schema, "a string"),
          Arguments.of(PropertyType.schema, "another string"),
          Arguments.of(PropertyType.schema, "a string".encodeToByteArray()),
          Arguments.of(PropertyType.schema, "another string".encodeToByteArray()),
          Arguments.of(PropertyType.schema, LocalDate.of(2019, 5, 1)),
          Arguments.of(PropertyType.schema, LocalDate.of(2019, 5, 1)),
          Arguments.of(PropertyType.schema, LocalDateTime.of(2019, 5, 1, 23, 59, 59, 999999999)),
          Arguments.of(PropertyType.schema, LocalDateTime.of(2019, 5, 1, 23, 59, 59, 999999999)),
          Arguments.of(PropertyType.schema, LocalTime.of(23, 59, 59, 999999999)),
          Arguments.of(PropertyType.schema, LocalTime.of(23, 59, 59, 999999999)),
          Arguments.of(
              PropertyType.schema,
              ZonedDateTime.of(2019, 5, 1, 23, 59, 59, 999999999, ZoneId.of("Europe/Istanbul"))),
          Arguments.of(
              PropertyType.schema,
              ZonedDateTime.of(2019, 5, 1, 23, 59, 59, 999999999, ZoneId.of("Europe/Istanbul"))),
          Arguments.of(
              PropertyType.schema, OffsetTime.of(23, 59, 59, 999999999, ZoneOffset.ofHours(2))),
          Arguments.of(
              PropertyType.schema,
              OffsetTime.of(23, 59, 59, 999999999, ZoneOffset.ofHoursMinutes(2, 30))),
          Arguments.of(PropertyType.schema, Values.isoDuration(5, 4, 3, 2).asIsoDuration()),
          Arguments.of(PropertyType.schema, Values.isoDuration(5, 4, 3, 2).asIsoDuration()),
          Arguments.of(PropertyType.schema, Values.point(7203, 2.3, 4.5).asPoint()),
          Arguments.of(PropertyType.schema, Values.point(7203, 2.3, 4.5).asPoint()),
          Arguments.of(PropertyType.schema, Values.point(4979, 2.3, 4.5, 0.0).asPoint()),
          Arguments.of(PropertyType.schema, Values.point(4979, 2.3, 4.5, 0.0).asPoint()),
      )
    }
  }

  @Neo4jSink(
      cypher =
          [
              CypherStrategy(
                  TOPIC,
                  """
                  UNWIND __value.array AS id
                  CREATE (n:Data) SET n.id = id
                  """)])
  @ParameterizedTest
  @ArgumentsSource(ArrayElementTypes::class)
  fun `should support arrays`(
      elementType: Schema,
      value: Iterable<Any>,
      expected: Iterable<Any>,
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
  ) = runTest {
    SchemaBuilder.array(elementType).build().let { array ->
      // Protobuf does not support top level ARRAY values, so we are wrapping it inside a struct
      SchemaBuilder.struct().field("array", array).build().let { wrapper ->
        producer.publish(valueSchema = wrapper, value = Struct(wrapper).put("array", value))
      }
    }

    eventually(30.seconds) {
      session.run("MATCH (n:Data) RETURN n.id ORDER BY id(n)", emptyMap()).list { r ->
        r.get(0).asObject()
      } shouldContainExactly expected
    }
  }

  object ArrayElementTypes : ArgumentsProvider {
    override fun provideArguments(context: ExtensionContext?): Stream<out Arguments> {
      return Stream.of(
          Arguments.of(
              Schema.INT8_SCHEMA, listOf(1.toByte(), 2.toByte(), 3.toByte()), listOf(1L, 2L, 3L)),
          Arguments.of(
              Schema.INT16_SCHEMA,
              listOf(1.toShort(), 2.toShort(), 3.toShort()),
              listOf(1L, 2L, 3L)),
          Arguments.of(Schema.INT32_SCHEMA, (1..10).toList(), (1L..10L).toList()),
          Arguments.of(Schema.INT64_SCHEMA, (1L..10L).toList(), (1L..10L).toList()),
          Arguments.of(
              Schema.FLOAT32_SCHEMA,
              listOf(1.0f, 1.1f, 1.2f),
              listOf(1.0f.toDouble(), 1.1f.toDouble(), 1.2f.toDouble())),
          Arguments.of(
              Schema.FLOAT64_SCHEMA, listOf(1.0, 1.1, 1.2, 1.3), listOf(1.0, 1.1, 1.2, 1.3)),
          Arguments.of(Schema.BOOLEAN_SCHEMA, listOf(true, false, true), listOf(true, false, true)),
          Arguments.of(
              Schema.STRING_SCHEMA, listOf("a", "b", "c", "d"), listOf("a", "b", "c", "d")))
    }
  }

  @Neo4jSink(
      cypher =
          [
              CypherStrategy(
                  TOPIC,
                  """
                  CREATE (n:Data) SET n = __value.map
                  """)])
  @Test
  fun `should support simple maps`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
  ) = runTest {
    SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build().let { map ->
      // Protobuf does not support top level MAP values, so we are wrapping it inside a struct
      SchemaBuilder.struct().field("map", map).build().let { wrapper ->
        producer.publish(
            valueSchema = wrapper,
            value = Struct(wrapper).put("map", mapOf("firstName" to "john", "lastName" to "doe")))
      }
    }

    eventually(30.seconds) { session.run("MATCH (n:Data) RETURN n", emptyMap()).single() }
        .get(0)
        .asNode()
        .asMap() shouldBe mapOf("firstName" to "john", "lastName" to "doe")
  }

  @Neo4jSink(
      cypher =
          [
              CypherStrategy(
                  TOPIC,
                  """
                  CREATE (n:Data) SET n = __value
                  """)])
  @Test
  fun `should support complex maps`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
  ) = runTest {
    val value =
        mapOf(
            "firstName" to "john",
            "lastName" to "doe",
            "dob" to LocalDate.of(1999, 1, 1),
            "siblings" to 3)
    val schema = DynamicTypes.toConnectSchema(value)

    producer.publish(valueSchema = schema, value = DynamicTypes.toConnectValue(schema, value))

    eventually(30.seconds) { session.run("MATCH (n:Data) RETURN n", emptyMap()).single() }
        .get(0)
        .asNode()
        .asMap() shouldBe value
  }

  @Neo4jSink(
      cypher =
          [
              CypherStrategy(
                  TOPIC,
                  """
                    CREATE (b:Body)
                    WITH __value.p AS paragraphList, __value.ul AS ulList, b
                    FOREACH (paragraph IN paragraphList | CREATE (b)-[:HAS_P]->(p:Paragraph{value: paragraph.value}))
        
                    WITH ulList, b
                    UNWIND ulList AS ulElem
                    CREATE (b)-[:HAS_UL]->(ul:UnorderedList)
        
                    WITH ulElem, ul
                    UNWIND ulElem.value AS liElem
                    CREATE (ul)-[:HAS_LI]->(li:ListItem{value: liElem.value, class: liElem.class})
                  """)])
  @Test
  fun `should support complex tree struct`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
  ) = runTest {
    producer.publish(valueSchema = TreeStruct.schema(), value = TreeStruct.value())

    eventually(30.seconds) {
      session.run("MATCH (n:Body) RETURN count(n)").single().get(0).asInt() shouldBe 1
      session.run("MATCH (n:Paragraph) RETURN count(n)").single().get(0).asInt() shouldBe 2
      session.run("MATCH (n:UnorderedList) RETURN count(n)").single().get(0).asInt() shouldBe 2
      session.run("MATCH (n:ListItem) RETURN count(n)").single().get(0).asInt() shouldBe 4
      session
          .run("MATCH (:Body)-[r:HAS_P]->(:Paragraph) RETURN count(r)")
          .single()
          .get(0)
          .asInt() shouldBe 2
      session
          .run("MATCH (:Body)-[r:HAS_UL]->(:UnorderedList) RETURN count(r)")
          .single()
          .get(0)
          .asInt() shouldBe 2
      session
          .run("MATCH (:UnorderedList)-[r:HAS_LI]->(:ListItem) RETURN count(r)")
          .single()
          .get(0)
          .asInt() shouldBe 4
      session
          .run("MATCH (li:ListItem{class:['ClassA', 'ClassB']}) RETURN count(li)")
          .single()
          .get(0)
          .asInt() shouldBe 1
    }
  }

  object TreeStruct {
    fun schema(): Schema = BODY_SCHEMA

    fun value(): Struct {
      val firstUL =
          Struct(UL_SCHEMA)
              .put(
                  "value",
                  listOf(
                      Struct(LI_SCHEMA).put("value", "First UL - First Element"),
                      Struct(LI_SCHEMA)
                          .put("value", "First UL - Second Element")
                          .put("class", listOf("ClassA", "ClassB"))))
      val secondUL =
          Struct(UL_SCHEMA)
              .put(
                  "value",
                  listOf(
                      Struct(LI_SCHEMA).put("value", "Second UL - First Element"),
                      Struct(LI_SCHEMA).put("value", "Second UL - Second Element")))
      val ulList = listOf(firstUL, secondUL)

      val pList =
          listOf(
              Struct(P_SCHEMA).put("value", "First Paragraph"),
              Struct(P_SCHEMA).put("value", "Second Paragraph"))

      return Struct(BODY_SCHEMA).put("ul", ulList).put("p", pList)
    }

    private val LI_SCHEMA =
        SchemaBuilder.struct()
            .name("org.neo4j.example.html.LI")
            .field("value", Schema.OPTIONAL_STRING_SCHEMA)
            .field("class", SchemaBuilder.array(Schema.STRING_SCHEMA).optional())
            .build()

    private val UL_SCHEMA =
        SchemaBuilder.struct()
            .name("org.neo4j.example.html.UL")
            .field("value", SchemaBuilder.array(LI_SCHEMA))
            .build()

    private val P_SCHEMA =
        SchemaBuilder.struct()
            .name("org.neo4j.example.html.P")
            .field("value", Schema.OPTIONAL_STRING_SCHEMA)
            .build()

    private val BODY_SCHEMA =
        SchemaBuilder.struct()
            .name("org.neo4j.example.html.BODY")
            .field("ul", SchemaBuilder.array(UL_SCHEMA).optional())
            .field("p", SchemaBuilder.array(P_SCHEMA).optional())
            .build()
  }

  @Neo4jSink(
      cypher =
          [
              CypherStrategy(
                  TOPIC,
                  """
                  CREATE (p:Person) SET 
                    p.id = __key,
                    p.name = __value.firstName, p.surname = __value.lastName,
                    p.createdBy = __header.createdBy,
                    p.createdAt = __timestamp
                  """)])
  @Test
  fun `should get message value from default timestamp, header, key and value binding`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    val timestamp = Instant.now().truncatedTo(ChronoUnit.MILLIS)

    producer.publish(
        headers = mapOf("createdBy" to "john-doe"),
        keySchema = Schema.INT64_SCHEMA,
        key = 64L,
        valueSchema = Schema.STRING_SCHEMA,
        value =
            ObjectMapper().writeValueAsString(mapOf("firstName" to "john", "lastName" to "doe")),
        timestamp = timestamp)

    eventually(30.seconds) { session.run("MATCH (n:Person) RETURN n", emptyMap()).single() }
        .get("n")
        .asNode() should
        {
          it.labels() shouldBe listOf("Person")
          it.asMap() shouldBe
              mapOf(
                  "id" to 64L,
                  "name" to "john",
                  "surname" to "doe",
                  "createdBy" to "john-doe",
                  "createdAt" to timestamp.atZone(ZoneOffset.UTC))
        }
  }

  @Neo4jSink(
      cypher =
          [
              CypherStrategy(
                  TOPIC,
                  """
                  CREATE (p:Person) SET 
                    p.id = custom_key,
                    p.name = custom_value.firstName, p.surname = custom_value.lastName,
                    p.createdBy = custom_header.createdBy,
                    p.createdAt = custom_timestamp
                  """,
                  bindTimestampAs = "custom_timestamp",
                  bindHeaderAs = "custom_header",
                  bindKeyAs = "custom_key",
                  bindValueAs = "custom_value")])
  @Test
  fun `should get message value from custom timestamp, header, key and value binding`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    val timestamp = Instant.now().truncatedTo(ChronoUnit.MILLIS)

    producer.publish(
        headers = mapOf("createdBy" to "john-doe"),
        keySchema = Schema.INT64_SCHEMA,
        key = 64L,
        valueSchema = Schema.STRING_SCHEMA,
        value =
            ObjectMapper().writeValueAsString(mapOf("firstName" to "john", "lastName" to "doe")),
        timestamp = timestamp)

    eventually(30.seconds) { session.run("MATCH (n:Person) RETURN n", emptyMap()).single() }
        .get("n")
        .asNode() should
        {
          it.labels() shouldBe listOf("Person")
          it.asMap() shouldBe
              mapOf(
                  "id" to 64L,
                  "name" to "john",
                  "surname" to "doe",
                  "createdBy" to "john-doe",
                  "createdAt" to timestamp.atZone(ZoneOffset.UTC))
        }
  }

  @Neo4jSink(cypher = [CypherStrategy(TOPIC, "Invalid Cypher Statement")])
  @Test
  fun `should not create any data when invalid cypher is provided`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    producer.publish(valueSchema = Schema.STRING_SCHEMA, value = "a value")

    continually(30.seconds) {
      session.run("MATCH (n) RETURN count(n)", emptyMap()).single().get(0).asInt() shouldBe 0
    }
  }

  @KeyValueConverter(key = KafkaConverter.AVRO, value = KafkaConverter.AVRO)
  class Neo4jCypherAvroIT : Neo4jCypherIT()

  @KeyValueConverter(key = KafkaConverter.JSON_SCHEMA, value = KafkaConverter.JSON_SCHEMA)
  class Neo4jCypherJsonIT : Neo4jCypherIT()

  @KeyValueConverter(key = KafkaConverter.PROTOBUF, value = KafkaConverter.PROTOBUF)
  class Neo4jCypherProtobufIT : Neo4jCypherIT()
}
