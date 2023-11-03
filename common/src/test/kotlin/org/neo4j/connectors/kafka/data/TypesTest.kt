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
package org.neo4j.connectors.kafka.data

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.assertions.withClue
import io.kotest.matchers.shouldBe
import io.kotest.matchers.throwable.shouldHaveMessage
import java.nio.ByteBuffer
import java.time.Duration
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.OffsetTime
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util.function.Function
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.junit.jupiter.api.Test
import org.neo4j.driver.Value
import org.neo4j.driver.Values
import org.neo4j.driver.types.Node
import org.neo4j.driver.types.Relationship

class DynamicTypesSchemaTest {

  @Test
  fun `should derive schema for simple types correctly`() {
    // NULL
    DynamicTypes.schemaFor(null, false) shouldBe SchemaBuilder.struct().optional().build()
    DynamicTypes.schemaFor(null, true) shouldBe SchemaBuilder.struct().optional().build()

    // Integer, Long, etc.
    listOf<Any>(8.toByte(), 8.toShort(), 8.toInt(), 8.toLong()).forEach { number ->
      withClue(number) {
        DynamicTypes.schemaFor(number, false) shouldBe SchemaBuilder.INT64_SCHEMA
        DynamicTypes.schemaFor(number, true) shouldBe SchemaBuilder.OPTIONAL_INT64_SCHEMA
      }
    }

    // Float, Double
    listOf<Any>(8.toFloat(), 8.toDouble()).forEach { number ->
      withClue(number) {
        DynamicTypes.schemaFor(number, false) shouldBe SchemaBuilder.FLOAT64_SCHEMA
        DynamicTypes.schemaFor(number, true) shouldBe SchemaBuilder.OPTIONAL_FLOAT64_SCHEMA
      }
    }

    // String
    listOf<Any>(
            "a string",
            "a char array".toCharArray(),
            StringBuilder("a string builder"),
            StringBuffer("a string buffer"))
        .forEach { string ->
          withClue(string) {
            DynamicTypes.schemaFor(string, false) shouldBe SchemaBuilder.STRING_SCHEMA
            DynamicTypes.schemaFor(string, true) shouldBe SchemaBuilder.OPTIONAL_STRING_SCHEMA
          }
        }

    // Byte Array
    listOf<Any>(ByteArray(0), ByteBuffer.allocate(0)).forEach { bytes ->
      withClue(bytes) {
        DynamicTypes.schemaFor(bytes, false) shouldBe SchemaBuilder.BYTES_SCHEMA
        DynamicTypes.schemaFor(bytes, true) shouldBe SchemaBuilder.OPTIONAL_BYTES_SCHEMA
      }
    }

    // Boolean Array
    listOf<Any>(BooleanArray(0), Array(1) { true }).forEach { array ->
      withClue(array) {
        DynamicTypes.schemaFor(array, false) shouldBe
            SchemaBuilder.array(Schema.BOOLEAN_SCHEMA).build()
        DynamicTypes.schemaFor(array, true) shouldBe
            SchemaBuilder.array(Schema.BOOLEAN_SCHEMA).optional().build()
      }
    }

    // Int Arrays
    listOf(
            ShortArray(1),
            IntArray(1),
            LongArray(1),
            Array(1) { i -> i },
            Array(1) { i -> i.toShort() },
            Array(1) { i -> i.toLong() })
        .forEach { array ->
          withClue(array) {
            DynamicTypes.schemaFor(array, false) shouldBe
                SchemaBuilder.array(Schema.INT64_SCHEMA).build()
            DynamicTypes.schemaFor(array, true) shouldBe
                SchemaBuilder.array(Schema.INT64_SCHEMA).optional().build()
          }
        }

    // Float Arrays
    listOf(
            FloatArray(1),
            DoubleArray(1),
            Array(1) { i -> i.toFloat() },
            Array(1) { i -> i.toDouble() })
        .forEach { array ->
          withClue(array) {
            DynamicTypes.schemaFor(array, false) shouldBe
                SchemaBuilder.array(Schema.FLOAT64_SCHEMA).build()
            DynamicTypes.schemaFor(array, true) shouldBe
                SchemaBuilder.array(Schema.FLOAT64_SCHEMA).optional().build()
          }
        }

    // String Array
    DynamicTypes.schemaFor(Array(1) { "a" }, false) shouldBe
        SchemaBuilder.array(Schema.STRING_SCHEMA).build()
    DynamicTypes.schemaFor(Array(1) { "a" }, true) shouldBe
        SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build()

    // Temporal Types
    DynamicTypes.schemaFor(LocalDate.of(1999, 12, 31), false) shouldBe SimpleTypes.LOCALDATE.schema
    DynamicTypes.schemaFor(LocalDate.of(1999, 12, 31), true) shouldBe
        SimpleTypes.LOCALDATE_NULLABLE.schema

    DynamicTypes.schemaFor(LocalTime.of(23, 59, 59), false) shouldBe SimpleTypes.LOCALTIME.schema
    DynamicTypes.schemaFor(LocalTime.of(23, 59, 59), true) shouldBe
        SimpleTypes.LOCALTIME_NULLABLE.schema

    DynamicTypes.schemaFor(LocalDateTime.of(1999, 12, 31, 23, 59, 59), false) shouldBe
        SimpleTypes.LOCALDATETIME.schema
    DynamicTypes.schemaFor(LocalDateTime.of(1999, 12, 31, 23, 59, 59), true) shouldBe
        SimpleTypes.LOCALDATETIME_NULLABLE.schema

    DynamicTypes.schemaFor(OffsetTime.of(23, 59, 59, 0, ZoneOffset.UTC), false) shouldBe
        SimpleTypes.OFFSETTIME.schema
    DynamicTypes.schemaFor(OffsetTime.of(23, 59, 59, 0, ZoneOffset.UTC), true) shouldBe
        SimpleTypes.OFFSETTIME_NULLABLE.schema

    DynamicTypes.schemaFor(
        OffsetDateTime.of(1999, 12, 31, 23, 59, 59, 0, ZoneOffset.UTC), false) shouldBe
        SimpleTypes.ZONEDDATETIME.schema
    DynamicTypes.schemaFor(
        OffsetDateTime.of(1999, 12, 31, 23, 59, 59, 0, ZoneOffset.UTC), true) shouldBe
        SimpleTypes.ZONEDDATETIME_NULLABLE.schema

    DynamicTypes.schemaFor(
        ZonedDateTime.of(1999, 12, 31, 23, 59, 59, 0, ZoneId.of("Europe/London")), false) shouldBe
        SimpleTypes.ZONEDDATETIME.schema
    DynamicTypes.schemaFor(
        ZonedDateTime.of(1999, 12, 31, 23, 59, 59, 0, ZoneId.of("Europe/London")), true) shouldBe
        SimpleTypes.ZONEDDATETIME_NULLABLE.schema

    DynamicTypes.schemaFor(Duration.parse("P1DT23H59M59S"), false) shouldBe
        SimpleTypes.DURATION.schema
    DynamicTypes.schemaFor(Duration.parse("P1DT23H59M59S"), true) shouldBe
        SimpleTypes.DURATION_NULLABLE.schema

    DynamicTypes.schemaFor(Values.isoDuration(12, 12, 59, 1230).asIsoDuration(), false) shouldBe
        SimpleTypes.DURATION.schema
    DynamicTypes.schemaFor(Values.isoDuration(12, 12, 59, 1230).asIsoDuration(), true) shouldBe
        SimpleTypes.DURATION_NULLABLE.schema

    // Point
    listOf(Values.point(4326, 1.0, 2.0).asPoint(), Values.point(4326, 1.0, 2.0, 3.0).asPoint())
        .forEach { point ->
          withClue(point) {
            DynamicTypes.schemaFor(point, false) shouldBe SimpleTypes.POINT.schema
            DynamicTypes.schemaFor(point, true) shouldBe SimpleTypes.POINT_NULLABLE.schema
          }
        }

    // Node
    DynamicTypes.schemaFor(
        TestNode(
            0,
            listOf("Person"),
            mapOf("name" to Values.value("john"), "surname" to Values.value("doe"))),
        false) shouldBe
        SchemaBuilder.struct()
            .name("org.neo4j.connectors.kafka.Node")
            .field("<id>", Schema.INT64_SCHEMA)
            .field("<labels>", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
            .field("name", Schema.STRING_SCHEMA)
            .field("surname", Schema.STRING_SCHEMA)
            .build()

    // Relationship
    DynamicTypes.schemaFor(
        TestRelationship(
            0,
            1,
            2,
            "KNOWS",
            mapOf("name" to Values.value("john"), "surname" to Values.value("doe"))),
        false) shouldBe
        SchemaBuilder.struct()
            .name("org.neo4j.connectors.kafka.Relationship")
            .field("<id>", Schema.INT64_SCHEMA)
            .field("<type>", SchemaBuilder.STRING_SCHEMA)
            .field("<start.id>", Schema.INT64_SCHEMA)
            .field("<end.id>", Schema.INT64_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .field("surname", Schema.STRING_SCHEMA)
            .build()
  }

  @Test
  fun `empty collections should map to an empty struct`() {
    listOf<Any>(listOf<Any>(), setOf<Any>()).forEach { collection ->
      withClue(collection) {
        DynamicTypes.schemaFor(collection, false) shouldBe SchemaBuilder.struct().build()
        DynamicTypes.schemaFor(collection, true) shouldBe SchemaBuilder.struct().optional().build()
      }
    }
  }

  @Test
  fun `collections with elements of single type should map to an array`() {
    listOf<Pair<Any, Schema>>(
            listOf(1, 2, 3) to Schema.INT64_SCHEMA,
            listOf("a", "b", "c") to Schema.STRING_SCHEMA,
            setOf(true) to Schema.BOOLEAN_SCHEMA)
        .forEach { (collection, elementSchema) ->
          withClue(collection) {
            DynamicTypes.schemaFor(collection, false) shouldBe
                SchemaBuilder.array(elementSchema).build()
            DynamicTypes.schemaFor(collection, true) shouldBe
                SchemaBuilder.array(elementSchema).optional().build()
          }
        }
  }

  @Test
  fun `collections with elements of different types should map to a struct`() {
    DynamicTypes.schemaFor(listOf(1, true, "a", 5.toFloat()), false) shouldBe
        SchemaBuilder.struct()
            .field("e0", Schema.INT64_SCHEMA)
            .field("e1", Schema.BOOLEAN_SCHEMA)
            .field("e2", Schema.STRING_SCHEMA)
            .field("e3", Schema.FLOAT64_SCHEMA)
            .build()

    DynamicTypes.schemaFor(listOf(1, true, "a", 5.toFloat()), true) shouldBe
        SchemaBuilder.struct()
            .field("e0", Schema.INT64_SCHEMA)
            .field("e1", Schema.BOOLEAN_SCHEMA)
            .field("e2", Schema.STRING_SCHEMA)
            .field("e3", Schema.FLOAT64_SCHEMA)
            .optional()
            .build()
  }

  @Test
  fun `empty maps should map to an empty struct`() {
    DynamicTypes.schemaFor(mapOf<String, Any>(), false) shouldBe SchemaBuilder.struct().build()
    DynamicTypes.schemaFor(mapOf<String, Any>(), true) shouldBe
        SchemaBuilder.struct().optional().build()
  }

  @Test
  fun `map keys should be enforced to be a string`() {
    shouldThrow<IllegalArgumentException> {
      DynamicTypes.schemaFor(mapOf(1 to 5, "a" to "b"), false)
    } shouldHaveMessage ("unsupported map key type java.lang.Integer")
  }

  @Test
  fun `maps with single typed values should map to a map`() {
    listOf(
            mapOf("a" to 1, "b" to 2, "c" to 3) to Schema.INT64_SCHEMA,
            mapOf("a" to "a", "b" to "b", "c" to "c") to Schema.STRING_SCHEMA,
            mapOf("a" to 1, "b" to 2.toShort(), "c" to 3.toLong()) to Schema.INT64_SCHEMA)
        .forEach { (map, valueSchema) ->
          withClue("not optional: $map") {
            DynamicTypes.schemaFor(map, false) shouldBe
                SchemaBuilder.map(Schema.STRING_SCHEMA, valueSchema).build()
          }
        }

    listOf(
            mapOf("a" to 1, "b" to 2, "c" to 3) to Schema.OPTIONAL_INT64_SCHEMA,
            mapOf("a" to "a", "b" to "b", "c" to "c") to Schema.OPTIONAL_STRING_SCHEMA,
            mapOf("a" to 1, "b" to 2.toShort(), "c" to 3.toLong()) to Schema.OPTIONAL_INT64_SCHEMA)
        .forEach { (map, valueSchema) ->
          withClue("optional: $map") {
            DynamicTypes.schemaFor(map, true) shouldBe
                SchemaBuilder.map(Schema.STRING_SCHEMA, valueSchema).optional().build()
          }
        }
  }

  @Test
  fun `maps with values of different types should map to a struct`() {
    DynamicTypes.schemaFor(
        mapOf("a" to 1, "b" to true, "c" to "string", "d" to 5.toFloat()), false) shouldBe
        SchemaBuilder.struct()
            .field("a", Schema.INT64_SCHEMA)
            .field("b", Schema.BOOLEAN_SCHEMA)
            .field("c", Schema.STRING_SCHEMA)
            .field("d", Schema.FLOAT64_SCHEMA)
            .build()

    DynamicTypes.schemaFor(
        mapOf("a" to 1, "b" to true, "c" to "string", "d" to 5.toFloat()), true) shouldBe
        SchemaBuilder.struct()
            .field("a", Schema.OPTIONAL_INT64_SCHEMA)
            .field("b", Schema.OPTIONAL_BOOLEAN_SCHEMA)
            .field("c", Schema.OPTIONAL_STRING_SCHEMA)
            .field("d", Schema.OPTIONAL_FLOAT64_SCHEMA)
            .optional()
            .build()
  }

  @Test
  fun `unsupported types should throw`() {
    data class Test(val a: String)

    listOf(object {}, java.sql.Date(0), object : Entity(emptyMap()) {}, Test("a string")).forEach {
        value ->
      shouldThrow<IllegalArgumentException> { DynamicTypes.schemaFor(value, false) }
    }
  }
}

class DynamicTypesValueTest {

  @Test
  fun `simple types should be returned as themselves`() {
    listOf(
            true to true,
            false to false,
            1.toShort() to 1.toLong(),
            2 to 2.toLong(),
            3.toLong() to 3.toLong(),
            4.toFloat() to 4.toDouble(),
            5.toDouble() to 5.toDouble(),
            'c' to "c",
            "string" to "string",
            "string".toCharArray() to "string",
            "string".toByteArray() to "string".toByteArray())
        .forEach { (value, expected) ->
          withClue(value) {
            val schema = DynamicTypes.schemaFor(value, false)
            val converted = DynamicTypes.valueFor(schema, value)

            converted shouldBe expected
          }
        }
  }

  @Test
  fun `temporal types should be returned as strings`() {
    listOf(
            LocalDate.of(1999, 12, 31) to "1999-12-31",
            LocalTime.of(23, 59, 59, 9999) to "23:59:59.000009999",
            LocalDateTime.of(1999, 12, 31, 23, 59, 59, 9999) to "1999-12-31T23:59:59.000009999",
            OffsetTime.of(23, 59, 59, 9999, ZoneOffset.UTC) to "23:59:59.000009999Z",
            OffsetDateTime.of(1999, 12, 31, 23, 59, 59, 9999, ZoneOffset.ofHours(1)) to
                "1999-12-31T23:59:59.000009999+01:00",
            ZonedDateTime.of(1999, 12, 31, 23, 59, 59, 9999, ZoneId.of("Europe/Istanbul")) to
                "1999-12-31T23:59:59.000009999+02:00[Europe/Istanbul]",
            Duration.parse("P2DT6H4M0.9999S") to "PT54H4M0.9999S",
            Values.isoDuration(5, 2, 0, 9999).asIsoDuration() to "P5M2DT0.000009999S")
        .forEach { (value, expected) ->
          withClue(value) {
            val schema = DynamicTypes.schemaFor(value, false)
            val converted = DynamicTypes.valueFor(schema, value)

            converted shouldBe expected
          }
        }
  }

  @Test
  fun `arrays and collections should be returned as arrays`() {
    listOf(
            ShortArray(1) { 1 } to LongArray(1) { 1.toLong() },
            IntArray(1) { 1 } to LongArray(1) { 1.toLong() },
            LongArray(1) { 1 } to LongArray(1) { 1 },
            FloatArray(1) { 1F } to DoubleArray(1) { 1.0 },
            DoubleArray(1) { 1.0 } to DoubleArray(1) { 1.0 },
            BooleanArray(1) { true } to BooleanArray(1) { true },
            Array(1) { 1 } to Array(1) { 1L },
            Array(1) { 1.toShort() } to Array(1) { 1L },
            Array(1) { "string" } to Array(1) { "string" },
            listOf(1, 2, 3) to arrayOf(1L, 2L, 3L),
            listOf("a", "b", "c") to arrayOf("a", "b", "c"),
            setOf(true, false) to arrayOf(true, false))
        .forEach { (value, expected) ->
          withClue(value) {
            val schema = DynamicTypes.schemaFor(value, false)
            val converted = DynamicTypes.valueFor(schema, value)

            converted shouldBe expected
          }
        }
  }

  @Test
  fun `maps should be returned as maps`() {
    listOf(
            mapOf("a" to "x", "b" to "y", "c" to "z") to mapOf("a" to "x", "b" to "y", "c" to "z"),
            mapOf("a" to 1, "b" to 2, "c" to 3) to mapOf("a" to 1L, "b" to 2L, "c" to 3L))
        .forEach { (value, expected) ->
          withClue(value) {
            val schema = DynamicTypes.schemaFor(value, false)
            val converted = DynamicTypes.valueFor(schema, value)

            converted shouldBe expected
          }
        }
  }

  @Test
  fun `points should be returned as structs`() {
    listOf(Values.point(4326, 1.0, 2.0).asPoint(), Values.point(4326, 1.0, 2.0, 3.0).asPoint())
        .forEach { point ->
          val schema = DynamicTypes.schemaFor(point, false)
          val converted = DynamicTypes.valueFor(schema, point)

          converted shouldBe
              Struct(schema)
                  .put("srid", point.srid())
                  .put("x", point.x())
                  .put("y", point.y())
                  .put("z", point.z())
        }
  }

  @Test
  fun `nodes should be returned as structs`() {
    val node =
        TestNode(
            0,
            listOf("Person", "Employee"),
            mapOf("name" to Values.value("john"), "surname" to Values.value("doe")))
    val schema = DynamicTypes.schemaFor(node, false)
    val converted = DynamicTypes.valueFor(schema, node)

    converted shouldBe
        Struct(schema)
            .put("<id>", 0L)
            .put("<labels>", listOf("Person", "Employee"))
            .put("name", "john")
            .put("surname", "doe")
  }

  @Test
  fun `relationships should be returned as structs`() {
    val rel =
        TestRelationship(
            0,
            1,
            2,
            "KNOWS",
            mapOf("name" to Values.value("john"), "surname" to Values.value("doe")))
    val schema = DynamicTypes.schemaFor(rel, false)
    val converted = DynamicTypes.valueFor(schema, rel)

    converted shouldBe
        Struct(schema)
            .put("<id>", 0L)
            .put("<start.id>", 1L)
            .put("<end.id>", 2L)
            .put("<type>", "KNOWS")
            .put("name", "john")
            .put("surname", "doe")
  }

  @Test
  fun `maps with values of different types should be returned as structs`() {
    val map =
        mapOf(
            "name" to "john",
            "age" to 21,
            "dob" to LocalDate.of(1999, 12, 31),
            "employed" to true,
            "nullable" to null)
    val schema = DynamicTypes.schemaFor(map, false)
    val converted = DynamicTypes.valueFor(schema, map)

    converted shouldBe
        Struct(schema)
            .put("name", "john")
            .put("age", 21L)
            .put("dob", "1999-12-31")
            .put("employed", true)
            .put("nullable", null)
  }

  @Test
  fun `collections with elements of different types should be returned as struct`() {
    val coll = listOf("john", 21, LocalDate.of(1999, 12, 31), true, null)
    val schema = DynamicTypes.schemaFor(coll, false)
    val converted = DynamicTypes.valueFor(schema, coll)

    converted shouldBe
        Struct(schema)
            .put("e0", "john")
            .put("e1", 21L)
            .put("e2", "1999-12-31")
            .put("e3", true)
            .put("e4", null)
  }
}

private abstract class Entity(val props: Map<String, Value>) {
  fun keys(): Iterable<String> = props.keys

  fun containsKey(key: String?): Boolean = props.containsKey(key)

  fun get(key: String?): Value? = props[key]

  fun size(): Int = props.size

  fun values(): Iterable<Value> = props.values

  fun <T : Any?> values(mapFunction: Function<Value, T>): Iterable<T> =
      props.values.map { mapFunction.apply(it) }

  fun asMap(): Map<String, Any> = props

  fun <T : Any?> asMap(mapFunction: Function<Value, T>): Map<String, T> =
      props.mapValues { mapFunction.apply(it.value) }
}

private class TestNode(val id: Long, val labels: List<String>, props: Map<String, Value>) :
    Entity(props), Node {

  override fun id(): Long = id

  override fun labels(): Iterable<String> = labels

  override fun hasLabel(label: String?): Boolean = label in labels
}

private class TestRelationship(
    val id: Long,
    val startId: Long,
    val endId: Long,
    val type: String,
    props: Map<String, Value>
) : Entity(props), Relationship {
  override fun id(): Long = id

  override fun startNodeId(): Long = startId

  override fun endNodeId(): Long = endId

  override fun type(): String = type

  override fun hasType(relationshipType: String?): Boolean = type == relationshipType
}
