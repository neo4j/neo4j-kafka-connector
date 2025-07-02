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
package org.neo4j.connectors.kafka.data

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.assertions.withClue
import io.kotest.matchers.shouldBe
import io.kotest.matchers.throwable.shouldHaveMessage
import java.nio.ByteBuffer
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.OffsetTime
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.junit.jupiter.api.Test
import org.neo4j.connectors.kafka.configuration.PayloadMode
import org.neo4j.driver.Values

class DynamicTypesCompactTest {

  companion object {
    val payloadMode = PayloadMode.COMPACT
  }

  @Test
  fun `should derive schema for simple types correctly`() {
    // NULL
    DynamicTypes.toConnectSchema(payloadMode, null, false) shouldBe SimpleTypes.NULL.schema()
    DynamicTypes.toConnectSchema(payloadMode, null, true) shouldBe SimpleTypes.NULL.schema()

    // Integer, Long, etc.
    listOf<Any>(8.toByte(), 8.toShort(), 8, 8.toLong()).forEach { number ->
      withClue(number) {
        DynamicTypes.toConnectSchema(payloadMode, number, false) shouldBe SchemaBuilder.INT64_SCHEMA
        DynamicTypes.toConnectSchema(payloadMode, number, true) shouldBe
            SchemaBuilder.OPTIONAL_INT64_SCHEMA
      }
    }

    // Float, Double
    listOf<Any>(8.toFloat(), 8.toDouble()).forEach { number ->
      withClue(number) {
        DynamicTypes.toConnectSchema(payloadMode, number, false) shouldBe
            SchemaBuilder.FLOAT64_SCHEMA
        DynamicTypes.toConnectSchema(payloadMode, number, true) shouldBe
            SchemaBuilder.OPTIONAL_FLOAT64_SCHEMA
      }
    }

    // String
    listOf<Any>(
            "a string",
            "a char array".toCharArray(),
            StringBuilder("a string builder"),
            StringBuffer("a string buffer"),
            object : CharSequence {
              private val value = "a char sequence"
              override val length: Int
                get() = value.length

              override fun get(index: Int): Char = value[index]

              override fun subSequence(startIndex: Int, endIndex: Int): CharSequence =
                  value.subSequence(startIndex, endIndex)
            })
        .forEach { string ->
          withClue(string) {
            DynamicTypes.toConnectSchema(payloadMode, string, false) shouldBe
                SchemaBuilder.STRING_SCHEMA
            DynamicTypes.toConnectSchema(payloadMode, string, true) shouldBe
                SchemaBuilder.OPTIONAL_STRING_SCHEMA
          }
        }

    // Byte Array
    listOf<Any>(ByteArray(0), ByteBuffer.allocate(0)).forEach { bytes ->
      withClue(bytes) {
        DynamicTypes.toConnectSchema(payloadMode, bytes, false) shouldBe SchemaBuilder.BYTES_SCHEMA
        DynamicTypes.toConnectSchema(payloadMode, bytes, true) shouldBe
            SchemaBuilder.OPTIONAL_BYTES_SCHEMA
      }
    }

    // Boolean Array (boolean[])
    listOf<Any>(BooleanArray(0), BooleanArray(1) { true }).forEach { array ->
      withClue(array) {
        DynamicTypes.toConnectSchema(payloadMode, array, false) shouldBe
            SchemaBuilder.array(Schema.BOOLEAN_SCHEMA).build()
        DynamicTypes.toConnectSchema(payloadMode, array, true) shouldBe
            SchemaBuilder.array(Schema.BOOLEAN_SCHEMA).optional().build()
      }
    }

    // Array of Boolean (Boolean[])
    listOf<Any>(Array(1) { true }).forEach { array ->
      withClue(array) {
        DynamicTypes.toConnectSchema(payloadMode, array, false) shouldBe
            SchemaBuilder.array(Schema.BOOLEAN_SCHEMA).build()
        DynamicTypes.toConnectSchema(payloadMode, array, true) shouldBe
            SchemaBuilder.array(Schema.OPTIONAL_BOOLEAN_SCHEMA).optional().build()
      }
    }

    // Int Arrays (short[], int[], long[])
    listOf(ShortArray(1), IntArray(1), LongArray(1)).forEach { array ->
      withClue(array) {
        DynamicTypes.toConnectSchema(payloadMode, array, false) shouldBe
            SchemaBuilder.array(Schema.INT64_SCHEMA).build()
        DynamicTypes.toConnectSchema(payloadMode, array, true) shouldBe
            SchemaBuilder.array(Schema.INT64_SCHEMA).optional().build()
      }
    }

    // Array of Integer (Short[], Integer[], Long[])
    listOf(Array(1) { i -> i }, Array(1) { i -> i.toShort() }, Array(1) { i -> i.toLong() })
        .forEach { array ->
          withClue(array) {
            DynamicTypes.toConnectSchema(payloadMode, array, false) shouldBe
                SchemaBuilder.array(Schema.INT64_SCHEMA).build()
            DynamicTypes.toConnectSchema(payloadMode, array, true) shouldBe
                SchemaBuilder.array(Schema.OPTIONAL_INT64_SCHEMA).optional().build()
          }
        }

    // Float Arrays (float[], double[])
    listOf(FloatArray(1), DoubleArray(1)).forEach { array ->
      withClue(array) {
        DynamicTypes.toConnectSchema(payloadMode, array, false) shouldBe
            SchemaBuilder.array(Schema.FLOAT64_SCHEMA).build()
        DynamicTypes.toConnectSchema(payloadMode, array, true) shouldBe
            SchemaBuilder.array(Schema.FLOAT64_SCHEMA).optional().build()
      }
    }

    // Float Arrays (Float[], Double[])
    listOf(Array(1) { i -> i.toFloat() }, Array(1) { i -> i.toDouble() }).forEach { array ->
      withClue(array) {
        DynamicTypes.toConnectSchema(payloadMode, array, false) shouldBe
            SchemaBuilder.array(Schema.FLOAT64_SCHEMA).build()
        DynamicTypes.toConnectSchema(payloadMode, array, true) shouldBe
            SchemaBuilder.array(Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build()
      }
    }

    // String Array
    DynamicTypes.toConnectSchema(payloadMode, Array(1) { "a" }, false) shouldBe
        SchemaBuilder.array(Schema.STRING_SCHEMA).build()
    DynamicTypes.toConnectSchema(payloadMode, Array(1) { "a" }, true) shouldBe
        SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build()

    // Temporal Types
    DynamicTypes.toConnectSchema(payloadMode, LocalDate.of(1999, 12, 31), optional = false) shouldBe
        SimpleTypes.LOCALDATE.schema()
    DynamicTypes.toConnectSchema(payloadMode, LocalDate.of(1999, 12, 31), optional = true) shouldBe
        SimpleTypes.LOCALDATE.schema(true)

    DynamicTypes.toConnectSchema(payloadMode, LocalTime.of(23, 59, 59), optional = false) shouldBe
        SimpleTypes.LOCALTIME.schema()
    DynamicTypes.toConnectSchema(payloadMode, LocalTime.of(23, 59, 59), optional = true) shouldBe
        SimpleTypes.LOCALTIME.schema(true)

    DynamicTypes.toConnectSchema(
        payloadMode, LocalDateTime.of(1999, 12, 31, 23, 59, 59), optional = false) shouldBe
        SimpleTypes.LOCALDATETIME.schema()
    DynamicTypes.toConnectSchema(
        payloadMode, LocalDateTime.of(1999, 12, 31, 23, 59, 59), optional = true) shouldBe
        SimpleTypes.LOCALDATETIME.schema(true)

    DynamicTypes.toConnectSchema(
        payloadMode, OffsetTime.of(23, 59, 59, 0, ZoneOffset.UTC), optional = false) shouldBe
        SimpleTypes.OFFSETTIME.schema()
    DynamicTypes.toConnectSchema(
        payloadMode, OffsetTime.of(23, 59, 59, 0, ZoneOffset.UTC), optional = true) shouldBe
        SimpleTypes.OFFSETTIME.schema(true)

    DynamicTypes.toConnectSchema(
        payloadMode,
        OffsetDateTime.of(1999, 12, 31, 23, 59, 59, 0, ZoneOffset.UTC),
        optional = false) shouldBe SimpleTypes.ZONEDDATETIME.schema()
    DynamicTypes.toConnectSchema(
        payloadMode, OffsetDateTime.of(1999, 12, 31, 23, 59, 59, 0, ZoneOffset.UTC), true) shouldBe
        SimpleTypes.ZONEDDATETIME.schema(true)

    DynamicTypes.toConnectSchema(
        payloadMode,
        ZonedDateTime.of(1999, 12, 31, 23, 59, 59, 0, ZoneId.of("Europe/London")),
        optional = false) shouldBe SimpleTypes.ZONEDDATETIME.schema()
    DynamicTypes.toConnectSchema(
        payloadMode,
        ZonedDateTime.of(1999, 12, 31, 23, 59, 59, 0, ZoneId.of("Europe/London")),
        optional = true) shouldBe SimpleTypes.ZONEDDATETIME.schema(true)

    DynamicTypes.toConnectSchema(
        payloadMode, Values.isoDuration(12, 12, 59, 1230).asIsoDuration(), false) shouldBe
        SimpleTypes.DURATION.schema()
    DynamicTypes.toConnectSchema(
        payloadMode, Values.isoDuration(12, 12, 59, 1230).asIsoDuration(), true) shouldBe
        SimpleTypes.DURATION.schema(true)

    // Point
    listOf(Values.point(4326, 1.0, 2.0).asPoint(), Values.point(4326, 1.0, 2.0, 3.0).asPoint())
        .forEach { point ->
          withClue(point) {
            DynamicTypes.toConnectSchema(payloadMode, point, false) shouldBe
                SimpleTypes.POINT.schema()
            DynamicTypes.toConnectSchema(payloadMode, point, true) shouldBe
                SimpleTypes.POINT.schema(true)
          }
        }

    // Node
    DynamicTypes.toConnectSchema(payloadMode, TestNode(0, emptyList(), emptyMap()), false) shouldBe
        SchemaBuilder.struct()
            .field("<id>", Schema.INT64_SCHEMA)
            .field("<labels>", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
            .build()

    DynamicTypes.toConnectSchema(
        payloadMode,
        TestNode(
            0,
            listOf("Person"),
            mapOf("name" to Values.value("john"), "surname" to Values.value("doe"))),
        false) shouldBe
        SchemaBuilder.struct()
            .field("<id>", Schema.INT64_SCHEMA)
            .field("<labels>", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
            .field("name", Schema.STRING_SCHEMA)
            .field("surname", Schema.STRING_SCHEMA)
            .build()

    // Relationship
    DynamicTypes.toConnectSchema(
        payloadMode, TestRelationship(0, 1, 2, "KNOWS", emptyMap()), false) shouldBe
        SchemaBuilder.struct()
            .field("<id>", Schema.INT64_SCHEMA)
            .field("<type>", SchemaBuilder.STRING_SCHEMA)
            .field("<start.id>", Schema.INT64_SCHEMA)
            .field("<end.id>", Schema.INT64_SCHEMA)
            .build()
    DynamicTypes.toConnectSchema(
        payloadMode,
        TestRelationship(
            0,
            1,
            2,
            "KNOWS",
            mapOf("name" to Values.value("john"), "surname" to Values.value("doe"))),
        false) shouldBe
        SchemaBuilder.struct()
            .field("<id>", Schema.INT64_SCHEMA)
            .field("<type>", SchemaBuilder.STRING_SCHEMA)
            .field("<start.id>", Schema.INT64_SCHEMA)
            .field("<end.id>", Schema.INT64_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .field("surname", Schema.STRING_SCHEMA)
            .build()
  }

  @Test
  fun `empty collections or arrays should map to an array schema`() {
    listOf(listOf<Any>(), setOf<Any>(), arrayOf<Any>()).forEach { collection ->
      withClue(collection) {
        DynamicTypes.toConnectSchema(payloadMode, collection, false) shouldBe
            SchemaBuilder.array(SimpleTypes.NULL.schema()).build()
        DynamicTypes.toConnectSchema(payloadMode, collection, true) shouldBe
            SchemaBuilder.array(SimpleTypes.NULL.schema()).optional().build()
      }
    }
  }

  @Test
  fun `collections with elements of single type should map to an array schema`() {
    listOf<Triple<Any, Schema, Schema>>(
            Triple(listOf(1, 2, 3), Schema.INT64_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA),
            Triple(listOf("a", "b", "c"), Schema.STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA),
            Triple(setOf(true), Schema.BOOLEAN_SCHEMA, Schema.OPTIONAL_BOOLEAN_SCHEMA),
        )
        .forEach { (collection, elementSchema, elementOptionalSchema) ->
          withClue(collection) {
            DynamicTypes.toConnectSchema(payloadMode, collection, false) shouldBe
                SchemaBuilder.array(elementSchema).build()
            DynamicTypes.toConnectSchema(payloadMode, collection, true) shouldBe
                SchemaBuilder.array(elementOptionalSchema).optional().build()
          }
        }
  }

  @Test
  fun `collections with elements of different types should map to a struct schema`() {
    DynamicTypes.toConnectSchema(payloadMode, listOf(1, true, "a", 5.toFloat()), false) shouldBe
        SchemaBuilder.struct()
            .field("e0", Schema.INT64_SCHEMA)
            .field("e1", Schema.BOOLEAN_SCHEMA)
            .field("e2", Schema.STRING_SCHEMA)
            .field("e3", Schema.FLOAT64_SCHEMA)
            .build()

    DynamicTypes.toConnectSchema(payloadMode, listOf(1, true, "a", 5.toFloat()), true) shouldBe
        SchemaBuilder.struct()
            .field("e0", Schema.OPTIONAL_INT64_SCHEMA)
            .field("e1", Schema.OPTIONAL_BOOLEAN_SCHEMA)
            .field("e2", Schema.OPTIONAL_STRING_SCHEMA)
            .field("e3", Schema.OPTIONAL_FLOAT64_SCHEMA)
            .optional()
            .build()
  }

  @Test
  fun `empty maps should map to an empty struct schema`() {
    DynamicTypes.toConnectSchema(payloadMode, mapOf<String, Any>(), false) shouldBe
        SchemaBuilder.struct().build()
    DynamicTypes.toConnectSchema(payloadMode, mapOf<String, Any>(), true) shouldBe
        SchemaBuilder.struct().optional().build()
  }

  @Test
  fun `map keys should be enforced to be a string`() {
    shouldThrow<IllegalArgumentException> {
      DynamicTypes.toConnectSchema(payloadMode, mapOf(1 to 5, "a" to "b"), false)
    } shouldHaveMessage ("unsupported map key type java.lang.Integer")
  }

  @Test
  fun `maps with single typed values should map to a map schema`() {
    listOf(
            mapOf("a" to 1, "b" to 2, "c" to 3) to Schema.INT64_SCHEMA,
            mapOf("a" to "a", "b" to "b", "c" to "c") to Schema.STRING_SCHEMA,
            mapOf("a" to 1, "b" to 2.toShort(), "c" to 3.toLong()) to Schema.INT64_SCHEMA)
        .forEach { (map, valueSchema) ->
          withClue("not optional: $map") {
            DynamicTypes.toConnectSchema(payloadMode, map, false) shouldBe
                SchemaBuilder.map(Schema.STRING_SCHEMA, valueSchema).build()
          }
        }

    listOf(
            mapOf("a" to 1, "b" to 2, "c" to 3) to Schema.OPTIONAL_INT64_SCHEMA,
            mapOf("a" to "a", "b" to "b", "c" to "c") to Schema.OPTIONAL_STRING_SCHEMA,
            mapOf("a" to 1, "b" to 2.toShort(), "c" to 3.toLong()) to Schema.OPTIONAL_INT64_SCHEMA)
        .forEach { (map, valueSchema) ->
          withClue("optional: $map") {
            DynamicTypes.toConnectSchema(payloadMode, map, true) shouldBe
                SchemaBuilder.map(Schema.STRING_SCHEMA, valueSchema).optional().build()
          }
        }
  }

  @Test
  fun `maps with values of different types should map to a struct schema`() {
    DynamicTypes.toConnectSchema(
        payloadMode,
        mapOf("a" to 1, "b" to true, "c" to "string", "d" to 5.toFloat()),
        false) shouldBe
        SchemaBuilder.struct()
            .field("a", Schema.INT64_SCHEMA)
            .field("b", Schema.BOOLEAN_SCHEMA)
            .field("c", Schema.STRING_SCHEMA)
            .field("d", Schema.FLOAT64_SCHEMA)
            .build()

    DynamicTypes.toConnectSchema(
        payloadMode,
        mapOf("a" to 1, "b" to true, "c" to "string", "d" to 5.toFloat()),
        true) shouldBe
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
      shouldThrow<IllegalArgumentException> {
        DynamicTypes.toConnectSchema(payloadMode, value, false)
      }
    }
  }

  @Test
  fun `simple types should be converted to themselves and should be converted back`() {
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
            val schema = DynamicTypes.toConnectSchema(payloadMode, value, false)
            val converted = DynamicTypes.toConnectValue(schema, value)
            val reverted = DynamicTypes.fromConnectValue(schema, converted)

            converted shouldBe expected
            reverted shouldBe expected
          }
        }
  }

  @Test
  fun `duration types should be returned as structs and should be converted back`() {
    listOf(
            Values.isoDuration(5, 2, 0, 9999).asIsoDuration() to
                Struct(SimpleTypes.DURATION.schema())
                    .put("months", 5L)
                    .put("days", 2L)
                    .put("seconds", 0L)
                    .put("nanoseconds", 9999))
        .forEach { (value, expected) ->
          withClue(value) {
            val schema = DynamicTypes.toConnectSchema(payloadMode, value, false)
            val converted = DynamicTypes.toConnectValue(schema, value)

            converted shouldBe expected

            val reverted = DynamicTypes.fromConnectValue(schema, converted)
            reverted shouldBe value
          }
        }
  }

  @Test
  fun `arrays and collections should be returned as arrays and should be converted back`() {
    fun primitiveToArray(value: Any): Any =
        when (value) {
          is BooleanArray -> value.toList()
          is ByteArray -> value.toList()
          is CharArray -> value.toList()
          is DoubleArray -> value.toList()
          is FloatArray -> value.toList()
          is IntArray -> value.toList()
          is LongArray -> value.toList()
          is ShortArray -> value.toList()
          else -> value
        }

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
            val schema = DynamicTypes.toConnectSchema(payloadMode, value, false)
            val converted = DynamicTypes.toConnectValue(schema, value)

            converted shouldBe expected

            val reverted = DynamicTypes.fromConnectValue(schema, converted)
            reverted shouldBe primitiveToArray(value)
          }
        }
  }

  @Test
  fun `maps should be returned as maps and should be converted back`() {
    listOf(
            mapOf("a" to "x", "b" to "y", "c" to "z") to mapOf("a" to "x", "b" to "y", "c" to "z"),
            mapOf("a" to 1, "b" to 2, "c" to 3) to mapOf("a" to 1L, "b" to 2L, "c" to 3L))
        .forEach { (value, expected) ->
          withClue(value) {
            val schema = DynamicTypes.toConnectSchema(payloadMode, value, false)
            val converted = DynamicTypes.toConnectValue(schema, value)

            converted shouldBe expected

            val reverted = DynamicTypes.fromConnectValue(schema, converted)
            reverted shouldBe value
          }
        }
  }

  @Test
  fun `2d points should be returned as structs and should be converted back`() {
    //    listOf(, Values.point(4326, 1.0, 2.0, 3.0).asPoint())
    val point = Values.point(4326, 1.0, 2.0).asPoint()
    val schema = DynamicTypes.toConnectSchema(payloadMode, point, false)
    val converted = DynamicTypes.toConnectValue(schema, point)

    converted shouldBe
        Struct(schema)
            .put("dimension", 2.toByte())
            .put("srid", point.srid())
            .put("x", point.x())
            .put("y", point.y())
            .put("z", null)

    val reverted = DynamicTypes.fromConnectValue(schema, converted)
    reverted shouldBe point
  }

  @Test
  fun `3d points should be returned as structs and should be converted back`() {
    val point = Values.point(4326, 1.0, 2.0, 3.0).asPoint()
    val schema = DynamicTypes.toConnectSchema(payloadMode, point, false)
    val converted = DynamicTypes.toConnectValue(schema, point)

    converted shouldBe
        Struct(schema)
            .put("dimension", 3.toByte())
            .put("srid", point.srid())
            .put("x", point.x())
            .put("y", point.y())
            .put("z", point.z())

    val reverted = DynamicTypes.fromConnectValue(schema, converted)
    reverted shouldBe point
  }

  @Test
  fun `nodes should be returned as structs and should be converted back as maps`() {
    val node =
        TestNode(
            0,
            listOf("Person", "Employee"),
            mapOf("name" to Values.value("john"), "surname" to Values.value("doe")))
    val schema = DynamicTypes.toConnectSchema(payloadMode, node, false)
    val converted = DynamicTypes.toConnectValue(schema, node)

    converted shouldBe
        Struct(schema)
            .put("<id>", 0L)
            .put("<labels>", listOf("Person", "Employee"))
            .put("name", "john")
            .put("surname", "doe")

    val reverted = DynamicTypes.fromConnectValue(schema, converted)
    reverted shouldBe
        mapOf(
            "<id>" to 0L,
            "<labels>" to listOf("Person", "Employee"),
            "name" to "john",
            "surname" to "doe")
  }

  @Test
  fun `relationships should be returned as structs and should be converted back as maps`() {
    val rel =
        TestRelationship(
            0,
            1,
            2,
            "KNOWS",
            mapOf("name" to Values.value("john"), "surname" to Values.value("doe")))
    val schema = DynamicTypes.toConnectSchema(payloadMode, rel, false)
    val converted = DynamicTypes.toConnectValue(schema, rel)

    converted shouldBe
        Struct(schema)
            .put("<id>", 0L)
            .put("<start.id>", 1L)
            .put("<end.id>", 2L)
            .put("<type>", "KNOWS")
            .put("name", "john")
            .put("surname", "doe")

    val reverted = DynamicTypes.fromConnectValue(schema, converted)
    reverted shouldBe
        mapOf(
            "<id>" to 0L,
            "<start.id>" to 1L,
            "<end.id>" to 2L,
            "<type>" to "KNOWS",
            "name" to "john",
            "surname" to "doe")
  }

  @Test
  fun `maps with values of different types should be returned as structs and should be converted back`() {
    val map =
        mapOf(
            "name" to "john",
            "age" to 21,
            "dob" to LocalDate.of(1999, 12, 31),
            "employed" to true,
            "nullable" to null)
    val schema = DynamicTypes.toConnectSchema(payloadMode, map, false)
    val converted = DynamicTypes.toConnectValue(schema, map)

    println(schema)
    println(converted)
    converted shouldBe
        Struct(schema)
            .put("name", "john")
            .put("age", 21L)
            .put("dob", DateTimeFormatter.ISO_DATE.format(LocalDate.of(1999, 12, 31)))
            .put("employed", true)
            .put("nullable", null)

    val reverted = DynamicTypes.fromConnectValue(schema, converted)
    reverted shouldBe map
  }

  @Test
  fun `collections with elements of different types should be returned as struct and should be converted back`() {
    val coll = listOf("john", 21, LocalDate.of(1999, 12, 31), true, null)
    val schema = DynamicTypes.toConnectSchema(payloadMode, coll, false)
    val converted = DynamicTypes.toConnectValue(schema, coll)

    converted shouldBe
        Struct(schema)
            .put("e0", "john")
            .put("e1", 21L)
            .put("e2", DateTimeFormatter.ISO_DATE.format(LocalDate.of(1999, 12, 31)))
            .put("e3", true)
            .put("e4", null)

    val reverted = DynamicTypes.fromConnectValue(schema, converted)
    reverted shouldBe coll
  }

  @Test
  fun `structs should be returned as maps`() {
    val schema =
        SchemaBuilder.struct()
            .field("id", Schema.INT32_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .field("last_name", Schema.STRING_SCHEMA)
            .field("dob", SimpleTypes.LOCALDATE.schema)
            .build()
    val struct =
        Struct(schema)
            .put("id", 1)
            .put("name", "john")
            .put("last_name", "doe")
            .put(
                "dob",
                DynamicTypes.toConnectValue(SimpleTypes.LOCALDATE.schema, LocalDate.of(2000, 1, 1)))

    DynamicTypes.fromConnectValue(schema, struct) shouldBe
        mapOf("id" to 1, "name" to "john", "last_name" to "doe", "dob" to LocalDate.of(2000, 1, 1))
  }

  @Test
  fun `structs with complex values should be returned as maps`() {
    val addressSchema =
        SchemaBuilder.struct()
            .field("city", Schema.STRING_SCHEMA)
            .field("country", Schema.STRING_SCHEMA)
            .build()
    val schema =
        SchemaBuilder.struct()
            .field("id", Schema.INT32_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .field("last_name", Schema.STRING_SCHEMA)
            .field("dob", SimpleTypes.LOCALDATE.schema)
            .field("address", addressSchema)
            .field("years_of_interest", SchemaBuilder.array(Schema.INT32_SCHEMA))
            .field(
                "events_of_interest", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA))
            .build()
    val struct =
        Struct(schema)
            .put("id", 1)
            .put("name", "john")
            .put("last_name", "doe")
            .put(
                "dob",
                DynamicTypes.toConnectValue(SimpleTypes.LOCALDATE.schema, LocalDate.of(2000, 1, 1)))
            .put("address", Struct(addressSchema).put("city", "london").put("country", "uk"))
            .put("years_of_interest", listOf(2000, 2005, 2017))
            .put(
                "events_of_interest",
                mapOf("2000" to "birth", "2005" to "school", "2017" to "college"))

    DynamicTypes.fromConnectValue(schema, struct) shouldBe
        mapOf(
            "id" to 1,
            "name" to "john",
            "last_name" to "doe",
            "dob" to LocalDate.of(2000, 1, 1),
            "address" to mapOf("city" to "london", "country" to "uk"),
            "years_of_interest" to listOf(2000, 2005, 2017),
            "events_of_interest" to
                mapOf("2000" to "birth", "2005" to "school", "2017" to "college"))
  }
}
