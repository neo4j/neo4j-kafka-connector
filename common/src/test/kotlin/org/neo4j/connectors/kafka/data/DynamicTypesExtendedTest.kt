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
import java.time.Duration
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.OffsetTime
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.stream.Stream
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.ArgumentsProvider
import org.junit.jupiter.params.provider.ArgumentsSource
import org.neo4j.connectors.kafka.configuration.PayloadMode
import org.neo4j.connectors.kafka.data.PropertyType.LOCAL_DATE
import org.neo4j.driver.Values
import org.neo4j.driver.types.Point

class DynamicTypesExtendedTest {

  companion object {
    val payloadMode = PayloadMode.EXTENDED
  }

  @ParameterizedTest(name = "{0}")
  @ArgumentsSource(PropertyTypedValueProvider::class)
  fun `should derive schema for property typed values and convert them back and forth`(
      name: String,
      value: Any?,
      expectedIfDifferent: Any?
  ) {
    DynamicTypes.toConnectSchema(payloadMode, value, false) shouldBe PropertyType.schema
    DynamicTypes.toConnectSchema(payloadMode, value, true) shouldBe PropertyType.schema

    val converted = DynamicTypes.toConnectValue(PropertyType.schema, value)
    converted shouldBe PropertyType.toConnectValue(value)

    val reverted = DynamicTypes.fromConnectValue(PropertyType.schema, converted)
    reverted shouldBe (expectedIfDifferent ?: value)
  }

  object PropertyTypedValueProvider : ArgumentsProvider {
    override fun provideArguments(ctx: ExtensionContext?): Stream<out Arguments> {
      return Stream.of(
          Arguments.of("null", null, null),
          Arguments.of("byte", 8.toByte(), 8L),
          Arguments.of("short", 8.toShort(), 8L),
          Arguments.of("int", 8, 8L),
          Arguments.of("long", 8.toLong(), null),
          Arguments.of("float", 8.toFloat(), 8.toDouble()),
          Arguments.of("double", 8.toDouble(), null),
          Arguments.of("string", "a string", null),
          Arguments.of("char array", "a char array".toCharArray(), "a char array"),
          Arguments.of("string builder", StringBuilder("a string builder"), "a string builder"),
          Arguments.of("string buffer", StringBuilder("a string buffer"), "a string buffer"),
          Arguments.of(
              "char sequence",
              object : CharSequence {
                private val value = "a char sequence"
                override val length: Int
                  get() = value.length

                override fun get(index: Int): Char = value[index]

                override fun subSequence(startIndex: Int, endIndex: Int): CharSequence =
                    value.subSequence(startIndex, endIndex)
              },
              "a char sequence"),
          Arguments.of("local date", LocalDate.of(1999, 12, 31), null),
          Arguments.of("local time", LocalTime.of(23, 59, 59), null),
          Arguments.of("local date time", LocalDateTime.of(1999, 12, 31, 23, 59, 59), null),
          Arguments.of("offset time", OffsetTime.of(23, 59, 59, 0, ZoneOffset.UTC), null),
          Arguments.of(
              "offset date time",
              OffsetDateTime.of(1999, 12, 31, 23, 59, 59, 0, ZoneOffset.UTC),
              null),
          Arguments.of(
              "zoned date time",
              ZonedDateTime.of(1999, 12, 31, 23, 59, 59, 0, ZoneId.of("Europe/London")),
              null),
          Arguments.of("duration", Values.isoDuration(12, 12, 59, 1230).asIsoDuration(), null),
          Arguments.of("point (2d)", Values.point(4326, 1.0, 2.0).asPoint(), null),
          Arguments.of("point (3d)", Values.point(4326, 1.0, 2.0, 3.0).asPoint(), null),
          Arguments.of("byte array", ByteArray(0), null),
          Arguments.of("byte buffer", ByteBuffer.allocate(0), ByteArray(0)),
          Arguments.of("array (byte)", Array(1) { 1.toByte() }, null),
          Arguments.of("bool array (empty)", BooleanArray(0), null),
          Arguments.of("bool array", BooleanArray(1) { true }, null),
          Arguments.of("array (bool)", Array(1) { true }, null),
          Arguments.of("list (bool)", listOf(true), null),
          Arguments.of("empty list (bool)", emptyList<Boolean>(), null),
          Arguments.of("short array (empty)", ShortArray(0), null),
          Arguments.of("short array", ShortArray(1) { 1.toShort() }, listOf(1L)),
          Arguments.of("array (short)", Array(1) { 1.toShort() }, listOf(1L)),
          Arguments.of("list (short)", listOf(1.toShort()), listOf(1L)),
          Arguments.of("empty list (short)", emptyList<Short>(), null),
          Arguments.of("int array (empty)", IntArray(0), null),
          Arguments.of("int array", IntArray(1) { 1 }, listOf(1L)),
          Arguments.of("array (int)", Array(1) { 1 }, listOf(1L)),
          Arguments.of("list (int)", listOf(1), listOf(1L)),
          Arguments.of("empty list (int)", emptyList<Int>(), null),
          Arguments.of("long array (empty)", LongArray(0), null),
          Arguments.of("long array", LongArray(1) { 1.toLong() }, null),
          Arguments.of("array (long)", Array(1) { 1.toLong() }, null),
          Arguments.of("list (long)", listOf(1L), null),
          Arguments.of("empty list (long)", emptyList<Long>(), null),
          Arguments.of("float array (empty)", FloatArray(0), null),
          Arguments.of("float array", FloatArray(1) { 1.toFloat() }, listOf(1.toDouble())),
          Arguments.of("array (float)", Array(1) { 1.toFloat() }, null),
          Arguments.of("list (float)", listOf(1.toFloat()), null),
          Arguments.of("empty list (float)", emptyList<Float>(), null),
          Arguments.of("double array (empty)", DoubleArray(0), null),
          Arguments.of("double array", DoubleArray(1) { 1.toDouble() }, null),
          Arguments.of("array (double)", Array(1) { 1.toDouble() }, null),
          Arguments.of("list (double)", listOf(1.toDouble()), null),
          Arguments.of("empty list (double)", emptyList<Double>(), null),
          Arguments.of("array (string)", Array(1) { "a" }, null),
          Arguments.of("list (string)", listOf("a"), null),
          Arguments.of("empty list (string)", emptyList<String>(), null),
          Arguments.of("array (local date)", Array(1) { LocalDate.of(1999, 12, 31) }, null),
          Arguments.of("list (local date)", listOf(LocalDate.of(1999, 12, 31)), null),
          Arguments.of("empty list (local date)", emptyList<LocalDate>(), null),
          Arguments.of("array (local time)", Array(1) { LocalTime.of(23, 59, 59) }, null),
          Arguments.of("list (local time)", listOf(LocalTime.of(23, 59, 59)), null),
          Arguments.of("empty list (local time)", emptyList<LocalTime>(), null),
          Arguments.of(
              "array (local date time)",
              Array(1) { LocalDateTime.of(1999, 12, 31, 23, 59, 59) },
              null),
          Arguments.of(
              "list (local date time)", listOf(LocalDateTime.of(1999, 12, 31, 23, 59, 59)), null),
          Arguments.of("empty list (local date time)", emptyList<LocalDateTime>(), null),
          Arguments.of(
              "array (offset time)",
              Array(1) { OffsetTime.of(23, 59, 59, 0, ZoneOffset.UTC) },
              null),
          Arguments.of(
              "list (offset time)", listOf(OffsetTime.of(23, 59, 59, 0, ZoneOffset.UTC)), null),
          Arguments.of("empty list (offset time)", emptyList<OffsetTime>(), null),
          Arguments.of(
              "array (offset date time)",
              Array(1) { OffsetDateTime.of(1999, 12, 31, 23, 59, 59, 0, ZoneOffset.UTC) },
              null),
          Arguments.of(
              "list (offset date time)",
              listOf(OffsetDateTime.of(1999, 12, 31, 23, 59, 59, 0, ZoneOffset.UTC)),
              null),
          Arguments.of("empty list (offset date time)", emptyList<OffsetDateTime>(), null),
          Arguments.of(
              "array (zoned date time)",
              Array(1) {
                ZonedDateTime.of(1999, 12, 31, 23, 59, 59, 0, ZoneId.of("Europe/London"))
              },
              null),
          Arguments.of(
              "list (zoned date time)",
              listOf(ZonedDateTime.of(1999, 12, 31, 23, 59, 59, 0, ZoneId.of("Europe/London"))),
              null),
          Arguments.of("empty list (zoned date time)", emptyList<ZonedDateTime>(), null),
          Arguments.of(
              "array (duration)",
              Array(1) { Values.isoDuration(12, 12, 59, 1230).asIsoDuration() },
              null),
          Arguments.of(
              "list (duration)",
              listOf(Values.isoDuration(12, 12, 59, 1230).asIsoDuration()),
              null),
          Arguments.of("empty list (duration)", emptyList<Duration>(), null),
          Arguments.of(
              "array (point (2d))", Array(1) { Values.point(4326, 1.0, 2.0).asPoint() }, null),
          Arguments.of("list (point (2d))", listOf(Values.point(4326, 1.0, 2.0).asPoint()), null),
          Arguments.of(
              "array (point (3d))", Array(1) { Values.point(4326, 1.0, 2.0, 3.0).asPoint() }, null),
          Arguments.of(
              "list (point (3d))", listOf(Values.point(4326, 1.0, 2.0, 3.0).asPoint()), null),
          Arguments.of("empty list (point)", emptyList<Point>(), null),
      )
    }
  }

  @Test
  fun `should derive schema for entity types correctly`() {
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
            .field("name", PropertyType.schema)
            .field("surname", PropertyType.schema)
            .build()

    // Relationship
    DynamicTypes.toConnectSchema(
        payloadMode, TestRelationship(0, 1, 2, "KNOWS", emptyMap()), false) shouldBe
        SchemaBuilder.struct()
            .field("<id>", Schema.INT64_SCHEMA)
            .field("<type>", Schema.STRING_SCHEMA)
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
            .field("<type>", Schema.STRING_SCHEMA)
            .field("<start.id>", Schema.INT64_SCHEMA)
            .field("<end.id>", Schema.INT64_SCHEMA)
            .field("name", PropertyType.schema)
            .field("surname", PropertyType.schema)
            .build()
  }

  @Test
  fun `empty collections should map to property type`() {
    listOf(listOf<Any>(), setOf<Any>()).forEach { collection ->
      withClue(collection) {
        DynamicTypes.toConnectSchema(payloadMode, collection, false) shouldBe PropertyType.schema
        DynamicTypes.toConnectSchema(payloadMode, collection, true) shouldBe PropertyType.schema
      }
    }
  }

  @Test
  fun `empty arrays should map to an array of property type`() {
    DynamicTypes.toConnectSchema(payloadMode, arrayOf<Any>(), false) shouldBe
        SchemaBuilder.array(PropertyType.schema).build()
    DynamicTypes.toConnectSchema(payloadMode, arrayOf<Any>(), true) shouldBe
        SchemaBuilder.array(PropertyType.schema).optional().build()
  }

  @Test
  fun `empty arrays of simple types should map to property type`() {
    listOf(
            arrayOf<Int>(),
            arrayOf<String>(),
            arrayOf<LocalDate>(),
            arrayOf<Boolean>(),
            arrayOf<Point>())
        .forEach { array ->
          withClue(array) {
            DynamicTypes.toConnectSchema(payloadMode, array, false) shouldBe PropertyType.schema
            DynamicTypes.toConnectSchema(payloadMode, array, true) shouldBe PropertyType.schema
          }
        }
  }

  @ParameterizedTest(name = "{0}")
  @ArgumentsSource(PropertyTypedCollectionProvider::class)
  fun `collections with elements of property types should map to an array schema`(
      name: String,
      value: Any?
  ) {
    DynamicTypes.toConnectSchema(payloadMode, value, false) shouldBe
        SchemaBuilder.array(PropertyType.schema).build()
    DynamicTypes.toConnectSchema(payloadMode, value, true) shouldBe
        SchemaBuilder.array(PropertyType.schema).optional().build()
  }

  object PropertyTypedCollectionProvider : ArgumentsProvider {
    override fun provideArguments(ctx: ExtensionContext?): Stream<out Arguments> {
      return Stream.of(
          Arguments.of(
              "list of mixed simple types",
              listOf(1, true, "a", 5.toFloat(), LocalDate.of(1999, 1, 1))),
          Arguments.of(
              "list of mixed types",
              listOf(
                  1,
                  true,
                  "a",
                  5.toFloat(),
                  LocalDate.of(1999, 1, 1),
                  IntArray(1) { 1 },
                  Array(1) { LocalTime.of(23, 59, 59) })))
    }
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

  @ParameterizedTest(name = "{0}")
  @ArgumentsSource(PropertyTypedMapProvider::class)
  fun `maps with property typed values should map to a map schema`(name: String, value: Any?) {
    DynamicTypes.toConnectSchema(payloadMode, value, false) shouldBe
        SchemaBuilder.map(Schema.STRING_SCHEMA, PropertyType.schema).build()
    DynamicTypes.toConnectSchema(payloadMode, value, true) shouldBe
        SchemaBuilder.map(Schema.STRING_SCHEMA, PropertyType.schema).optional().build()
  }

  object PropertyTypedMapProvider : ArgumentsProvider {
    override fun provideArguments(ctx: ExtensionContext?): Stream<out Arguments> {
      return Stream.of(
          Arguments.of("string to int", mapOf("a" to 1, "b" to 2, "c" to 3)),
          Arguments.of("string to string", mapOf("a" to "a", "b" to "b", "c" to "c")),
          Arguments.of(
              "string to numeric",
              mapOf("a" to 1, "b" to 2.toShort(), "c" to 3.toLong(), "d" to 4.toFloat())),
          Arguments.of(
              "string to mixed simple type",
              mapOf("a" to 1, "b" to true, "c" to "string", "d" to 4.toFloat())),
          Arguments.of(
              "string to mixed",
              mapOf(
                  "a" to 1,
                  "b" to true,
                  "c" to "string",
                  "d" to 4.toFloat(),
                  "e" to Array(1) { LocalDate.of(1999, 1, 1) })),
      )
    }
  }

  @Test
  fun `unsupported types should throw`() {
    data class Test(val a: String)

    listOf(object {}, java.sql.Date(0), object : Entity(emptyMap()) {}, Test("a string")).forEach {
      shouldThrow<IllegalArgumentException> { DynamicTypes.toConnectSchema(payloadMode, it, false) }
    }
  }

  @Test
  fun `maps should be returned as maps of structs and should be converted back`() {
    listOf(
            mapOf("a" to "x", "b" to "y", "c" to "z") to
                mapOf(
                    "a" to PropertyType.toConnectValue("x"),
                    "b" to PropertyType.toConnectValue("y"),
                    "c" to PropertyType.toConnectValue("z")),
            mapOf("a" to 1, "b" to 2, "c" to 3) to
                mapOf(
                    "a" to PropertyType.toConnectValue(1L),
                    "b" to PropertyType.toConnectValue(2L),
                    "c" to PropertyType.toConnectValue(3L)))
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
            .put("name", PropertyType.toConnectValue("john"))
            .put("surname", PropertyType.toConnectValue("doe"))

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
            .put("name", PropertyType.toConnectValue("john"))
            .put("surname", PropertyType.toConnectValue("doe"))

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
  fun `maps with values of different simple types should be returned as map of property types and should be converted back`() {
    val map =
        mapOf(
            "name" to "john",
            "age" to 21,
            "dob" to LocalDate.of(1999, 12, 31),
            "employed" to true,
            "nullable" to null)
    val schema = DynamicTypes.toConnectSchema(payloadMode, map, false)
    val converted = DynamicTypes.toConnectValue(schema, map)

    converted shouldBe
        mapOf(
            "name" to PropertyType.toConnectValue("john"),
            "age" to PropertyType.toConnectValue(21L),
            "dob" to
                PropertyType.getPropertyStruct(
                    LOCAL_DATE, DateTimeFormatter.ISO_DATE.format(LocalDate.of(1999, 12, 31))),
            "employed" to PropertyType.toConnectValue(true),
            "nullable" to null)

    val reverted = DynamicTypes.fromConnectValue(schema, converted)
    reverted shouldBe map
  }

  @Test
  fun `collections with elements of different types should be returned as list of property types and should be converted back`() {
    val coll = listOf("john", 21, LocalDate.of(1999, 12, 31), true, null)
    val schema = DynamicTypes.toConnectSchema(payloadMode, coll, false)
    val converted = DynamicTypes.toConnectValue(schema, coll)

    converted shouldBe
        listOf(
            PropertyType.toConnectValue("john"),
            PropertyType.toConnectValue(21L),
            PropertyType.getPropertyStruct(
                LOCAL_DATE, DateTimeFormatter.ISO_DATE.format(LocalDate.of(1999, 12, 31))),
            PropertyType.toConnectValue(true),
            null)

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
            .field("dob", PropertyType.schema)
            .build()
    val struct =
        Struct(schema)
            .put("id", 1)
            .put("name", "john")
            .put("last_name", "doe")
            .put("dob", PropertyType.toConnectValue(LocalDate.of(2000, 1, 1)))

    DynamicTypes.fromConnectValue(schema, struct) shouldBe
        mapOf("id" to 1, "name" to "john", "last_name" to "doe", "dob" to LocalDate.of(2000, 1, 1))
  }

  @Test
  fun `structs with complex values should be returned as maps`() {
    val addressSchema =
        SchemaBuilder.struct()
            .field("city", PropertyType.schema)
            .field("country", PropertyType.schema)
            .build()
    val schema =
        SchemaBuilder.struct()
            .field("id", PropertyType.schema)
            .field("name", PropertyType.schema)
            .field("last_name", PropertyType.schema)
            .field("dob", PropertyType.schema)
            .field("address", addressSchema)
            .field("years_of_interest", PropertyType.schema)
            .field(
                "events_of_interest", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA))
            .build()
    val struct =
        Struct(schema)
            .put("id", PropertyType.toConnectValue(1L))
            .put("name", PropertyType.toConnectValue("john"))
            .put("last_name", PropertyType.toConnectValue("doe"))
            .put("dob", PropertyType.toConnectValue(LocalDate.of(2000, 1, 1)))
            .put(
                "address",
                Struct(addressSchema)
                    .put("city", PropertyType.toConnectValue("london"))
                    .put("country", PropertyType.toConnectValue("uk")))
            .put("years_of_interest", PropertyType.toConnectValue(listOf(2000L, 2005L, 2017L)))
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
