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
package org.neo4j.connectors.kafka.data.converter

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.assertions.withClue
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.types.shouldBeInstanceOf
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.DynamicTest.dynamicTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestFactory
import org.neo4j.connectors.kafka.configuration.MapEncoding
import org.neo4j.connectors.kafka.data.DynamicTypes
import org.neo4j.connectors.kafka.data.PropertyType
import org.neo4j.connectors.kafka.data.Schemas
import org.neo4j.connectors.kafka.exceptions.InvalidDataException

/**
 * How `neo4j.query.map-encoding` describes a Neo4j map, and the bugs the encoding used to cause.
 * Every schema here is built the way the source connector builds one, with optional schemas.
 */
class MapEncodingTest {

  private val struct = CompactValueConverter(MapEncoding.STRUCT)
  private val map = CompactValueConverter(MapEncoding.MAP)
  private val legacy = CompactValueConverter(MapEncoding.LEGACY)

  private val unknown = Schemas.UNKNOWN

  @TestFactory
  fun `struct encoding should describe every map as a struct`(): List<DynamicTest> =
      listOf(
              mapOf("a" to 1L, "b" to 2L) to
                  structSchema(
                      "a" to Schema.OPTIONAL_INT64_SCHEMA,
                      "b" to Schema.OPTIONAL_INT64_SCHEMA,
                  ),
              mapOf("a" to 1L, "b" to "x") to
                  structSchema(
                      "a" to Schema.OPTIONAL_INT64_SCHEMA,
                      "b" to Schema.OPTIONAL_STRING_SCHEMA,
                  ),
              mapOf("o" to mapOf("x" to 1L, "y" to 2L)) to
                  structSchema(
                      "o" to
                          structSchema(
                              "x" to Schema.OPTIONAL_INT64_SCHEMA,
                              "y" to Schema.OPTIONAL_INT64_SCHEMA,
                          )
                  ),
          )
          .map { (value, expected) ->
            dynamicTest("$value") {
              withClue("$value") { struct.schema(value, true) shouldBe expected }
            }
          }

  @TestFactory
  fun `map encoding should describe every map as a map`(): List<DynamicTest> =
      listOf(
              mapOf("a" to 1L, "b" to 2L) to mapSchema(Schema.OPTIONAL_INT64_SCHEMA),
              mapOf("o" to mapOf("x" to 1L, "y" to 2L)) to
                  mapSchema(mapSchema(Schema.OPTIONAL_INT64_SCHEMA)),
          )
          .map { (value, expected) ->
            dynamicTest("$value") {
              withClue("$value") { map.schema(value, true) shouldBe expected }
            }
          }

  @TestFactory
  fun `map encoding should fail when the values of a map have no shared type`(): List<DynamicTest> =
      listOf(
              mapOf("a" to 1L, "b" to 2.5),
              mapOf("a" to 1L, "b" to "x"),
              mapOf("a" to mapOf("x" to 1L), "b" to 2L),
              mapOf("a" to emptyMap<String, Any>(), "b" to 1L),
          )
          .map { value ->
            dynamicTest("$value") { shouldThrow<InvalidDataException> { map.schema(value, true) } }
          }

  @Test
  fun `the error of map encoding should name the keys and the types found`() {
    val message =
        shouldThrow<InvalidDataException> { map.schema(mapOf("a" to 1L, "b" to "x"), true) }
            .message!!

    message shouldContain "'a' is INT64"
    message shouldContain "'b' is STRING"
    message shouldContain "neo4j.query.map-encoding"
  }

  @TestFactory
  fun `legacy encoding should describe a map as a map only when its values share a schema`():
      List<DynamicTest> =
      listOf(
              mapOf("a" to 1L, "b" to 2L) to mapSchema(Schema.OPTIONAL_INT64_SCHEMA),
              mapOf("a" to 1L, "b" to "x") to
                  structSchema(
                      "a" to Schema.OPTIONAL_INT64_SCHEMA,
                      "b" to Schema.OPTIONAL_STRING_SCHEMA,
                  ),
              // a map at one key and a number at another have no shared schema, so the outer map
              // is a struct
              mapOf("a" to mapOf("x" to 1L), "b" to 2L) to
                  structSchema(
                      "a" to mapSchema(Schema.OPTIONAL_INT64_SCHEMA),
                      "b" to Schema.OPTIONAL_INT64_SCHEMA,
                  ),
          )
          .map { (value, expected) ->
            dynamicTest("$value") {
              withClue("$value") { legacy.schema(value, true) shouldBe expected }
            }
          }

  @TestFactory
  fun `an empty map should be described as an empty struct unless a map is asked for`():
      List<DynamicTest> =
      listOf(
              Triple("STRUCT", struct, emptyMap<String, Any>()) to structSchema(),
              Triple("LEGACY", legacy, emptyMap<String, Any>()) to structSchema(),
              Triple("MAP", map, emptyMap<String, Any>()) to mapSchema(unknown),
              // a map holding only nulls has no value type to go on either
              Triple("STRUCT", struct, mapOf("a" to null)) to structSchema("a" to unknown),
              Triple("LEGACY", legacy, mapOf("a" to null)) to structSchema("a" to unknown),
          )
          .map { (case, expected) ->
            val (name, converter, value) = case
            dynamicTest("$name of $value") {
              withClue("$name of $value") { converter.schema(value, true) shouldBe expected }
            }
          }

  @TestFactory
  fun `an empty map beside a non-empty one should take the value type of the non-empty one`():
      List<DynamicTest> =
      listOf(
              "MAP" to (map to arraySchema(mapSchema(Schema.OPTIONAL_INT64_SCHEMA))),
              "STRUCT" to (struct to arraySchema(structSchema("a" to Schema.OPTIONAL_INT64_SCHEMA))),
          )
          .map { (name, case) ->
            val (converter, expected) = case
            dynamicTest(name) {
              converter.schema(listOf(emptyMap<String, Any>(), mapOf("a" to 1L)), true) shouldBe
                  expected
            }
          }

  @TestFactory
  fun `a row should always be a struct holding the columns in the order the query returned them`():
      List<DynamicTest> {
    val row = linkedMapOf<String, Any?>("name" to "john", "age" to 21L, "id" to 1L)
    val expected =
        SchemaBuilder.struct()
            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
            .field("age", Schema.OPTIONAL_INT64_SCHEMA)
            .field("id", Schema.OPTIONAL_INT64_SCHEMA)
            .optional()
            .build()

    return listOf("STRUCT" to struct, "MAP" to map, "LEGACY" to legacy).map { (name, converter) ->
      dynamicTest(name) { converter.rowSchema(row, true) shouldBe expected }
    }
  }

  @Test
  fun `a row whose columns share a type should still be a struct`() {
    val row = linkedMapOf<String, Any?>("b" to 2L, "a" to 1L)

    map.rowSchema(row, true) shouldBe
        SchemaBuilder.struct()
            .field("b", Schema.OPTIONAL_INT64_SCHEMA)
            .field("a", Schema.OPTIONAL_INT64_SCHEMA)
            .optional()
            .build()
  }

  @Test
  fun `the fields of a nested map should be sorted at every level`() {
    val value = mapOf("b" to 1L, "a" to mapOf("z" to "s", "y" to 2L))

    struct.schema(value, true).fields().map { it.name() } shouldBe listOf("a", "b")
    struct.schema(value, true).field("a").schema().fields().map { it.name() } shouldBe
        listOf("y", "z")
  }

  @TestFactory
  fun `a map holding an empty map next to a number should not be described as a map`():
      List<DynamicTest> =
      // an empty map and a number have no shared type, so the outer map cannot be described as
      // a MAP. It used to be, and building the message then failed reading the empty map as a
      // number.
      listOf("STRUCT" to struct, "LEGACY" to legacy).map { (name, converter) ->
        dynamicTest(name) {
          val value = mapOf("a" to emptyMap<String, Any>(), "b" to 1L)
          val schema = converter.schema(value, true)

          schema shouldBe structSchema("a" to structSchema(), "b" to Schema.OPTIONAL_INT64_SCHEMA)

          // the message used to fail here, reading the empty map as a number
          converter.value(schema, value).shouldBeInstanceOf<Struct>()
        }
      }

  @TestFactory
  fun `a list element whose values are all null should keep its keys`(): List<DynamicTest> =
      // an element holding only nulls used to be left out when the element schemas were
      // compared, so its key went missing from the schema and its value was dropped
      listOf(
              listOf(mapOf("a" to null), mapOf("b" to 2L)),
              listOf(mapOf("a" to null), mapOf("b" to 2L), mapOf("b" to 3L)),
          )
          .map { value ->
            dynamicTest("$value") {
              val schema = struct.schema(value, true)
              schema.valueSchema().fields().map { it.name() } shouldBe listOf("a", "b")

              val converted = struct.value(schema, value) as List<*>
              (converted.first() as Struct).get("a") shouldBe null
            }
          }

  @Test
  fun `the same data should produce the same struct whether or not a key is missing`() {
    // the field order and the optionality of the fields used to depend on whether a key was
    // missing from one of the elements
    val sameKeys = listOf(mapOf("a" to 1L, "b" to 2L), mapOf("a" to 3L, "b" to 4L))
    val missingKey = listOf(mapOf("a" to 1L, "b" to 2L), mapOf("a" to 3L))

    val expected =
        arraySchema(
            structSchema("a" to Schema.OPTIONAL_INT64_SCHEMA, "b" to Schema.OPTIONAL_INT64_SCHEMA)
        )

    struct.schema(sameKeys, true) shouldBe expected
    struct.schema(missingKey, true) shouldBe expected
  }

  @TestFactory
  fun `a map value type should admit a null entry`(): List<DynamicTest> =
      // a null entry used to leave the value type required, which the serialiser then rejected
      listOf("MAP" to map, "LEGACY" to legacy).map { (name, converter) ->
        dynamicTest(name) {
          val value = mapOf("a" to 1L, "b" to null)
          val schema = converter.schema(value, optional = false)

          schema shouldBe
              SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA).build()
          converter.value(schema, value) shouldBe mapOf("a" to 1L, "b" to null)
        }
      }

  @Test
  fun `an array should be typed from all of its elements`() {
    // an array used to take the schema of its first element and never look at the rest
    struct.schema(arrayOf(mapOf("a" to 1L), mapOf("b" to 2L)), true) shouldBe
        arraySchema(
            structSchema("a" to Schema.OPTIONAL_INT64_SCHEMA, "b" to Schema.OPTIONAL_INT64_SCHEMA)
        )
  }

  @Test
  fun `an array whose elements have no shared schema should keep one field per position`() {
    struct.schema(arrayOf<Any?>(1L, "x"), true) shouldBe
        SchemaBuilder.struct()
            .field("e0", Schema.OPTIONAL_INT64_SCHEMA)
            .field("e1", Schema.OPTIONAL_STRING_SCHEMA)
            .optional()
            .build()
  }

  @TestFactory
  fun `every encoding should decode back to the map it came from`(): List<DynamicTest> {
    val value = mapOf("a" to 1L, "b" to 2L)

    return listOf("STRUCT" to struct, "MAP" to map, "LEGACY" to legacy).map { (name, converter) ->
      dynamicTest(name) {
        val schema = converter.schema(value, true)
        DynamicTypes.fromConnectValue(schema, converter.value(schema, value)) shouldBe value
      }
    }
  }

  @TestFactory
  fun `legacy encoding should fall back to one field per position when list elements disagree`():
      List<DynamicTest> =
      // under LEGACY an element whose values share a schema is a MAP, while one whose values do
      // not, one holding only nulls and an empty one are all STRUCTs. A MAP and a STRUCT have no
      // shared schema, so such a list cannot be described as an array of one element schema.
      listOf(
              listOf(mapOf("a" to 1L), mapOf("a" to 1L, "b" to "s")),
              listOf(mapOf("a" to null), mapOf("b" to 2L)),
              listOf(emptyMap<String, Any>(), mapOf("a" to 1L)),
          )
          .map { value ->
            dynamicTest("$value") {
              legacy.schema(value, true).fields().map { it.name() } shouldBe listOf("e0", "e1")
            }
          }

  @Test
  fun `struct encoding should merge the list elements legacy encoding cannot`() {
    struct.schema(listOf(mapOf("a" to 1L), mapOf("a" to 1L, "b" to "s")), true) shouldBe
        arraySchema(
            structSchema("a" to Schema.OPTIONAL_INT64_SCHEMA, "b" to Schema.OPTIONAL_STRING_SCHEMA)
        )
  }

  @TestFactory
  fun `extended payload mode should follow the map encoding too`(): List<DynamicTest> {
    val value = mapOf("b" to 1L, "a" to "x")

    return listOf(
            // every property value has the same property-type schema, so a mixture of them still
            // shares one
            MapEncoding.MAP to mapSchema(PropertyType.schema),
            MapEncoding.LEGACY to mapSchema(PropertyType.schema),
            MapEncoding.STRUCT to
                structSchema("a" to PropertyType.schema, "b" to PropertyType.schema),
        )
        .map { (encoding, expected) ->
          dynamicTest(encoding.name) {
            ExtendedValueConverter(encoding).schema(value, true) shouldBe expected
          }
        }
  }

  @Test
  fun `extended map encoding should fail on a map holding a property value and a nested map`() {
    shouldThrow<InvalidDataException> {
      ExtendedValueConverter(MapEncoding.MAP)
          .schema(mapOf("a" to 1L, "b" to mapOf("x" to 1L)), true)
    }
  }

  private fun structSchema(vararg fields: Pair<String, Schema>): Schema =
      SchemaBuilder.struct()
          .apply { fields.sortedBy { it.first }.forEach { field(it.first, it.second) } }
          .optional()
          .build()

  private fun mapSchema(valueSchema: Schema): Schema =
      SchemaBuilder.map(Schema.STRING_SCHEMA, valueSchema).optional().build()

  private fun arraySchema(elementSchema: Schema): Schema =
      SchemaBuilder.array(elementSchema).optional().build()
}
