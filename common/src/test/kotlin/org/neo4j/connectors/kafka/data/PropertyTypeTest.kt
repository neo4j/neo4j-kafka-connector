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
import io.kotest.matchers.shouldBe
import io.kotest.matchers.throwable.shouldHaveMessage
import java.nio.ByteBuffer
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.OffsetTime
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util.stream.Stream
import org.apache.kafka.connect.data.Struct
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.ArgumentsProvider
import org.junit.jupiter.params.provider.ArgumentsSource
import org.neo4j.connectors.kafka.data.PropertyType.BOOLEAN
import org.neo4j.connectors.kafka.data.PropertyType.BYTES
import org.neo4j.connectors.kafka.data.PropertyType.DAYS
import org.neo4j.connectors.kafka.data.PropertyType.DIMENSION
import org.neo4j.connectors.kafka.data.PropertyType.DURATION
import org.neo4j.connectors.kafka.data.PropertyType.DURATION_LIST
import org.neo4j.connectors.kafka.data.PropertyType.FLOAT
import org.neo4j.connectors.kafka.data.PropertyType.FLOAT_LIST
import org.neo4j.connectors.kafka.data.PropertyType.LOCAL_DATE
import org.neo4j.connectors.kafka.data.PropertyType.LOCAL_DATE_LIST
import org.neo4j.connectors.kafka.data.PropertyType.LOCAL_DATE_TIME
import org.neo4j.connectors.kafka.data.PropertyType.LOCAL_DATE_TIME_LIST
import org.neo4j.connectors.kafka.data.PropertyType.LOCAL_TIME
import org.neo4j.connectors.kafka.data.PropertyType.LOCAL_TIME_LIST
import org.neo4j.connectors.kafka.data.PropertyType.LONG
import org.neo4j.connectors.kafka.data.PropertyType.LONG_LIST
import org.neo4j.connectors.kafka.data.PropertyType.MONTHS
import org.neo4j.connectors.kafka.data.PropertyType.NANOS
import org.neo4j.connectors.kafka.data.PropertyType.OFFSET_TIME_LIST
import org.neo4j.connectors.kafka.data.PropertyType.POINT
import org.neo4j.connectors.kafka.data.PropertyType.POINT_LIST
import org.neo4j.connectors.kafka.data.PropertyType.SECONDS
import org.neo4j.connectors.kafka.data.PropertyType.SR_ID
import org.neo4j.connectors.kafka.data.PropertyType.STRING
import org.neo4j.connectors.kafka.data.PropertyType.STRING_LIST
import org.neo4j.connectors.kafka.data.PropertyType.THREE_D
import org.neo4j.connectors.kafka.data.PropertyType.TWO_D
import org.neo4j.connectors.kafka.data.PropertyType.X
import org.neo4j.connectors.kafka.data.PropertyType.Y
import org.neo4j.connectors.kafka.data.PropertyType.Z
import org.neo4j.connectors.kafka.data.PropertyType.ZONED_DATE_TIME
import org.neo4j.connectors.kafka.data.PropertyType.ZONED_DATE_TIME_LIST
import org.neo4j.driver.Values

class PropertyTypeTest {

  @ParameterizedTest(name = "{0}")
  @ArgumentsSource(PropertyTypedValues::class)
  fun `simple types should be converted back and forth`(
      name: String,
      value: Any?,
      expectedConverted: Any?,
      expectedReverted: Any?
  ) {
    val converted = PropertyType.toConnectValue(value)
    converted shouldBe expectedConverted

    val reverted = PropertyType.fromConnectValue(converted)
    reverted shouldBe expectedReverted
  }

  object PropertyTypedValues : ArgumentsProvider {
    override fun provideArguments(p0: ExtensionContext?): Stream<out Arguments> {
      return Stream.of(
          Arguments.of("null", null, null, null),
          Arguments.of("boolean", true, Struct(PropertyType.schema).put(BOOLEAN, true), true),
          Arguments.of("byte", 1.toByte(), Struct(PropertyType.schema).put(LONG, 1L), 1L),
          Arguments.of("short", 1.toShort(), Struct(PropertyType.schema).put(LONG, 1L), 1L),
          Arguments.of("int", 1, Struct(PropertyType.schema).put(LONG, 1L), 1L),
          Arguments.of("long", 1L, Struct(PropertyType.schema).put(LONG, 1L), 1L),
          Arguments.of(
              "float",
              1.toFloat(),
              Struct(PropertyType.schema).put(FLOAT, 1.toDouble()),
              1.toDouble()),
          Arguments.of(
              "double",
              1.toDouble(),
              Struct(PropertyType.schema).put(FLOAT, 1.toDouble()),
              1.toDouble()),
          Arguments.of("char", 'c', Struct(PropertyType.schema).put(STRING, "c"), "c"),
          Arguments.of(
              "string", "string", Struct(PropertyType.schema).put(STRING, "string"), "string"),
          Arguments.of(
              "char array",
              "string".toCharArray(),
              Struct(PropertyType.schema).put(STRING, "string"),
              "string"),
          Arguments.of(
              "string builder",
              StringBuilder().append("string"),
              Struct(PropertyType.schema).put(STRING, "string"),
              "string"),
          Arguments.of(
              "string buffer",
              StringBuffer().append("string"),
              Struct(PropertyType.schema).put(STRING, "string"),
              "string"),
          Arguments.of(
              "local date",
              LocalDate.of(1999, 1, 1),
              Struct(PropertyType.schema).put(LOCAL_DATE, "1999-01-01"),
              LocalDate.of(1999, 1, 1)),
          Arguments.of(
              "local time",
              LocalTime.of(23, 59, 59, 999999999),
              Struct(PropertyType.schema).put(LOCAL_TIME, "23:59:59.999999999"),
              LocalTime.of(23, 59, 59, 999999999)),
          Arguments.of(
              "local date time",
              LocalDateTime.of(1999, 1, 1, 23, 59, 59, 999999999),
              Struct(PropertyType.schema).put(LOCAL_DATE_TIME, "1999-01-01T23:59:59.999999999"),
              LocalDateTime.of(1999, 1, 1, 23, 59, 59, 999999999)),
          Arguments.of(
              "offset date time",
              OffsetDateTime.of(1999, 1, 1, 23, 59, 59, 999999999, ZoneOffset.ofHours(1)),
              Struct(PropertyType.schema)
                  .put(ZONED_DATE_TIME, "1999-01-01T23:59:59.999999999+01:00"),
              OffsetDateTime.of(1999, 1, 1, 23, 59, 59, 999999999, ZoneOffset.ofHours(1))),
          Arguments.of(
              "zoned date time",
              ZonedDateTime.of(1999, 1, 1, 23, 59, 59, 999999999, ZoneId.of("Europe/Istanbul")),
              Struct(PropertyType.schema)
                  .put(ZONED_DATE_TIME, "1999-01-01T23:59:59.999999999+02:00[Europe/Istanbul]"),
              ZonedDateTime.of(1999, 1, 1, 23, 59, 59, 999999999, ZoneId.of("Europe/Istanbul"))),
          Arguments.of(
              "duration",
              Values.isoDuration(5, 20, 10000, 999999999).asIsoDuration(),
              Struct(PropertyType.schema)
                  .put(
                      DURATION,
                      Struct(PropertyType.durationSchema)
                          .put(MONTHS, 5L)
                          .put(DAYS, 20L)
                          .put(SECONDS, 10000L)
                          .put(NANOS, 999999999)),
              Values.isoDuration(5, 20, 10000, 999999999).asIsoDuration()),
          Arguments.of(
              "point (2d)",
              Values.point(4326, 1.0, 2.0).asPoint(),
              Struct(PropertyType.schema)
                  .put(
                      POINT,
                      Struct(PropertyType.pointSchema)
                          .put(SR_ID, 4326)
                          .put(X, 1.0)
                          .put(Y, 2.0)
                          .put(DIMENSION, TWO_D)),
              Values.point(4326, 1.0, 2.0).asPoint()),
          Arguments.of(
              "point (3d)",
              Values.point(4326, 1.0, 2.0, 3.0).asPoint(),
              Struct(PropertyType.schema)
                  .put(
                      POINT,
                      Struct(PropertyType.pointSchema)
                          .put(SR_ID, 4326)
                          .put(X, 1.0)
                          .put(Y, 2.0)
                          .put(Z, 3.0)
                          .put(DIMENSION, THREE_D)),
              Values.point(4326, 1.0, 2.0, 3.0).asPoint()),
          Arguments.of(
              "byte array",
              "a string".toByteArray(),
              Struct(PropertyType.schema).put(BYTES, "a string".toByteArray()),
              "a string".toByteArray()),
          Arguments.of(
              "byte buffer",
              ByteBuffer.allocate(1).put(1),
              Struct(PropertyType.schema).put(BYTES, ByteArray(1) { 1 }),
              ByteArray(1) { 1 }),
          Arguments.of(
              "array (byte)",
              Array(1) { 1.toByte() },
              Struct(PropertyType.schema).put(BYTES, ByteArray(1) { 1 }),
              ByteArray(1) { 1 }),
          Arguments.of(
              "list (byte)",
              listOf(1.toByte(), 1.toByte()),
              Struct(PropertyType.schema).put(BYTES, ByteArray(2) { 1 }),
              ByteArray(2) { 1.toByte() }),
          Arguments.of(
              "short array (empty)",
              ShortArray(0),
              Struct(PropertyType.schema).put(LONG_LIST, emptyList<Long>()),
              emptyList<Long>()),
          Arguments.of(
              "short array",
              ShortArray(1) { 1.toShort() },
              Struct(PropertyType.schema).put(LONG_LIST, listOf(1L)),
              listOf(1L)),
          Arguments.of(
              "array (short)",
              Array(1) { 1.toShort() },
              Struct(PropertyType.schema).put(LONG_LIST, listOf(1L)),
              listOf(1L)),
          Arguments.of(
              "list (short)",
              listOf(1.toShort(), 2.toShort()),
              Struct(PropertyType.schema).put(LONG_LIST, listOf(1L, 2L)),
              listOf(1L, 2L)),
          Arguments.of(
              "int array (empty)",
              IntArray(0),
              Struct(PropertyType.schema).put(LONG_LIST, emptyList<Long>()),
              emptyList<Long>()),
          Arguments.of(
              "int array",
              IntArray(1) { 1 },
              Struct(PropertyType.schema).put(LONG_LIST, listOf(1L)),
              listOf(1L)),
          Arguments.of(
              "array (int)",
              Array(1) { 1 },
              Struct(PropertyType.schema).put(LONG_LIST, listOf(1L)),
              listOf(1L)),
          Arguments.of(
              "list (int)",
              listOf(1, 2),
              Struct(PropertyType.schema).put(LONG_LIST, listOf(1L, 2L)),
              listOf(1L, 2L)),
          Arguments.of(
              "long array (empty)",
              LongArray(0),
              Struct(PropertyType.schema).put(LONG_LIST, emptyList<Long>()),
              emptyList<Long>()),
          Arguments.of(
              "long array",
              LongArray(1) { 1.toLong() },
              Struct(PropertyType.schema).put(LONG_LIST, listOf(1L)),
              listOf(1L)),
          Arguments.of(
              "array (long)",
              Array(1) { 1.toLong() },
              Struct(PropertyType.schema).put(LONG_LIST, listOf(1L)),
              listOf(1L)),
          Arguments.of(
              "list (long)",
              listOf(1.toLong(), 2.toLong()),
              Struct(PropertyType.schema).put(LONG_LIST, listOf(1L, 2L)),
              listOf(1L, 2L)),
          Arguments.of(
              "float array (empty)",
              FloatArray(0),
              Struct(PropertyType.schema).put(FLOAT_LIST, emptyList<Double>()),
              emptyList<Float>()),
          Arguments.of(
              "float array",
              FloatArray(1) { 1.toFloat() },
              Struct(PropertyType.schema).put(FLOAT_LIST, listOf(1.toDouble())),
              listOf(1.toDouble())),
          Arguments.of(
              "array (float)",
              Array(1) { 1.toFloat() },
              Struct(PropertyType.schema).put(FLOAT_LIST, listOf(1.toDouble())),
              listOf(1.toDouble())),
          Arguments.of(
              "list (float)",
              listOf(1.toFloat(), 2.toFloat()),
              Struct(PropertyType.schema).put(FLOAT_LIST, listOf(1.toDouble(), 2.toDouble())),
              listOf(1.toDouble(), 2.toDouble())),
          Arguments.of(
              "double array (empty)",
              DoubleArray(0),
              Struct(PropertyType.schema).put(FLOAT_LIST, emptyList<Double>()),
              emptyList<Double>()),
          Arguments.of(
              "double array",
              DoubleArray(1) { 1.toDouble() },
              Struct(PropertyType.schema).put(FLOAT_LIST, listOf(1.toDouble())),
              listOf(1.toDouble())),
          Arguments.of(
              "array (double)",
              Array(1) { 1.toDouble() },
              Struct(PropertyType.schema).put(FLOAT_LIST, listOf(1.toDouble())),
              listOf(1.toDouble())),
          Arguments.of(
              "list (double)",
              listOf(1.toDouble(), 2.toDouble()),
              Struct(PropertyType.schema).put(FLOAT_LIST, listOf(1.toDouble(), 2.toDouble())),
              listOf(1.toDouble(), 2.toDouble())),
          Arguments.of(
              "array (string)",
              Array(1) { "a" },
              Struct(PropertyType.schema).put(STRING_LIST, listOf("a")),
              listOf("a")),
          Arguments.of(
              "list (string)",
              listOf("a", "b"),
              Struct(PropertyType.schema).put(STRING_LIST, listOf("a", "b")),
              listOf("a", "b")),
          Arguments.of(
              "array (local date)",
              Array(1) { LocalDate.of(1999, 1, 1) },
              Struct(PropertyType.schema).put(LOCAL_DATE_LIST, listOf("1999-01-01")),
              listOf(LocalDate.of(1999, 1, 1))),
          Arguments.of(
              "list (local date)",
              listOf(LocalDate.of(1999, 1, 1)),
              Struct(PropertyType.schema).put(LOCAL_DATE_LIST, listOf("1999-01-01")),
              listOf(LocalDate.of(1999, 1, 1))),
          Arguments.of(
              "array (local time)",
              Array(1) { LocalTime.of(23, 59, 59, 999999999) },
              Struct(PropertyType.schema).put(LOCAL_TIME_LIST, listOf("23:59:59.999999999")),
              listOf(LocalTime.of(23, 59, 59, 999999999))),
          Arguments.of(
              "list (local time)",
              listOf(LocalTime.of(23, 59, 59, 999999999)),
              Struct(PropertyType.schema).put(LOCAL_TIME_LIST, listOf("23:59:59.999999999")),
              listOf(LocalTime.of(23, 59, 59, 999999999))),
          Arguments.of(
              "array (local date time)",
              Array(1) { LocalDateTime.of(1999, 1, 1, 23, 59, 59, 999999999) },
              Struct(PropertyType.schema)
                  .put(LOCAL_DATE_TIME_LIST, listOf("1999-01-01T23:59:59.999999999")),
              listOf(LocalDateTime.of(1999, 1, 1, 23, 59, 59, 999999999))),
          Arguments.of(
              "list (local date time)",
              listOf(LocalDateTime.of(1999, 1, 1, 23, 59, 59, 999999999)),
              Struct(PropertyType.schema)
                  .put(LOCAL_DATE_TIME_LIST, listOf("1999-01-01T23:59:59.999999999")),
              listOf(LocalDateTime.of(1999, 1, 1, 23, 59, 59, 999999999))),
          Arguments.of(
              "array (offset date time)",
              Array(1) {
                OffsetDateTime.of(1999, 1, 1, 23, 59, 59, 999999999, ZoneOffset.ofHours(2))
              },
              Struct(PropertyType.schema)
                  .put(ZONED_DATE_TIME_LIST, listOf("1999-01-01T23:59:59.999999999+02:00")),
              listOf(OffsetDateTime.of(1999, 1, 1, 23, 59, 59, 999999999, ZoneOffset.ofHours(2)))),
          Arguments.of(
              "list (offset date time)",
              listOf(OffsetDateTime.of(1999, 1, 1, 23, 59, 59, 999999999, ZoneOffset.ofHours(2))),
              Struct(PropertyType.schema)
                  .put(ZONED_DATE_TIME_LIST, listOf("1999-01-01T23:59:59.999999999+02:00")),
              listOf(OffsetDateTime.of(1999, 1, 1, 23, 59, 59, 999999999, ZoneOffset.ofHours(2)))),
          Arguments.of(
              "array (zoned date time)",
              Array(1) {
                ZonedDateTime.of(1999, 1, 1, 23, 59, 59, 999999999, ZoneId.of("Europe/Istanbul"))
              },
              Struct(PropertyType.schema)
                  .put(
                      ZONED_DATE_TIME_LIST,
                      listOf("1999-01-01T23:59:59.999999999+02:00[Europe/Istanbul]")),
              listOf(
                  ZonedDateTime.of(
                      1999, 1, 1, 23, 59, 59, 999999999, ZoneId.of("Europe/Istanbul")))),
          Arguments.of(
              "list (zoned date time)",
              listOf(
                  ZonedDateTime.of(
                      1999, 1, 1, 23, 59, 59, 999999999, ZoneId.of("Europe/Istanbul"))),
              Struct(PropertyType.schema)
                  .put(
                      ZONED_DATE_TIME_LIST,
                      listOf("1999-01-01T23:59:59.999999999+02:00[Europe/Istanbul]")),
              listOf(
                  ZonedDateTime.of(
                      1999, 1, 1, 23, 59, 59, 999999999, ZoneId.of("Europe/Istanbul")))),
          Arguments.of(
              "array (offset time)",
              Array(1) { OffsetTime.of(23, 59, 59, 999999999, ZoneOffset.ofHours(2)) },
              Struct(PropertyType.schema).put(OFFSET_TIME_LIST, listOf("23:59:59.999999999+02:00")),
              listOf(OffsetTime.of(23, 59, 59, 999999999, ZoneOffset.ofHours(2)))),
          Arguments.of(
              "list (offset time)",
              listOf(OffsetTime.of(23, 59, 59, 999999999, ZoneOffset.ofHours(2))),
              Struct(PropertyType.schema).put(OFFSET_TIME_LIST, listOf("23:59:59.999999999+02:00")),
              listOf(OffsetTime.of(23, 59, 59, 999999999, ZoneOffset.ofHours(2)))),
          Arguments.of(
              "array (duration)",
              Array(1) { Values.isoDuration(12, 12, 59, 1230).asIsoDuration() },
              Struct(PropertyType.schema)
                  .put(
                      DURATION_LIST,
                      listOf(
                          Struct(PropertyType.durationSchema)
                              .put(MONTHS, 12L)
                              .put(DAYS, 12L)
                              .put(SECONDS, 59L)
                              .put(NANOS, 1230))),
              listOf(Values.isoDuration(12, 12, 59, 1230).asIsoDuration())),
          Arguments.of(
              "list (duration)",
              listOf(Values.isoDuration(12, 12, 59, 1230).asIsoDuration()),
              Struct(PropertyType.schema)
                  .put(
                      DURATION_LIST,
                      listOf(
                          Struct(PropertyType.durationSchema)
                              .put(MONTHS, 12L)
                              .put(DAYS, 12L)
                              .put(SECONDS, 59L)
                              .put(NANOS, 1230))),
              listOf(Values.isoDuration(12, 12, 59, 1230).asIsoDuration())),
          Arguments.of(
              "array (point (2d))",
              Array(1) { Values.point(4326, 1.0, 2.0).asPoint() },
              Struct(PropertyType.schema)
                  .put(
                      POINT_LIST,
                      listOf(
                          Struct(PropertyType.pointSchema)
                              .put(SR_ID, 4326)
                              .put(X, 1.0)
                              .put(Y, 2.0)
                              .put(DIMENSION, TWO_D))),
              listOf(Values.point(4326, 1.0, 2.0).asPoint())),
          Arguments.of(
              "list (point (2d))",
              listOf(Values.point(4326, 1.0, 2.0).asPoint()),
              Struct(PropertyType.schema)
                  .put(
                      POINT_LIST,
                      listOf(
                          Struct(PropertyType.pointSchema)
                              .put(SR_ID, 4326)
                              .put(X, 1.0)
                              .put(Y, 2.0)
                              .put(DIMENSION, TWO_D))),
              listOf(Values.point(4326, 1.0, 2.0).asPoint())),
          Arguments.of(
              "array (point (3d))",
              Array(1) { Values.point(4326, 1.0, 2.0, 3.0).asPoint() },
              Struct(PropertyType.schema)
                  .put(
                      POINT_LIST,
                      listOf(
                          Struct(PropertyType.pointSchema)
                              .put(SR_ID, 4326)
                              .put(X, 1.0)
                              .put(Y, 2.0)
                              .put(Z, 3.0)
                              .put(DIMENSION, THREE_D))),
              listOf(Values.point(4326, 1.0, 2.0, 3.0).asPoint())),
          Arguments.of(
              "list (point (3d))",
              listOf(Values.point(4326, 1.0, 2.0, 3.0).asPoint()),
              Struct(PropertyType.schema)
                  .put(
                      POINT_LIST,
                      listOf(
                          Struct(PropertyType.pointSchema)
                              .put(SR_ID, 4326)
                              .put(X, 1.0)
                              .put(Y, 2.0)
                              .put(Z, 3.0)
                              .put(DIMENSION, THREE_D))),
              listOf(Values.point(4326, 1.0, 2.0, 3.0).asPoint())),
          Arguments.of(
              "empty list (any)",
              emptyList<Any>(),
              Struct(PropertyType.schema).put(LONG_LIST, emptyList<Long>()),
              emptyList<Any>()),
          Arguments.of(
              "empty list (typed)",
              emptyList<Int>(),
              Struct(PropertyType.schema).put(LONG_LIST, emptyList<Long>()),
              emptyList<Any>()),
      )
    }
  }

  @Test
  fun `should throw when unsupported type is provided`() {
    shouldThrow<IllegalArgumentException> {
      PropertyType.toConnectValue(java.sql.Timestamp.from(Instant.now()))
    } shouldHaveMessage "unsupported property type: java.sql.Timestamp"
  }

  @Test
  fun `should throw when unsupported array type is provided`() {
    shouldThrow<IllegalArgumentException> {
      PropertyType.toConnectValue(Array(1) { java.sql.Timestamp.from(Instant.now()) })
    } shouldHaveMessage "unsupported array type: array of java.sql.Timestamp"
  }
}
