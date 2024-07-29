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

import java.nio.ByteBuffer
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.OffsetTime
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.TemporalQueries
import kotlin.reflect.KClass
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.neo4j.driver.Values
import org.neo4j.driver.types.IsoDuration
import org.neo4j.driver.types.Point

@Suppress("UNCHECKED_CAST")
object PropertyType {
  internal const val MONTHS = "months"
  internal const val DAYS = "days"
  internal const val SECONDS = "seconds"
  internal const val NANOS = "nanoseconds"
  internal const val SR_ID = "srid"
  internal const val X = "x"
  internal const val Y = "y"
  internal const val Z = "z"
  internal const val DIMENSION = "dimension"
  internal const val TWO_D: Byte = 2
  internal const val THREE_D: Byte = 3

  internal const val BOOLEAN = "B"
  internal const val BOOLEAN_LIST = "LB"
  internal const val LONG = "I64"
  internal const val LONG_LIST = "LI64"
  internal const val FLOAT = "F64"
  internal const val FLOAT_LIST = "LF64"
  internal const val STRING = "S"
  internal const val STRING_LIST = "LS"
  internal const val BYTES = "BA"
  internal const val LOCAL_DATE = "TLD"
  internal const val LOCAL_DATE_LIST = "LTLD"
  internal const val LOCAL_DATE_TIME = "TLDT"
  internal const val LOCAL_DATE_TIME_LIST = "LTLDT"
  internal const val LOCAL_TIME = "TLT"
  internal const val LOCAL_TIME_LIST = "LTLT"
  internal const val ZONED_DATE_TIME = "TZDT"
  internal const val ZONED_DATE_TIME_LIST = "LZDT"
  internal const val OFFSET_TIME = "TOT"
  internal const val OFFSET_TIME_LIST = "LTOT"
  internal const val DURATION = "TD"
  internal const val DURATION_LIST = "LTD"
  internal const val POINT = "SP"
  internal const val POINT_LIST = "LSP"

  internal val durationSchema: Schema =
      SchemaBuilder(Schema.Type.STRUCT)
          .field(MONTHS, Schema.INT64_SCHEMA)
          .field(DAYS, Schema.INT64_SCHEMA)
          .field(SECONDS, Schema.INT64_SCHEMA)
          .field(NANOS, Schema.INT32_SCHEMA)
          .optional()
          .build()

  internal val pointSchema: Schema =
      SchemaBuilder(Schema.Type.STRUCT)
          .field(DIMENSION, Schema.INT8_SCHEMA)
          .field(SR_ID, Schema.INT32_SCHEMA)
          .field(X, Schema.FLOAT64_SCHEMA)
          .field(Y, Schema.FLOAT64_SCHEMA)
          .field(Z, Schema.OPTIONAL_FLOAT64_SCHEMA)
          .optional()
          .build()

  val schema: Schema =
      SchemaBuilder.struct()
          .namespaced("Neo4jPropertyType")
          .field(BOOLEAN, Schema.OPTIONAL_BOOLEAN_SCHEMA)
          .field(LONG, Schema.OPTIONAL_INT64_SCHEMA)
          .field(FLOAT, Schema.OPTIONAL_FLOAT64_SCHEMA)
          .field(STRING, Schema.OPTIONAL_STRING_SCHEMA)
          .field(BYTES, Schema.OPTIONAL_BYTES_SCHEMA)
          .field(LOCAL_DATE, Schema.OPTIONAL_STRING_SCHEMA)
          .field(LOCAL_DATE_TIME, Schema.OPTIONAL_STRING_SCHEMA)
          .field(LOCAL_TIME, Schema.OPTIONAL_STRING_SCHEMA)
          .field(ZONED_DATE_TIME, Schema.OPTIONAL_STRING_SCHEMA)
          .field(OFFSET_TIME, Schema.OPTIONAL_STRING_SCHEMA)
          .field(DURATION, durationSchema)
          .field(POINT, pointSchema)
          .field(BOOLEAN_LIST, SchemaBuilder.array(Schema.BOOLEAN_SCHEMA).optional().build())
          .field(LONG_LIST, SchemaBuilder.array(Schema.INT64_SCHEMA).optional().build())
          .field(FLOAT_LIST, SchemaBuilder.array(Schema.FLOAT64_SCHEMA).optional().build())
          .field(STRING_LIST, SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())
          .field(LOCAL_DATE_LIST, SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())
          .field(LOCAL_DATE_TIME_LIST, SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())
          .field(LOCAL_TIME_LIST, SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())
          .field(ZONED_DATE_TIME_LIST, SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())
          .field(OFFSET_TIME_LIST, SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())
          .field(DURATION_LIST, SchemaBuilder.array(durationSchema).optional().build())
          .field(POINT_LIST, SchemaBuilder.array(pointSchema).optional().build())
          .optional()
          .build()

  fun toConnectValue(value: Any?): Struct? {
    return when (value) {
      null -> null
      is Boolean -> Struct(schema).put(BOOLEAN, value)
      is Float -> Struct(schema).put(FLOAT, value.toDouble())
      is Double -> Struct(schema).put(FLOAT, value)
      is Number -> Struct(schema).put(LONG, value.toLong())
      is String -> Struct(schema).put(STRING, value)
      is Char -> Struct(schema).put(STRING, value.toString())
      is CharArray -> Struct(schema).put(STRING, String(value))
      is CharSequence ->
          Struct(schema).put(STRING, value.codePoints().toArray().let { String(it, 0, it.size) })
      is ByteArray -> Struct(schema).put(BYTES, value)
      is ByteBuffer -> Struct(schema).put(BYTES, value.array())
      is LocalDate -> Struct(schema).put(LOCAL_DATE, DateTimeFormatter.ISO_DATE.format(value))
      is LocalDateTime ->
          Struct(schema).put(LOCAL_DATE_TIME, DateTimeFormatter.ISO_DATE_TIME.format(value))
      is LocalTime -> Struct(schema).put(LOCAL_TIME, DateTimeFormatter.ISO_TIME.format(value))
      is OffsetDateTime ->
          Struct(schema).put(ZONED_DATE_TIME, DateTimeFormatter.ISO_DATE_TIME.format(value))
      is ZonedDateTime ->
          Struct(schema).put(ZONED_DATE_TIME, DateTimeFormatter.ISO_DATE_TIME.format(value))
      is OffsetTime -> Struct(schema).put(OFFSET_TIME, DateTimeFormatter.ISO_TIME.format(value))
      is IsoDuration ->
          Struct(schema)
              .put(
                  DURATION,
                  Struct(durationSchema)
                      .put(MONTHS, value.months())
                      .put(DAYS, value.days())
                      .put(SECONDS, value.seconds())
                      .put(NANOS, value.nanoseconds()))
      is Point ->
          Struct(schema)
              .put(
                  POINT,
                  Struct(pointSchema)
                      .put(SR_ID, value.srid())
                      .put(X, value.x())
                      .put(Y, value.y())
                      .also {
                        it.put(DIMENSION, if (value.z().isNaN()) TWO_D else THREE_D)
                        if (!value.z().isNaN()) {
                          it.put(Z, value.z())
                        }
                      })
      is ShortArray -> Struct(schema).put(LONG_LIST, value.map { s -> s.toLong() }.toList())
      is IntArray -> Struct(schema).put(LONG_LIST, value.map { s -> s.toLong() }.toList())
      is LongArray -> Struct(schema).put(LONG_LIST, value.toList())
      is FloatArray -> Struct(schema).put(FLOAT_LIST, value.map { s -> s.toDouble() }.toList())
      is DoubleArray -> Struct(schema).put(FLOAT_LIST, value.toList())
      is BooleanArray -> Struct(schema).put(BOOLEAN_LIST, value.toList())
      is Array<*> -> asList(value.toList(), value::class.java.componentType.kotlin)
      is Iterable<*> -> {
        val elementTypes = value.map { it?.javaClass?.kotlin }.toSet()
        val elementType = elementTypes.singleOrNull()
        if (elementType != null) {
          return asList(value, elementType)
        }

        throw IllegalArgumentException(
            "collections with multiple element types are not supported: ${elementTypes.joinToString { it?.java?.name ?: "null" }}")
      }
      else -> throw IllegalArgumentException("unsupported property type: ${value.javaClass.name}")
    }
  }

  private fun asList(value: Iterable<*>, componentType: KClass<*>): Struct? =
      when (componentType) {
        Boolean::class -> Struct(schema).put(BOOLEAN_LIST, value)
        Byte::class -> Struct(schema).put(BYTES, (value as List<Byte>).toByteArray())
        Short::class ->
            Struct(schema).put(LONG_LIST, (value as List<Short>).map { s -> s.toLong() })
        Int::class -> Struct(schema).put(LONG_LIST, (value as List<Int>).map { s -> s.toLong() })
        Long::class -> Struct(schema).put(LONG_LIST, value)
        Float::class ->
            Struct(schema).put(FLOAT_LIST, (value as List<Float>).map { s -> s.toDouble() })
        Double::class -> Struct(schema).put(FLOAT_LIST, value)
        String::class -> Struct(schema).put(STRING_LIST, value)
        LocalDate::class ->
            Struct(schema)
                .put(
                    LOCAL_DATE_LIST,
                    (value as List<LocalDate>).map { s -> DateTimeFormatter.ISO_DATE.format(s) })
        LocalDateTime::class ->
            Struct(schema)
                .put(
                    LOCAL_DATE_TIME_LIST,
                    (value as List<LocalDateTime>).map { s ->
                      DateTimeFormatter.ISO_DATE_TIME.format(s)
                    })
        LocalTime::class ->
            Struct(schema)
                .put(
                    LOCAL_TIME_LIST,
                    (value as List<LocalTime>).map { s -> DateTimeFormatter.ISO_TIME.format(s) })
        OffsetDateTime::class ->
            Struct(schema)
                .put(
                    ZONED_DATE_TIME_LIST,
                    (value as List<OffsetDateTime>).map { s ->
                      DateTimeFormatter.ISO_DATE_TIME.format(s)
                    })
        ZonedDateTime::class ->
            Struct(schema)
                .put(
                    ZONED_DATE_TIME_LIST,
                    (value as List<ZonedDateTime>).map { s ->
                      DateTimeFormatter.ISO_DATE_TIME.format(s)
                    })
        OffsetTime::class ->
            Struct(schema)
                .put(
                    OFFSET_TIME_LIST,
                    (value as List<OffsetTime>).map { s -> DateTimeFormatter.ISO_TIME.format(s) })
        else ->
            if (IsoDuration::class.java.isAssignableFrom(componentType.java)) {
              Struct(schema)
                  .put(
                      DURATION_LIST,
                      value
                          .map { s -> s as IsoDuration }
                          .map {
                            Struct(durationSchema)
                                .put(MONTHS, it.months())
                                .put(DAYS, it.days())
                                .put(SECONDS, it.seconds())
                                .put(NANOS, it.nanoseconds())
                          })
            } else if (Point::class.java.isAssignableFrom(componentType.java)) {
              Struct(schema)
                  .put(
                      POINT_LIST,
                      value
                          .map { s -> s as Point }
                          .map { s ->
                            Struct(pointSchema)
                                .put(SR_ID, s.srid())
                                .put(X, s.x())
                                .put(Y, s.y())
                                .also {
                                  it.put(DIMENSION, if (s.z().isNaN()) TWO_D else THREE_D)
                                  if (!s.z().isNaN()) {
                                    it.put(Z, s.z())
                                  }
                                }
                          })
            } else {
              throw IllegalArgumentException(
                  "unsupported array type: array of ${componentType.java.name}")
            }
      }

  fun fromConnectValue(value: Struct?): Any? {
    return value?.let {
      for (f in it.schema().fields()) {
        val fieldValue = it.getWithoutDefault(f.name())
        // not set list fields are returned back as empty lists, so we are looking for a non-empty
        // field here
        if (fieldValue == null || (fieldValue is Collection<*> && fieldValue.isEmpty())) {
          continue
        }

        return when (f.name()) {
          BOOLEAN -> it.get(f) as Boolean
          BOOLEAN_LIST -> it.get(f) as List<*>
          LONG -> it.get(f) as Long
          LONG_LIST -> it.get(f) as List<*>
          FLOAT -> it.get(f) as Double
          FLOAT_LIST -> it.get(f) as List<*>
          STRING -> it.get(f) as String
          STRING_LIST -> it.get(f) as List<*>
          BYTES ->
              when (val bytes = it.get(f)) {
                is ByteArray -> bytes
                is ByteBuffer -> bytes.array()
                else ->
                    throw IllegalArgumentException(
                        "unsupported BYTES value: ${bytes.javaClass.name}")
              }
          LOCAL_DATE -> parseLocalDate((it.get(f) as String))
          LOCAL_DATE_LIST -> (it.get(f) as List<String>).map { s -> parseLocalDate(s) }
          LOCAL_TIME -> parseLocalTime((it.get(f) as String))
          LOCAL_TIME_LIST -> (it.get(f) as List<String>).map { s -> parseLocalTime(s) }
          LOCAL_DATE_TIME -> parseLocalDateTime((it.get(f) as String))
          LOCAL_DATE_TIME_LIST -> (it.get(f) as List<String>).map { s -> parseLocalDateTime(s) }
          ZONED_DATE_TIME -> parseZonedDateTime((it.get(f) as String))
          ZONED_DATE_TIME_LIST -> (it.get(f) as List<String>).map { s -> parseZonedDateTime(s) }
          OFFSET_TIME -> parseOffsetTime((it.get(f) as String))
          OFFSET_TIME_LIST -> (it.get(f) as List<String>).map { s -> parseOffsetTime(s) }
          DURATION -> toDuration((it.get(f) as Struct))
          DURATION_LIST -> (it.get(f) as List<Struct>).map { s -> toDuration(s) }
          POINT -> toPoint((it.get(f) as Struct))
          POINT_LIST -> (it.get(f) as List<Struct>).map { s -> toPoint(s) }
          else -> throw IllegalArgumentException("unsupported neo4j type: ${f.name()}")
        }
      }

      return null
    }
  }

  private fun parseLocalDate(s: String): LocalDate =
      DateTimeFormatter.ISO_DATE.parse(s) { parsed -> LocalDate.from(parsed) }

  private fun parseLocalTime(s: String): LocalTime =
      DateTimeFormatter.ISO_TIME.parse(s) { parsed -> LocalTime.from(parsed) }

  private fun parseLocalDateTime(s: String): LocalDateTime =
      DateTimeFormatter.ISO_DATE_TIME.parse(s) { parsed -> LocalDateTime.from(parsed) }

  private fun parseZonedDateTime(s: String) =
      DateTimeFormatter.ISO_DATE_TIME.parse(s) { parsed ->
        val zoneId = parsed.query(TemporalQueries.zone())

        if (zoneId is ZoneOffset) {
          OffsetDateTime.from(parsed)
        } else {
          ZonedDateTime.from(parsed)
        }
      }

  private fun parseOffsetTime(s: String): OffsetTime =
      DateTimeFormatter.ISO_TIME.parse(s) { parsed -> OffsetTime.from(parsed) }

  private fun toDuration(s: Struct): IsoDuration =
      Values.isoDuration(
              s.getInt64(MONTHS),
              s.getInt64(DAYS),
              s.getInt64(SECONDS),
              s.getInt32(NANOS),
          )
          .asIsoDuration()

  private fun toPoint(s: Struct): Point =
      when (val dimension = s.getInt8(DIMENSION)) {
        TWO_D -> Values.point(s.getInt32(SR_ID), s.getFloat64(X), s.getFloat64(Y))
        THREE_D ->
            Values.point(
                s.getInt32(SR_ID),
                s.getFloat64(X),
                s.getFloat64(Y),
                s.getFloat64(Z),
            )
        else -> throw IllegalArgumentException("unsupported dimension value ${dimension}")
      }.asPoint()
}
