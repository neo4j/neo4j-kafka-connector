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
  internal const val TYPE = "type"

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

  private val SIMPLE_TYPE_FIELDS =
      listOf(
          BOOLEAN,
          LONG,
          FLOAT,
          STRING,
          BYTES,
          LOCAL_DATE,
          LOCAL_DATE_TIME,
          LOCAL_TIME,
          ZONED_DATE_TIME,
          OFFSET_TIME,
          DURATION,
          POINT)
  private val LIST_TYPE_FIELDS =
      listOf(
          BOOLEAN_LIST,
          LONG_LIST,
          FLOAT_LIST,
          STRING_LIST,
          LOCAL_DATE_LIST,
          LOCAL_DATE_TIME_LIST,
          LOCAL_TIME_LIST,
          ZONED_DATE_TIME_LIST,
          OFFSET_TIME_LIST,
          DURATION_LIST,
          POINT_LIST)

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
          .field(TYPE, Schema.STRING_SCHEMA)
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
      is Boolean -> getPropertyStruct(BOOLEAN, value)
      is Float -> getPropertyStruct(FLOAT, value.toDouble())
      is Double -> getPropertyStruct(FLOAT, value)
      is Number -> getPropertyStruct(LONG, value.toLong())
      is String -> getPropertyStruct(STRING, value)
      is Char -> getPropertyStruct(STRING, value.toString())
      is CharArray -> getPropertyStruct(STRING, String(value))
      is CharSequence ->
          getPropertyStruct(STRING, value.codePoints().toArray().let { String(it, 0, it.size) })
      is ByteArray -> getPropertyStruct(BYTES, value)
      is ByteBuffer -> getPropertyStruct(BYTES, value.array())
      is LocalDate -> getPropertyStruct(LOCAL_DATE, DateTimeFormatter.ISO_DATE.format(value))
      is LocalDateTime ->
          getPropertyStruct(LOCAL_DATE_TIME, DateTimeFormatter.ISO_DATE_TIME.format(value))
      is LocalTime -> getPropertyStruct(LOCAL_TIME, DateTimeFormatter.ISO_TIME.format(value))
      is OffsetDateTime ->
          getPropertyStruct(ZONED_DATE_TIME, DateTimeFormatter.ISO_DATE_TIME.format(value))
      is ZonedDateTime ->
          getPropertyStruct(ZONED_DATE_TIME, DateTimeFormatter.ISO_DATE_TIME.format(value))
      is OffsetTime -> getPropertyStruct(OFFSET_TIME, DateTimeFormatter.ISO_TIME.format(value))
      is IsoDuration -> {
        val durationStruct =
            Struct(durationSchema)
                .put(MONTHS, value.months())
                .put(DAYS, value.days())
                .put(SECONDS, value.seconds())
                .put(NANOS, value.nanoseconds())
        getPropertyStruct(DURATION, durationStruct)
      }
      is Point -> {
        val pointStruct =
            Struct(pointSchema).put(SR_ID, value.srid()).put(X, value.x()).put(Y, value.y()).also {
              it.put(DIMENSION, if (value.z().isNaN()) TWO_D else THREE_D)
              if (!value.z().isNaN()) {
                it.put(Z, value.z())
              }
            }
        getPropertyStruct(POINT, pointStruct)
      }
      is ShortArray -> getPropertyStruct(LONG_LIST, value.map { it.toLong() })
      is IntArray -> getPropertyStruct(LONG_LIST, value.map { it.toLong() })
      is LongArray -> getPropertyStruct(LONG_LIST, value.toList())
      is FloatArray -> getPropertyStruct(FLOAT_LIST, value.map { it.toDouble() })
      is DoubleArray -> getPropertyStruct(FLOAT_LIST, value.toList())
      is BooleanArray -> getPropertyStruct(BOOLEAN_LIST, value.toList())
      is Array<*> -> asList(value.toList(), value::class.java.componentType.kotlin)
      is Iterable<*> -> {
        val elementTypes = value.map { it?.javaClass?.kotlin }.toSet()
        if (elementTypes.isEmpty()) {
          return asList(value, Int::class)
        }
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

  private fun asList(value: Iterable<*>, componentType: KClass<*>): Struct? {
    return when (componentType) {
      Boolean::class -> getPropertyStruct(BOOLEAN_LIST, value)
      Byte::class -> getPropertyStruct(BYTES, (value as List<Byte>).toByteArray())
      Short::class -> getPropertyStruct(LONG_LIST, (value as List<Short>).map { it.toLong() })
      Int::class -> getPropertyStruct(LONG_LIST, (value as List<Int>).map { it.toLong() })
      Long::class -> getPropertyStruct(LONG_LIST, value)
      Float::class -> getPropertyStruct(FLOAT_LIST, (value as List<Float>).map { it.toDouble() })
      Double::class -> getPropertyStruct(FLOAT_LIST, value)
      String::class -> getPropertyStruct(STRING_LIST, value)
      LocalDate::class ->
          getPropertyStruct(
              LOCAL_DATE_LIST,
              (value as List<LocalDate>).map { DateTimeFormatter.ISO_DATE.format(it) })
      LocalDateTime::class ->
          getPropertyStruct(
              LOCAL_DATE_TIME_LIST,
              (value as List<LocalDateTime>).map { DateTimeFormatter.ISO_DATE_TIME.format(it) })
      LocalTime::class ->
          getPropertyStruct(
              LOCAL_TIME_LIST,
              (value as List<LocalTime>).map { DateTimeFormatter.ISO_TIME.format(it) })
      OffsetDateTime::class ->
          getPropertyStruct(
              ZONED_DATE_TIME_LIST,
              (value as List<OffsetDateTime>).map { DateTimeFormatter.ISO_DATE_TIME.format(it) })
      ZonedDateTime::class ->
          getPropertyStruct(
              ZONED_DATE_TIME_LIST,
              (value as List<ZonedDateTime>).map { DateTimeFormatter.ISO_DATE_TIME.format(it) })
      OffsetTime::class ->
          getPropertyStruct(
              OFFSET_TIME_LIST,
              (value as List<OffsetTime>).map { DateTimeFormatter.ISO_TIME.format(it) })
      else -> {
        if (IsoDuration::class.java.isAssignableFrom(componentType.java)) {
          getPropertyStruct(
              DURATION_LIST,
              value
                  .map { it as IsoDuration }
                  .map {
                    Struct(durationSchema)
                        .put(MONTHS, it.months())
                        .put(DAYS, it.days())
                        .put(SECONDS, it.seconds())
                        .put(NANOS, it.nanoseconds())
                  })
        } else if (Point::class.java.isAssignableFrom(componentType.java)) {
          getPropertyStruct(
              POINT_LIST,
              value
                  .map { it as Point }
                  .map { point ->
                    Struct(pointSchema)
                        .put(SR_ID, point.srid())
                        .put(X, point.x())
                        .put(Y, point.y())
                        .also {
                          it.put(DIMENSION, if (point.z().isNaN()) TWO_D else THREE_D)
                          if (!point.z().isNaN()) {
                            it.put(Z, point.z())
                          }
                        }
                  })
        } else {
          throw IllegalArgumentException(
              "unsupported array type: array of ${componentType.java.name}")
        }
      }
    }
  }

  fun fromConnectValue(value: Struct?): Any? {
    return value?.let {
      val fieldType = it.getString(TYPE)
      if (SIMPLE_TYPE_FIELDS.contains(fieldType)) {
        return when (fieldType) {
          BOOLEAN -> it.getWithoutDefault(BOOLEAN) as Boolean
          LONG -> it.getWithoutDefault(LONG) as Long
          FLOAT -> it.getWithoutDefault(FLOAT) as Double
          STRING -> it.getWithoutDefault(STRING) as String
          BYTES -> {
            when (val bytes = it.getWithoutDefault(BYTES)) {
              is ByteArray -> bytes
              is ByteBuffer -> bytes.array()
              else ->
                  throw IllegalArgumentException(
                      "unsupported BYTES value: ${bytes?.javaClass?.name}")
            }
          }
          LOCAL_DATE -> parseLocalDate(it.getWithoutDefault(LOCAL_DATE) as String)
          LOCAL_TIME -> parseLocalTime(it.getWithoutDefault(LOCAL_TIME) as String)
          LOCAL_DATE_TIME -> parseLocalDateTime(it.getWithoutDefault(LOCAL_DATE_TIME) as String)
          ZONED_DATE_TIME -> parseZonedDateTime(it.getWithoutDefault(ZONED_DATE_TIME) as String)
          OFFSET_TIME -> parseOffsetTime(it.getWithoutDefault(OFFSET_TIME) as String)
          DURATION -> toDuration(it.getWithoutDefault(DURATION) as Struct)
          POINT -> toPoint(it.getWithoutDefault(POINT) as Struct)
          else ->
              throw IllegalArgumentException(
                  "unsupported simple type: $fieldType: ${it.getWithoutDefault(fieldType)?.javaClass?.name}")
        }
      }

      if (LIST_TYPE_FIELDS.contains(fieldType)) {
        val fieldValue = it.getWithoutDefault(fieldType)
        if (fieldValue != null && (fieldValue is Collection<*> && fieldValue.isNotEmpty())) {
          return when (fieldType) {
            BOOLEAN_LIST,
            LONG_LIST,
            FLOAT_LIST,
            STRING_LIST -> fieldValue as List<*>
            LOCAL_DATE_LIST -> (fieldValue as List<String>).map { s -> parseLocalDate(s) }
            LOCAL_TIME_LIST -> (fieldValue as List<String>).map { s -> parseLocalTime(s) }
            LOCAL_DATE_TIME_LIST -> (fieldValue as List<String>).map { s -> parseLocalDateTime(s) }
            ZONED_DATE_TIME_LIST -> (fieldValue as List<String>).map { s -> parseZonedDateTime(s) }
            OFFSET_TIME_LIST -> (fieldValue as List<String>).map { s -> parseOffsetTime(s) }
            DURATION_LIST -> (fieldValue as List<Struct>).map { s -> toDuration(s) }
            POINT_LIST -> (fieldValue as List<Struct>).map { s -> toPoint(s) }
            else ->
                throw IllegalArgumentException(
                    "unsupported list type: ${fieldType}: ${fieldValue.javaClass.name}")
          }
        }
      }

      // Protobuf does not support NULLs in repeated fields, so we always receive LIST typed fields
      // as empty lists. If we could not find a simple field and also a non-empty list field, we
      // assume the value is an empty list.
      return emptyList<Any>()
    }
  }

  fun getPropertyStruct(type: String, value: Any?): Struct {
    return Struct(schema).put(TYPE, type).put(type, value)
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
