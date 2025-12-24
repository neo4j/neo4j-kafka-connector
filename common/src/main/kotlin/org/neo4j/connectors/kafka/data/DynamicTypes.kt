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

import java.math.BigDecimal
import java.math.BigInteger
import java.nio.ByteBuffer
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.OffsetTime
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.TemporalQueries
import org.apache.kafka.connect.data.Date
import org.apache.kafka.connect.data.Decimal
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.data.Time
import org.apache.kafka.connect.data.Timestamp
import org.neo4j.driver.Values

object DynamicTypes {
  const val MILLIS_PER_DAY = 24 * 60 * 60 * 1000

  private const val PROTOBUF_DECIMAL_TYPE = "confluent.type.Decimal"

  private const val PROTOBUF_DATE_TYPE = "google.type.Date"

  private const val PROTOBUF_TIME_TYPE = "google.type.TimeOfDay"

  private const val PROTOBUF_TIMESTAMP_TYPE = "google.protobuf.Timestamp"

  fun fromConnectValue(schema: Schema, value: Any?, skipNullValuesInMaps: Boolean = false): Any? {
    if (value == null) {
      return null
    }

    // check for Kafka Connect types first
    return when (schema.name()) {
      Date.LOGICAL_NAME -> fromConnectDate(value, schema)
      PROTOBUF_DATE_TYPE -> fromProtobufDate(value)
      Time.LOGICAL_NAME -> fromConnectTime(value)
      PROTOBUF_TIME_TYPE -> fromProtobufTime(value)
      Timestamp.LOGICAL_NAME -> fromConnectTimestamp(value, schema)
      PROTOBUF_TIMESTAMP_TYPE -> fromProtobufTimestamp(value)
      Decimal.LOGICAL_NAME -> fromConnectDecimal(value)
      PROTOBUF_DECIMAL_TYPE -> fromProtobufDecimal(value)
      else ->
          when (schema.type()) {
            Schema.Type.BOOLEAN -> value as Boolean?
            Schema.Type.INT8 -> value as Byte?
            Schema.Type.INT16 -> value as Short?
            Schema.Type.INT32 -> value as Int?
            Schema.Type.INT64 -> value as Long?
            Schema.Type.FLOAT32 -> value as Float?
            Schema.Type.FLOAT64 -> value as Double?
            Schema.Type.BYTES -> fromBytes(value)
            Schema.Type.STRING -> fromString(schema, value)
            Schema.Type.STRUCT -> fromStruct(schema, value, skipNullValuesInMaps)
            Schema.Type.ARRAY -> fromArray(value, schema, skipNullValuesInMaps)
            Schema.Type.MAP -> fromMap(value, schema, skipNullValuesInMaps)
            else ->
                throw IllegalArgumentException(
                    "unsupported schema ($schema) and value type (${value.javaClass.name})"
                )
          }
    }
  }

  private fun fromMap(
      value: Any,
      schema: Schema,
      skipNullValuesInMaps: Boolean,
  ): MutableMap<String, Any?> {
    val result = mutableMapOf<String, Any?>()
    val map = value as Map<*, *>

    for (entry in map.entries) {
      if (entry.key !is String) {
        throw IllegalArgumentException(
            "invalid key type (${entry.key?.javaClass?.name} in map value"
        )
      }

      result[entry.key as String] =
          fromConnectValue(schema.valueSchema(), entry.value, skipNullValuesInMaps)
    }

    return result
  }

  private fun fromArray(value: Any, schema: Schema, skipNullValuesInMaps: Boolean): List<Any?> {
    val result = mutableListOf<Any?>()

    when {
      value.javaClass.isArray ->
          for (i in 0..<java.lang.reflect.Array.getLength(value)) {
            result.add(
                fromConnectValue(
                    schema.valueSchema(),
                    java.lang.reflect.Array.get(value, i),
                    skipNullValuesInMaps,
                )
            )
          }

      value is Iterable<*> ->
          for (element in value) {
            result.add(fromConnectValue(schema.valueSchema(), element, skipNullValuesInMaps))
          }

      else -> throw IllegalArgumentException("unsupported array type ${value.javaClass.name}")
    }

    return result.toList()
  }

  private fun fromStruct(schema: Schema, value: Any, skipNullValuesInMaps: Boolean): Any? =
      when {
        PropertyType.schema.matches(schema) -> PropertyType.fromConnectValue(value as Struct?)
        SimpleTypes.POINT.matches(schema) ->
            (value as Struct?)
                ?.let {
                  when (val dimension = it.getInt8(DIMENSION)) {
                    TWO_D -> Values.point(it.getInt32(SR_ID), it.getFloat64(X), it.getFloat64(Y))

                    THREE_D ->
                        Values.point(
                            it.getInt32(SR_ID),
                            it.getFloat64(X),
                            it.getFloat64(Y),
                            it.getFloat64(Z),
                        )

                    else -> throw IllegalArgumentException("unsupported dimension value $dimension")
                  }
                }
                ?.asPoint()

        SimpleTypes.DURATION.matches(schema) ->
            (value as Struct?)
                ?.let {
                  Values.isoDuration(
                      it.getInt64(MONTHS),
                      it.getInt64(DAYS),
                      it.getInt64(SECONDS),
                      it.getInt32(NANOS),
                  )
                }
                ?.asIsoDuration()

        else -> {
          val result = mutableMapOf<String, Any?>()
          val struct = value as Struct

          for (field in schema.fields()) {
            val fieldValue =
                fromConnectValue(field.schema(), struct.get(field), skipNullValuesInMaps)

            if (fieldValue != null || !skipNullValuesInMaps) {
              result[field.name()] = fieldValue
            }
          }

          if (
              result.isNotEmpty() &&
                  result.keys.all { it.startsWith("e") && it.substring(1).toIntOrNull() != null }
          ) {
            result
                .mapKeys { it.key.substring(1).toInt() }
                .entries
                .sortedBy { it.key }
                .map { it.value }
                .toList()
          } else {
            result
          }
        }
      }

  private fun fromString(schema: Schema, value: Any): Any {
    val parsedValue =
        when {
          SimpleTypes.LOCALDATE.matches(schema) ->
              (value as String?)?.let {
                DateTimeFormatter.ISO_DATE.parse(it) { parsed -> LocalDate.from(parsed) }
              }

          SimpleTypes.LOCALTIME.matches(schema) ->
              (value as String?)?.let {
                DateTimeFormatter.ISO_TIME.parse(it) { parsed -> LocalTime.from(parsed) }
              }

          SimpleTypes.LOCALDATETIME.matches(schema) ->
              (value as String?)?.let {
                DateTimeFormatter.ISO_DATE_TIME.parse(it) { parsed -> LocalDateTime.from(parsed) }
              }

          SimpleTypes.ZONEDDATETIME.matches(schema) ->
              (value as String?)?.let {
                DateTimeFormatter.ISO_DATE_TIME.parse(it) { parsed ->
                  val zoneId = parsed.query(TemporalQueries.zone())
                  if (zoneId is ZoneOffset) {
                    OffsetDateTime.from(parsed)
                  } else {
                    ZonedDateTime.from(parsed)
                  }
                }
              }

          SimpleTypes.OFFSETTIME.matches(schema) ->
              (value as String?)?.let {
                DateTimeFormatter.ISO_TIME.parse(it) { parsed -> OffsetTime.from(parsed) }
              }

          else -> value
        }
    return when (parsedValue) {
      is String -> parsedValue
      is Char -> parsedValue.toString()
      is CharArray -> parsedValue.concatToString()
      is CharSequence -> parsedValue.toString()
      is LocalDate -> parsedValue
      is LocalTime -> parsedValue
      is LocalDateTime -> parsedValue
      is OffsetDateTime -> parsedValue
      is ZonedDateTime -> parsedValue
      is OffsetTime -> parsedValue
      else ->
          throw IllegalArgumentException("Unsupported string schema type: ${value.javaClass.name}")
    }
  }

  private fun fromBytes(value: Any): ByteArray? =
      when (value) {
        is ByteArray -> value
        is ByteBuffer -> value.array()
        else -> throw IllegalArgumentException("unsupported bytes type ${value.javaClass.name}")
      }

  private fun fromProtobufDecimal(value: Any): String =
      when (value) {
        is Struct ->
            BigDecimal(BigInteger(value.getBytes("value")), value.getInt32("scale")).toString()

        else ->
            throw IllegalArgumentException(
                "unsupported protobuf decimal type ${value.javaClass.name}"
            )
      }

  private fun fromConnectDecimal(value: Any): String =
      // because Neo4j does not have a built-in DECIMAL value, and converting the incoming decimal
      // value into a double might end-up in value loss, we explicitly represent these decimal
      // values as strings so the user can consciously decide what to do with them.
      when (value) {
        is BigDecimal -> value.toString()
        else ->
            throw IllegalArgumentException(
                "unsupported Kafka Connect decimal type ${value.javaClass.name}"
            )
      }

  private fun fromProtobufTimestamp(value: Any): LocalDateTime? =
      when (value) {
        is Struct ->
            LocalDateTime.ofEpochSecond(
                value.getInt64("seconds"),
                value.getInt32("nanos"),
                ZoneOffset.UTC,
            )

        else ->
            throw IllegalArgumentException(
                "unsupported protobuf timestamp type ${value.javaClass.name}"
            )
      }

  private fun fromConnectTimestamp(value: Any, schema: Schema): LocalDateTime? =
      when (value) {
        is java.util.Date ->
            LocalDateTime.ofInstant(
                Instant.ofEpochMilli(Timestamp.fromLogical(schema, value)),
                ZoneOffset.UTC,
            )

        else ->
            throw IllegalArgumentException(
                "unsupported Kafka Connect time type ${value.javaClass.name}"
            )
      }

  private fun fromProtobufTime(value: Any): LocalTime? =
      when (value) {
        is Struct ->
            LocalTime.of(
                value.getInt32("hours"),
                value.getInt32("minutes"),
                value.getInt32("seconds"),
                value.getInt32("nanos"),
            )

        else ->
            throw IllegalArgumentException("unsupported protobuf time type ${value.javaClass.name}")
      }

  private fun fromConnectTime(value: Any): LocalTime? =
      when (value) {
        is java.util.Date -> LocalTime.ofInstant(value.toInstant(), ZoneOffset.UTC)
        else ->
            throw IllegalArgumentException(
                "unsupported Kafka Connect time type ${value.javaClass.name}"
            )
      }

  private fun fromProtobufDate(value: Any): LocalDate? =
      when (value) {
        is Struct ->
            LocalDate.of(value.getInt32("year"), value.getInt32("month"), value.getInt32("day"))

        else ->
            throw IllegalArgumentException("unsupported protobuf date type ${value.javaClass.name}")
      }

  private fun fromConnectDate(value: Any, schema: Schema): LocalDate? =
      when (value) {
        is java.util.Date -> LocalDate.ofEpochDay(Date.fromLogical(schema, value).toLong())
        else ->
            throw IllegalArgumentException(
                "unsupported Kafka Connect date type ${value.javaClass.name}"
            )
      }

  internal fun Any?.notNullOrEmpty(): Boolean =
      when (val value = this) {
        null -> false
        is Collection<*> -> value.isNotEmpty() && value.any { it.notNullOrEmpty() }
        is Array<*> -> value.isNotEmpty() && value.any { it.notNullOrEmpty() }
        is Map<*, *> -> value.isNotEmpty() && value.values.any { it.notNullOrEmpty() }
        else -> true
      }
}
