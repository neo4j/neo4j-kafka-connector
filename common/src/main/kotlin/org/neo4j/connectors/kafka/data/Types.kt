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
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.neo4j.driver.Values
import org.neo4j.driver.types.IsoDuration
import org.neo4j.driver.types.Node
import org.neo4j.driver.types.Point
import org.neo4j.driver.types.Relationship

internal fun SchemaBuilder.namespaced(vararg paths: String): SchemaBuilder =
    this.name("org.neo4j.connectors.kafka." + paths.joinToString("."))

internal fun Schema.id(): String = this.name().orEmpty().ifEmpty { this.type().name }

internal fun Schema.shortId(): String = this.id().split('.').last()

@Suppress("UNCHECKED_CAST")
object PropertyType {
  const val MONTHS = "months"
  const val DAYS = "days"
  const val SECONDS = "seconds"
  const val NANOS = "nanoseconds"
  const val SR_ID = "srid"
  const val X = "x"
  const val Y = "y"
  const val Z = "z"
  const val DIMENSION = "dimension"
  const val TWO_D: Byte = 2
  const val THREE_D: Byte = 3

  const val BOOLEAN = "B"
  const val BOOLEAN_LIST = "LB"
  const val LONG = "I64"
  const val LONG_LIST = "LI64"
  const val FLOAT = "F64"
  const val FLOAT_LIST = "LF64"
  const val STRING = "S"
  const val STRING_LIST = "LS"
  const val BYTES = "BA"
  const val LOCAL_DATE = "TLD"
  const val LOCAL_DATE_LIST = "LTLD"
  const val LOCAL_DATE_TIME = "TLDT"
  const val LOCAL_DATE_TIME_LIST = "LTLDT"
  const val LOCAL_TIME = "TLT"
  const val LOCAL_TIME_LIST = "LTLT"
  const val ZONED_DATE_TIME = "TZDT"
  const val ZONED_DATE_TIME_LIST = "LZDT"
  const val OFFSET_TIME = "TOT"
  const val OFFSET_TIME_LIST = "LTOT"
  const val DURATION = "TD"
  const val DURATION_LIST = "LTD"
  const val POINT = "SP"
  const val POINT_LIST = "LSP"

  val durationSchema: Schema =
      SchemaBuilder(Schema.Type.STRUCT)
          .field(MONTHS, Schema.INT64_SCHEMA)
          .field(DAYS, Schema.INT64_SCHEMA)
          .field(SECONDS, Schema.INT64_SCHEMA)
          .field(NANOS, Schema.INT32_SCHEMA)
          .optional()
          .build()

  val pointSchema: Schema =
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
      is Boolean -> Struct(schema).put(BOOLEAN, value)
      is Float -> Struct(schema).put(FLOAT, value.toDouble())
      is Double -> Struct(schema).put(FLOAT, value)
      is Number -> Struct(schema).put(LONG, value.toLong())
      is String -> Struct(schema).put(STRING, value)
      is Char -> Struct(schema).put(STRING, value.toString())
      is CharArray -> Struct(schema).put(STRING, String(value))
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
      is Array<*> ->
          when (val componentType = value::class.java.componentType.kotlin) {
            Boolean::class -> Struct(schema).put(BOOLEAN_LIST, value.toList())
            Byte::class -> Struct(schema).put(BYTES, (value as Array<Byte>).toByteArray())
            Short::class ->
                Struct(schema)
                    .put(LONG_LIST, (value as Array<Short>).map { s -> s.toLong() }.toList())
            Int::class ->
                Struct(schema)
                    .put(LONG_LIST, (value as Array<Int>).map { s -> s.toLong() }.toList())
            Long::class -> Struct(schema).put(LONG_LIST, (value as Array<Long>).toList())
            Float::class ->
                Struct(schema)
                    .put(FLOAT_LIST, (value as Array<Float>).map { s -> s.toDouble() }.toList())
            Double::class -> Struct(schema).put(FLOAT_LIST, (value as Array<Double>).toList())
            String::class -> Struct(schema).put(STRING_LIST, value.toList())
            LocalDate::class ->
                Struct(schema)
                    .put(
                        LOCAL_DATE_LIST,
                        (value as Array<LocalDate>)
                            .map { s -> DateTimeFormatter.ISO_DATE.format(s) }
                            .toList())
            LocalDateTime::class ->
                Struct(schema)
                    .put(
                        LOCAL_DATE_TIME_LIST,
                        (value as Array<LocalDateTime>)
                            .map { s -> DateTimeFormatter.ISO_DATE_TIME.format(s) }
                            .toList())
            LocalTime::class ->
                Struct(schema)
                    .put(
                        LOCAL_TIME_LIST,
                        (value as Array<LocalTime>)
                            .map { s -> DateTimeFormatter.ISO_TIME.format(s) }
                            .toList())
            OffsetDateTime::class ->
                Struct(schema)
                    .put(
                        ZONED_DATE_TIME_LIST,
                        (value as Array<OffsetDateTime>)
                            .map { s -> DateTimeFormatter.ISO_DATE_TIME.format(s) }
                            .toList())
            ZonedDateTime::class ->
                Struct(schema)
                    .put(
                        ZONED_DATE_TIME_LIST,
                        (value as Array<ZonedDateTime>)
                            .map { s -> DateTimeFormatter.ISO_DATE_TIME.format(s) }
                            .toList())
            OffsetTime::class ->
                Struct(schema)
                    .put(
                        OFFSET_TIME_LIST,
                        (value as Array<OffsetTime>)
                            .map { s -> DateTimeFormatter.ISO_TIME.format(s) }
                            .toList())
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
                              }
                              .toList())
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
                              }
                              .toList())
                } else {
                  throw IllegalArgumentException(
                      "unsupported array type: array of ${value.javaClass.componentType.name}")
                }
          }
      else -> throw IllegalArgumentException("unsupported property type: ${value?.javaClass?.name}")
    }
  }

  fun fromConnectValue(value: Struct?): Any? {
    return value?.let {
      for (f in it.schema().fields()) {
        if (it.getWithoutDefault(f.name()) == null) {
          continue
        }

        return when (f.name()) {
          BOOLEAN -> it.get(f) as Boolean?
          BOOLEAN_LIST -> it.get(f) as List<*>?
          LONG -> it.get(f) as Long?
          LONG_LIST -> it.get(f) as List<*>?
          FLOAT -> it.get(f) as Double?
          FLOAT_LIST -> it.get(f) as List<*>?
          STRING -> it.get(f) as String?
          STRING_LIST -> it.get(f) as List<*>?
          BYTES -> it.get(f) as ByteArray?
          LOCAL_DATE ->
              (it.get(f) as String?)?.let { s ->
                DateTimeFormatter.ISO_DATE.parse(s) { parsed -> LocalDate.from(parsed) }
              }
          LOCAL_DATE_LIST -> it.get(f) as List<*>?
          LOCAL_TIME ->
              (it.get(f) as String?)?.let { s ->
                DateTimeFormatter.ISO_TIME.parse(s) { parsed -> LocalTime.from(parsed) }
              }
          LOCAL_TIME_LIST -> it.get(f) as List<*>?
          LOCAL_DATE_TIME ->
              (it.get(f) as String?)?.let { s ->
                DateTimeFormatter.ISO_DATE_TIME.parse(s) { parsed -> LocalDateTime.from(parsed) }
              }
          LOCAL_DATE_TIME_LIST -> it.get(f) as List<*>?
          ZONED_DATE_TIME ->
              (it.get(f) as String?)?.let { s ->
                DateTimeFormatter.ISO_DATE_TIME.parse(s) { parsed ->
                  val zoneId = parsed.query(TemporalQueries.zone())

                  if (zoneId is ZoneOffset) {
                    OffsetDateTime.from(parsed)
                  } else {
                    ZonedDateTime.from(parsed)
                  }
                }
              }
          ZONED_DATE_TIME_LIST -> it.get(f) as List<*>?
          OFFSET_TIME ->
              (it.get(f) as String?)?.let { s ->
                DateTimeFormatter.ISO_TIME.parse(s) { parsed -> OffsetTime.from(parsed) }
              }
          OFFSET_TIME_LIST -> it.get(f) as List<*>?
          DURATION ->
              (it.get(f) as Struct?)
                  ?.let { s ->
                    Values.isoDuration(
                        s.getInt64(MONTHS),
                        s.getInt64(DAYS),
                        s.getInt64(SECONDS),
                        s.getInt32(NANOS))
                  }
                  ?.asIsoDuration()
          DURATION_LIST -> it.get(f) as List<*>?
          POINT ->
              (it.get(f) as Struct?)
                  ?.let { s ->
                    when (val dimension = s.getInt8(DIMENSION)) {
                      TWO_D -> Values.point(s.getInt32(SR_ID), s.getFloat64(X), s.getFloat64(Y))
                      THREE_D ->
                          Values.point(
                              s.getInt32(SR_ID), s.getFloat64(X), s.getFloat64(Y), s.getFloat64(Z))
                      else ->
                          throw IllegalArgumentException("unsupported dimension value ${dimension}")
                    }
                  }
                  ?.asPoint()
          POINT_LIST -> it.get(f) as List<*>?
          else -> throw IllegalArgumentException("unsupported neo4j type: ${f.name()}")
        }
      }

      return null
    }
  }
}

fun Schema.matches(other: Schema): Boolean {
  return this.id() == other.id() || this.shortId() == other.shortId()
}

object DynamicTypes {

  @Suppress("UNCHECKED_CAST")
  fun toConnectValue(schema: Schema, value: Any?): Any? {
    if (value == null) {
      return null
    }

    if (schema == PropertyType.schema) {
      return PropertyType.toConnectValue(value)
    }

    return when (schema.type()) {
      Schema.Type.ARRAY ->
          when (value) {
            is Collection<*> -> value.map { toConnectValue(schema.valueSchema(), it) }
            else -> throw IllegalArgumentException("unsupported array type ${value.javaClass.name}")
          }
      Schema.Type.MAP ->
          when (value) {
            is Map<*, *> -> value.mapValues { toConnectValue(schema.valueSchema(), it.value) }
            else -> throw IllegalArgumentException("unsupported map type ${value.javaClass.name}")
          }
      Schema.Type.STRUCT ->
          when (value) {
            is Node ->
                Struct(schema).apply {
                  put("<id>", value.id())
                  put("<labels>", value.labels().toList())

                  value
                      .asMap { it.asObject() }
                      .forEach { e -> put(e.key, PropertyType.toConnectValue(e.value)) }
                }
            is Relationship ->
                Struct(schema).apply {
                  put("<id>", value.id())
                  put("<type>", value.type())
                  put("<start.id>", value.startNodeId())
                  put("<end.id>", value.endNodeId())

                  value
                      .asMap { it.asObject() }
                      .forEach { e -> put(e.key, PropertyType.toConnectValue(e.value)) }
                }
            is Map<*, *> ->
                Struct(schema).apply {
                  schema.fields().forEach {
                    put(it.name(), toConnectValue(it.schema(), value[it.name()]))
                  }
                }
            is Collection<*> ->
                Struct(schema).apply {
                  schema.fields().forEach {
                    put(
                        it.name(),
                        toConnectValue(
                            it.schema(), value.elementAt(it.name().substring(1).toInt())))
                  }
                }
            else ->
                throw IllegalArgumentException("unsupported struct type ${value.javaClass.name}")
          }
      else -> value
    }
  }

  fun fromConnectValue(schema: Schema, value: Any?, skipNullValuesInMaps: Boolean = false): Any? {
    if (value == null) {
      return null
    }

    return when (schema.type()) {
      Schema.Type.BOOLEAN -> value as Boolean?
      Schema.Type.INT8 -> value as Byte?
      Schema.Type.INT16 -> value as Short?
      Schema.Type.INT32 -> value as Int?
      Schema.Type.INT64 -> value as Long?
      Schema.Type.FLOAT32 -> value as Float?
      Schema.Type.FLOAT64 -> value as Double?
      Schema.Type.BYTES ->
          when (value) {
            is ByteArray -> value
            is ByteBuffer -> value.array()
            else -> throw IllegalArgumentException("unsupported bytes type ${value.javaClass.name}")
          }
      Schema.Type.STRING ->
          when (value) {
            is Char -> value.toString()
            is CharArray -> value.toString()
            is CharSequence -> value.toString()
            else ->
                throw IllegalArgumentException("unsupported string type ${value.javaClass.name}")
          }
      Schema.Type.STRUCT ->
          when {
            PropertyType.schema.matches(schema) -> PropertyType.fromConnectValue(value as Struct?)
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

              if (result.isNotEmpty() &&
                  result.keys.all { it.startsWith("e") && it.substring(1).toIntOrNull() != null }) {
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
      Schema.Type.ARRAY -> {
        val result = mutableListOf<Any?>()

        when {
          value.javaClass.isArray ->
              for (i in 0 ..< java.lang.reflect.Array.getLength(value)) {
                result.add(
                    fromConnectValue(
                        schema.valueSchema(),
                        java.lang.reflect.Array.get(value, i),
                        skipNullValuesInMaps))
              }
          value is Iterable<*> ->
              for (element in value) {
                result.add(fromConnectValue(schema.valueSchema(), element, skipNullValuesInMaps))
              }
          else -> throw IllegalArgumentException("unsupported array type ${value.javaClass.name}")
        }

        result.toList()
      }
      Schema.Type.MAP -> {
        val result = mutableMapOf<String, Any?>()
        val map = value as Map<*, *>

        for (entry in map.entries) {
          if (entry.key !is String) {
            throw IllegalArgumentException(
                "invalid key type (${entry.key?.javaClass?.name} in map value")
          }

          result[entry.key as String] =
              fromConnectValue(schema.valueSchema(), entry.value, skipNullValuesInMaps)
        }

        result
      }
      else ->
          throw IllegalArgumentException(
              "unsupported schema ($schema) and value type (${value.javaClass.name})")
    }
  }

  fun toConnectSchema(
      value: Any?,
      optional: Boolean = false,
      forceMapsAsStruct: Boolean = false,
  ): Schema =
      when (value) {
        null -> PropertyType.schema
        is Boolean,
        is Float,
        is Double,
        is Number,
        is Char,
        is CharArray,
        is CharSequence,
        is ByteBuffer,
        is ByteArray,
        is ShortArray,
        is IntArray,
        is LongArray,
        is FloatArray,
        is DoubleArray,
        is BooleanArray -> PropertyType.schema
        is Array<*> -> {
          when (val componentType = value::class.java.componentType.kotlin) {
            Boolean::class,
            Byte::class,
            Short::class,
            Int::class,
            Long::class,
            Float::class,
            Double::class,
            String::class,
            LocalDate::class,
            LocalDateTime::class,
            LocalTime::class,
            OffsetDateTime::class,
            ZonedDateTime::class,
            OffsetTime::class -> PropertyType.schema
            else ->
                if (IsoDuration::class.java.isAssignableFrom(componentType.java)) {
                  PropertyType.schema
                } else if (Point::class.java.isAssignableFrom(componentType.java)) {
                  PropertyType.schema
                } else {
                  val first = value.firstOrNull { it.notNullOrEmpty() }
                  val schema = toConnectSchema(first, optional, forceMapsAsStruct)
                  SchemaBuilder.array(schema).apply { if (optional) optional() }.build()
                }
          }
        }
        is LocalDate,
        is LocalDateTime,
        is LocalTime,
        is OffsetDateTime,
        is ZonedDateTime,
        is OffsetTime,
        is IsoDuration,
        is Point -> PropertyType.schema
        is Node ->
            SchemaBuilder.struct()
                .apply {
                  field("<id>", Schema.INT64_SCHEMA)
                  field("<labels>", SchemaBuilder.array(Schema.STRING_SCHEMA).build())

                  value.keys().forEach { field(it, PropertyType.schema) }

                  if (optional) optional()
                }
                .build()
        is Relationship ->
            SchemaBuilder.struct()
                .apply {
                  field("<id>", Schema.INT64_SCHEMA)
                  field("<type>", Schema.STRING_SCHEMA)
                  field("<start.id>", Schema.INT64_SCHEMA)
                  field("<end.id>", Schema.INT64_SCHEMA)

                  value.keys().forEach { field(it, PropertyType.schema) }

                  if (optional) optional()
                }
                .build()
        is Collection<*> -> {
          val nonEmptyElementTypes =
              value
                  .filter { it.notNullOrEmpty() }
                  .map { toConnectSchema(it, optional, forceMapsAsStruct) }

          when (nonEmptyElementTypes.toSet().size) {
            0 -> SchemaBuilder.array(PropertyType.schema).apply { if (optional) optional() }.build()
            1 ->
                SchemaBuilder.array(nonEmptyElementTypes.first())
                    .apply { if (optional) optional() }
                    .build()
            else ->
                SchemaBuilder.struct()
                    .apply {
                      value.forEachIndexed { i, v ->
                        this.field("e${i}", toConnectSchema(v, optional, forceMapsAsStruct))
                      }
                    }
                    .apply { if (optional) optional() }
                    .build()
          }
        }
        is Map<*, *> -> {
          val elementTypes =
              value
                  .mapKeys {
                    when (val key = it.key) {
                      is String -> key
                      else ->
                          throw IllegalArgumentException(
                              "unsupported map key type ${key?.javaClass?.name}")
                    }
                  }
                  .filter { e -> e.value.notNullOrEmpty() }
                  .mapValues { e -> toConnectSchema(e.value, optional, forceMapsAsStruct) }

          val valueSet = elementTypes.values.toSet()
          when {
            valueSet.isEmpty() ->
                SchemaBuilder.struct()
                    .apply {
                      value.forEach {
                        this.field(
                            it.key as String,
                            toConnectSchema(it.value, optional, forceMapsAsStruct))
                      }
                    }
                    .apply { if (optional) optional() }
                    .build()
            valueSet.singleOrNull() != null && !forceMapsAsStruct ->
                SchemaBuilder.map(Schema.STRING_SCHEMA, elementTypes.values.first())
                    .apply { if (optional) optional() }
                    .build()
            else ->
                SchemaBuilder.struct()
                    .apply {
                      value.forEach {
                        this.field(
                            it.key as String,
                            toConnectSchema(it.value, optional, forceMapsAsStruct))
                      }
                    }
                    .apply { if (optional) optional() }
                    .build()
          }
        }
        else -> throw IllegalArgumentException("unsupported type ${value.javaClass.name}")
      }

  private fun Any?.notNullOrEmpty(): Boolean =
      when (val value = this) {
        null -> false
        is Collection<*> -> value.isNotEmpty() && value.any { it.notNullOrEmpty() }
        is Array<*> -> value.isNotEmpty() && value.any { it.notNullOrEmpty() }
        is Map<*, *> -> value.isNotEmpty() && value.values.any { it.notNullOrEmpty() }
        else -> true
      }
}
