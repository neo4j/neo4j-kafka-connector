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
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.OffsetTime
import java.time.ZoneId
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

const val EPOCH_DAYS = "epochDays"
const val NANOS_OF_DAY = "nanosOfDay"
const val EPOCH_SECONDS = "epochSeconds"
const val NANOS_OF_SECOND = "nanosOfSecond"
const val ZONE_ID = "zoneId"
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

@Suppress("SpellCheckingInspection")
enum class SimpleTypes(builder: () -> SchemaBuilder) {
  NULL({ SchemaBuilder.struct().optional().namespaced("NULL") }),
  BOOLEAN({ SchemaBuilder.bool() }),
  LONG({ SchemaBuilder.int64() }),
  FLOAT({ SchemaBuilder.float64() }),
  STRING({ SchemaBuilder.string() }),
  BYTES({ SchemaBuilder.bytes() }),
  LOCALDATE({ SchemaBuilder.string().namespaced("LocalDate") }),
  LOCALDATETIME({ SchemaBuilder.string().namespaced("LocalDateTime") }),
  LOCALTIME({ SchemaBuilder.string().namespaced("LocalTime") }),
  ZONEDDATETIME({ SchemaBuilder.string().namespaced("ZonedDateTime") }),
  OFFSETTIME({ SchemaBuilder.string().namespaced("OffsetTime") }),
  // PROTOBUF converter does not persist our provided version value hence looses it during reads
  // That's why we suffix the type names with an explicit version specifier
  LOCALDATE_STRUCT({
    SchemaBuilder.struct().namespaced("LocalDateStruct").field(EPOCH_DAYS, Schema.INT64_SCHEMA)
  }),
  LOCALDATETIME_STRUCT({
    SchemaBuilder.struct()
        .namespaced("LocalDateTimeStruct")
        .field(EPOCH_DAYS, Schema.INT64_SCHEMA)
        .field(NANOS_OF_DAY, Schema.INT64_SCHEMA)
  }),
  LOCALTIME_STRUCT({
    SchemaBuilder.struct().namespaced("LocalTimeStruct").field(NANOS_OF_DAY, Schema.INT64_SCHEMA)
  }),
  ZONEDDATETIME_STRUCT({
    SchemaBuilder.struct()
        .namespaced("ZonedDateTimeStruct")
        .field(EPOCH_SECONDS, Schema.INT64_SCHEMA)
        .field(NANOS_OF_SECOND, Schema.INT32_SCHEMA)
        .field(ZONE_ID, Schema.STRING_SCHEMA)
  }),
  OFFSETTIME_STRUCT({
    SchemaBuilder.struct()
        .namespaced("OffsetTimeStruct")
        .field(NANOS_OF_DAY, Schema.INT64_SCHEMA)
        .field(ZONE_ID, Schema.STRING_SCHEMA)
  }),
  DURATION({
    SchemaBuilder(Schema.Type.STRUCT)
        .namespaced("Duration")
        .field(MONTHS, Schema.INT64_SCHEMA)
        .field(DAYS, Schema.INT64_SCHEMA)
        .field(SECONDS, Schema.INT64_SCHEMA)
        .field(NANOS, Schema.INT32_SCHEMA)
  }),
  POINT({
    SchemaBuilder(Schema.Type.STRUCT)
        .namespaced("Point")
        .field(DIMENSION, Schema.INT8_SCHEMA)
        .field(SR_ID, Schema.INT32_SCHEMA)
        .field(X, Schema.FLOAT64_SCHEMA)
        .field(Y, Schema.FLOAT64_SCHEMA)
        .field(Z, Schema.OPTIONAL_FLOAT64_SCHEMA)
  });

  // fully namespaced schema name
  val id: String
  // just schema name, excluding namespace
  private val shortId: String
  val schema: Schema = builder().build()
  private val optionalSchema: Schema = builder().optional().build()

  init {
    if (schema.type() == Schema.Type.STRUCT && schema.name().isNullOrBlank()) {
      throw IllegalArgumentException("schema name is required for STRUCT types")
    }

    id = schema.id()
    shortId = schema.shortId()
  }

  fun schema(optional: Boolean = false) = if (optional) optionalSchema else schema

  fun matches(other: Schema): Boolean {
    return this.id == other.id() || this.shortId == other.shortId()
  }
}

object DynamicTypes {

  fun toConnectValue(schema: Schema, value: Any?): Any? {
    if (value == null) {
      return null
    }

    return when (schema.type()) {
      Schema.Type.BYTES ->
          when (value) {
            is ByteArray -> value
            is ByteBuffer -> value.array()
            else -> throw IllegalArgumentException("unsupported bytes type ${value.javaClass.name}")
          }
      Schema.Type.ARRAY ->
          when (value) {
            is Collection<*> -> value.map { toConnectValue(schema.valueSchema(), it) }
            is Array<*> -> value.map { toConnectValue(schema.valueSchema(), it) }.toList()
            is ShortArray -> value.map { s -> s.toLong() }.toList()
            is IntArray -> value.map { i -> i.toLong() }.toList()
            is FloatArray -> value.map { f -> f.toDouble() }.toList()
            is BooleanArray -> value.toList()
            is LongArray -> value.toList()
            is DoubleArray -> value.toList()
            else -> throw IllegalArgumentException("unsupported array type ${value.javaClass.name}")
          }
      Schema.Type.MAP ->
          when (value) {
            is Map<*, *> -> value.mapValues { toConnectValue(schema.valueSchema(), it.value) }
            else -> throw IllegalArgumentException("unsupported map type ${value.javaClass.name}")
          }
      Schema.Type.STRUCT ->
          when (value) {
            is LocalDate -> Struct(schema).put(EPOCH_DAYS, value.toEpochDay())
            is LocalTime -> Struct(schema).put(NANOS_OF_DAY, value.toNanoOfDay())
            is LocalDateTime ->
                Struct(schema)
                    .put(EPOCH_DAYS, value.toLocalDate().toEpochDay())
                    .put(NANOS_OF_DAY, value.toLocalTime().toNanoOfDay())
            is OffsetTime ->
                Struct(schema)
                    .put(NANOS_OF_DAY, value.toLocalTime().toNanoOfDay())
                    .put(ZONE_ID, value.offset.id)
            is ZonedDateTime ->
                Struct(schema)
                    .put(EPOCH_SECONDS, value.toEpochSecond())
                    .put(NANOS_OF_SECOND, value.nano)
                    .put(ZONE_ID, value.zone.id)
            is OffsetDateTime ->
                Struct(schema)
                    .put(EPOCH_SECONDS, value.toEpochSecond())
                    .put(NANOS_OF_SECOND, value.nano)
                    .put(ZONE_ID, value.offset.id)
            is IsoDuration ->
                Struct(schema)
                    .put(MONTHS, value.months())
                    .put(DAYS, value.days())
                    .put(SECONDS, value.seconds())
                    .put(NANOS, value.nanoseconds())
            is Point ->
                Struct(schema).put(SR_ID, value.srid()).put(X, value.x()).put(Y, value.y()).also {
                  it.put(DIMENSION, if (value.z().isNaN()) TWO_D else THREE_D)
                  if (!value.z().isNaN()) {
                    it.put(Z, value.z())
                  }
                }
            is Node ->
                Struct(schema).apply {
                  put("<id>", value.id())
                  put("<labels>", value.labels())

                  value
                      .asMap { it.asObject() }
                      .forEach { e ->
                        put(e.key, toConnectValue(schema.field(e.key).schema(), e.value))
                      }
                }
            is Relationship ->
                Struct(schema).apply {
                  put("<id>", value.id())
                  put("<type>", value.type())
                  put("<start.id>", value.startNodeId())
                  put("<end.id>", value.endNodeId())

                  value
                      .asMap { it.asObject() }
                      .forEach { e ->
                        put(e.key, toConnectValue(schema.field(e.key).schema(), e.value))
                      }
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
      Schema.Type.STRING ->
          when (value) {
            is String -> value
            is Char -> value.toString()
            is CharArray -> String(value)
            else ->
                throw IllegalArgumentException("unsupported string type ${value.javaClass.name}")
          }
      Schema.Type.INT64,
      Schema.Type.FLOAT64 ->
          when (value) {
            is Float -> value.toDouble()
            is Double -> value
            is Number -> value.toLong()
            else ->
                throw IllegalArgumentException("unsupported numeric type ${value.javaClass.name}")
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
            else -> value as String?
          }
      Schema.Type.STRUCT ->
          when {
            SimpleTypes.LOCALDATE_STRUCT.matches(schema) ->
                (value as Struct?)?.let { LocalDate.ofEpochDay(it.getInt64(EPOCH_DAYS)) }
            SimpleTypes.LOCALTIME_STRUCT.matches(schema) ->
                (value as Struct?)?.let { LocalTime.ofNanoOfDay(it.getInt64(NANOS_OF_DAY)) }
            SimpleTypes.LOCALDATETIME_STRUCT.matches(schema) ->
                (value as Struct?)?.let {
                  LocalDateTime.of(
                      LocalDate.ofEpochDay(it.getInt64(EPOCH_DAYS)),
                      LocalTime.ofNanoOfDay(it.getInt64(NANOS_OF_DAY)))
                }
            SimpleTypes.ZONEDDATETIME_STRUCT.matches(schema) ->
                (value as Struct?)?.let {
                  val instant =
                      Instant.ofEpochSecond(
                          it.getInt64(EPOCH_SECONDS), it.getInt32(NANOS_OF_SECOND).toLong())
                  val zoneId = ZoneId.of(it.getString(ZONE_ID))
                  if (zoneId is ZoneOffset) {
                    OffsetDateTime.ofInstant(instant, zoneId)
                  } else {
                    ZonedDateTime.ofInstant(instant, zoneId)
                  }
                }
            SimpleTypes.OFFSETTIME_STRUCT.matches(schema) ->
                (value as Struct?)?.let {
                  OffsetTime.of(
                      LocalTime.ofNanoOfDay(it.getInt64(NANOS_OF_DAY)),
                      ZoneOffset.of(it.getString(ZONE_ID)))
                }
            SimpleTypes.DURATION.matches(schema) ->
                (value as Struct?)
                    ?.let {
                      Values.isoDuration(
                          it.getInt64(MONTHS),
                          it.getInt64(DAYS),
                          it.getInt64(SECONDS),
                          it.getInt32(NANOS))
                    }
                    ?.asIsoDuration()
            SimpleTypes.POINT.matches(schema) ->
                (value as Struct?)
                    ?.let {
                      when (val dimension = it.getInt8(DIMENSION)) {
                        TWO_D ->
                            Values.point(it.getInt32(SR_ID), it.getFloat64(X), it.getFloat64(Y))
                        THREE_D ->
                            Values.point(
                                it.getInt32(SR_ID),
                                it.getFloat64(X),
                                it.getFloat64(Y),
                                it.getFloat64(Z))
                        else ->
                            throw IllegalArgumentException(
                                "unsupported dimension value ${dimension}")
                      }
                    }
                    ?.asPoint()
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
      forceMapsAsStruct: Boolean = false
  ): Schema =
      when (value) {
        null -> SimpleTypes.NULL.schema(true)
        is Boolean -> SimpleTypes.BOOLEAN.schema(optional)
        is Float,
        is Double -> SimpleTypes.FLOAT.schema(optional)
        is Number -> SimpleTypes.LONG.schema(optional)
        is Char,
        is CharArray,
        is CharSequence -> SimpleTypes.STRING.schema(optional)
        is ByteBuffer,
        is ByteArray -> SimpleTypes.BYTES.schema(optional)
        is ShortArray,
        is IntArray,
        is LongArray ->
            SchemaBuilder.array(Schema.INT64_SCHEMA).apply { if (optional) optional() }.build()
        is FloatArray,
        is DoubleArray ->
            SchemaBuilder.array(Schema.FLOAT64_SCHEMA).apply { if (optional) optional() }.build()
        is BooleanArray ->
            SchemaBuilder.array(Schema.BOOLEAN_SCHEMA).apply { if (optional) optional() }.build()
        is Array<*> -> {
          val first = value.firstOrNull { it.notNullOrEmpty() }
          val schema = toConnectSchema(first, optional, forceMapsAsStruct)
          SchemaBuilder.array(schema).apply { if (optional) optional() }.build()
        }
        is LocalDate -> SimpleTypes.LOCALDATE_STRUCT.schema(optional)
        is LocalDateTime -> SimpleTypes.LOCALDATETIME_STRUCT.schema(optional)
        is LocalTime -> SimpleTypes.LOCALTIME_STRUCT.schema(optional)
        is OffsetDateTime -> SimpleTypes.ZONEDDATETIME_STRUCT.schema(optional)
        is ZonedDateTime -> SimpleTypes.ZONEDDATETIME_STRUCT.schema(optional)
        is OffsetTime -> SimpleTypes.OFFSETTIME_STRUCT.schema(optional)
        is IsoDuration -> SimpleTypes.DURATION.schema(optional)
        is Point -> SimpleTypes.POINT.schema(optional)
        is Node ->
            SchemaBuilder.struct()
                .apply {
                  field("<id>", SimpleTypes.LONG.schema())
                  field("<labels>", SchemaBuilder.array(SimpleTypes.STRING.schema()).build())

                  value.keys().forEach {
                    field(
                        it, toConnectSchema(value.get(it).asObject(), optional, forceMapsAsStruct))
                  }

                  if (optional) optional()
                }
                .build()
        is Relationship ->
            SchemaBuilder.struct()
                .apply {
                  field("<id>", SimpleTypes.LONG.schema())
                  field("<type>", SimpleTypes.STRING.schema())
                  field("<start.id>", SimpleTypes.LONG.schema())
                  field("<end.id>", SimpleTypes.LONG.schema())

                  value.keys().forEach {
                    field(
                        it, toConnectSchema(value.get(it).asObject(), optional, forceMapsAsStruct))
                  }

                  if (optional) optional()
                }
                .build()
        is Collection<*> -> {
          val nonEmptyElementTypes =
              value
                  .filter { it.notNullOrEmpty() }
                  .map { toConnectSchema(it, optional, forceMapsAsStruct) }

          when (nonEmptyElementTypes.toSet().size) {
            0 ->
                SchemaBuilder.array(SimpleTypes.NULL.schema(true))
                    .apply { if (optional) optional() }
                    .build()
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

  fun Any?.notNullOrEmpty(): Boolean =
      when (val value = this) {
        null -> false
        is Collection<*> -> value.isNotEmpty() && value.any { it.notNullOrEmpty() }
        is Array<*> -> value.isNotEmpty() && value.any { it.notNullOrEmpty() }
        is Map<*, *> -> value.isNotEmpty() && value.values.any { it.notNullOrEmpty() }
        else -> true
      }
}
