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

import java.nio.ByteBuffer
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.OffsetTime
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.TemporalAccessor
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

enum class SimpleTypes(private val schema: Schema, private val optionalSchema: Schema) {
  BOOLEAN(Schema.BOOLEAN_SCHEMA, Schema.OPTIONAL_BOOLEAN_SCHEMA),
  LONG(Schema.INT64_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA),
  FLOAT(Schema.FLOAT64_SCHEMA, Schema.OPTIONAL_FLOAT64_SCHEMA),
  STRING(Schema.STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA),
  BYTES(Schema.BYTES_SCHEMA, Schema.OPTIONAL_BYTES_SCHEMA),
  LOCALDATE(
      SchemaBuilder(Schema.Type.STRING).namespaced("LocalDate").version(1).build(),
      SchemaBuilder(Schema.Type.STRING).namespaced("LocalDate").version(1).optional().build()),
  LOCALDATETIME(
      SchemaBuilder(Schema.Type.STRING).namespaced("LocalDateTime").version(1).build(),
      SchemaBuilder(Schema.Type.STRING).namespaced("LocalDateTime").version(1).optional().build()),
  LOCALTIME(
      SchemaBuilder(Schema.Type.STRING).namespaced("LocalTime").version(1).build(),
      SchemaBuilder(Schema.Type.STRING).namespaced("LocalTime").version(1).optional().build()),
  @Suppress("SpellCheckingInspection")
  ZONEDDATETIME(
      SchemaBuilder(Schema.Type.STRING).namespaced("ZonedDateTime").version(1).build(),
      SchemaBuilder(Schema.Type.STRING).namespaced("ZonedDateTime").version(1).optional().build()),
  @Suppress("SpellCheckingInspection")
  OFFSETTIME(
      SchemaBuilder(Schema.Type.STRING).namespaced("OffsetTime").version(1).build(),
      SchemaBuilder(Schema.Type.STRING).namespaced("OffsetTime").version(1).optional().build()),
  DURATION(
      SchemaBuilder(Schema.Type.STRUCT)
          .namespaced("Duration")
          .version(1)
          .field("months", Schema.INT64_SCHEMA)
          .field("days", Schema.INT64_SCHEMA)
          .field("seconds", Schema.INT64_SCHEMA)
          .field("nanoseconds", Schema.INT32_SCHEMA)
          .build(),
      SchemaBuilder(Schema.Type.STRUCT)
          .namespaced("Duration")
          .version(1)
          .field("months", Schema.INT64_SCHEMA)
          .field("days", Schema.INT64_SCHEMA)
          .field("seconds", Schema.INT64_SCHEMA)
          .field("nanoseconds", Schema.INT32_SCHEMA)
          .optional()
          .build()),
  POINT(
      SchemaBuilder(Schema.Type.STRUCT)
          .namespaced("Point")
          .version(1)
          .field("srid", Schema.INT32_SCHEMA)
          .field("x", Schema.FLOAT64_SCHEMA)
          .field("y", Schema.FLOAT64_SCHEMA)
          .field("z", Schema.FLOAT64_SCHEMA)
          .build(),
      SchemaBuilder(Schema.Type.STRUCT)
          .namespaced("Point")
          .version(1)
          .field("srid", Schema.INT32_SCHEMA)
          .field("x", Schema.FLOAT64_SCHEMA)
          .field("y", Schema.FLOAT64_SCHEMA)
          .field("z", Schema.FLOAT64_SCHEMA)
          .optional()
          .build());

  fun schema(optional: Boolean = false): Schema = if (optional) this.optionalSchema else this.schema
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
            is IsoDuration ->
                Struct(schema)
                    .put("months", value.months())
                    .put("days", value.days())
                    .put("seconds", value.seconds())
                    .put("nanoseconds", value.nanoseconds())
            is Point ->
                Struct(schema)
                    .put("srid", value.srid())
                    .put("x", value.x())
                    .put("y", value.y())
                    .put("z", value.z())
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
            is LocalDate -> DateTimeFormatter.ISO_DATE.format(value)
            is OffsetTime -> DateTimeFormatter.ISO_TIME.format(value)
            is LocalTime -> DateTimeFormatter.ISO_TIME.format(value)
            is TemporalAccessor -> DateTimeFormatter.ISO_DATE_TIME.format(value)
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

    return when (schema) {
      SimpleTypes.BOOLEAN.schema(false),
      SimpleTypes.BOOLEAN.schema(true) -> value as Boolean?
      SimpleTypes.LONG.schema(false),
      SimpleTypes.LONG.schema(true) -> value as Long?
      SimpleTypes.FLOAT.schema(false),
      SimpleTypes.FLOAT.schema(true) -> value as Double?
      SimpleTypes.STRING.schema(false),
      SimpleTypes.STRING.schema(true) -> value as String?
      SimpleTypes.BYTES.schema(false),
      SimpleTypes.BYTES.schema(true) ->
          when (value) {
            is ByteArray -> value
            is ByteBuffer -> value.array()
            else -> throw IllegalArgumentException("unsupported bytes type ${value.javaClass.name}")
          }
      SimpleTypes.LOCALDATE.schema(false),
      SimpleTypes.LOCALDATE.schema(true) ->
          (value as String?)?.let {
            DateTimeFormatter.ISO_DATE.parse(it) { parsed -> LocalDate.from(parsed) }
          }
      SimpleTypes.LOCALTIME.schema(false),
      SimpleTypes.LOCALTIME.schema(true) ->
          (value as String?)?.let {
            DateTimeFormatter.ISO_TIME.parse(it) { parsed -> LocalTime.from(parsed) }
          }
      SimpleTypes.LOCALDATETIME.schema(false),
      SimpleTypes.LOCALDATETIME.schema(true) ->
          (value as String?)?.let {
            DateTimeFormatter.ISO_DATE_TIME.parse(it) { parsed -> LocalDateTime.from(parsed) }
          }
      SimpleTypes.ZONEDDATETIME.schema(false),
      SimpleTypes.ZONEDDATETIME.schema(true) ->
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
      SimpleTypes.OFFSETTIME.schema(false),
      SimpleTypes.OFFSETTIME.schema(true) ->
          (value as String?)?.let {
            DateTimeFormatter.ISO_TIME.parse(it) { parsed -> OffsetTime.from(parsed) }
          }
      SimpleTypes.DURATION.schema(false),
      SimpleTypes.DURATION.schema(true) ->
          (value as Struct?)
              ?.let {
                Values.isoDuration(
                    it.getInt64("months"),
                    it.getInt64("days"),
                    it.getInt64("seconds"),
                    it.getInt32("nanoseconds"))
              }
              ?.asIsoDuration()
      SimpleTypes.POINT.schema(false),
      SimpleTypes.POINT.schema(true) ->
          (value as Struct?)
              ?.let {
                if (it.getFloat64("z").isNaN())
                    Values.point(it.getInt32("srid"), it.getFloat64("x"), it.getFloat64("y"))
                else
                    Values.point(
                        it.getInt32("srid"),
                        it.getFloat64("x"),
                        it.getFloat64("y"),
                        it.getFloat64("z"))
              }
              ?.asPoint()
      else ->
          return when (schema.type()) {
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
                      result.add(
                          fromConnectValue(schema.valueSchema(), element, skipNullValuesInMaps))
                    }
                else ->
                    throw IllegalArgumentException("unsupported array type ${value.javaClass.name}")
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
            Schema.Type.STRUCT -> {
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
            Schema.Type.BYTES ->
                when (value) {
                  is ByteArray -> value
                  is ByteBuffer -> value.array()
                  else ->
                      throw IllegalArgumentException(
                          "unsupported bytes type ${value.javaClass.name}")
                }
            Schema.Type.STRING -> value as String?
            Schema.Type.INT8 -> value as Byte?
            Schema.Type.INT16 -> value as Short?
            Schema.Type.INT32 -> value as Int?
            Schema.Type.INT64 -> value as Long?
            Schema.Type.FLOAT32 -> value as Float?
            Schema.Type.FLOAT64 -> value as Double?
            Schema.Type.BOOLEAN -> value as Boolean?
            else ->
                throw IllegalArgumentException(
                    "unsupported schema ($schema) and value type (${value.javaClass.name})")
          }
    }
  }

  fun toConnectSchema(
      value: Any?,
      optional: Boolean = false,
      forceMapsAsStruct: Boolean = false
  ): Schema =
      when (value) {
        null -> SchemaBuilder.struct().optional().build()
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
        is Array<*> ->
            value
                .firstNotNullOfOrNull { toConnectSchema(it, false, forceMapsAsStruct) }
                ?.run { SchemaBuilder.array(this).apply { if (optional) optional() }.build() }
                ?: SchemaBuilder.struct().apply { if (optional) optional() }.build()
        is LocalDate -> SimpleTypes.LOCALDATE.schema(optional)
        is LocalDateTime -> SimpleTypes.LOCALDATETIME.schema(optional)
        is LocalTime -> SimpleTypes.LOCALTIME.schema(optional)
        is OffsetDateTime -> SimpleTypes.ZONEDDATETIME.schema(optional)
        is ZonedDateTime -> SimpleTypes.ZONEDDATETIME.schema(optional)
        is OffsetTime -> SimpleTypes.OFFSETTIME.schema(optional)
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
          val elementTypes = value.map { toConnectSchema(it, false, forceMapsAsStruct) }

          when (elementTypes.toSet().size) {
            0 -> SchemaBuilder.struct().also { if (optional) it.optional() }.build()
            1 ->
                SchemaBuilder.array(elementTypes.first()).apply { if (optional) optional() }.build()
            else ->
                SchemaBuilder.struct()
                    .apply { elementTypes.forEachIndexed { i, v -> this.field("e${i}", v) } }
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
                  .mapValues { e -> toConnectSchema(e.value, optional, forceMapsAsStruct) }

          val valueSet = elementTypes.values.toSet()
          when {
            valueSet.isEmpty() -> SchemaBuilder.struct().apply { if (optional) optional() }.build()
            valueSet.singleOrNull() != null && !forceMapsAsStruct ->
                SchemaBuilder.map(Schema.STRING_SCHEMA, elementTypes.values.first())
                    .apply { if (optional) optional() }
                    .build()
            else ->
                SchemaBuilder.struct()
                    .apply { elementTypes.forEach { (k, v) -> this.field(k, v) } }
                    .apply { if (optional) optional() }
                    .build()
          }
        }
        else -> throw IllegalArgumentException("unsupported type ${value.javaClass.name}")
      }
}
