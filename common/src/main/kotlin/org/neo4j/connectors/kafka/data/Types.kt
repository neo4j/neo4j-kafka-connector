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
import java.time.Duration
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.OffsetTime
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.TemporalAccessor
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
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
      SchemaBuilder(Schema.Type.STRING).namespaced("LocalDate").build(),
      SchemaBuilder(Schema.Type.STRING).namespaced("OptionalLocalDate").optional().build()),
  LOCALDATETIME(
      SchemaBuilder(Schema.Type.STRING).namespaced("LocalDateTime").build(),
      SchemaBuilder(Schema.Type.STRING).namespaced("OptionalLocalDateTime").optional().build()),
  LOCALTIME(
      SchemaBuilder(Schema.Type.STRING).namespaced("LocalTime").build(),
      SchemaBuilder(Schema.Type.STRING).namespaced("OptionalLocalTime").optional().build()),
  ZONEDDATETIME(
      SchemaBuilder(Schema.Type.STRING).namespaced("ZonedDateTime").build(),
      SchemaBuilder(Schema.Type.STRING).namespaced("OptionalZonedDateTime").optional().build()),
  OFFSETTIME(
      SchemaBuilder(Schema.Type.STRING).namespaced("OffsetTime").build(),
      SchemaBuilder(Schema.Type.STRING).namespaced("OptionalOffsetTime").optional().build()),
  DURATION(
      SchemaBuilder(Schema.Type.STRING).namespaced("Duration").build(),
      SchemaBuilder(Schema.Type.STRING).namespaced("OptionalDuration").optional().build()),
  POINT(
      SchemaBuilder(Schema.Type.STRUCT)
          .namespaced("Point")
          .field("srid", Schema.INT32_SCHEMA)
          .field("x", Schema.FLOAT64_SCHEMA)
          .field("y", Schema.FLOAT64_SCHEMA)
          .field("z", Schema.FLOAT64_SCHEMA)
          .build(),
      SchemaBuilder(Schema.Type.STRUCT)
          .namespaced("OptionalPoint")
          .field("srid", Schema.INT32_SCHEMA)
          .field("x", Schema.FLOAT64_SCHEMA)
          .field("y", Schema.FLOAT64_SCHEMA)
          .field("z", Schema.FLOAT64_SCHEMA)
          .optional()
          .build());

  fun schema(optional: Boolean = false): Schema = if (optional) this.optionalSchema else this.schema
}

object DynamicTypes {

  fun valueFor(schema: Schema, value: Any?): Any? {
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
            is Collection<*> -> value.map { valueFor(schema.valueSchema(), it) }
            is Array<*> -> value.map { valueFor(schema.valueSchema(), it) }.toTypedArray()
            is ShortArray -> value.map { s -> s.toLong() }.toLongArray()
            is IntArray -> value.map { i -> i.toLong() }.toLongArray()
            is FloatArray -> value.map { f -> f.toDouble() }.toDoubleArray()
            is BooleanArray,
            is LongArray,
            is DoubleArray -> value
            else -> throw IllegalArgumentException("unsupported array type ${value.javaClass.name}")
          }
      Schema.Type.MAP ->
          when (value) {
            is Map<*, *> -> value.mapValues { valueFor(schema.valueSchema(), it.value) }
            else -> throw IllegalArgumentException("unsupported map type ${value.javaClass.name}")
          }
      Schema.Type.STRUCT ->
          when (value) {
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
                      .forEach { e -> put(e.key, valueFor(schema.field(e.key).schema(), e.value)) }
                }
            is Relationship ->
                Struct(schema).apply {
                  put("<id>", value.id())
                  put("<type>", value.type())
                  put("<start.id>", value.startNodeId())
                  put("<end.id>", value.endNodeId())

                  value
                      .asMap { it.asObject() }
                      .forEach { e -> put(e.key, valueFor(schema.field(e.key).schema(), e.value)) }
                }
            is Map<*, *> ->
                Struct(schema).apply {
                  schema.fields().forEach {
                    put(it.name(), valueFor(it.schema(), value[it.name()]))
                  }
                }
            is Collection<*> ->
                Struct(schema).apply {
                  schema.fields().forEach {
                    put(
                        it.name(),
                        valueFor(it.schema(), value.elementAt(it.name().substring(1).toInt())))
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
            is Duration -> value.toString()
            is IsoDuration -> value.toString()
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

  fun schemaFor(value: Any?, optional: Boolean = false): Schema =
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
                .firstNotNullOfOrNull { schemaFor(it, false) }
                ?.run { SchemaBuilder.array(this).apply { if (optional) optional() }.build() }
                ?: SchemaBuilder.struct().apply { if (optional) optional() }.build()
        is LocalDate -> SimpleTypes.LOCALDATE.schema(optional)
        is LocalDateTime -> SimpleTypes.LOCALDATETIME.schema(optional)
        is LocalTime -> SimpleTypes.LOCALTIME.schema(optional)
        is OffsetDateTime -> SimpleTypes.ZONEDDATETIME.schema(optional)
        is ZonedDateTime -> SimpleTypes.ZONEDDATETIME.schema(optional)
        is OffsetTime -> SimpleTypes.OFFSETTIME.schema(optional)
        is Duration,
        is IsoDuration -> SimpleTypes.DURATION.schema(optional)
        is Point -> SimpleTypes.POINT.schema(optional)
        is Node ->
            SchemaBuilder.struct()
                .namespaced("Node")
                .apply {
                  field("<id>", SimpleTypes.LONG.schema())
                  field("<labels>", SchemaBuilder.array(SimpleTypes.STRING.schema()).build())

                  value.keys().forEach { field(it, schemaFor(value.get(it).asObject(), optional)) }

                  if (optional) optional()
                }
                .build()
        is Relationship ->
            SchemaBuilder.struct()
                .namespaced("Relationship")
                .apply {
                  field("<id>", SimpleTypes.LONG.schema())
                  field("<type>", SimpleTypes.STRING.schema())
                  field("<start.id>", SimpleTypes.LONG.schema())
                  field("<end.id>", SimpleTypes.LONG.schema())

                  value.keys().forEach { field(it, schemaFor(value.get(it).asObject(), optional)) }

                  if (optional) optional()
                }
                .build()
        is Collection<*> -> {
          val elementTypes = value.map { schemaFor(it, false) }

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
                  .mapValues { e -> schemaFor(e.value, optional) }

          when (elementTypes.values.toSet().size) {
            0 -> SchemaBuilder.struct().apply { if (optional) optional() }.build()
            1 ->
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
