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

import java.nio.ByteBuffer
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.OffsetTime
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.neo4j.connectors.kafka.data.DAYS
import org.neo4j.connectors.kafka.data.DIMENSION
import org.neo4j.connectors.kafka.data.DynamicTypes.notNullOrEmpty
import org.neo4j.connectors.kafka.data.MONTHS
import org.neo4j.connectors.kafka.data.NANOS
import org.neo4j.connectors.kafka.data.SECONDS
import org.neo4j.connectors.kafka.data.SR_ID
import org.neo4j.connectors.kafka.data.SimpleTypes
import org.neo4j.connectors.kafka.data.THREE_D
import org.neo4j.connectors.kafka.data.TWO_D
import org.neo4j.connectors.kafka.data.ValueConverter
import org.neo4j.connectors.kafka.data.X
import org.neo4j.connectors.kafka.data.Y
import org.neo4j.connectors.kafka.data.Z
import org.neo4j.driver.types.IsoDuration
import org.neo4j.driver.types.Node
import org.neo4j.driver.types.Point
import org.neo4j.driver.types.Relationship

class CompactValueConverter : ValueConverter {

  override fun schema(value: Any?, optional: Boolean, forceMapsAsStruct: Boolean): Schema {
    return when (value) {
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
        val schema = schema(first, optional, forceMapsAsStruct)
        SchemaBuilder.array(schema).apply { if (optional) optional() }.build()
      }

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
                  field(it, schema(value.get(it).asObject(), optional, forceMapsAsStruct))
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
                  field(it, schema(value.get(it).asObject(), optional, forceMapsAsStruct))
                }
                if (optional) optional()
              }
              .build()

      is Collection<*> -> {
        val nonEmptyElementTypes =
            value.filter { it.notNullOrEmpty() }.map { schema(it, optional, forceMapsAsStruct) }
        when (nonEmptyElementTypes.toSet().size) {
          0 ->
              SchemaBuilder.array(SimpleTypes.NULL.schema(true))
                  .apply { if (optional) optional() }
                  .build()

          1 ->
              SchemaBuilder.array(nonEmptyElementTypes.first())
                  .apply { if (optional) optional() }
                  .build()

          else -> {
            val nonEmptyElements = value.filter { it.notNullOrEmpty() }
            val merged =
                if (nonEmptyElements.all { it is Map<*, *> }) {
                  @Suppress("UNCHECKED_CAST")
                  mergeMapElementSchemas(nonEmptyElements as List<Map<*, *>>, optional)
                } else null

            if (merged != null) {
              SchemaBuilder.array(if (optional) makeOptional(merged) else merged)
                  .apply { if (optional) optional() }
                  .build()
            } else {
              SchemaBuilder.struct()
                  .apply {
                    value.forEachIndexed { i, v ->
                      this.field("e${i}", schema(v, optional, forceMapsAsStruct))
                    }
                  }
                  .apply { if (optional) optional() }
                  .build()
            }
          }
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
                            "unsupported map key type ${key?.javaClass?.name}"
                        )
                  }
                }
                .filter { e -> e.value.notNullOrEmpty() }
                .mapValues { e -> schema(e.value, optional, forceMapsAsStruct) }
        val valueSet = elementTypes.values.toSet()
        when {
          valueSet.isEmpty() ->
              SchemaBuilder.struct()
                  .apply {
                    value.forEach {
                      this.field(it.key as String, schema(it.value, optional, forceMapsAsStruct))
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
                      this.field(it.key as String, schema(it.value, optional, forceMapsAsStruct))
                    }
                  }
                  .apply { if (optional) optional() }
                  .build()
        }
      }

      else -> throw IllegalArgumentException("unsupported type ${value.javaClass.name}")
    }
  }

  override fun value(schema: Schema, value: Any?): Any? {
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
            is Collection<*> -> value.map { value(schema.valueSchema(), it) }
            is Array<*> -> value.map { value(schema.valueSchema(), it) }.toList()
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
            is Map<*, *> -> value.mapValues { value(schema.valueSchema(), it.value) }
            else -> throw IllegalArgumentException("unsupported map type ${value.javaClass.name}")
          }

      Schema.Type.STRUCT ->
          when (value) {
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
                      .forEach { e -> put(e.key, value(schema.field(e.key).schema(), e.value)) }
                }

            is Relationship ->
                Struct(schema).apply {
                  put("<id>", value.id())
                  put("<type>", value.type())
                  put("<start.id>", value.startNodeId())
                  put("<end.id>", value.endNodeId())
                  value
                      .asMap { it.asObject() }
                      .forEach { e -> put(e.key, value(schema.field(e.key).schema(), e.value)) }
                }

            is Map<*, *> ->
                Struct(schema).apply {
                  schema.fields().forEach { put(it.name(), value(it.schema(), value[it.name()])) }
                }

            is Collection<*> ->
                Struct(schema).apply {
                  schema.fields().forEach {
                    put(
                        it.name(),
                        value(it.schema(), value.elementAt(it.name().substring(1).toInt())),
                    )
                  }
                }

            else ->
                throw IllegalArgumentException("unsupported struct type ${value.javaClass.name}")
          }

      Schema.Type.STRING ->
          when (value) {
            is LocalDate -> DateTimeFormatter.ISO_DATE.format(value)
            is LocalDateTime -> DateTimeFormatter.ISO_DATE_TIME.format(value)
            is LocalTime -> DateTimeFormatter.ISO_TIME.format(value)
            is OffsetDateTime -> DateTimeFormatter.ISO_DATE_TIME.format(value)
            is ZonedDateTime -> DateTimeFormatter.ISO_DATE_TIME.format(value)
            is OffsetTime -> DateTimeFormatter.ISO_TIME.format(value)
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

  /**
   * Builds a merged STRUCT schema covering the union of keys across all element Maps. Keys absent
   * (or null/empty) in some elements are marked optional. Element values are re-inferred with
   * [forceMapsAsStruct] = true so a key whose value is a homogeneously-typed nested Map (which
   * would otherwise be inferred as MAP) is rendered as a STRUCT, comparable field-by-field with
   * sibling elements that already produced a STRUCT for the same key.
   *
   * Returns null when any key has multiple incompatible non-null value types across elements, so
   * the caller falls back to the indexed `{e0, e1, ...}` representation.
   */
  private fun mergeMapElementSchemas(elements: List<Map<*, *>>, optional: Boolean): Schema? {
    val allKeys = linkedSetOf<String>()
    for (element in elements) {
      for (key in element.keys) {
        if (key !is String) return null
        allKeys.add(key)
      }
    }

    val builder = SchemaBuilder.struct()
    for (key in allKeys) {
      val fieldValues =
          elements.filter { it.containsKey(key) && it[key].notNullOrEmpty() }.map { it[key] }

      val resolvedSchema =
          if (fieldValues.isEmpty()) {
            SimpleTypes.NULL.schema(true)
          } else {
            val fieldSchemas = fieldValues.map { schema(it, optional, true) }.toSet()
            when {
              fieldSchemas.size == 1 -> fieldSchemas.first()
              fieldValues.all { it is Map<*, *> } -> {
                @Suppress("UNCHECKED_CAST")
                mergeMapElementSchemas(fieldValues as List<Map<*, *>>, optional) ?: return null
              }
              else -> return null
            }
          }

      val presentInAll = elements.all { it.containsKey(key) && it[key].notNullOrEmpty() }
      builder.field(key, if (presentInAll) resolvedSchema else makeOptional(resolvedSchema))
    }
    return builder.build()
  }

  private fun makeOptional(schema: Schema): Schema {
    if (schema.isOptional) return schema
    return when (schema.type()) {
      Schema.Type.STRUCT ->
          SchemaBuilder.struct()
              .apply {
                schema.fields().forEach { field(it.name(), it.schema()) }
                optional()
              }
              .build()
      Schema.Type.ARRAY -> SchemaBuilder.array(schema.valueSchema()).optional().build()
      Schema.Type.MAP ->
          SchemaBuilder.map(schema.keySchema(), schema.valueSchema()).optional().build()
      else -> SchemaBuilder.type(schema.type()).optional().build()
    }
  }
}
