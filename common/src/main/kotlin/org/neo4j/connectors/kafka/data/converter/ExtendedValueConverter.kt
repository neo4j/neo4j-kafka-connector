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
import kotlin.reflect.KClass
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.neo4j.connectors.kafka.data.DAYS
import org.neo4j.connectors.kafka.data.DIMENSION
import org.neo4j.connectors.kafka.data.DynamicTypes.notNullOrEmpty
import org.neo4j.connectors.kafka.data.MONTHS
import org.neo4j.connectors.kafka.data.NANOS
import org.neo4j.connectors.kafka.data.PropertyType
import org.neo4j.connectors.kafka.data.SECONDS
import org.neo4j.connectors.kafka.data.SR_ID
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

class ExtendedValueConverter : ValueConverter {

  override fun schema(value: Any?, optional: Boolean, forceMapsAsStruct: Boolean): Schema {
    return when (value) {
      null -> PropertyType.schema
      is Boolean,
      is Float,
      is Double,
      is Number,
      is Char,
      is LocalDate,
      is LocalDateTime,
      is LocalTime,
      is OffsetDateTime,
      is ZonedDateTime,
      is OffsetTime,
      is IsoDuration,
      is Point,
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

      is Array<*> ->
          if (value::class.java.componentType.kotlin.isSimplePropertyType()) {
            PropertyType.schema
          } else {
            val first = value.firstOrNull { it.notNullOrEmpty() }
            val schema = schema(first, optional, forceMapsAsStruct)
            SchemaBuilder.array(schema).apply { if (optional) optional() }.build()
          }

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
        val elementTypes = value.map { it?.javaClass?.kotlin }.toSet()
        if (elementTypes.isEmpty()) {
          return PropertyType.schema
        }

        val elementType = elementTypes.singleOrNull()
        if (elementType != null && elementType.isSimplePropertyType()) {
          return PropertyType.schema
        }

        val nonEmptyElementTypes =
            value.filter { it.notNullOrEmpty() }.map { schema(it, optional, forceMapsAsStruct) }

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
                      this.field("e${i}", schema(v, optional, forceMapsAsStruct))
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
                            "unsupported map key type ${key?.javaClass?.name}"
                        )
                  }
                }
                .filter { e -> e.value.notNullOrEmpty() }
                .mapValues { e -> schema(e.value, optional, forceMapsAsStruct) }

        val elementValueTypesSet = elementTypes.values.toSet()
        when {
          elementValueTypesSet.isEmpty() ->
              SchemaBuilder.struct()
                  .apply {
                    value.forEach {
                      this.field(it.key as String, schema(it.value, optional, forceMapsAsStruct))
                    }
                  }
                  .apply { if (optional) optional() }
                  .build()

          elementValueTypesSet.singleOrNull() != null && !forceMapsAsStruct ->
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

    if (schema == PropertyType.schema) {
      return PropertyType.toConnectValue(value)
    }

    return when (schema.type()) {
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

      else -> value
    }
  }

  private fun KClass<*>.isSimplePropertyType(): Boolean =
      when (this) {
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
        OffsetTime::class -> true

        else ->
            if (IsoDuration::class.java.isAssignableFrom(this.java)) {
              true
            } else if (Point::class.java.isAssignableFrom(this.java)) {
              true
            } else {
              false
            }
      }
}
