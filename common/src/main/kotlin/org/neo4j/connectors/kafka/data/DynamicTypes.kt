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
import java.time.ZonedDateTime
import kotlin.reflect.KClass
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.neo4j.driver.types.IsoDuration
import org.neo4j.driver.types.Node
import org.neo4j.driver.types.Point
import org.neo4j.driver.types.Relationship

internal fun SchemaBuilder.namespaced(vararg paths: String): SchemaBuilder =
    this.name("org.neo4j.connectors.kafka." + paths.joinToString("."))

internal fun Schema.id(): String = this.name().orEmpty().ifEmpty { this.type().name }

internal fun Schema.shortId(): String = this.id().split('.').last()

fun Schema.matches(other: Schema): Boolean {
  return this.id() == other.id() || this.shortId() == other.shortId()
}

object DynamicTypes {
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
  ): Schema {
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
          if (isSimplePropertyType(value::class.java.componentType.kotlin)) {
            PropertyType.schema
          } else {
            val first = value.firstOrNull { it.notNullOrEmpty() }
            val schema = toConnectSchema(first, optional, forceMapsAsStruct)
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
        if (elementType != null && isSimplePropertyType(elementType)) {
          return PropertyType.schema
        }

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

        val elementValueTypesSet = elementTypes.values.toSet()
        when {
          elementValueTypesSet.isEmpty() ->
              SchemaBuilder.struct()
                  .apply {
                    value.forEach {
                      this.field(
                          it.key as String, toConnectSchema(it.value, optional, forceMapsAsStruct))
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
                      this.field(
                          it.key as String, toConnectSchema(it.value, optional, forceMapsAsStruct))
                    }
                  }
                  .apply { if (optional) optional() }
                  .build()
        }
      }
      else -> throw IllegalArgumentException("unsupported type ${value.javaClass.name}")
    }
  }

  private fun isSimplePropertyType(cls: KClass<*>): Boolean =
      when (cls) {
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
            if (IsoDuration::class.java.isAssignableFrom(cls.java)) {
              true
            } else if (Point::class.java.isAssignableFrom(cls.java)) {
              true
            } else {
              false
            }
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
