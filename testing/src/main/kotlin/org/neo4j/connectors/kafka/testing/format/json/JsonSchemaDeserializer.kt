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
package org.neo4j.connectors.kafka.testing.format.json

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.neo4j.cdc.client.model.ChangeEvent
import org.neo4j.connectors.kafka.testing.format.ChangeEventSupport.mapToChangeEvent
import org.neo4j.connectors.kafka.testing.format.KafkaRecordDeserializer
import org.neo4j.connectors.kafka.testing.format.MappingException

object JsonSchemaDeserializer : KafkaRecordDeserializer {

  @Suppress("UNCHECKED_CAST")
  override fun <K> deserialize(sourceValue: Any?, targetClass: Class<K>): K? {
    if (sourceValue == null) {
      return null
    }

    val resultValue =
        when (sourceValue) {
          is Map<*, *> ->
              when (targetClass) {
                Map::class.java -> (sourceValue as Map<String, Any?>).normalize()
                ChangeEvent::class.java ->
                    mapToChangeEvent((sourceValue as Map<String, Any?>).normalize())
                Struct::class.java -> (sourceValue as Map<String, Any?>).asStruct()
                else -> throw MappingException(sourceValue, targetClass)
              }
          is List<*> ->
              when (targetClass) {
                List::class.java -> sourceValue.map { it?.normalizeValue() }
                else -> throw MappingException(sourceValue, targetClass)
              }
          is String -> sourceValue
          else -> throw MappingException(sourceValue)
        }
    return resultValue as K?
  }

  private fun Map<String, Any?>.normalize(): Map<String, Any?> {
    return this.filter { it.value != null }.mapValues { it.value?.normalizeValue() }
  }

  @Suppress("UNCHECKED_CAST")
  private fun Any.normalizeValue(): Any {
    return when (this) {
      is Long -> this
      is Int -> this.toLong()
      is Map<*, *> -> (this as Map<String, Any?>).normalize()
      is List<*> -> this.map { it?.normalizeValue() }
      else -> this
    }
  }

  private fun Map<String, Any?>.asStruct(): Struct {
    val schema = this.toSchema()
    val struct = Struct(schema)

    schema.fields().forEach { field ->
      struct.put(field.name(), castValue(field.schema(), this[field.name()]!!))
    }

    return struct
  }

  private fun Map<String, Any?>.toSchema(): Schema {
    val builder = SchemaBuilder.struct()
    this.keys.forEach { builder.field(it, valueToSchema(this[it]!!)) }
    return builder.build()
  }

  private fun castValue(schema: Schema, value: Any): Any =
      when (schema.type()) {
        Schema.Type.INT32 -> value as Int
        Schema.Type.INT64 -> value as Long
        Schema.Type.FLOAT32 -> value as Float
        Schema.Type.FLOAT64 -> value as Double
        Schema.Type.STRUCT -> {
          when (value) {
            is Map<*, *> -> {
              val struct = Struct(schema)
              schema.fields().forEach { field ->
                castValue(field.schema(), value[field.name()]!!).let { struct.put(field, it) }
              }
              struct
            }
            else -> throw MappingException(value)
          }
        }
        else -> value.toString()
      }

  @Suppress("UNCHECKED_CAST")
  private fun valueToSchema(value: Any): Schema =
      when (value) {
        is Int -> SchemaBuilder.int32()
        is Long -> SchemaBuilder.int64()
        is Float -> SchemaBuilder.float32()
        is Double -> SchemaBuilder.float64()
        is Map<*, *> -> (value as Map<String, Any?>).toSchema()
        else -> SchemaBuilder.string()
      }
}
