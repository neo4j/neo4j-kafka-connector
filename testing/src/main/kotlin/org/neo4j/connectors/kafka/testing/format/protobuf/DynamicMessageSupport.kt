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
package org.neo4j.connectors.kafka.testing.format.protobuf

import com.google.protobuf.DynamicMessage
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.neo4j.connectors.kafka.testing.format.MappingException

object DynamicMessageSupport {

  fun DynamicMessage.asMap(): Map<String, Any> {
    return this.allFields.entries
        .filter { field -> field.value != null }
        .associate { field -> field.key.name to castValue(field.value) }
  }

  fun DynamicMessage.asStruct(): Struct {
    val schema = this.toSchema()
    val struct = Struct(schema)

    schema.fields().forEach { field ->
      struct.put(field.name(), castValue(field.schema(), get(field.name())))
    }

    return struct
  }

  private fun DynamicMessage.toSchema(): Schema {
    val builder = SchemaBuilder.struct()
    this.allFields.entries.forEach { builder.field(it.key.name, valueToSchema(it.value)) }
    return builder.build()
  }

  private fun castValue(value: Any): Any =
      when (value) {
        is Boolean -> value
        is String -> value
        is Number -> value
        is DynamicMessage -> value.asMap()
        is List<*> -> castList(value)
        else -> value.toString()
      }

  private fun castList(list: List<*>): Any {
    if (list.isEmpty()) {
      return list
    }
    return when (val firstValue = list.first()) {
      // This is the workaround to restore Map object. According to our schema protobuf
      is DynamicMessage -> {
        if (firstValue.allFields.entries.map { it.key.name }.toSet() == setOf("key", "value")) {
          list.associate { v ->
            val dm = v as DynamicMessage
            val key = castValue(dm.allFields.entries.first { it.key.name == "key" }.value)
            val value = castValue(dm.allFields.entries.first { it.key.name == "value" }.value)
            key to value
          }
        } else {
          list.mapNotNull { castValue(it!!) }
        }
      }
      else -> list.mapNotNull { castValue(it!!) }
    }
  }

  private fun castValue(schema: Schema, value: Any): Any =
      when (schema.type()) {
        Schema.Type.INT32 -> value as Int
        Schema.Type.INT64 -> value as Long
        Schema.Type.FLOAT32 -> value as Float
        Schema.Type.FLOAT64 -> value as Double
        Schema.Type.STRUCT -> {
          when (value) {
            is DynamicMessage -> {
              val struct = Struct(schema)
              schema.fields().forEach { field ->
                castValue(field.schema(), value.get(field.name())).let { struct.put(field, it) }
              }
              struct
            }
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

  fun DynamicMessage.get(key: String): Any {
    return this.asMap()[key]!!
  }

  private fun valueToSchema(value: Any): Schema =
      when (value) {
        is Int -> SchemaBuilder.int32()
        is Long -> SchemaBuilder.int64()
        is Float -> SchemaBuilder.float32()
        is Double -> SchemaBuilder.float64()
        is Map<*, *> -> {
          val structMap = SchemaBuilder.struct().optional()
          value.forEach { entry ->
            valueToSchema(entry.value!!).let { structMap.field(entry.key.toString(), it) }
          }
          structMap.build()
        }
        is DynamicMessage -> value.toSchema()
        else -> SchemaBuilder.string()
      }
}
