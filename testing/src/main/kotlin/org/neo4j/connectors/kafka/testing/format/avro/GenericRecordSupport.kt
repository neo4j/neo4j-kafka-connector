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
package org.neo4j.connectors.kafka.testing.format.avro

import org.apache.avro.generic.GenericArray
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.neo4j.connectors.kafka.testing.format.MappingException
import org.neo4j.connectors.kafka.testing.format.avro.GenericRecordSupport.asMap

object GenericRecordSupport {

  fun GenericRecord.asMap(): Map<String, Any> {
    return this.schema.fields
        .filter { field -> this.get(field.name()) != null }
        .associate { field -> field.name() to castValue(get(field.name())) }
  }

  fun GenericArray<*>.asList(): List<Any> {
    return this.map { castValue(it) }.toList()
  }

  fun GenericRecord.asStruct(): Struct {
    val schema = this.toSchema()
    val struct = Struct(schema)

    schema.fields().forEach { field ->
      struct.put(field.name(), castValue(field.schema(), get(field.name())))
    }

    return struct
  }

  private fun GenericRecord.toSchema(): Schema {
    val builder = SchemaBuilder.struct()
    this.schema.fields
        .filter { field -> this.get(field.name()) != null }
        .forEach { builder.field(it.name(), valueToSchema(this.get(it.name()))) }
    return builder.build()
  }

  private fun castValue(value: Any): Any =
      when (value) {
        is Int -> value.toLong()
        is Long -> value
        is Map<*, *> ->
            value
                .filter { it.key != null && it.value != null }
                .mapKeys { castValue(it.key!!) }
                .mapValues { castValue(it.value!!) }
        is GenericArray<*> -> value.asList()
        is GenericRecord -> value.asMap()
        else -> value.toString()
      }

  private fun castValue(schema: Schema, value: Any): Any =
      when (schema.type()) {
        Schema.Type.INT32 -> value as Int
        Schema.Type.INT64 -> value as Long
        Schema.Type.FLOAT32 -> value as Float
        Schema.Type.FLOAT64 -> value as Double
        Schema.Type.STRUCT -> {
          when (value) {
            is GenericRecord -> {
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
        is GenericRecord -> value.toSchema()
        else -> SchemaBuilder.string()
      }
}
