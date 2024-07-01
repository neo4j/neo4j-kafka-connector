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
package org.neo4j.connectors.kafka.sink.strategy.legacy

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import org.neo4j.connectors.kafka.events.StreamsTransactionEvent
import org.neo4j.connectors.kafka.sink.converters.Neo4jValueConverter
import org.neo4j.connectors.kafka.utils.JSONUtils
import org.neo4j.connectors.kafka.utils.asStreamsMap
import org.neo4j.driver.Record
import org.neo4j.driver.types.Node
import org.neo4j.driver.types.Point
import org.neo4j.driver.types.Relationship

data class StreamsSinkEntity(val key: Any?, val value: Any?)

fun StreamsSinkEntity.toStreamsTransactionEvent(
    evaluation: (StreamsTransactionEvent) -> Boolean
): StreamsTransactionEvent? =
    if (this.value != null) {
      val data = JSONUtils.asStreamsTransactionEvent(this.value)
      if (evaluation(data)) data else null
    } else {
      null
    }

fun SinkRecord.toStreamsSinkEntity(): StreamsSinkEntity =
    StreamsSinkEntity(convertData(this.key(), true), convertData(this.value()))

private val converter = Neo4jValueConverter()

private fun convertData(data: Any?, stringWhenFailure: Boolean = false) =
    when (data) {
      is Struct -> converter.convert(data)
      null -> null
      else -> JSONUtils.readValue<Any>(data, stringWhenFailure)
    }

fun Record.schema(asMap: Map<String, Any> = this.asMap()): Schema {
  val structBuilder = SchemaBuilder.struct()
  asMap.forEach { structBuilder.field(it.key, neo4jValueSchema(it.value)) }
  return structBuilder.build()
}

private fun neo4jValueSchema(value: Any?): Schema? =
    when (value) {
      null -> null
      is Long -> Schema.OPTIONAL_INT64_SCHEMA
      is Double -> Schema.OPTIONAL_FLOAT64_SCHEMA
      is Boolean -> Schema.OPTIONAL_BOOLEAN_SCHEMA
      is Collection<*> -> {
        val schema = value.firstNotNullOfOrNull { neo4jValueSchema(it) }
        if (schema == null) null else SchemaBuilder.array(schema).optional()
      }
      is Array<*> -> {
        val schema = value.firstNotNullOfOrNull { neo4jValueSchema(it) }
        if (schema == null) null else SchemaBuilder.array(schema).optional()
      }
      is Map<*, *> -> {
        if (value.isEmpty()) {
          SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA).optional().build()
        } else {
          val valueTypes =
              value.values
                  .mapNotNull { elem -> elem?.let { it::class.java.simpleName } }
                  .filter { !it.lowercase().startsWith("empty") }
                  .toSet()
          if (valueTypes.size == 1) {
            neo4jValueSchema(value.values.first())?.let {
              SchemaBuilder.map(Schema.STRING_SCHEMA, it).optional().build()
            }
          } else {
            val structMap = SchemaBuilder.struct().optional()
            value.forEach {
              val entry = it
              neo4jValueSchema(entry.value)?.let { structMap.field(entry.key.toString(), it) }
            }
            if (structMap.fields().isEmpty()) null else structMap.build()
          }
        }
      }
      is Point -> neo4jValueSchema(JSONUtils.readValue<Map<String, Any>>(value))
      is Node -> neo4jValueSchema(value.asStreamsMap())
      is Relationship -> neo4jValueSchema(value.asStreamsMap())
      else -> Schema.OPTIONAL_STRING_SCHEMA
    }
