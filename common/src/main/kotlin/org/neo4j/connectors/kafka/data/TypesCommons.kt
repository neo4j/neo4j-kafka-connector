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

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder

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

internal fun SchemaBuilder.namespaced(vararg paths: String): SchemaBuilder =
    this.name("org.neo4j.connectors.kafka." + paths.joinToString("."))

internal fun Schema.id(): String = this.name().orEmpty().ifEmpty { this.type().name }

internal fun Schema.shortId(): String = this.id().split('.').last()

fun Schema.matches(other: Schema): Boolean {
  return this.id() == other.id() || this.shortId() == other.shortId()
}

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
