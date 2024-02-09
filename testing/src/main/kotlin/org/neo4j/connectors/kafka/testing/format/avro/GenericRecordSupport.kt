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
package org.neo4j.connectors.kafka.testing.format.avro

import org.apache.avro.generic.GenericArray
import org.apache.avro.generic.GenericRecord

object GenericRecordSupport {

  fun GenericRecord.asMap(): Map<String, Any> {
    return this.schema.fields
        .filter { field -> this.get(field.name()) != null }
        .associate { field -> field.name() to castValue(this.get(field.name())) }
  }

  fun GenericArray<*>.asList(): List<Any> {
    return this.map { castValue(it) }.toList()
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
}
