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

object DynamicMessageSupport {

  fun DynamicMessage.asMap(): Map<String, Any> {
    return this.allFields.entries
        .filter { field -> field.value != null }
        .associate { field -> field.key.name to castValue(field.value) }
  }

  private fun castValue(value: Any): Any =
      when (value) {
        is Long -> value
        is Int -> value.toLong()
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
}
