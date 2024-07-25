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
package org.neo4j.connectors.kafka.sink.strategy.pattern

import org.neo4j.cypherdsl.core.internal.SchemaNames

interface Pattern {
  val text: String
  val includeAllValueProperties: Boolean
  val keyProperties: Set<PropertyMapping>
  val includeProperties: Set<PropertyMapping>
  val excludeProperties: Set<String>

  companion object {
    fun parse(expression: String): Pattern {
      return Visitors.parse(expression)
    }
  }
}

internal fun trySanitize(identifier: String): String {
  return SchemaNames.sanitize(identifier).orElseThrow {
    IllegalArgumentException("unable to escape identifier '$identifier'")
  }
}

internal fun Pattern.propertiesAsText(): String {
  return StringBuilder()
      .append("{")
      .apply {
        this.append(
            keyProperties.joinToString(", ") { m ->
              "!${trySanitize(m.to)}: ${trySanitize(m.from)}"
            })
      }
      .apply {
        if (keyProperties.isNotEmpty() && includeProperties.isNotEmpty()) {
          this.append(", ")
        }
        this.append(
            includeProperties.joinToString(", ") { m ->
              "${trySanitize(m.to)}: ${trySanitize(m.from)}"
            })
      }
      .apply {
        if ((keyProperties.isNotEmpty() || includeProperties.isNotEmpty()) &&
            excludeProperties.isNotEmpty()) {
          this.append(", ")
        }
        this.append(excludeProperties.joinToString(", ") { "-" + trySanitize(it) })
      }
      .apply {
        if ((keyProperties.isNotEmpty() ||
            includeProperties.isNotEmpty() ||
            excludeProperties.isNotEmpty()) && includeAllValueProperties) {
          this.append(", ")
        }

        if (includeAllValueProperties) {
          this.append("*")
        }
      }
      .append("}")
      .toString()
}

data class PropertyMapping(val from: String, val to: String) {

  companion object {
    val WILDCARD = PropertyMapping("*", "*")
  }
}
