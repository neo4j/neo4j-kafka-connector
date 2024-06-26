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
package org.neo4j.connectors.kafka.sink.legacy.strategy

object IngestionUtils {
  const val labelSeparator = ":"
  const val keySeparator = ", "

  fun getLabelsAsString(labels: Collection<String>): String =
      labels
          .map { it.quote() }
          .joinToString(labelSeparator)
          .let { if (it.isNotBlank()) "$labelSeparator$it" else it }

  fun getNodeKeysAsString(prefix: String = "properties", keys: Set<String>): String =
      keys.map { toQuotedProperty(prefix, it) }.joinToString(keySeparator)

  private fun toQuotedProperty(prefix: String = "properties", property: String): String {
    val quoted = property.quote()
    return "$quoted: event.$prefix.$quoted"
  }

  fun getNodeMergeKeys(prefix: String, keys: Set<String>): String =
      keys
          .map {
            val quoted = it.quote()
            "$quoted: event.$prefix.$quoted"
          }
          .joinToString(keySeparator)

  fun containsProp(key: String, properties: Collection<String>): Boolean =
      if (key.contains(".")) {
        properties.contains(key) || properties.any { key.startsWith("$it.") }
      } else {
        properties.contains(key)
      }
}
