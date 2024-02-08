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
package org.neo4j.connectors.kafka.testing

import java.lang.IllegalStateException

object MapSupport {

  @Suppress("UNCHECKED_CAST")
  internal fun <K, Any> MutableMap<K, Any>.nestUnder(
      key: K,
      values: Map<K, Any>
  ): MutableMap<K, Any> {
    val map = this[key]
    if (map !is Map<*, *>) {
      throw IllegalStateException("entry at key $key is not a mutable map")
    }
    (map as MutableMap<K, Any>).putAll(values)
    return this
  }

  /**
   * Filters out all specified keys from map
   *
   * @throws IllegalArgumentException if any of the specified keys are not part of this map set of
   *   keys
   */
  fun <K, V> Map<K, V>.excludingKeys(vararg keys: K): Map<K, V> {
    val missing = keys.filter { !this.keys.contains(it) }
    if (missing.isNotEmpty()) {
      throw IllegalArgumentException(
          "Cannot exclude keys ${missing.joinToString()}: they are missing from map $this")
    }
    val exclusions = setOf(*keys)
    return this.filterKeys { !exclusions.contains(it) }
  }

  fun <T : Any> MutableMap<String, Any>.putConditionally(
      key: String,
      value: T,
      condition: (T) -> Boolean
  ): MutableMap<String, Any> {
    if (!condition(value)) {
      return this
    }
    this[key] = value
    return this
  }
}
