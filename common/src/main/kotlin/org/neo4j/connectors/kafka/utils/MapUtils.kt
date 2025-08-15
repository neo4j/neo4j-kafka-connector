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
package org.neo4j.connectors.kafka.utils

import org.neo4j.connectors.kafka.exceptions.InvalidDataException

object MapUtils {

  inline fun <reified T> Map<*, *>.getIterable(key: String): Iterable<T>? {
    return when (val value = this[key]) {
      null -> null
      is Iterable<*> ->
          value.map {
            if (T::class.isInstance(it)) {
              it as T
            } else {
              throw InvalidDataException(
                  "Elements of '$key' is not an instance of ${T::class.simpleName}"
              )
            }
          }
      else -> throw InvalidDataException("Map element '$key' is not an instance of Iterable")
    }
  }

  inline fun <reified K, reified V> Map<*, *>.getMap(key: String): Map<K, V?>? {
    return when (val value = this[key]) {
      null -> null
      is Map<*, *> ->
          value
              .map {
                if (!K::class.isInstance(it.key)) {
                  throw InvalidDataException(
                      "Keys of '$key' is not an instance of ${K::class.simpleName}"
                  )
                }
                if (!V::class.isInstance(it.value)) {
                  throw InvalidDataException(
                      "Values of '$key' is not an instance of ${V::class.simpleName}"
                  )
                }

                (it.key as K) to (it.value as V)
              }
              .toMap()
      else -> throw InvalidDataException("Map element '$key' is not an instance of Map")
    }
  }

  inline fun <reified T> Map<*, *>.getTyped(key: String): T? {
    val value = this[key] ?: return null

    if (T::class.isInstance(value)) {
      return value as T
    }

    throw InvalidDataException("Map element '$key' is not an instance of ${T::class.simpleName}")
  }

  @Suppress("UNCHECKED_CAST")
  fun Map<String, Any?>.flatten(
      map: Map<String, Any?> = this,
      prefix: String = "",
  ): Map<String, Any?> {
    return map.flatMap {
          val key = it.key
          val value = it.value
          val newKey = if (prefix != "") "$prefix.$key" else key
          if (value is Map<*, *>) {
            flatten(value as Map<String, Any?>, newKey).toList()
          } else {
            listOf(newKey to value)
          }
        }
        .toMap()
  }
}
