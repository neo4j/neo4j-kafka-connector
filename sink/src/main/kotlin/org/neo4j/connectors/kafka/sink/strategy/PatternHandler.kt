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
package org.neo4j.connectors.kafka.sink.strategy

import java.time.Instant
import java.time.ZoneOffset
import org.apache.kafka.connect.errors.ConnectException
import org.neo4j.connectors.kafka.extensions.flatten
import org.neo4j.connectors.kafka.sink.SinkConfiguration
import org.neo4j.connectors.kafka.sink.SinkMessage
import org.neo4j.connectors.kafka.sink.strategy.pattern.Pattern

abstract class PatternHandler<T : Pattern>(
    protected val bindTimestampAs: String = SinkConfiguration.DEFAULT_BIND_TIMESTAMP_ALIAS,
    protected val bindHeaderAs: String = SinkConfiguration.DEFAULT_BIND_HEADER_ALIAS,
    protected val bindKeyAs: String = SinkConfiguration.DEFAULT_BIND_KEY_ALIAS,
    protected val bindValueAs: String = SinkConfiguration.DEFAULT_BIND_VALUE_ALIAS,
) : AbstractHandler() {
  abstract val pattern: T

  @Suppress("UNCHECKED_CAST")
  protected fun flattenMessage(message: SinkMessage): Map<String, Any?> {
    return buildMap<String, Any?> {
          this.putAll(
              mapOf(
                  bindTimestampAs to
                      Instant.ofEpochMilli(message.record.timestamp()).atOffset(ZoneOffset.UTC),
                  bindHeaderAs to message.headerFromConnectValue(),
                  bindKeyAs to message.keyFromConnectValue()))
          if (message.value != null) {
            this[bindValueAs] =
                when (val value = message.valueFromConnectValue()) {
                  is Map<*, *> -> value as Map<String, Any?>
                  else -> throw ConnectException("Message value must be convertible to a Map.")
                }
          }
        }
        .flatten()
  }

  /**
   * Checks if given <strong>from</strong> key is explicitly defined, i.e. something starting with
   * __key, __value or __header.
   */
  private fun isExplicit(from: String): Boolean =
      from.startsWith("$bindValueAs.") ||
          from.startsWith("$bindKeyAs.") ||
          from.startsWith("$bindHeaderAs.") ||
          from == bindTimestampAs

  /**
   * Extracts key properties from flattened message properties, trying different prefixes in turn
   * for implicitly defined property mappings (those that do not start with __key, __value or
   * __header). Adds used keys to the usage tracker so that we can determine which properties are
   * considered within wildcard style inclusions.
   */
  protected fun extractKeys(
      pattern: Pattern,
      flattened: Map<String, Any?>,
      usedTracker: MutableSet<String>,
      vararg prefixes: String
  ): Map<String, Any?> =
      pattern.keyProperties
          .associateBy { it.to }
          .mapValues { (_, mapping) ->
            if (isExplicit(mapping.from)) {
              usedTracker += mapping.from
              return@mapValues flattened[mapping.from]
            }

            for (prefix in prefixes) {
              val key = "$prefix.${mapping.from}"

              if (flattened.containsKey(key)) {
                usedTracker += key
                return@mapValues flattened[key]
              }
            }
          }

  /**
   * Extracts properties from flattened message properties, excluding previously used keys from the
   * computed properties.
   *
   * Adds used keys to the usage tracker so that we can determine which properties are considered
   * within wildcard style inclusions.
   */
  protected fun computeProperties(
      pattern: Pattern,
      flattened: Map<String, Any?>,
      used: MutableSet<String>
  ): Map<String, Any?> {
    return buildMap {
      if (pattern.includeAllValueProperties) {
        this.putAll(
            flattened
                .filterKeys { !used.contains(it) }
                .filterKeys { it.startsWith(bindValueAs) }
                .mapKeys { it.key.substring(bindValueAs.length + 1) })
      }

      pattern.includeProperties.forEach { mapping ->
        val key = if (isExplicit(mapping.from)) mapping.from else "$bindValueAs.${mapping.from}"
        if (flattened.containsKey(key)) {
          used += key
          this[mapping.to] = flattened[key]
        } else {
          this.putAll(
              flattened
                  .filterKeys { it.startsWith(key) }
                  .mapKeys {
                    used += it.key
                    mapping.to + it.key.substring(key.length)
                  })
        }
      }

      pattern.excludeProperties.forEach { exclude ->
        if (this.containsKey(exclude)) {
          this.remove(exclude)
        } else {
          this.keys.filter { it.startsWith("$exclude.") }.forEach { this.remove(it) }
        }
      }
    }
  }
}
