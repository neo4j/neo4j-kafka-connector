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
import org.neo4j.connectors.kafka.extensions.flatten
import org.neo4j.connectors.kafka.sink.ChangeQuery
import org.neo4j.connectors.kafka.sink.SinkConfiguration
import org.neo4j.connectors.kafka.sink.SinkMessage
import org.neo4j.connectors.kafka.sink.SinkStrategy
import org.neo4j.connectors.kafka.sink.strategy.pattern.NodePattern
import org.neo4j.connectors.kafka.sink.strategy.pattern.Pattern
import org.neo4j.cypherdsl.core.Cypher
import org.neo4j.cypherdsl.core.renderer.Renderer
import org.neo4j.driver.Query
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@Suppress("MemberVisibilityCanBePrivate", "UNUSED_PARAMETER", "UNCHECKED_CAST")
class NodePatternHandler(
    val topic: String,
    patternString: String,
    mergeProperties: Boolean,
    renderer: Renderer,
    private val batchSize: Int,
    private val bindTimestampAs: String = SinkConfiguration.DEFAULT_BIND_TIMESTAMP_ALIAS,
    private val bindHeaderAs: String = SinkConfiguration.DEFAULT_BIND_HEADER_ALIAS,
    private val bindKeyAs: String = SinkConfiguration.DEFAULT_BIND_KEY_ALIAS,
    private val bindValueAs: String = SinkConfiguration.DEFAULT_BIND_VALUE_ALIAS,
) : AbstractHandler() {
  private val logger: Logger = LoggerFactory.getLogger(javaClass)
  private val pattern: NodePattern
  private val query: String

  init {
    val parsed = Pattern.parse(patternString)
    if (parsed !is NodePattern) {
      throw IllegalArgumentException(
          "Invalid pattern provided for NodePatternHandler: ${parsed.javaClass.name}")
    }
    pattern = parsed

    val event = Cypher.name("event")
    val node =
        Cypher.node(pattern.labels.first(), pattern.labels.drop(1))
            .withProperties(
                pattern.keyProperties
                    .map { it.key to event.property("keys").property(it.key) }
                    .toMap())
            .named("n")
    val createStatement =
        renderer.render(
            Cypher.merge(node)
                .let {
                  if (mergeProperties) {
                    it.mutate(node, event.property("properties"))
                  } else {
                    it.set(node, event.property("properties"))
                  }
                }
                .mutate(node, event.property("keys"))
                .build())
    val deleteStatement = renderer.render(Cypher.merge(node).detachDelete(node).build())

    query =
        """
        UNWIND ${'$'}messages AS event
        WITH
            CASE WHEN event[0] = 'C' THEN [1] ELSE [] END AS create,
            CASE WHEN event[0] = 'D' THEN [1] ELSE [] END AS delete,
            event[1] AS event
        FOREACH (i IN create | $createStatement)
        FOREACH (i IN delete | $deleteStatement)
        """
            .trimIndent()
    logger.debug("using Cypher delete query '{}' for topic '{}'", query, topic)
  }

  override fun strategy() = SinkStrategy.NODE_PATTERN

  override fun handle(messages: Iterable<SinkMessage>): Iterable<Iterable<ChangeQuery>> {
    return messages
        .asSequence()
        .onEach { logger.trace("received message: '{}'", it) }
        .map {
          val isTombstoneMessage = it.value == null
          val flattened =
              buildMap<String, Any?> {
                    this.putAll(
                        mapOf(
                            bindTimestampAs to
                                Instant.ofEpochMilli(it.record.timestamp())
                                    .atOffset(ZoneOffset.UTC),
                            bindHeaderAs to it.headerFromConnectValue(),
                            bindKeyAs to it.keyFromConnectValue()))
                    if (!isTombstoneMessage) {
                      this[bindValueAs] = it.valueFromConnectValue() as Map<String, Any>
                    }
                  }
                  .flatten()

          if (isTombstoneMessage) {
            listOf(
                "D",
                mapOf(
                    "keys" to
                        pattern.keyProperties.map { kvp ->
                          kvp.key to
                              flattened[
                                  if (kvp.value.startsWith(bindValueAs) ||
                                      kvp.value.startsWith(bindKeyAs) ||
                                      kvp.value.startsWith(bindTimestampAs) ||
                                      kvp.value.startsWith(bindHeaderAs))
                                      kvp.value
                                  else "$bindKeyAs.${kvp.value}"]
                        },
                ))
          } else {
            val value = it.valueFromConnectValue() as Map<String, Any>
            val mapped =
                listOf(
                    "C",
                    mapOf(
                        "keys" to
                            pattern.keyProperties.map { kvp ->
                              kvp.key to
                                  flattened[
                                      if (kvp.value.startsWith(bindValueAs) ||
                                          kvp.value.startsWith(bindKeyAs) ||
                                          kvp.value.startsWith(bindTimestampAs) ||
                                          kvp.value.startsWith(bindHeaderAs))
                                          kvp.value
                                      else "$bindValueAs.${kvp.value}"]
                            },
                        "properties" to computeProperties(value, flattened)))
            logger.trace("message '{}' mapped to: '{}'", it, mapped)
            mapped
          }
        }
        .chunked(batchSize)
        .map { listOf(ChangeQuery(null, null, Query(query, mapOf("events" to it)))) }
        .onEach { logger.trace("mapped messages: '{}'", it) }
        .toList()
  }

  private fun computeProperties(value: Map<String, Any>, flattened: Map<String, Any?>) {
    buildMap<String, Any?> {
      if (pattern.includeProperties.containsKey("*")) {
        this.putAll(value)
      }
      pattern.includeProperties.forEach { kvp ->
        if (kvp.key != "*") {
          this[kvp.key] =
              flattened[
                  if (kvp.value.startsWith(bindValueAs)) kvp.value else "$bindValueAs.${kvp.value}",
              ]
        }
      }
      pattern.excludeProperties.forEach { excludedProperty -> this.remove(excludedProperty) }
    }
  }
}
