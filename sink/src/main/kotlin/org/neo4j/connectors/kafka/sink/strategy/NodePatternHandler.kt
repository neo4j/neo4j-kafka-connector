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
import org.neo4j.connectors.kafka.sink.ChangeQuery
import org.neo4j.connectors.kafka.sink.SinkConfiguration
import org.neo4j.connectors.kafka.sink.SinkMessage
import org.neo4j.connectors.kafka.sink.SinkStrategy
import org.neo4j.connectors.kafka.sink.strategy.pattern.NodePattern
import org.neo4j.connectors.kafka.sink.strategy.pattern.Pattern
import org.neo4j.cypherdsl.core.Clauses
import org.neo4j.cypherdsl.core.Cypher
import org.neo4j.cypherdsl.core.Operations
import org.neo4j.cypherdsl.core.UpdatingClause
import org.neo4j.cypherdsl.core.renderer.Renderer
import org.neo4j.driver.Query
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@Suppress("UNCHECKED_CAST")
class NodePatternHandler(
    val topic: String,
    patternString: String,
    private val mergeProperties: Boolean,
    private val renderer: Renderer,
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
    query = buildStatement()

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
                      this[bindValueAs] =
                          when (val value = it.valueFromConnectValue()) {
                            is Map<*, *> -> value as Map<String, Any?>
                            else ->
                                throw ConnectException(
                                    "Message value must be convertible to a Map.")
                          }
                    }
                  }
                  .flatten()

          val mapped =
              if (isTombstoneMessage) {
                listOf(
                    "D",
                    mapOf(
                        "keys" to extractKeys(flattened, bindKeyAs),
                    ))
              } else {
                val keys = extractKeys(flattened, bindValueAs, bindKeyAs)

                listOf(
                    "C",
                    mapOf("keys" to keys, "properties" to computeProperties(flattened, keys.keys)))
              }

          logger.trace("message '{}' mapped to: '{}'", it, mapped)

          mapped
        }
        .chunked(batchSize)
        .map { listOf(ChangeQuery(null, null, Query(query, mapOf("events" to it)))) }
        .onEach { logger.trace("mapped messages: '{}'", it) }
        .toList()
  }

  private fun extractKeys(
      flattened: Map<String, Any?>,
      vararg prefixes: String
  ): Map<String, Any?> =
      pattern.keyProperties
          .associateBy { it.to }
          .mapValues { (_, mapping) ->
            if (isExplicitlyDefined(mapping.from)) {
              return@mapValues flattened[mapping.from]
            }

            for (prefix in prefixes) {
              val key = "$prefix.${mapping.from}"

              if (flattened.containsKey(key)) {
                return@mapValues flattened[key]
              }
            }
          }

  private fun isExplicitlyDefined(from: String): Boolean =
      from.startsWith(bindValueAs) ||
          from.startsWith(bindKeyAs) ||
          from.startsWith(bindTimestampAs) ||
          from.startsWith(bindHeaderAs)

  private fun computeProperties(
      flattened: Map<String, Any?>,
      used: Set<String>
  ): Map<String, Any?> {
    return buildMap {
      if (pattern.includeAllValueProperties) {
        this.putAll(
            flattened
                .filterKeys { it.startsWith(bindValueAs) }
                .mapKeys { it.key.substring(bindValueAs.length + 1) })
      }

      pattern.includeProperties.forEach { mapping ->
        val key =
            if (isExplicitlyDefined(mapping.from)) mapping.from else "$bindValueAs.${mapping.from}"
        if (flattened.containsKey(key)) {
          this[mapping.to] = flattened[key]
        } else {
          this.putAll(
              flattened
                  .filterKeys { it.startsWith(key) }
                  .mapKeys { mapping.to + it.key.substring(key.length) })
        }
      }

      pattern.excludeProperties.forEach { exclude ->
        if (this.containsKey(exclude)) {
          this.remove(exclude)
        } else {
          this.keys.filter { it.startsWith("$exclude.") }.forEach { this.remove(it) }
        }
      }

      used.forEach { this.remove(it) }
    }
  }

  private fun buildStatement(): String {
    val event = Cypher.name("event")
    val create = Cypher.name("create")
    val delete = Cypher.name("delete")
    val createOperation = Cypher.literalOf<String>("C")
    val deleteOperation = Cypher.literalOf<String>("D")

    val singletonList = Cypher.listOf(Cypher.literalOf<Int>(1))
    val emptyList = Cypher.listOf()
    val node =
        Cypher.node(pattern.labels.first(), pattern.labels.drop(1))
            .withProperties(
                pattern.keyProperties.associate { it.to to event.property("keys").property(it.to) },
            )
            .named("n")
    val merge = Clauses.merge(listOf(node), emptyList())

    return renderer.render(
        Cypher.unwind(Cypher.parameter("messages"))
            .`as`(event)
            .with(
                Cypher.caseExpression()
                    .`when`(Cypher.valueAt(event, 0).eq(createOperation))
                    .then(singletonList)
                    .elseDefault(emptyList)
                    .`as`(create),
                Cypher.caseExpression()
                    .`when`(Cypher.valueAt(event, 0).eq(deleteOperation))
                    .then(singletonList)
                    .elseDefault(emptyList)
                    .`as`(delete),
                Cypher.valueAt(event, 1).`as`(event),
            )
            .foreach(Cypher.name("i"))
            .`in`(create)
            .apply(
                merge as UpdatingClause,
                Clauses.set(
                    listOf(
                        if (mergeProperties) {
                          Operations.mutate(
                              node.asExpression(),
                              Cypher.property("event", "properties"),
                          )
                        } else {
                          Operations.set(
                              node.asExpression(),
                              Cypher.property("event", "properties"),
                          )
                        },
                    ),
                ) as UpdatingClause,
                Clauses.set(
                    listOf(
                        Operations.mutate(
                            node.asExpression(),
                            Cypher.parameter("event", "keys"),
                        ),
                    ),
                ) as UpdatingClause,
            )
            .foreach(Cypher.name("i"))
            .`in`(delete)
            .apply(merge, Clauses.delete(true, listOf(node.asExpression())) as UpdatingClause)
            .build(),
    )
  }
}
