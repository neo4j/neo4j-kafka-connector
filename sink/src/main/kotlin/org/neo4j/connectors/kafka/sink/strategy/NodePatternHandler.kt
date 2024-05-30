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

class NodePatternHandler(
    val topic: String,
    patternString: String,
    private val mergeProperties: Boolean,
    private val renderer: Renderer,
    private val batchSize: Int,
    bindTimestampAs: String = SinkConfiguration.DEFAULT_BIND_TIMESTAMP_ALIAS,
    bindHeaderAs: String = SinkConfiguration.DEFAULT_BIND_HEADER_ALIAS,
    bindKeyAs: String = SinkConfiguration.DEFAULT_BIND_KEY_ALIAS,
    bindValueAs: String = SinkConfiguration.DEFAULT_BIND_VALUE_ALIAS,
) :
    PatternHandler<NodePattern>(
        bindTimestampAs = bindTimestampAs,
        bindHeaderAs = bindHeaderAs,
        bindKeyAs = bindKeyAs,
        bindValueAs = bindValueAs) {
  private val logger: Logger = LoggerFactory.getLogger(javaClass)
  override val pattern: NodePattern
  internal val query: String

  init {
    val parsed = Pattern.parse(patternString)
    if (parsed !is NodePattern) {
      throw IllegalArgumentException(
          "Invalid pattern provided for NodePatternHandler: ${parsed.javaClass.name}")
    }
    pattern = parsed
    query = buildStatement()

    logger.debug("using Cypher query '{}' for topic '{}'", query, topic)
  }

  override fun strategy() = SinkStrategy.NODE_PATTERN

  override fun handle(messages: Iterable<SinkMessage>): Iterable<Iterable<ChangeQuery>> {
    return messages
        .asSequence()
        .onEach { logger.trace("received message: '{}'", it) }
        .map {
          val isTombstoneMessage = it.value == null
          val flattened = flattenMessage(it)

          val used = mutableSetOf<String>()
          val keys = extractKeys(pattern, flattened, used, bindValueAs, bindKeyAs)
          val mapped =
              if (isTombstoneMessage) {
                listOf("D", mapOf("keys" to keys))
              } else {
                listOf(
                    "C",
                    mapOf(
                        "keys" to keys,
                        "properties" to computeProperties(pattern, flattened, used)))
              }

          logger.trace("message '{}' mapped to: '{}'", it, mapped)

          mapped
        }
        .chunked(batchSize)
        .map { listOf(ChangeQuery(null, null, Query(query, mapOf("events" to it)))) }
        .onEach { logger.trace("mapped messages: '{}'", it) }
        .toList()
  }

  private fun buildStatement(): String {
    val event = Cypher.name("event")
    val created = Cypher.name("created")
    val deleted = Cypher.name("deleted")
    val createOperation = Cypher.literalOf<String>("C")
    val deleteOperation = Cypher.literalOf<String>("D")

    val node =
        Cypher.node(pattern.labels.first(), pattern.labels.drop(1))
            .withProperties(
                pattern.keyProperties.associate { it.to to event.property("keys").property(it.to) },
            )
            .named("n")

    return renderer.render(
        Cypher.unwind(Cypher.parameter("messages"))
            .`as`(event)
            .call(
                Cypher.with(event)
                    .with(event)
                    .where(Cypher.valueAt(event, 0).eq(createOperation))
                    .with(Cypher.valueAt(event, 1).`as`(event))
                    .merge(node)
                    .let {
                      if (mergeProperties) {
                        it.mutate(
                            node.asExpression(),
                            Cypher.property("event", "properties"),
                        )
                      } else {
                        it.set(
                                node.asExpression(),
                                Cypher.property("event", "properties"),
                            )
                            .mutate(node.asExpression(), Cypher.property("event", "keys"))
                      }
                    }
                    .returning(
                        Cypher.raw("count(${'$'}E)", node.requiredSymbolicName).`as`(created))
                    .build())
            .call(
                Cypher.with(event)
                    .with(event)
                    .where(Cypher.valueAt(event, 0).eq(deleteOperation))
                    .with(Cypher.valueAt(event, 1).`as`(event))
                    .match(node)
                    .detachDelete(node)
                    .returning(
                        Cypher.raw("count(${'$'}E)", node.requiredSymbolicName).`as`(deleted))
                    .build())
            .returning(
                Cypher.raw("sum(${'$'}E)", created).`as`(created),
                Cypher.raw("sum(${'$'}E)", deleted).`as`(deleted))
            .build(),
    )
  }
}
