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
import org.neo4j.connectors.kafka.sink.ChangeQuery
import org.neo4j.connectors.kafka.sink.SinkConfiguration
import org.neo4j.connectors.kafka.sink.SinkMessage
import org.neo4j.connectors.kafka.sink.SinkStrategy
import org.neo4j.connectors.kafka.sink.SinkStrategyHandler
import org.neo4j.cypherdsl.core.Cypher
import org.neo4j.cypherdsl.core.renderer.Renderer
import org.neo4j.driver.Query
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class CypherHandler(
    val topic: String,
    query: String,
    renderer: Renderer,
    val batchSize: Int,
    bindTimestampAs: String = SinkConfiguration.DEFAULT_BIND_TIMESTAMP_ALIAS,
    bindHeaderAs: String = SinkConfiguration.DEFAULT_BIND_HEADER_ALIAS,
    bindKeyAs: String = SinkConfiguration.DEFAULT_BIND_KEY_ALIAS,
    bindValueAs: String = SinkConfiguration.DEFAULT_BIND_VALUE_ALIAS,
    bindValueAsEvent: Boolean = SinkConfiguration.DEFAULT_CYPHER_BIND_VALUE_AS_EVENT
) : SinkStrategyHandler {
  private val logger: Logger = LoggerFactory.getLogger(javaClass)
  private val rewrittenQuery: String

  init {
    val message = Cypher.name("message")

    rewrittenQuery =
        renderer.render(
            Cypher.unwind(Cypher.parameter("events"))
                .`as`(message)
                .with(
                    buildList {
                      if (bindValueAsEvent) {
                        add(message.property("value").`as`("event"))
                      }
                      if (bindTimestampAs.isNotEmpty()) {
                        add(message.property("timestamp").`as`(bindTimestampAs))
                      }
                      if (bindHeaderAs.isNotEmpty()) {
                        add(message.property("header").`as`(bindHeaderAs))
                      }
                      if (bindKeyAs.isNotEmpty()) {
                        add(message.property("key").`as`(bindKeyAs))
                      }
                      if (bindValueAs.isNotEmpty()) {
                        add(message.property("value").`as`(bindValueAs))
                      }

                      if (isEmpty()) {
                        throw IllegalArgumentException(
                            "no effective accessors specified for binding the message into cypher template for topic '$topic'")
                      }
                    })
                .callRawCypher("WITH * $query")
                .build())

    logger.debug("using cypher query '{}' for topic '{}'", rewrittenQuery, topic)
  }

  override fun strategy() = SinkStrategy.CYPHER

  override fun handle(messages: Iterable<SinkMessage>): Iterable<Iterable<ChangeQuery>> {
    return messages
        .asSequence()
        .onEach { logger.trace("received message: '{}'", it) }
        .map {
          val mapped =
              mapOf(
                  "timestamp" to
                      Instant.ofEpochMilli(it.record.timestamp()).atOffset(ZoneOffset.UTC),
                  "header" to it.headerFromConnectValue(),
                  "key" to it.keyFromConnectValue(),
                  "value" to it.valueFromConnectValue())

          logger.trace("message '{}' mapped to: '{}'", it, mapped)

          it to mapped
        }
        .chunked(batchSize)
        .map {
          listOf(
              ChangeQuery(
                  null,
                  null,
                  it.map { x -> x.first },
                  Query(rewrittenQuery, mapOf("events" to it.map { x -> x.second }))))
        }
        .onEach { logger.trace("mapped messages: '{}'", it) }
        .toList()
  }
}
