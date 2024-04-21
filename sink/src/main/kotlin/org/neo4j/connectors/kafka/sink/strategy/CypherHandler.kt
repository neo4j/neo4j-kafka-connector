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
package org.neo4j.connectors.kafka.sink.strategy

import com.fasterxml.jackson.core.JsonParseException
import org.apache.kafka.connect.data.Schema
import org.neo4j.connectors.kafka.data.DynamicTypes
import org.neo4j.connectors.kafka.sink.ChangeQuery
import org.neo4j.connectors.kafka.sink.SinkConfiguration
import org.neo4j.connectors.kafka.sink.SinkMessage
import org.neo4j.connectors.kafka.sink.SinkStrategy
import org.neo4j.connectors.kafka.sink.SinkStrategyHandler
import org.neo4j.connectors.kafka.utils.JSONUtils
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
    bindHeaderAs: String = SinkConfiguration.DEFAULT_CYPHER_BIND_HEADER_ALIAS,
    bindKeyAs: String = SinkConfiguration.DEFAULT_CYPHER_BIND_KEY_ALIAS,
    bindValueAs: String = SinkConfiguration.DEFAULT_CYPHER_BIND_VALUE_ALIAS,
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
                  "header" to it.headerFromConnectValue(),
                  "key" to it.keyFromConnectValue(),
                  "value" to it.valueFromConnectValue())

          logger.trace("message '{}' mapped to: '{}'", it, mapped)

          mapped
        }
        .chunked(batchSize)
        .map { listOf(ChangeQuery(null, null, Query(rewrittenQuery, mapOf("events" to it)))) }
        .onEach { logger.trace("mapped messages: '{}'", it) }
        .toList()
  }

  private fun SinkMessage.valueFromConnectValue(): Any? {
    return fromConnectValue(valueSchema, value)
  }

  private fun SinkMessage.keyFromConnectValue(): Any? {
    return fromConnectValue(keySchema, key)
  }

  private fun SinkMessage.headerFromConnectValue(): Map<String, Any?> {
    return headers.associate { it.key() to fromConnectValue(it.schema(), it.value()) }
  }

  private fun fromConnectValue(schema: Schema?, value: Any?): Any? {
    return schema?.let {
      var converted = DynamicTypes.fromConnectValue(schema, value)
      if (converted is String || converted is ByteArray) {
        converted =
            try {
              JSONUtils.readValue<Any?>(converted)
            } catch (ex: JsonParseException) {
              converted
            }
      }
      converted
    }
  }
}
