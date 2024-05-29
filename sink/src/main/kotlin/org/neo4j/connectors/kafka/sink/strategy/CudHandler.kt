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

import org.apache.kafka.connect.errors.ConnectException
import org.neo4j.connectors.kafka.sink.ChangeQuery
import org.neo4j.connectors.kafka.sink.SinkMessage
import org.neo4j.connectors.kafka.sink.SinkStrategy
import org.neo4j.connectors.kafka.sink.strategy.cud.Operation
import org.neo4j.cypherdsl.core.renderer.Renderer
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class CudHandler(val topic: String, private val renderer: Renderer, private val batchSize: Int) :
    AbstractHandler() {
  private val logger: Logger = LoggerFactory.getLogger(javaClass)

  override fun strategy() = SinkStrategy.CUD

  @Suppress("UNCHECKED_CAST")
  override fun handle(messages: Iterable<SinkMessage>): Iterable<Iterable<ChangeQuery>> {
    return messages
        .asSequence()
        .onEach { logger.trace("received message: '{}'", it) }
        .map {
          val value =
              when (val value = it.valueFromConnectValue()) {
                is Map<*, *> -> value as Map<String, Any?>
                else -> throw ConnectException("Message value must be convertible to a Map.")
              }
          val cud = Operation.from(value)
          cud.toQuery(renderer)
        }
        .chunked(batchSize)
        .map { it.map { q -> ChangeQuery(null, null, q) } }
        .onEach { logger.trace("mapped messages: '{}'", it) }
        .toList()
  }
}
