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

import org.apache.kafka.connect.data.Struct
import org.neo4j.cdc.client.model.ChangeEvent
import org.neo4j.cdc.client.model.EntityOperation
import org.neo4j.cdc.client.model.NodeEvent
import org.neo4j.cdc.client.model.RelationshipEvent
import org.neo4j.connectors.kafka.data.ChangeEventExtensions.toChangeEvent
import org.neo4j.connectors.kafka.data.StreamsTransactionEventExtensions.toChangeEvent
import org.neo4j.connectors.kafka.sink.ChangeQuery
import org.neo4j.connectors.kafka.sink.SinkMessage
import org.neo4j.connectors.kafka.sink.SinkStrategyHandler
import org.neo4j.connectors.kafka.sink.utils.toStreamsSinkEntity
import org.neo4j.connectors.kafka.utils.SchemaUtils
import org.neo4j.driver.Query
import org.slf4j.Logger
import org.slf4j.LoggerFactory

abstract class CdcHandler : SinkStrategyHandler {
  private val logger: Logger = LoggerFactory.getLogger(javaClass)

  override fun handle(messages: Iterable<SinkMessage>): Iterable<Iterable<ChangeQuery>> {
    return messages
        .onEach { logger.trace("received message: {}", it) }
        .map { it.toChangeEvent() }
        .map { it.txId to it }
        .onEach { logger.trace("converted message: {} to {}", it.first, it.second) }
        .groupBy(
            { it.first },
            {
              ChangeQuery(
                  it.second.txId,
                  it.second.seq,
                  when (val event = it.second.event) {
                    is NodeEvent ->
                        when (event.operation) {
                          EntityOperation.CREATE -> transformCreate(event)
                          EntityOperation.UPDATE -> transformUpdate(event)
                          EntityOperation.DELETE -> transformDelete(event)
                          else ->
                              throw IllegalArgumentException("unknown operation ${event.operation}")
                        }
                    is RelationshipEvent ->
                        when (event.operation) {
                          EntityOperation.CREATE -> transformCreate(event)
                          EntityOperation.UPDATE -> transformUpdate(event)
                          EntityOperation.DELETE -> transformDelete(event)
                          else ->
                              throw IllegalArgumentException("unknown operation ${event.operation}")
                        }
                    else ->
                        throw IllegalArgumentException("unsupported event type ${event.eventType}")
                  })
            })
        .onEach { logger.trace("mapped messages: {} to {}", it.key, it.value) }
        .values
  }

  protected abstract fun transformCreate(event: NodeEvent): Query

  protected abstract fun transformUpdate(event: NodeEvent): Query

  protected abstract fun transformDelete(event: NodeEvent): Query

  protected abstract fun transformCreate(event: RelationshipEvent): Query

  protected abstract fun transformUpdate(event: RelationshipEvent): Query

  protected abstract fun transformDelete(event: RelationshipEvent): Query
}

internal fun SinkMessage.toChangeEvent(): ChangeEvent =
    when {
      this.isCdcMessage -> parseCdcChangeEvent(this)
      else -> parseStreamsChangeEvent(this)
    }

internal fun parseCdcChangeEvent(message: SinkMessage): ChangeEvent =
    when (val value = message.value) {
      is Struct -> value.toChangeEvent()
      else ->
          throw IllegalArgumentException(
              "unexpected message value type ${value?.javaClass?.name} in $message")
    }

internal fun parseStreamsChangeEvent(message: SinkMessage): ChangeEvent {
  val event =
      SchemaUtils.toStreamsTransactionEvent(message.record.toStreamsSinkEntity()) { _ -> true }
          ?: throw IllegalArgumentException("unsupported change event message in $message")

  return event.toChangeEvent()
}
