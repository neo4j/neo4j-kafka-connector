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
package org.neo4j.connectors.kafka.sink

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.header.Header
import org.apache.kafka.connect.sink.SinkRecord
import org.neo4j.connectors.kafka.data.isCdcMessage

enum class SinkStrategy {
  CDC_SOURCE_ID,
  CDC_SCHEMA,
  CUD,
  PATTERN_NODE,
  PATTERN_RELATIONSHIP,
  CYPHER
}

data class SinkMessage(private val record: SinkRecord) {

  val keySchema
    get(): Schema = record.keySchema()

  val key
    get(): Any? = record.key()

  val valueSchema
    get(): Schema = record.valueSchema()

  val value
    get(): Any? = record.value()

  val headers
    get(): Iterable<Header> = record.headers()

  fun isCdcMessage(): Boolean = record.isCdcMessage()

  override fun toString(): String {
    return "SinkMessage{offset=${record.kafkaOffset()},timestamp=${record.timestamp()},timestampType=${record.timestampType()}}"
  }
}

interface SinkStrategyHandler {

  fun handle(messages: Iterable<SinkMessage>)
}
