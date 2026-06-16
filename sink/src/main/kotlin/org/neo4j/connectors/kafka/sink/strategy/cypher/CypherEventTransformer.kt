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
package org.neo4j.connectors.kafka.sink.strategy.cypher

import org.neo4j.connectors.kafka.sink.SinkConfiguration
import org.neo4j.connectors.kafka.sink.SinkMessage
import org.neo4j.connectors.kafka.sink.strategy.CypherSinkAction
import org.neo4j.connectors.kafka.sink.strategy.SinkAction
import org.neo4j.connectors.kafka.sink.strategy.SinkEventTransformer
import java.time.Instant
import java.time.ZoneOffset

class CypherEventTransformer(
    private val topic: String,
    private val query: String,
    bindTimestampAs: String = SinkConfiguration.DEFAULT_BIND_TIMESTAMP_ALIAS,
    bindHeaderAs: String = SinkConfiguration.DEFAULT_BIND_HEADER_ALIAS,
    bindKeyAs: String = SinkConfiguration.DEFAULT_BIND_KEY_ALIAS,
    bindValueAs: String = SinkConfiguration.DEFAULT_BIND_VALUE_ALIAS,
    bindValueAsEvent: Boolean = SinkConfiguration.DEFAULT_CYPHER_BIND_VALUE_AS_EVENT,
) : SinkEventTransformer {

  private val aliasProjection: List<Pair<String, String>> = buildList {
    if (bindValueAsEvent) {
      add("event" to "value")
    }
    if (bindTimestampAs.isNotEmpty()) {
      add(bindTimestampAs to "timestamp")
    }
    if (bindHeaderAs.isNotEmpty()) {
      add(bindHeaderAs to "header")
    }
    if (bindKeyAs.isNotEmpty()) {
      add(bindKeyAs to "key")
    }
    if (bindValueAs.isNotEmpty()) {
      add(bindValueAs to "value")
    }

    if (isEmpty()) {
      throw IllegalArgumentException(
          "no effective accessors specified for binding the message into cypher template for topic '$topic'"
      )
    }
  }

  override fun transform(message: SinkMessage): SinkAction {
    return CypherSinkAction(
        query,
        mapOf(
            "timestamp" to
                Instant.ofEpochMilli(message.record.timestamp()).atOffset(ZoneOffset.UTC),
            "header" to message.headerFromConnectValue(),
            "key" to message.keyFromConnectValue(),
            "value" to message.valueFromConnectValue()
        ),
        aliasProjection
    )
  }
}
