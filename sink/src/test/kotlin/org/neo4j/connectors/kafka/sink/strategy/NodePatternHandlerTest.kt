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

import io.kotest.matchers.shouldBe
import java.time.Instant
import java.time.ZoneOffset
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.header.Header
import org.apache.kafka.connect.sink.SinkRecord
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.neo4j.connectors.kafka.sink.ChangeQuery
import org.neo4j.connectors.kafka.sink.SinkMessage
import org.neo4j.connectors.kafka.sink.strategy.CypherHandlerTest.Companion.TIMESTAMP
import org.neo4j.cypherdsl.core.renderer.Renderer
import org.neo4j.driver.Query

class NodePatternHandlerTest {

  @Test
  fun `should generate correct statement`() {
    val handler =
        NodePatternHandler("my-topic", "(:ALabel {!id})", false, Renderer.getDefaultRenderer(), 1)

    handler.handle(
        listOf(
            newMessage(
                Schema.STRING_SCHEMA,
                """
              {"id": 42}
            """
                    .trimIndent()),
        )) shouldBe
        listOf(
            listOf(
                ChangeQuery(
                    null,
                    null,
                    Query(
                        "TODO",
                        mapOf(
                            "events" to
                                listOf(
                                    mapOf(
                                        "timestamp" to
                                            Instant.ofEpochMilli(TIMESTAMP)
                                                .atOffset(ZoneOffset.UTC),
                                        "header" to mapOf<String, Any>("test" to "b"),
                                        "key" to "{a: b}".toByteArray(),
                                        "value" to "{]")))))))
  }

  // TODO: extract this and share between CypherHandlerTest and here
  private fun newMessage(
      valueSchema: Schema?,
      value: Any?,
      keySchema: Schema? = null,
      key: Any? = null,
      headers: Iterable<Header> = emptyList()
  ): SinkMessage {
    return SinkMessage(
        SinkRecord(
            "my-topic",
            0,
            keySchema,
            key,
            valueSchema,
            value,
            0,
            TIMESTAMP,
            TimestampType.CREATE_TIME,
            headers))
  }
}
