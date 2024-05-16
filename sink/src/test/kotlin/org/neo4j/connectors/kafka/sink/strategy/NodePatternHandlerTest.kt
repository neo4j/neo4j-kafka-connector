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
import org.neo4j.cypherdsl.parser.CypherParser
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
                        CypherParser.parse(
                                """
                          UNWIND ${'$'}messages AS event
                          WITH
                            CASE WHEN event[0] = 'C' THEN [1] ELSE [] END AS create,
                            CASE WHEN event[0] = 'D' THEN [1] ELSE [] END AS delete,
                            event[1] AS event
                          FOREACH (i IN create | MERGE (n:`ALabel` {id: event.keys.id}) SET n = event.properties SET n += ${'$'}event) 
                          FOREACH (i IN delete | MERGE (n:`ALabel` {id: event.keys.id}) DETACH DELETE n)
                        """
                                    .trimIndent())
                            .cypher,
                        mapOf(
                            "events" to
                                listOf(
                                    listOf(
                                        "C",
                                        mapOf(
                                            "keys" to mapOf("id" to 42),
                                            "properties" to emptyMap<String, Any?>()))))))))
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
