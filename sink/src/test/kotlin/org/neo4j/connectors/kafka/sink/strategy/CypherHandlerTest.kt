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
import io.kotest.matchers.throwable.shouldHaveMessage
import java.time.Instant
import java.time.ZoneOffset
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaAndValue
import org.apache.kafka.connect.header.Header
import org.apache.kafka.connect.sink.SinkRecord
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.neo4j.connectors.kafka.connect.ConnectHeader
import org.neo4j.connectors.kafka.sink.ChangeQuery
import org.neo4j.connectors.kafka.sink.SinkMessage
import org.neo4j.cypherdsl.core.renderer.Renderer
import org.neo4j.driver.Query

class CypherHandlerTest {

  @Test
  fun `should generate correct statement without new accessors`() {
    val handler =
        CypherHandler(
            "my-topic",
            "CREATE (n:Node) SET n = event",
            Renderer.getDefaultRenderer(),
            1,
            "",
            "",
            "",
            "",
            true)

    handler.handle(listOf(newMessage(Schema.STRING_SCHEMA, "{}"))) shouldBe
        listOf(
            listOf(
                ChangeQuery(
                    null,
                    null,
                    Query(
                        "UNWIND ${'$'}events AS message WITH message.value AS event CALL {WITH * CREATE (n:Node) SET n = event}",
                        mapOf(
                            "events" to
                                listOf(
                                    mapOf(
                                        "timestamp" to
                                            Instant.ofEpochMilli(TIMESTAMP)
                                                .atOffset(ZoneOffset.UTC),
                                        "header" to emptyMap<String, Any>(),
                                        "key" to null,
                                        "value" to emptyMap<String, Any>())))))))
  }

  @Test
  fun `should generate correct statement with event and value accessors`() {
    val handler =
        CypherHandler(
            "my-topic",
            "CREATE (n:Node) SET n = __value",
            Renderer.getDefaultRenderer(),
            1,
            "",
            "",
            "",
            "__value",
            true)

    handler.handle(listOf(newMessage(Schema.STRING_SCHEMA, "{}"))) shouldBe
        listOf(
            listOf(
                ChangeQuery(
                    null,
                    null,
                    Query(
                        "UNWIND ${'$'}events AS message WITH message.value AS event, message.value AS __value CALL {WITH * CREATE (n:Node) SET n = __value}",
                        mapOf(
                            "events" to
                                listOf(
                                    mapOf(
                                        "timestamp" to
                                            Instant.ofEpochMilli(TIMESTAMP)
                                                .atOffset(ZoneOffset.UTC),
                                        "header" to emptyMap<String, Any>(),
                                        "key" to null,
                                        "value" to emptyMap<String, Any>())))))))
  }

  @Test
  fun `should generate correct statement with event, key and value accessors`() {
    val handler =
        CypherHandler(
            "my-topic",
            "CREATE (n:Node) SET n = __key",
            Renderer.getDefaultRenderer(),
            1,
            "",
            "",
            "__key",
            "__value",
            true)

    handler.handle(
        listOf(newMessage(Schema.STRING_SCHEMA, "{}", Schema.INT64_SCHEMA, 32L))) shouldBe
        listOf(
            listOf(
                ChangeQuery(
                    null,
                    null,
                    Query(
                        "UNWIND ${'$'}events AS message WITH message.value AS event, message.key AS __key, message.value AS __value CALL {WITH * CREATE (n:Node) SET n = __key}",
                        mapOf(
                            "events" to
                                listOf(
                                    mapOf(
                                        "timestamp" to
                                            Instant.ofEpochMilli(TIMESTAMP)
                                                .atOffset(ZoneOffset.UTC),
                                        "header" to emptyMap<String, Any>(),
                                        "key" to 32L,
                                        "value" to emptyMap<String, Any>())))))))
  }

  @Test
  fun `should generate correct statement with event, header, key and value accessors`() {
    val handler =
        CypherHandler(
            "my-topic",
            "CREATE (n:Node) SET n = __header",
            Renderer.getDefaultRenderer(),
            1,
            "",
            "__header",
            "__key",
            "__value",
            true)

    handler.handle(
        listOf(
            newMessage(
                Schema.STRING_SCHEMA,
                "{}",
                Schema.INT64_SCHEMA,
                32L,
                listOf(ConnectHeader("age", SchemaAndValue(Schema.INT32_SCHEMA, 24)))))) shouldBe
        listOf(
            listOf(
                ChangeQuery(
                    null,
                    null,
                    Query(
                        "UNWIND ${'$'}events AS message WITH message.value AS event, message.header AS __header, message.key AS __key, message.value AS __value CALL {WITH * CREATE (n:Node) SET n = __header}",
                        mapOf(
                            "events" to
                                listOf(
                                    mapOf(
                                        "timestamp" to
                                            Instant.ofEpochMilli(TIMESTAMP)
                                                .atOffset(ZoneOffset.UTC),
                                        "header" to mapOf<String, Any>("age" to 24),
                                        "key" to 32L,
                                        "value" to emptyMap<String, Any>())))))))
  }

  @Test
  fun `should generate correct statement with header, key and value accessors`() {
    val handler =
        CypherHandler(
            "my-topic",
            "CREATE (n:Node) SET n = __header",
            Renderer.getDefaultRenderer(),
            1,
            "",
            "__header",
            "__key",
            "__value",
            false)

    handler.handle(
        listOf(
            newMessage(
                Schema.STRING_SCHEMA,
                "{}",
                Schema.INT64_SCHEMA,
                32L,
                listOf(ConnectHeader("age", SchemaAndValue(Schema.INT32_SCHEMA, 24)))))) shouldBe
        listOf(
            listOf(
                ChangeQuery(
                    null,
                    null,
                    Query(
                        "UNWIND ${'$'}events AS message WITH message.header AS __header, message.key AS __key, message.value AS __value CALL {WITH * CREATE (n:Node) SET n = __header}",
                        mapOf(
                            "events" to
                                listOf(
                                    mapOf(
                                        "timestamp" to
                                            Instant.ofEpochMilli(TIMESTAMP)
                                                .atOffset(ZoneOffset.UTC),
                                        "header" to mapOf<String, Any>("age" to 24),
                                        "key" to 32L,
                                        "value" to emptyMap<String, Any>())))))))
  }

  @Test
  fun `should generate correct statement with timestamp, header, key and value accessors`() {
    val handler =
        CypherHandler(
            "my-topic",
            "CREATE (n:Node) SET n = __header",
            Renderer.getDefaultRenderer(),
            1,
            "__timestamp",
            "__header",
            "__key",
            "__value",
            false)

    handler.handle(
        listOf(
            newMessage(
                Schema.STRING_SCHEMA,
                "{}",
                Schema.INT64_SCHEMA,
                32L,
                listOf(ConnectHeader("age", SchemaAndValue(Schema.INT32_SCHEMA, 24)))))) shouldBe
        listOf(
            listOf(
                ChangeQuery(
                    null,
                    null,
                    Query(
                        "UNWIND ${'$'}events AS message WITH message.timestamp AS __timestamp, message.header AS __header, message.key AS __key, message.value AS __value CALL {WITH * CREATE (n:Node) SET n = __header}",
                        mapOf(
                            "events" to
                                listOf(
                                    mapOf(
                                        "timestamp" to
                                            Instant.ofEpochMilli(TIMESTAMP)
                                                .atOffset(ZoneOffset.UTC),
                                        "header" to mapOf<String, Any>("age" to 24),
                                        "key" to 32L,
                                        "value" to emptyMap<String, Any>())))))))
  }

  @Test
  fun `should fail if no accessor specified`() {
    assertThrows<IllegalArgumentException> {
      CypherHandler(
          "my-topic",
          "CREATE (n:Node) SET n = __value",
          Renderer.getDefaultRenderer(),
          1,
          "",
          "",
          "",
          "",
          false)
    } shouldHaveMessage
        "no effective accessors specified for binding the message into cypher template for topic 'my-topic'"
  }

  @Test
  fun `should split messages into batches`() {
    val handler =
        CypherHandler(
            "my-topic",
            "CREATE (n:Node) SET n.id = __value",
            Renderer.getDefaultRenderer(),
            5,
            "__timestamp",
            "__header",
            "__key",
            "__value",
            false)

    val result = handler.handle((1..13).map { newMessage(Schema.INT64_SCHEMA, it.toLong()) })

    result shouldBe
        listOf(
            listOf(
                ChangeQuery(
                    null,
                    null,
                    Query(
                        "UNWIND ${'$'}events AS message WITH message.timestamp AS __timestamp, message.header AS __header, message.key AS __key, message.value AS __value CALL {WITH * CREATE (n:Node) SET n.id = __value}",
                        mapOf(
                            "events" to
                                (1..5).map { seq ->
                                  mapOf(
                                      "timestamp" to
                                          Instant.ofEpochMilli(TIMESTAMP).atOffset(ZoneOffset.UTC),
                                      "header" to emptyMap<String, Any>(),
                                      "key" to null,
                                      "value" to seq)
                                })))),
            listOf(
                ChangeQuery(
                    null,
                    null,
                    Query(
                        "UNWIND ${'$'}events AS message WITH message.timestamp AS __timestamp, message.header AS __header, message.key AS __key, message.value AS __value CALL {WITH * CREATE (n:Node) SET n.id = __value}",
                        mapOf(
                            "events" to
                                (6..10).map { seq ->
                                  mapOf(
                                      "timestamp" to
                                          Instant.ofEpochMilli(TIMESTAMP).atOffset(ZoneOffset.UTC),
                                      "header" to emptyMap<String, Any>(),
                                      "key" to null,
                                      "value" to seq)
                                })))),
            listOf(
                ChangeQuery(
                    null,
                    null,
                    Query(
                        "UNWIND ${'$'}events AS message WITH message.timestamp AS __timestamp, message.header AS __header, message.key AS __key, message.value AS __value CALL {WITH * CREATE (n:Node) SET n.id = __value}",
                        mapOf(
                            "events" to
                                (11..13).map { seq ->
                                  mapOf(
                                      "timestamp" to
                                          Instant.ofEpochMilli(TIMESTAMP).atOffset(ZoneOffset.UTC),
                                      "header" to emptyMap<String, Any>(),
                                      "key" to null,
                                      "value" to seq)
                                })))))
  }

  @Test
  fun `should perform json conversion on string values`() {
    val handler =
        CypherHandler("my-topic", "CREATE (n:Node) SET n = event", Renderer.getDefaultRenderer(), 1)

    handler.handle(
        listOf(
            newMessage(Schema.STRING_SCHEMA, "{\"x\": 123, \"y\": [1,2,3], \"z\": true}"))) shouldBe
        listOf(
            listOf(
                ChangeQuery(
                    null,
                    null,
                    Query(
                        "UNWIND ${'$'}events AS message WITH message.value AS event, message.timestamp AS __timestamp, message.header AS __header, message.key AS __key, message.value AS __value CALL {WITH * CREATE (n:Node) SET n = event}",
                        mapOf(
                            "events" to
                                listOf(
                                    mapOf(
                                        "timestamp" to
                                            Instant.ofEpochMilli(TIMESTAMP)
                                                .atOffset(ZoneOffset.UTC),
                                        "header" to emptyMap<String, Any>(),
                                        "key" to null,
                                        "value" to
                                            mapOf(
                                                "x" to 123,
                                                "y" to listOf(1, 2, 3),
                                                "z" to true))))))))
  }

  @Test
  fun `should perform json conversion on string keys`() {
    val handler =
        CypherHandler("my-topic", "CREATE (n:Node) SET n = event", Renderer.getDefaultRenderer(), 1)

    handler.handle(
        listOf(
            newMessage(
                null,
                null,
                Schema.STRING_SCHEMA,
                "{\"x\": 123, \"y\": [1,2,3], \"z\": true}"))) shouldBe
        listOf(
            listOf(
                ChangeQuery(
                    null,
                    null,
                    Query(
                        "UNWIND ${'$'}events AS message WITH message.value AS event, message.timestamp AS __timestamp, message.header AS __header, message.key AS __key, message.value AS __value CALL {WITH * CREATE (n:Node) SET n = event}",
                        mapOf(
                            "events" to
                                listOf(
                                    mapOf(
                                        "timestamp" to
                                            Instant.ofEpochMilli(TIMESTAMP)
                                                .atOffset(ZoneOffset.UTC),
                                        "header" to emptyMap<String, Any>(),
                                        "key" to
                                            mapOf("x" to 123, "y" to listOf(1, 2, 3), "z" to true),
                                        "value" to null)))))))
  }

  @Test
  fun `should perform json conversion on string headers`() {
    val handler =
        CypherHandler("my-topic", "CREATE (n:Node) SET n = event", Renderer.getDefaultRenderer(), 1)

    handler.handle(
        listOf(
            newMessage(
                null,
                null,
                Schema.INT64_SCHEMA,
                32L,
                listOf(
                    ConnectHeader("number", SchemaAndValue(Schema.INT32_SCHEMA, 24)),
                    ConnectHeader(
                        "test",
                        SchemaAndValue(
                            Schema.STRING_SCHEMA,
                            "{\"x\": 123, \"y\": [1,2,3], \"z\": true}")))))) shouldBe
        listOf(
            listOf(
                ChangeQuery(
                    null,
                    null,
                    Query(
                        "UNWIND ${'$'}events AS message WITH message.value AS event, message.timestamp AS __timestamp, message.header AS __header, message.key AS __key, message.value AS __value CALL {WITH * CREATE (n:Node) SET n = event}",
                        mapOf(
                            "events" to
                                listOf(
                                    mapOf(
                                        "timestamp" to
                                            Instant.ofEpochMilli(TIMESTAMP)
                                                .atOffset(ZoneOffset.UTC),
                                        "header" to
                                            mapOf(
                                                "number" to 24,
                                                "test" to
                                                    mapOf(
                                                        "x" to 123,
                                                        "y" to listOf(1, 2, 3),
                                                        "z" to true)),
                                        "key" to 32L,
                                        "value" to null)))))))
  }

  @Test
  fun `should return original string when not valid json`() {
    val handler =
        CypherHandler("my-topic", "CREATE (n:Node) SET n = event", Renderer.getDefaultRenderer(), 1)

    handler.handle(
        listOf(
            newMessage(
                Schema.STRING_SCHEMA,
                "{]",
                Schema.BYTES_SCHEMA,
                "{a: b}".toByteArray(),
                listOf(ConnectHeader("test", SchemaAndValue(Schema.STRING_SCHEMA, "b")))))) shouldBe
        listOf(
            listOf(
                ChangeQuery(
                    null,
                    null,
                    Query(
                        "UNWIND ${'$'}events AS message WITH message.value AS event, message.header AS __header, message.key AS __key, message.value AS __value CALL {WITH * CREATE (n:Node) SET n = event}",
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

  companion object {
    val TIMESTAMP: Long = Instant.now().toEpochMilli()
  }
}
