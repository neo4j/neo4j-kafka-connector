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

import com.squareup.wire.Instant
import io.kotest.matchers.shouldBe
import io.kotest.matchers.throwable.shouldHaveMessage
import java.time.ZoneOffset
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaAndValue
import org.junit.Test
import org.junit.jupiter.api.assertThrows
import org.neo4j.connectors.kafka.connect.ConnectHeader
import org.neo4j.connectors.kafka.sink.strategy.CypherSinkAction
import org.neo4j.connectors.kafka.sink.strategy.HandlerTest

class CypherEventTransformerTest : HandlerTest() {

  @Test
  fun `should project only the value-as-event accessor when others disabled`() {
    val transformer =
        CypherEventTransformer(
            "my-topic",
            "CREATE (n:Node) SET n = event",
            bindTimestampAs = "",
            bindHeaderAs = "",
            bindKeyAs = "",
            bindValueAs = "",
            bindValueAsEvent = true,
        )

    transformer.transform(newMessage(Schema.STRING_SCHEMA, "{}")) shouldBe
        CypherSinkAction(
            "CREATE (n:Node) SET n = event",
            mapOf(
                "timestamp" to Instant.ofEpochMilli(TIMESTAMP).atOffset(ZoneOffset.UTC),
                "header" to emptyMap<String, Any>(),
                "key" to null,
                "value" to emptyMap<String, Any>(),
            ),
            listOf("event" to "value"),
        )
  }

  @Test
  fun `should project all accessors in canonical order`() {
    val transformer =
        CypherEventTransformer(
            "my-topic",
            "CREATE (n:Node) SET n = __value",
            bindTimestampAs = "__timestamp",
            bindHeaderAs = "__header",
            bindKeyAs = "__key",
            bindValueAs = "__value",
            bindValueAsEvent = true,
        )

    val message =
        newMessage(
            Schema.STRING_SCHEMA,
            "{}",
            Schema.INT64_SCHEMA,
            32L,
            listOf(ConnectHeader("age", SchemaAndValue(Schema.INT32_SCHEMA, 24))),
        )

    transformer.transform(message) shouldBe
        CypherSinkAction(
            "CREATE (n:Node) SET n = __value",
            mapOf(
                "timestamp" to Instant.ofEpochMilli(TIMESTAMP).atOffset(ZoneOffset.UTC),
                "header" to mapOf("age" to 24),
                "key" to 32L,
                "value" to emptyMap<String, Any>(),
            ),
            listOf(
                "event" to "value",
                "__timestamp" to "timestamp",
                "__header" to "header",
                "__key" to "key",
                "__value" to "value",
            ),
        )
  }

  @Test
  fun `should omit value-as-event when disabled`() {
    val transformer =
        CypherEventTransformer(
            "my-topic",
            "CREATE (n:Node) SET n = __value",
            bindTimestampAs = "",
            bindHeaderAs = "__header",
            bindKeyAs = "__key",
            bindValueAs = "__value",
            bindValueAsEvent = false,
        )

    transformer.transform(newMessage(Schema.STRING_SCHEMA, "{}")) shouldBe
        CypherSinkAction(
            "CREATE (n:Node) SET n = __value",
            mapOf(
                "timestamp" to Instant.ofEpochMilli(TIMESTAMP).atOffset(ZoneOffset.UTC),
                "header" to emptyMap<String, Any>(),
                "key" to null,
                "value" to emptyMap<String, Any>(),
            ),
            listOf("__header" to "header", "__key" to "key", "__value" to "value"),
        )
  }

  @Test
  fun `should perform json conversion on string values`() {
    val transformer = CypherEventTransformer("my-topic", "CREATE (n:Node) SET n = event")

    val action =
        transformer.transform(
            newMessage(Schema.STRING_SCHEMA, "{\"x\": 123, \"y\": [1,2,3], \"z\": true}")
        ) as CypherSinkAction

    action.bindings["value"] shouldBe mapOf("x" to 123, "y" to listOf(1, 2, 3), "z" to true)
  }

  @Test
  fun `should fail if no accessor specified`() {
    assertThrows<IllegalArgumentException> {
      CypherEventTransformer(
          "my-topic",
          "CREATE (n:Node) SET n = __value",
          bindTimestampAs = "",
          bindHeaderAs = "",
          bindKeyAs = "",
          bindValueAs = "",
          bindValueAsEvent = false,
      )
    } shouldHaveMessage
        "no effective accessors specified for binding the message into cypher template for topic 'my-topic'"
  }
}
