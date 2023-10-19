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
package org.neo4j.connectors.kafka.testing.sink

import kotlin.test.assertIs
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class Neo4jSinkRegistrationTest {

  @Test
  fun `creates payload`() {
    val expectedConfig =
        mapOf(
            "topics" to "my-topic",
            "connector.class" to "streams.kafka.connect.sink.Neo4jSinkConnector",
            "key.converter" to "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable" to false,
            "value.converter" to "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable" to false,
            "errors.retry.timeout" to -1L,
            "errors.retry.delay.max.ms" to 1000L,
            "errors.tolerance" to "all",
            "errors.log.enable" to true,
            "errors.log.include.messages" to true,
            "neo4j.server.uri" to "neo4j://example.com",
            "neo4j.authentication.basic.username" to "user",
            "neo4j.authentication.basic.password" to "password",
            "neo4j.topic.cypher.my-topic" to "MERGE ()")
    val registration =
        Neo4jSinkRegistration(
            topicQuerys = mapOf("my-topic" to "MERGE ()"),
            neo4jUri = "neo4j://example.com",
            neo4jUser = "user",
            neo4jPassword = "password",
        )

    val payload = registration.getPayload()

    assertTrue((payload["name"] as String).startsWith("Neo4jSinkConnector"))
    assertEquals(expectedConfig, payload["config"])
  }

  @Test
  fun `creates payload with multiple topics`() {
    val registration =
        Neo4jSinkRegistration(
            topicQuerys = mapOf("topic1" to "MERGE ()", "topic2" to "CREATE ()"),
            neo4jUri = "neo4j://example.com",
            neo4jUser = "user",
            neo4jPassword = "password",
        )

    val payload = registration.getPayload()

    val config = payload["config"]
    assertIs<Map<*, *>>(config)
    assertEquals("MERGE ()", config["neo4j.topic.cypher.topic1"])
    assertEquals("CREATE ()", config["neo4j.topic.cypher.topic2"])
  }
}
