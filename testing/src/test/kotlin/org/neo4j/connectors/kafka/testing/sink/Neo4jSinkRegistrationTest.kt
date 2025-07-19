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
package org.neo4j.connectors.kafka.testing.sink

import kotlin.test.assertIs
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.neo4j.connectors.kafka.testing.format.KafkaConverter

class Neo4jSinkRegistrationTest {

  @Test
  fun `creates payload`() {
    val expectedConfig =
        mapOf(
            "topics" to "my-topic",
            "connector.class" to "org.neo4j.connectors.kafka.sink.Neo4jConnector",
            "key.converter" to "io.confluent.connect.avro.AvroConverter",
            "key.converter.schema.registry.url" to "http://example.com",
            "value.converter" to "io.confluent.connect.avro.AvroConverter",
            "value.converter.schema.registry.url" to "http://example.com",
            "errors.retry.timeout" to -1L,
            "errors.retry.delay.max.ms" to 1000L,
            "errors.tolerance" to "none",
            "errors.deadletterqueue.context.headers.enable" to false,
            "errors.log.enable" to true,
            "errors.log.include.messages" to true,
            "neo4j.uri" to "neo4j://example.com",
            "neo4j.database" to "database",
            "neo4j.authentication.type" to "BASIC",
            "neo4j.authentication.basic.username" to "user",
            "neo4j.authentication.basic.password" to "password",
            "neo4j.cypher.topic.my-topic" to "MERGE ()",
        )
    val registration =
        Neo4jSinkRegistration(
            neo4jUri = "neo4j://example.com",
            neo4jUser = "user",
            neo4jPassword = "password",
            neo4jDatabase = "database",
            schemaControlRegistryUri = "http://example.com",
            keyConverter = KafkaConverter.AVRO,
            valueConverter = KafkaConverter.AVRO,
            topics = listOf("my-topic"),
            strategies = mapOf("neo4j.cypher.topic.my-topic" to "MERGE ()"),
        )

    val payload = registration.getPayload()

    assertTrue((payload["name"] as String).startsWith("Neo4jSinkConnector"))
    assertEquals(expectedConfig, payload["config"])
  }

  @Test
  fun `creates payload with dlq topic`() {
    val expectedConfig =
        mapOf(
            "topics" to "my-topic",
            "connector.class" to "org.neo4j.connectors.kafka.sink.Neo4jConnector",
            "key.converter" to "io.confluent.connect.avro.AvroConverter",
            "key.converter.schema.registry.url" to "http://example.com",
            "value.converter" to "io.confluent.connect.avro.AvroConverter",
            "value.converter.schema.registry.url" to "http://example.com",
            "errors.retry.timeout" to -1L,
            "errors.retry.delay.max.ms" to 1000L,
            "errors.tolerance" to "none",
            "errors.deadletterqueue.topic.name" to "dlq-topic",
            "errors.deadletterqueue.topic.replication.factor" to 1,
            "errors.deadletterqueue.context.headers.enable" to false,
            "errors.log.enable" to true,
            "errors.log.include.messages" to true,
            "neo4j.uri" to "neo4j://example.com",
            "neo4j.database" to "database",
            "neo4j.authentication.type" to "BASIC",
            "neo4j.authentication.basic.username" to "user",
            "neo4j.authentication.basic.password" to "password",
            "neo4j.cypher.topic.my-topic" to "MERGE ()",
        )
    val registration =
        Neo4jSinkRegistration(
            neo4jUri = "neo4j://example.com",
            neo4jUser = "user",
            neo4jPassword = "password",
            neo4jDatabase = "database",
            schemaControlRegistryUri = "http://example.com",
            keyConverter = KafkaConverter.AVRO,
            valueConverter = KafkaConverter.AVRO,
            errorDlqTopic = "dlq-topic",
            topics = listOf("my-topic"),
            strategies = mapOf("neo4j.cypher.topic.my-topic" to "MERGE ()"),
        )

    val payload = registration.getPayload()

    assertTrue((payload["name"] as String).startsWith("Neo4jSinkConnector"))
    assertEquals(expectedConfig, payload["config"])
  }

  @Test
  fun `creates payload with multiple topics`() {
    val registration =
        Neo4jSinkRegistration(
            neo4jUri = "neo4j://example.com",
            neo4jUser = "user",
            neo4jPassword = "password",
            neo4jDatabase = "database",
            schemaControlRegistryUri = "http://example.com",
            keyConverter = KafkaConverter.AVRO,
            valueConverter = KafkaConverter.AVRO,
            topics = listOf("topic1", "topic2"),
            strategies =
                mapOf("neo4j.cypher.topic.topic1" to "MERGE ()", "neo4j.cud.topics" to "topic2"),
        )

    val payload = registration.getPayload()

    val config = payload["config"]
    assertIs<Map<*, *>>(config)
    assertEquals("MERGE ()", config["neo4j.cypher.topic.topic1"])
    assertEquals("topic2", config["neo4j.cud.topics"])
  }
}
