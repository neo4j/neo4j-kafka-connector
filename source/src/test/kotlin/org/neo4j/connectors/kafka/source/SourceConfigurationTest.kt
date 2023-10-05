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
package org.neo4j.connectors.kafka.source

import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.minutes
import org.apache.kafka.common.config.ConfigException
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.neo4j.connectors.kafka.configuration.Neo4jConfiguration
import streams.kafka.connect.source.SourceType
import streams.kafka.connect.source.StreamingFrom

class SourceConfigurationTest {

  @Test
  fun `config should be successful`() {
    assertDoesNotThrow { SourceConfiguration.config() }
  }

  @Test
  fun `invalid data`() {
    assertFailsWith(ConfigException::class) {
          SourceConfiguration(
              mapOf(
                  Neo4jConfiguration.URI to "neo4j://localhost",
                  SourceConfiguration.STRATEGY to "none"))
        }
        .also {
          assertEquals(
              "Invalid value none for configuration neo4j.source-strategy: Must be one of: 'QUERY'.",
              it.message)
        }

    assertFailsWith(ConfigException::class) {
          SourceConfiguration(
              mapOf(
                  Neo4jConfiguration.URI to "neo4j://localhost",
                  SourceConfiguration.STRATEGY to "QUERY"))
        }
        .also {
          assertEquals(
              "Missing required configuration \"neo4j.query\" which has no default value.",
              it.message)
        }

    assertFailsWith(ConfigException::class) {
          SourceConfiguration(
              mapOf(
                  Neo4jConfiguration.URI to "neo4j://localhost",
                  SourceConfiguration.STRATEGY to "none"))
        }
        .also {
          assertEquals(
              "Invalid value none for configuration neo4j.source-strategy: Must be one of: 'QUERY'.",
              it.message)
        }

    assertFailsWith(ConfigException::class) {
          SourceConfiguration(
              mapOf(
                  Neo4jConfiguration.URI to "neo4j://localhost",
                  SourceConfiguration.STRATEGY to "QUERY",
                  SourceConfiguration.QUERY to "MATCH (n) RETURN n"))
        }
        .also {
          assertEquals(
              "Missing required configuration \"topic\" which has no default value.", it.message)
        }

    assertFailsWith(ConfigException::class) {
          SourceConfiguration(
              mapOf(
                  Neo4jConfiguration.URI to "neo4j://localhost",
                  SourceConfiguration.STRATEGY to "QUERY",
                  SourceConfiguration.QUERY to "MATCH (n) RETURN n",
                  SourceConfiguration.TOPIC to "my-topic",
                  SourceConfiguration.STREAM_FROM to "none"))
        }
        .also {
          assertEquals(
              "Invalid value none for configuration neo4j.stream-from: Must be one of: 'ALL', 'NOW', 'LAST_COMMITTED'.",
              it.message)
        }

    assertFailsWith(ConfigException::class) {
          SourceConfiguration(
              mapOf(
                  Neo4jConfiguration.URI to "neo4j://localhost",
                  SourceConfiguration.STRATEGY to "QUERY",
                  SourceConfiguration.QUERY to "MATCH (n) RETURN n",
                  SourceConfiguration.TOPIC to "my-topic",
                  SourceConfiguration.STREAM_FROM to "ALL",
                  SourceConfiguration.QUERY_POLL_INTERVAL to "1k"))
        }
        .also {
          assertEquals(
              "Invalid value 1k for configuration neo4j.query.poll-interval: Must match pattern '(\\d+(ms|s|m|h|d))+'.",
              it.message)
        }

    assertFailsWith(ConfigException::class) {
          SourceConfiguration(
              mapOf(
                  Neo4jConfiguration.URI to "neo4j://localhost",
                  SourceConfiguration.STRATEGY to "QUERY",
                  SourceConfiguration.QUERY to "MATCH (n) RETURN n",
                  SourceConfiguration.TOPIC to "my-topic",
                  SourceConfiguration.STREAM_FROM to "ALL",
                  SourceConfiguration.QUERY_POLL_INTERVAL to "1m",
                  SourceConfiguration.QUERY_TIMEOUT to "1k"))
        }
        .also {
          assertEquals(
              "Invalid value 1k for configuration neo4j.query.timeout: Must match pattern '(\\d+(ms|s|m|h|d))+'.",
              it.message)
        }

    assertFailsWith(ConfigException::class) {
          SourceConfiguration(
              mapOf(
                  Neo4jConfiguration.URI to "neo4j://localhost",
                  SourceConfiguration.STRATEGY to "QUERY",
                  SourceConfiguration.QUERY to "MATCH (n) RETURN n",
                  SourceConfiguration.TOPIC to "my-topic",
                  SourceConfiguration.STREAM_FROM to "ALL",
                  SourceConfiguration.QUERY_POLL_INTERVAL to "1m",
                  SourceConfiguration.QUERY_TIMEOUT to "5m",
                  SourceConfiguration.QUERY_BATCH_SIZE to "-1"))
        }
        .also {
          assertEquals(
              "Invalid value -1 for configuration neo4j.query.batch-size: Value must be at least 1",
              it.message)
        }

    assertFailsWith(ConfigException::class) {
          SourceConfiguration(
              mapOf(
                  Neo4jConfiguration.URI to "neo4j://localhost",
                  SourceConfiguration.STRATEGY to "QUERY",
                  SourceConfiguration.QUERY to "MATCH (n) RETURN n",
                  SourceConfiguration.TOPIC to "my-topic",
                  SourceConfiguration.STREAM_FROM to "ALL",
                  SourceConfiguration.QUERY_POLL_INTERVAL to "1m",
                  SourceConfiguration.QUERY_TIMEOUT to "5m",
                  SourceConfiguration.QUERY_BATCH_SIZE to "50",
                  SourceConfiguration.ENFORCE_SCHEMA to "disabled"))
        }
        .also {
          assertEquals(
              "Invalid value disabled for configuration neo4j.enforce-schema: Expected value to be either true or false",
              it.message)
        }
  }

  @Test
  fun `valid config without streaming property`() {
    val config =
        SourceConfiguration(
            mapOf(
                Neo4jConfiguration.URI to "neo4j://localhost",
                SourceConfiguration.STRATEGY to "QUERY",
                SourceConfiguration.QUERY to "MATCH (n) RETURN n",
                SourceConfiguration.TOPIC to "my-topic",
                SourceConfiguration.STREAM_FROM to "ALL",
                SourceConfiguration.QUERY_POLL_INTERVAL to "1m",
                SourceConfiguration.QUERY_TIMEOUT to "5m",
                SourceConfiguration.QUERY_BATCH_SIZE to "50",
                SourceConfiguration.ENFORCE_SCHEMA to "true"))

    assertEquals(SourceType.QUERY, config.strategy)
    assertEquals("MATCH (n) RETURN n", config.query)
    assertEquals("", config.queryStreamingProperty)
    assertEquals("my-topic", config.topic)
    assertEquals(StreamingFrom.ALL, config.streamFrom)
    assertEquals(1.minutes, config.queryPollingInterval)
    assertEquals(5.minutes, config.queryTimeout)
    assertEquals(50, config.queryBatchSize)
    assertTrue(config.enforceSchema)
  }

  @Test
  fun `valid config with streaming property`() {
    val config =
        SourceConfiguration(
            mapOf(
                Neo4jConfiguration.URI to "neo4j://localhost",
                SourceConfiguration.STRATEGY to "QUERY",
                SourceConfiguration.QUERY to "MATCH (n) RETURN n",
                SourceConfiguration.QUERY_STREAMING_PROPERTY to "timestamp",
                SourceConfiguration.TOPIC to "my-topic",
                SourceConfiguration.STREAM_FROM to "ALL",
                SourceConfiguration.QUERY_POLL_INTERVAL to "1m",
                SourceConfiguration.QUERY_TIMEOUT to "5m",
                SourceConfiguration.QUERY_BATCH_SIZE to "50",
                SourceConfiguration.ENFORCE_SCHEMA to "true"))

    assertEquals(SourceType.QUERY, config.strategy)
    assertEquals("MATCH (n) RETURN n", config.query)
    assertEquals("timestamp", config.queryStreamingProperty)
    assertEquals("my-topic", config.topic)
    assertEquals(StreamingFrom.ALL, config.streamFrom)
    assertEquals(1.minutes, config.queryPollingInterval)
    assertEquals(5.minutes, config.queryTimeout)
    assertEquals(50, config.queryBatchSize)
    assertTrue(config.enforceSchema)
  }
}
