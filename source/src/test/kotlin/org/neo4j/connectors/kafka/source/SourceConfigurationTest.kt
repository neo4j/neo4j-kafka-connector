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

import io.kotest.matchers.maps.shouldContainAll
import io.kotest.matchers.maps.shouldContainExactly
import io.kotest.matchers.shouldBe
import io.kotest.matchers.throwable.shouldHaveMessage
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import org.apache.kafka.common.config.ConfigException
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.neo4j.cdc.client.selector.NodeSelector
import org.neo4j.cdc.client.selector.RelationshipNodeSelector
import org.neo4j.cdc.client.selector.RelationshipSelector
import org.neo4j.connectors.kafka.configuration.Neo4jConfiguration

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
          it shouldHaveMessage
              "Invalid value none for configuration neo4j.source-strategy: Must be one of: 'QUERY', 'CDC'."
        }

    assertFailsWith(ConfigException::class) {
          SourceConfiguration(
              mapOf(
                  Neo4jConfiguration.URI to "neo4j://localhost",
                  SourceConfiguration.STRATEGY to "none"))
        }
        .also {
          it shouldHaveMessage
              "Invalid value none for configuration neo4j.source-strategy: Must be one of: 'QUERY', 'CDC'."
        }

    assertFailsWith(ConfigException::class) {
          SourceConfiguration(
              mapOf(
                  Neo4jConfiguration.URI to "neo4j://localhost",
                  SourceConfiguration.STRATEGY to "QUERY",
                  SourceConfiguration.QUERY to "MATCH (n) RETURN n",
                  SourceConfiguration.TOPIC to "my-topic",
                  SourceConfiguration.START_FROM to "none"))
        }
        .also {
          it shouldHaveMessage
              "Invalid value none for configuration neo4j.start-from: Must be one of: 'EARLIEST', 'NOW', 'USER_PROVIDED'."
        }

    assertFailsWith(ConfigException::class) {
          SourceConfiguration(
              mapOf(
                  Neo4jConfiguration.URI to "neo4j://localhost",
                  SourceConfiguration.STRATEGY to "QUERY",
                  SourceConfiguration.QUERY to "MATCH (n) RETURN n",
                  SourceConfiguration.TOPIC to "my-topic",
                  SourceConfiguration.START_FROM to "EARLIEST",
                  SourceConfiguration.QUERY_POLL_INTERVAL to "1k"))
        }
        .also {
          it shouldHaveMessage
              "Invalid value 1k for configuration neo4j.query.poll-interval: Must match pattern '(\\d+(ms|s|m|h|d))+'."
        }

    assertFailsWith(ConfigException::class) {
          SourceConfiguration(
              mapOf(
                  Neo4jConfiguration.URI to "neo4j://localhost",
                  SourceConfiguration.STRATEGY to "QUERY",
                  SourceConfiguration.QUERY to "MATCH (n) RETURN n",
                  SourceConfiguration.TOPIC to "my-topic",
                  SourceConfiguration.START_FROM to "EARLIEST",
                  SourceConfiguration.QUERY_POLL_INTERVAL to "1m",
                  SourceConfiguration.QUERY_TIMEOUT to "1k"))
        }
        .also {
          it shouldHaveMessage
              "Invalid value 1k for configuration neo4j.query.timeout: Must match pattern '(\\d+(ms|s|m|h|d))+'."
        }

    assertFailsWith(ConfigException::class) {
          SourceConfiguration(
              mapOf(
                  Neo4jConfiguration.URI to "neo4j://localhost",
                  SourceConfiguration.STRATEGY to "QUERY",
                  SourceConfiguration.QUERY to "MATCH (n) RETURN n",
                  SourceConfiguration.TOPIC to "my-topic",
                  SourceConfiguration.START_FROM to "EARLIEST",
                  SourceConfiguration.QUERY_POLL_INTERVAL to "1m",
                  SourceConfiguration.QUERY_TIMEOUT to "5m",
                  SourceConfiguration.BATCH_SIZE to "-1"))
        }
        .also {
          it shouldHaveMessage
              "Invalid value -1 for configuration neo4j.batch-size: Value must be at least 1"
        }

    assertFailsWith(ConfigException::class) {
          SourceConfiguration(
              mapOf(
                  Neo4jConfiguration.URI to "neo4j://localhost",
                  SourceConfiguration.STRATEGY to "QUERY",
                  SourceConfiguration.QUERY to "MATCH (n) RETURN n",
                  SourceConfiguration.TOPIC to "my-topic",
                  SourceConfiguration.START_FROM to "EARLIEST",
                  SourceConfiguration.QUERY_POLL_INTERVAL to "1m",
                  SourceConfiguration.QUERY_TIMEOUT to "5m",
                  SourceConfiguration.BATCH_SIZE to "50",
                  SourceConfiguration.ENFORCE_SCHEMA to "disabled"))
        }
        .also {
          it shouldHaveMessage
              "Invalid value disabled for configuration neo4j.enforce-schema: Expected value to be either true or false"
        }

    assertFailsWith(ConfigException::class) {
          SourceConfiguration(
              mapOf(
                  Neo4jConfiguration.URI to "neo4j://localhost",
                  SourceConfiguration.STRATEGY to "QUERY",
                  SourceConfiguration.QUERY to "MATCH (n) RETURN n",
                  SourceConfiguration.TOPIC to "my-topic",
                  SourceConfiguration.START_FROM to "USER_PROVIDED",
                  SourceConfiguration.QUERY_POLL_INTERVAL to "1m",
                  SourceConfiguration.QUERY_TIMEOUT to "5m",
                  SourceConfiguration.BATCH_SIZE to "50",
                  SourceConfiguration.ENFORCE_SCHEMA to "disabled"))
        }
        .also {
          it shouldHaveMessage
              "Invalid value disabled for configuration neo4j.enforce-schema: Expected value to be either true or false"
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
                SourceConfiguration.START_FROM to "EARLIEST",
                SourceConfiguration.QUERY_POLL_INTERVAL to "1m",
                SourceConfiguration.QUERY_TIMEOUT to "5m",
                SourceConfiguration.BATCH_SIZE to "50",
                SourceConfiguration.ENFORCE_SCHEMA to "true"))

    assertEquals(SourceType.QUERY, config.strategy)
    assertEquals("MATCH (n) RETURN n", config.query)
    assertEquals("", config.queryStreamingProperty)
    assertEquals("my-topic", config.topic)
    assertEquals(StartFrom.EARLIEST, config.startFrom)
    assertEquals(1.minutes, config.queryPollingInterval)
    assertEquals(5.minutes, config.queryTimeout)
    assertEquals(50, config.batchSize)
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
                SourceConfiguration.START_FROM to "EARLIEST",
                SourceConfiguration.QUERY_POLL_INTERVAL to "1m",
                SourceConfiguration.QUERY_TIMEOUT to "5m",
                SourceConfiguration.BATCH_SIZE to "50",
                SourceConfiguration.ENFORCE_SCHEMA to "true"))

    assertEquals(SourceType.QUERY, config.strategy)
    assertEquals("MATCH (n) RETURN n", config.query)
    assertEquals("timestamp", config.queryStreamingProperty)
    assertEquals("my-topic", config.topic)
    assertEquals(StartFrom.EARLIEST, config.startFrom)
    assertEquals(1.minutes, config.queryPollingInterval)
    assertEquals(5.minutes, config.queryTimeout)
    assertEquals(50, config.batchSize)
    assertTrue(config.enforceSchema)
  }

  @Test
  fun `valid config with cdc to single topic`() {
    val config =
        SourceConfiguration(
            mapOf(
                Neo4jConfiguration.URI to "neo4j://localhost",
                SourceConfiguration.STRATEGY to "CDC",
                SourceConfiguration.START_FROM to "EARLIEST",
                SourceConfiguration.BATCH_SIZE to "10000",
                SourceConfiguration.ENFORCE_SCHEMA to "true",
                SourceConfiguration.CDC_POLL_INTERVAL to "5s",
                "neo4j.cdc.topic.topic-1" to "(),()-[]-()"))

    config.strategy shouldBe SourceType.CDC
    config.startFrom shouldBe StartFrom.EARLIEST
    config.batchSize shouldBe 10000
    config.cdcPollingInterval shouldBe 5.seconds
    config.enforceSchema shouldBe true
    config.cdcSelectorsToTopics shouldContainExactly
        mapOf(
            NodeSelector(null, emptySet(), emptySet(), emptyMap()) to listOf("topic-1"),
            RelationshipSelector(
                null,
                emptySet(),
                null,
                RelationshipNodeSelector(emptySet(), emptyMap()),
                RelationshipNodeSelector(emptySet(), emptyMap()),
                emptyMap()) to listOf("topic-1"))
  }

  @Test
  fun `valid config with cdc to multiple topics`() {
    val config =
        SourceConfiguration(
            mapOf(
                Neo4jConfiguration.URI to "neo4j://localhost",
                SourceConfiguration.STRATEGY to "CDC",
                SourceConfiguration.START_FROM to "EARLIEST",
                SourceConfiguration.BATCH_SIZE to "10000",
                SourceConfiguration.ENFORCE_SCHEMA to "true",
                SourceConfiguration.CDC_POLL_INTERVAL to "5s",
                "neo4j.cdc.topic.people" to "(:Person)",
                "neo4j.cdc.topic.company" to "(:Company)",
                "neo4j.cdc.topic.works_for" to "(:Person)-[:WORKS_FOR]->(:Company)",
                "neo4j.cdc.topic.topic-1" to "(:Person)",
                "neo4j.cdc.topic.topic-2" to "(:Person {id})"))

    config.strategy shouldBe SourceType.CDC
    config.startFrom shouldBe StartFrom.EARLIEST
    config.batchSize shouldBe 10000
    config.cdcPollingInterval shouldBe 5.seconds
    config.enforceSchema shouldBe true
    config.cdcSelectorsToTopics shouldContainAll
        mapOf(
            NodeSelector(null, emptySet(), setOf("Person"), emptyMap()) to
                listOf("people", "topic-1"),
            NodeSelector(null, emptySet(), setOf("Person"), emptyMap(), setOf("id"), emptySet()) to
                listOf("topic-2"),
            NodeSelector(null, emptySet(), setOf("Company"), emptyMap()) to listOf("company"),
            RelationshipSelector(
                null,
                emptySet(),
                "WORKS_FOR",
                RelationshipNodeSelector(setOf("Person"), emptyMap()),
                RelationshipNodeSelector(setOf("Company"), emptyMap()),
                emptyMap()) to listOf("works_for"))
  }
}
