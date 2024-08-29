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
package org.neo4j.connectors.kafka.source

import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.maps.shouldContainAll
import io.kotest.matchers.maps.shouldContainExactly
import io.kotest.matchers.shouldBe
import io.kotest.matchers.throwable.shouldHaveMessage
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import org.apache.kafka.common.config.ConfigException
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.neo4j.cdc.client.model.EntityOperation
import org.neo4j.cdc.client.selector.NodeSelector
import org.neo4j.cdc.client.selector.RelationshipNodeSelector
import org.neo4j.cdc.client.selector.RelationshipSelector
import org.neo4j.connectors.kafka.configuration.AuthenticationType
import org.neo4j.connectors.kafka.configuration.Neo4jConfiguration
import org.neo4j.driver.TransactionConfig

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
                  SourceConfiguration.QUERY_TOPIC to "my-topic",
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
                  SourceConfiguration.QUERY_TOPIC to "my-topic",
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
                  SourceConfiguration.QUERY_TOPIC to "my-topic",
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
                  SourceConfiguration.QUERY_TOPIC to "my-topic",
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
                  SourceConfiguration.QUERY_TOPIC to "my-topic",
                  SourceConfiguration.START_FROM to "EARLIEST",
                  SourceConfiguration.QUERY_POLL_INTERVAL to "1m",
                  SourceConfiguration.QUERY_STREAMING_PROPERTY to "",
                  SourceConfiguration.QUERY_TIMEOUT to "5m",
                  SourceConfiguration.BATCH_SIZE to "50"))
        }
        .also {
          it shouldHaveMessage
              "Invalid value  for configuration neo4j.query.streaming-property: Must not be blank."
        }
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
                SourceConfiguration.QUERY_TOPIC to "my-topic",
                SourceConfiguration.START_FROM to "EARLIEST",
                SourceConfiguration.QUERY_POLL_INTERVAL to "1m",
                SourceConfiguration.QUERY_TIMEOUT to "5m",
                SourceConfiguration.BATCH_SIZE to "50"))

    assertEquals(SourceType.QUERY, config.strategy)
    assertEquals("MATCH (n) RETURN n", config.query)
    assertEquals("timestamp", config.queryStreamingProperty)
    assertEquals("my-topic", config.topic)
    assertEquals(StartFrom.EARLIEST, config.startFrom)
    assertEquals(1.minutes, config.queryPollingInterval)
    assertEquals(5.minutes, config.queryTimeout)
    assertEquals(50, config.batchSize)
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
                SourceConfiguration.CDC_POLL_INTERVAL to "5s",
                "neo4j.cdc.topic.topic-1.patterns" to "(),()-[]-()"))

    config.strategy shouldBe SourceType.CDC
    config.startFrom shouldBe StartFrom.EARLIEST
    config.batchSize shouldBe 10000
    config.cdcPollingInterval shouldBe 5.seconds
    config.cdcSelectorsToTopics shouldContainExactly
        mapOf(
            NodeSelector.builder().build() to listOf("topic-1"),
            RelationshipSelector.builder().build() to listOf("topic-1")
        )
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
                SourceConfiguration.CDC_POLL_INTERVAL to "5s",
                "neo4j.cdc.topic.people.patterns" to "(:Person)",
                "neo4j.cdc.topic.company.patterns" to "(:Company)",
                "neo4j.cdc.topic.works_for.patterns" to "(:Person)-[:WORKS_FOR]->(:Company)",
                "neo4j.cdc.topic.topic-1.patterns" to "(:Person)",
                "neo4j.cdc.topic.topic-2.patterns" to "(:Person {id})",
                "neo4j.cdc.topic.topic-3.patterns.0.pattern" to "(:User)",
                "neo4j.cdc.topic.topic-3.patterns.0.operation" to "create",
                "neo4j.cdc.topic.topic-3.patterns.0.changesTo" to "name, age",
                "neo4j.cdc.topic.topic-3.patterns.0.metadata.authenticatedUser" to "someone",
                "neo4j.cdc.topic.topic-3.patterns.0.metadata.executingUser" to "someoneElse",
                "neo4j.cdc.topic.topic-3.patterns.0.metadata.txMetadata.app" to "neo4j-browser",
            ))

    config.strategy shouldBe SourceType.CDC
    config.startFrom shouldBe StartFrom.EARLIEST
    config.batchSize shouldBe 10000
    config.cdcPollingInterval shouldBe 5.seconds
    config.cdcSelectorsToTopics shouldContainAll
        mapOf(
            NodeSelector.builder().withLabels(setOf("Person")).build() to
                listOf("people", "topic-1"),
            NodeSelector.builder()
                .withLabels(setOf("Person"))
                .includingProperties(setOf("id"))
                .build() to listOf("topic-2"),
            NodeSelector.builder().withLabels(setOf("Company")).build() to listOf("company"),
            RelationshipSelector.builder()
                .withType("WORKS_FOR")
                .withStart(RelationshipNodeSelector.builder().withLabels(setOf("Person")).build())
                .withEnd(RelationshipNodeSelector.builder().withLabels(setOf("Company")).build())
                .build() to listOf("works_for"),
            NodeSelector.builder()
                .withOperation(EntityOperation.CREATE)
                .withChangesTo(setOf("name", "age"))
                .withLabels(setOf("User"))
                .withTxMetadata(mapOf("app" to "neo4j-browser"))
                .withExecutingUser("someoneElse")
                .withAuthenticatedUser("someone")
                .build() to listOf("topic-3"))
  }

  @Test
  fun `fail on mixing old and new style (positional pattern)`() {

    assertFailsWith(ConfigException::class) {
          SourceConfiguration(
                  mapOf(
                      Neo4jConfiguration.URI to "neo4j://localhost",
                      SourceConfiguration.STRATEGY to "CDC",
                      SourceConfiguration.START_FROM to "EARLIEST",
                      SourceConfiguration.BATCH_SIZE to "10000",
                      SourceConfiguration.CDC_POLL_INTERVAL to "5s",
                      "neo4j.cdc.topic.people.patterns" to "(:Person)",
                      "neo4j.cdc.topic.people.patterns.0.pattern" to "(:User)",
                  ))
              .cdcSelectors
        }
        .also {
          it shouldHaveMessage
              "It's not allowed to mix positional and non-positional configuration for the same topic."
        }
  }

  @Test
  fun `fail on mixing old and new style (positional operation)`() {

    assertFailsWith(ConfigException::class) {
          SourceConfiguration(
                  mapOf(
                      Neo4jConfiguration.URI to "neo4j://localhost",
                      SourceConfiguration.STRATEGY to "CDC",
                      SourceConfiguration.START_FROM to "EARLIEST",
                      SourceConfiguration.BATCH_SIZE to "10000",
                      SourceConfiguration.CDC_POLL_INTERVAL to "5s",
                      "neo4j.cdc.topic.people.patterns" to "(:Person)",
                      "neo4j.cdc.topic.people.patterns.0.operation" to "create",
                  ))
              .cdcSelectors
        }
        .also {
          it shouldHaveMessage
              "It's not allowed to mix positional and non-positional configuration for the same topic."
        }
  }

  @Test
  fun `fail on mixing old and new style (positional changesTo)`() {

    assertFailsWith(ConfigException::class) {
          SourceConfiguration(
                  mapOf(
                      Neo4jConfiguration.URI to "neo4j://localhost",
                      SourceConfiguration.STRATEGY to "CDC",
                      SourceConfiguration.START_FROM to "EARLIEST",
                      SourceConfiguration.BATCH_SIZE to "10000",
                      SourceConfiguration.CDC_POLL_INTERVAL to "5s",
                      "neo4j.cdc.topic.people.patterns" to "(:Person)",
                      "neo4j.cdc.topic.people.patterns.0.changesTo" to "name",
                  ))
              .cdcSelectors
        }
        .also {
          it shouldHaveMessage
              "It's not allowed to mix positional and non-positional configuration for the same topic."
        }
  }

  @Test
  fun `fail on mixing old and new style (positional metadata)`() {

    assertFailsWith(ConfigException::class) {
          SourceConfiguration(
                  mapOf(
                      Neo4jConfiguration.URI to "neo4j://localhost",
                      SourceConfiguration.STRATEGY to "CDC",
                      SourceConfiguration.START_FROM to "EARLIEST",
                      SourceConfiguration.BATCH_SIZE to "10000",
                      SourceConfiguration.CDC_POLL_INTERVAL to "5s",
                      "neo4j.cdc.topic.people.patterns" to "(:Person)",
                      "neo4j.cdc.topic.people.patterns.0.metadata.authenticatedUser" to "neo4j",
                  ))
              .cdcSelectors
        }
        .also {
          it shouldHaveMessage
              "It's not allowed to mix positional and non-positional configuration for the same topic."
        }
  }

  @Test
  fun `fail on multiple patterns in positional style`() {

    assertFailsWith(ConfigException::class) {
          SourceConfiguration(
                  mapOf(
                      Neo4jConfiguration.URI to "neo4j://localhost",
                      SourceConfiguration.STRATEGY to "CDC",
                      SourceConfiguration.START_FROM to "EARLIEST",
                      SourceConfiguration.BATCH_SIZE to "10000",
                      SourceConfiguration.CDC_POLL_INTERVAL to "5s",
                      "neo4j.cdc.topic.people.patterns.0.pattern" to "(:User)-[]-(),()",
                      "neo4j.cdc.topic.people.patterns.0.operation" to "create",
                  ))
              .cdcSelectors
        }
        .also {
          it shouldHaveMessage
              "Too many patterns. Only one pattern allowed for positional pattern configuration."
        }
  }

  @Test
  fun `fail on index out of bounds positional style (pattern)`() {

    assertFailsWith(ConfigException::class) {
          SourceConfiguration(
                  mapOf(
                      Neo4jConfiguration.URI to "neo4j://localhost",
                      SourceConfiguration.STRATEGY to "CDC",
                      SourceConfiguration.START_FROM to "EARLIEST",
                      SourceConfiguration.BATCH_SIZE to "10000",
                      SourceConfiguration.CDC_POLL_INTERVAL to "5s",
                      "neo4j.cdc.topic.people.patterns.1.pattern" to "(:User)",
                  ))
              .cdcSelectors
        }
        .also {
          it shouldHaveMessage
              "Index 1 out of bounds. Please ensure that you started the definition with a 0-based index."
        }
  }

  @Test
  fun `fail on non-existing pattern (changesTo)`() {

    assertFailsWith(ConfigException::class) {
          SourceConfiguration(
                  mapOf(
                      Neo4jConfiguration.URI to "neo4j://localhost",
                      SourceConfiguration.STRATEGY to "CDC",
                      SourceConfiguration.START_FROM to "EARLIEST",
                      SourceConfiguration.BATCH_SIZE to "10000",
                      SourceConfiguration.CDC_POLL_INTERVAL to "5s",
                      "neo4j.cdc.topic.people.patterns.1.changesTo" to "name",
                  ))
              .cdcSelectors
        }
        .also {
          it shouldHaveMessage
              "Cannot assign config value because pattern is not defined for index 1."
        }
  }

  @Test
  fun `fail on non-existing pattern (operation)`() {

    assertFailsWith(ConfigException::class) {
          SourceConfiguration(
                  mapOf(
                      Neo4jConfiguration.URI to "neo4j://localhost",
                      SourceConfiguration.STRATEGY to "CDC",
                      SourceConfiguration.START_FROM to "EARLIEST",
                      SourceConfiguration.BATCH_SIZE to "10000",
                      SourceConfiguration.CDC_POLL_INTERVAL to "5s",
                      "neo4j.cdc.topic.people.patterns.1.operation" to "create",
                  ))
              .cdcSelectors
        }
        .also {
          it shouldHaveMessage
              "Cannot assign config value because pattern is not defined for index 1."
        }
  }

  @Test
  fun `fail on non-existing pattern (metadata)`() {

    assertFailsWith(ConfigException::class) {
          SourceConfiguration(
                  mapOf(
                      Neo4jConfiguration.URI to "neo4j://localhost",
                      SourceConfiguration.STRATEGY to "CDC",
                      SourceConfiguration.START_FROM to "EARLIEST",
                      SourceConfiguration.BATCH_SIZE to "10000",
                      SourceConfiguration.CDC_POLL_INTERVAL to "5s",
                      "neo4j.cdc.topic.people.patterns.1.metadata.authenticatedUser" to "neo4j",
                  ))
              .cdcSelectors
        }
        .also {
          it shouldHaveMessage
              "Cannot assign config value because pattern is not defined for index 1."
        }
  }

  @Test
  fun `fail on unknown operation parameter`() {

    assertFailsWith(ConfigException::class) {
          SourceConfiguration(
                  mapOf(
                      Neo4jConfiguration.URI to "neo4j://localhost",
                      SourceConfiguration.STRATEGY to "CDC",
                      SourceConfiguration.START_FROM to "EARLIEST",
                      SourceConfiguration.BATCH_SIZE to "10000",
                      SourceConfiguration.CDC_POLL_INTERVAL to "5s",
                      "neo4j.cdc.topic.people.patterns.0.pattern" to "(:User)",
                      "neo4j.cdc.topic.people.patterns.0.operation" to "wurstsalat",
                  ))
              .cdcSelectors
        }
        .also {
          it shouldHaveMessage
              "Cannot parse wurstsalat as an operation. Allowed operations are create, delete or update."
        }
  }

  @Test
  fun `should validate a valid configuration successfully`() {
    assertDoesNotThrow {
      val config =
          SourceConfiguration(
              mapOf(
                  "neo4j.uri" to "neo4j://neo4j:7687",
                  "neo4j.authentication.type" to "BASIC",
                  "neo4j.authentication.basic.username" to "neo4j",
                  "neo4j.authentication.basic.password" to "password",
                  "neo4j.source-strategy" to "CDC",
                  "neo4j.start-from" to "NOW",
                  "neo4j.cdc.poll-interval" to "5s",
                  "neo4j.cdc.topic.creates.patterns.0.pattern" to "(:TestSource)",
                  "neo4j.cdc.topic.creates.patterns.0.operation" to "CREATE",
                  "neo4j.cdc.topic.updates.patterns.0.pattern" to "(:TestSource)",
                  "neo4j.cdc.topic.updates.patterns.0.operation" to "UPDATE",
                  "neo4j.cdc.topic.deletes.patterns.0.pattern" to "(:TestSource)",
                  "neo4j.cdc.topic.deletes.patterns.0.operation" to "DELETE"))

      config.validate()
    }
  }

  @Test
  fun `should extract selectors correctly with indexed patterns`() {
    assertDoesNotThrow {
      val configuration =
          SourceConfiguration(
              mapOf(
                  "neo4j.uri" to "neo4j://neo4j:7687",
                  "neo4j.authentication.type" to "BASIC",
                  "neo4j.authentication.basic.username" to "neo4j",
                  "neo4j.authentication.basic.password" to "password",
                  "neo4j.source-strategy" to "CDC",
                  "neo4j.start-from" to "NOW",
                  "neo4j.cdc.poll-interval" to "5s",
                  "neo4j.cdc.topic.my-topic.patterns.0.pattern" to "(:Person)",
                  "neo4j.cdc.topic.my-topic.patterns.0.operation" to "create",
                  "neo4j.cdc.topic.my-topic.patterns.0.changesTo" to "name,surname",
                  "neo4j.cdc.topic.my-topic.patterns.0.metadata.authenticatedUser" to
                      "pattern0-authUser",
                  "neo4j.cdc.topic.my-topic.patterns.0.metadata.executingUser" to
                      "pattern0-execUser",
                  "neo4j.cdc.topic.my-topic.patterns.0.metadata.txMetadata.key0" to
                      "pattern0-value0",
                  "neo4j.cdc.topic.my-topic.patterns.1.pattern" to "(:Person)-[:KNOWS]->(:Person)",
                  "neo4j.cdc.topic.my-topic.patterns.1.operation" to "update",
                  "neo4j.cdc.topic.my-topic.patterns.1.changesTo" to "since",
                  "neo4j.cdc.topic.my-topic.patterns.1.metadata.authenticatedUser" to
                      "pattern1-authUser",
                  "neo4j.cdc.topic.my-topic.patterns.1.metadata.executingUser" to
                      "pattern1-execUser",
                  "neo4j.cdc.topic.my-topic.patterns.1.metadata.txMetadata.key0" to
                      "pattern1-value0",
                  "neo4j.cdc.topic.my-topic.patterns.1.metadata.txMetadata.key1" to
                      "pattern1-value1",
              ),
          )

      configuration.validate()
      configuration.cdcSelectors shouldHaveSize 2
      configuration.cdcSelectors shouldBe
          setOf(
              NodeSelector.builder()
                  .withOperation(EntityOperation.CREATE)
                  .withChangesTo(setOf("name", "surname"))
                  .withLabels(setOf("Person"))
                  .withTxMetadata(mapOf("key0" to "pattern0-value0"))
                  .withExecutingUser("pattern0-execUser")
                  .withAuthenticatedUser("pattern0-authUser")
                  .build(),
              RelationshipSelector.builder()
                  .withOperation(EntityOperation.UPDATE)
                  .withChangesTo(setOf("since"))
                  .withType("KNOWS")
                  .withStart(RelationshipNodeSelector.builder().withLabels(setOf("Person")).build())
                  .withEnd(RelationshipNodeSelector.builder().withLabels(setOf("Person")).build())
                  .withTxMetadata(mapOf("key0" to "pattern1-value0", "key1" to "pattern1-value1"))
                  .withExecutingUser("pattern1-execUser")
                  .withAuthenticatedUser("pattern1-authUser")
                  .build())
      configuration.cdcSelectorsToTopics shouldBe
          mapOf(
              NodeSelector.builder()
                  .withOperation(EntityOperation.CREATE)
                  .withChangesTo(setOf("name", "surname"))
                  .withLabels(setOf("Person"))
                  .withTxMetadata(mapOf("key0" to "pattern0-value0"))
                  .withExecutingUser("pattern0-execUser")
                  .withAuthenticatedUser("pattern0-authUser")
                  .build() to listOf("my-topic"),
              RelationshipSelector.builder()
                  .withOperation(EntityOperation.UPDATE)
                  .withChangesTo(setOf("since"))
                  .withType("KNOWS")
                  .withStart(RelationshipNodeSelector.builder().withLabels(setOf("Person")).build())
                  .withEnd(RelationshipNodeSelector.builder().withLabels(setOf("Person")).build())
                  .withTxMetadata(mapOf("key0" to "pattern1-value0", "key1" to "pattern1-value1"))
                  .withExecutingUser("pattern1-execUser")
                  .withAuthenticatedUser("pattern1-authUser")
                  .build() to listOf("my-topic"),
          )
    }
  }

  @Test
  fun `fail on invalid metadata key`() {
    val config =
        SourceConfiguration(
            mapOf(
                "neo4j.uri" to "neo4j://neo4j:7687",
                "neo4j.authentication.type" to "BASIC",
                "neo4j.authentication.basic.username" to "neo4j",
                "neo4j.authentication.basic.password" to "password",
                "neo4j.source-strategy" to "CDC",
                "neo4j.start-from" to "NOW",
                "neo4j.cdc.poll-interval" to "5s",
                "neo4j.cdc.topic.myTopic.patterns.0.pattern" to "(:TestSource)",
                "neo4j.cdc.topic.myTopic.patterns.0.operation" to "CREATE",
                "neo4j.cdc.topic.myTopic.patterns.0.metadata.txMetadata.app" to
                    "something-AI-something",
                "neo4j.cdc.topic.myTopic.patterns.0.metadata.txMetadataButNotReally.key" to
                    "value"))

    assertFailsWith(ConfigException::class) { config.cdcSelectorsToTopics }
        .also {
          it shouldHaveMessage
              "Unexpected metadata key: 'txMetadataButNotReally.key' found in configuration property 'neo4j.cdc.topic.myTopic.patterns.0.metadata.txMetadataButNotReally.key'. Valid keys are 'authenticatedUser', 'executingUser', or keys starting with 'txMetadata.*'."
        }
  }

  @Test
  fun `fail validation on invalid CDC key serialization strategy`() {
    assertFailsWith(ConfigException::class) {
          SourceConfiguration(
                  mapOf(
                      Neo4jConfiguration.URI to "neo4j://localhost",
                      Neo4jConfiguration.AUTHENTICATION_TYPE to AuthenticationType.NONE.name,
                      SourceConfiguration.STRATEGY to "CDC",
                      SourceConfiguration.START_FROM to "EARLIEST",
                      SourceConfiguration.BATCH_SIZE to "10000",
                      SourceConfiguration.CDC_POLL_INTERVAL to "5s",
                      "neo4j.cdc.topic.topic-1.patterns" to "(),()-[]-()",
                      "neo4j.cdc.topic.topic-1.key-strategy" to "INVALID",
                  ),
              )
              .validate()
        }
        .also {
          it shouldHaveMessage
              "Invalid value INVALID for configuration neo4j.cdc.topic.topic-1.key-strategy: Must be one of: 'SKIP', 'ELEMENT_ID', 'ENTITY_KEYS', 'WHOLE_VALUE'."
        }
  }

  @Test
  fun `fail validation on invalid CDC value serialization strategy`() {
    assertFailsWith(ConfigException::class) {
          SourceConfiguration(
                  mapOf(
                      Neo4jConfiguration.URI to "neo4j://localhost",
                      Neo4jConfiguration.AUTHENTICATION_TYPE to AuthenticationType.NONE.name,
                      SourceConfiguration.STRATEGY to "CDC",
                      SourceConfiguration.START_FROM to "EARLIEST",
                      SourceConfiguration.BATCH_SIZE to "10000",
                      SourceConfiguration.CDC_POLL_INTERVAL to "5s",
                      "neo4j.cdc.topic.topic-1.patterns" to "(),()-[]-()",
                      "neo4j.cdc.topic.topic-1.value-strategy" to "INVALID"))
              .validate()
        }
        .also {
          it shouldHaveMessage
              "Invalid value INVALID for configuration neo4j.cdc.topic.topic-1.value-strategy: Must be one of: 'CHANGE_EVENT', 'ENTITY_EVENT'."
        }
  }

  @Test
  fun `should return correct telemetry data for cdc strategy`() {
    val originals =
        mapOf(
            "neo4j.uri" to "neo4j://neo4j:7687",
            "neo4j.authentication.type" to "BASIC",
            "neo4j.authentication.basic.username" to "neo4j",
            "neo4j.authentication.basic.password" to "password",
            "neo4j.source-strategy" to "CDC",
            "neo4j.start-from" to "NOW",
            "neo4j.cdc.poll-interval" to "5s",
            "neo4j.cdc.topic.creates.patterns.0.pattern" to "(:TestSource)",
            "neo4j.cdc.topic.creates.patterns.0.operation" to "CREATE",
            "neo4j.cdc.topic.updates.patterns.0.pattern" to "(:TestSource)",
            "neo4j.cdc.topic.updates.patterns.0.operation" to "UPDATE",
            "neo4j.cdc.topic.deletes.patterns.0.pattern" to "(:TestSource)",
            "neo4j.cdc.topic.deletes.patterns.0.operation" to "DELETE")
    val config = SourceConfiguration(originals)

    config.userAgentComment() shouldBe "cdc"
    config.txConfig() shouldBe
        TransactionConfig.builder().withMetadata(mapOf("app" to "kafka-source")).build()
  }

  @Test
  fun `should return correct telemetry data for query strategy`() {
    val originals =
        mapOf(
            "neo4j.uri" to "neo4j://neo4j:7687",
            "neo4j.authentication.type" to "BASIC",
            "neo4j.authentication.basic.username" to "neo4j",
            "neo4j.authentication.basic.password" to "password",
            "neo4j.source-strategy" to "QUERY",
            "neo4j.query" to "RETURN 1",
            "neo4j.start-from" to "NOW",
            "neo4j.topic" to "my-topic")
    val config = SourceConfiguration(originals)

    config.userAgentComment() shouldBe "query"
    config.txConfig() shouldBe
        TransactionConfig.builder().withMetadata(mapOf("app" to "kafka-source")).build()
  }
}
