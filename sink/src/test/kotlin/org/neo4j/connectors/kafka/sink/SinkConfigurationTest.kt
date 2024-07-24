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
package org.neo4j.connectors.kafka.sink

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.maps.shouldHaveKey
import io.kotest.matchers.shouldBe
import io.kotest.matchers.throwable.shouldHaveMessage
import io.kotest.matchers.types.instanceOf
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.sink.SinkConnector
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.neo4j.connectors.kafka.configuration.Neo4jConfiguration
import org.neo4j.connectors.kafka.sink.strategy.CdcSchemaHandler
import org.neo4j.connectors.kafka.sink.strategy.CdcSourceIdHandler
import org.neo4j.connectors.kafka.sink.strategy.CudHandler
import org.neo4j.connectors.kafka.sink.strategy.CypherHandler
import org.neo4j.connectors.kafka.sink.strategy.NodePatternHandler
import org.neo4j.connectors.kafka.sink.strategy.pattern.NodePattern
import org.neo4j.connectors.kafka.sink.strategy.pattern.PropertyMapping
import org.neo4j.cypherdsl.core.renderer.Renderer
import org.neo4j.driver.TransactionConfig

class SinkConfigurationTest {

  @Test
  fun `should throw a ConfigException because of mismatch`() {
    shouldThrow<ConfigException> {
      val originals =
          mapOf(
              Neo4jConfiguration.URI to "bolt://neo4j:7687",
              Neo4jConfiguration.AUTHENTICATION_TYPE to "NONE",
              SinkConnector.TOPICS_CONFIG to "foo, bar",
              "${SinkConfiguration.CYPHER_TOPIC_PREFIX}foo" to
                  "CREATE (p:Person{name: event.firstName})")
      SinkConfiguration(originals, Renderer.getDefaultRenderer())
    } shouldHaveMessage "Topic 'bar' is not assigned a sink strategy"
  }

  @Test
  fun `should throw a ConfigException because of cross defined topics`() {
    shouldThrow<ConfigException> {
      val originals =
          mapOf(
              Neo4jConfiguration.URI to "bolt://neo4j:7687",
              Neo4jConfiguration.AUTHENTICATION_TYPE to "NONE",
              SinkConnector.TOPICS_CONFIG to "foo, bar",
              "${SinkConfiguration.CYPHER_TOPIC_PREFIX}foo" to
                  "CREATE (p:Person{name: event.firstName})",
              "${SinkConfiguration.CYPHER_TOPIC_PREFIX}bar" to
                  "CREATE (p:Person{name: event.firstName})",
              SinkConfiguration.CDC_SOURCE_ID_TOPICS to "foo")

      SinkConfiguration(originals, Renderer.getDefaultRenderer())
    } shouldHaveMessage "Topic 'foo' has multiple strategies defined"
  }

  @Test
  fun `should return the configuration`() {
    val originals =
        mapOf(
            Neo4jConfiguration.URI to "bolt://neo4j:7687",
            Neo4jConfiguration.AUTHENTICATION_TYPE to "NONE",
            SinkConnector.TOPICS_CONFIG to "foo",
            "${SinkConfiguration.CYPHER_TOPIC_PREFIX}foo" to
                "CREATE (p:Person{name: event.firstName})",
            SinkConfiguration.BATCH_SIZE to "10",
            Neo4jConfiguration.DATABASE to "customers")
    val config = SinkConfiguration(originals, Renderer.getDefaultRenderer())

    config.batchSize shouldBe 10
    config.topicHandlers shouldHaveKey "foo"
    config.topicHandlers["foo"] shouldBe instanceOf<CypherHandler>()
    (config.topicHandlers["foo"] as CypherHandler).query shouldBe
        "CREATE (p:Person{name: event.firstName})"
  }

  @Test
  fun `should return the configuration with shuffled topic order`() {
    val originals =
        mapOf(
            Neo4jConfiguration.URI to "bolt://neo4j:7687",
            Neo4jConfiguration.AUTHENTICATION_TYPE to "NONE",
            SinkConnector.TOPICS_CONFIG to "bar,foo",
            "${SinkConfiguration.PATTERN_TOPIC_PREFIX}foo" to "(:Foo{!fooId,fooName})",
            "${SinkConfiguration.PATTERN_TOPIC_PREFIX}bar" to "(:Bar{!barId,barName})",
            SinkConfiguration.BATCH_SIZE to "10")
    val config = SinkConfiguration(originals, Renderer.getDefaultRenderer())

    config.batchSize shouldBe 10
    config.topicHandlers shouldHaveKey "foo"
    config.topicHandlers["foo"] shouldBe instanceOf<NodePatternHandler>()
    (config.topicHandlers["foo"] as NodePatternHandler).pattern shouldBe
        NodePattern(
            setOf("Foo"),
            false,
            setOf(PropertyMapping("fooId", "fooId")),
            setOf(PropertyMapping("fooName", "fooName")),
            emptySet())

    config.topicHandlers shouldHaveKey "bar"
    config.topicHandlers["bar"] shouldBe instanceOf<NodePatternHandler>()
    (config.topicHandlers["bar"] as NodePatternHandler).pattern shouldBe
        NodePattern(
            setOf("Bar"),
            false,
            setOf(PropertyMapping("barId", "barId")),
            setOf(PropertyMapping("barName", "barName")),
            emptySet())
  }

  @Test
  fun `should return specified CDC sourceId label and id names`() {
    val testLabel = "TestCdcLabel"
    val testId = "test_id"
    val originals =
        mapOf(
            Neo4jConfiguration.URI to "bolt://neo4j:7687",
            Neo4jConfiguration.AUTHENTICATION_TYPE to "NONE",
            SinkConnector.TOPICS_CONFIG to "bar,foo",
            SinkConfiguration.CDC_SOURCE_ID_TOPICS to "bar,foo",
            SinkConfiguration.CDC_SOURCE_ID_LABEL_NAME to testLabel,
            SinkConfiguration.CDC_SOURCE_ID_PROPERTY_NAME to testId)
    val config = SinkConfiguration(originals, Renderer.getDefaultRenderer())

    config.topicHandlers shouldHaveKey "foo"
    config.topicHandlers["foo"] shouldBe instanceOf<CdcSourceIdHandler>()
    (config.topicHandlers["foo"] as CdcSourceIdHandler).labelName shouldBe "TestCdcLabel"
    (config.topicHandlers["foo"] as CdcSourceIdHandler).propertyName shouldBe "test_id"

    config.topicHandlers shouldHaveKey "bar"
    config.topicHandlers["bar"] shouldBe instanceOf<CdcSourceIdHandler>()
    (config.topicHandlers["bar"] as CdcSourceIdHandler).labelName shouldBe "TestCdcLabel"
    (config.topicHandlers["bar"] as CdcSourceIdHandler).propertyName shouldBe "test_id"
  }

  @Test
  fun `should return multiple CDC schema topics`() {
    val originals =
        mapOf(
            Neo4jConfiguration.URI to "bolt://neo4j:7687",
            Neo4jConfiguration.AUTHENTICATION_TYPE to "NONE",
            SinkConnector.TOPICS_CONFIG to "bar,foo",
            SinkConfiguration.CDC_SCHEMA_TOPICS to "bar,foo",
        )
    val config = SinkConfiguration(originals, Renderer.getDefaultRenderer())

    config.topicHandlers shouldHaveKey "foo"
    config.topicHandlers["foo"] shouldBe instanceOf<CdcSchemaHandler>()

    config.topicHandlers shouldHaveKey "bar"
    config.topicHandlers["bar"] shouldBe instanceOf<CdcSchemaHandler>()
  }

  @Test
  fun `should return multiple CUD topics`() {
    val originals =
        mapOf(
            Neo4jConfiguration.URI to "bolt://neo4j:7687",
            Neo4jConfiguration.AUTHENTICATION_TYPE to "NONE",
            SinkConnector.TOPICS_CONFIG to "bar,foo",
            SinkConfiguration.CUD_TOPICS to "bar,foo",
        )
    val config = SinkConfiguration(originals, Renderer.getDefaultRenderer())

    config.topicHandlers shouldHaveKey "foo"
    config.topicHandlers["foo"] shouldBe instanceOf<CudHandler>()

    config.topicHandlers shouldHaveKey "bar"
    config.topicHandlers["bar"] shouldBe instanceOf<CudHandler>()
  }

  @ParameterizedTest
  @EnumSource(SinkStrategy::class, names = ["CDC_SOURCE_ID", "CDC_SCHEMA", "CUD"])
  fun `should return correct telemetry data for cdc and cud strategies`(strategy: SinkStrategy) {
    val originals =
        mapOf(
            Neo4jConfiguration.URI to "bolt://neo4j:7687",
            Neo4jConfiguration.AUTHENTICATION_TYPE to "NONE",
            SinkConnector.TOPICS_CONFIG to "bar",
            when (strategy) {
              SinkStrategy.CDC_SCHEMA -> SinkConfiguration.CDC_SCHEMA_TOPICS
              SinkStrategy.CDC_SOURCE_ID -> SinkConfiguration.CDC_SOURCE_ID_TOPICS
              SinkStrategy.CUD -> SinkConfiguration.CUD_TOPICS
              else -> throw IllegalArgumentException(strategy.name)
            } to "bar",
        )
    val config = SinkConfiguration(originals, Renderer.getDefaultRenderer())

    config.userAgentComment() shouldBe strategy.description
    config.txConfig() shouldBe
        TransactionConfig.builder().withMetadata(mapOf("app" to "kafka-sink")).build()
  }

  @Test
  fun `should return correct telemetry data for cypher strategy`() {
    val originals =
        mapOf(
            Neo4jConfiguration.URI to "bolt://neo4j:7687",
            Neo4jConfiguration.AUTHENTICATION_TYPE to "NONE",
            SinkConnector.TOPICS_CONFIG to "bar",
            SinkConfiguration.CYPHER_TOPIC_PREFIX + "bar" to "RETURN 1")
    val config = SinkConfiguration(originals, Renderer.getDefaultRenderer())

    config.userAgentComment() shouldBe "cypher"
    config.txConfig() shouldBe
        TransactionConfig.builder().withMetadata(mapOf("app" to "kafka-sink")).build()
  }

  @Test
  fun `should return correct telemetry data for node pattern strategy`() {
    val originals =
        mapOf(
            Neo4jConfiguration.URI to "bolt://neo4j:7687",
            Neo4jConfiguration.AUTHENTICATION_TYPE to "NONE",
            SinkConnector.TOPICS_CONFIG to "bar",
            SinkConfiguration.PATTERN_TOPIC_PREFIX + "bar" to "Label{!id}")
    val config = SinkConfiguration(originals, Renderer.getDefaultRenderer())

    config.userAgentComment() shouldBe "node-pattern"
    config.txConfig() shouldBe
        TransactionConfig.builder().withMetadata(mapOf("app" to "kafka-sink")).build()
  }

  @Test
  fun `should return correct telemetry data for relationship pattern strategy`() {
    val originals =
        mapOf(
            Neo4jConfiguration.URI to "bolt://neo4j:7687",
            Neo4jConfiguration.AUTHENTICATION_TYPE to "NONE",
            SinkConnector.TOPICS_CONFIG to "bar",
            SinkConfiguration.PATTERN_TOPIC_PREFIX + "bar" to
                "LabelA{!id} REL_TYPE{id} LabelB{!targetId}")
    val config = SinkConfiguration(originals, Renderer.getDefaultRenderer())

    config.userAgentComment() shouldBe "relationship-pattern"
    config.txConfig() shouldBe
        TransactionConfig.builder().withMetadata(mapOf("app" to "kafka-sink")).build()
  }

  @Test
  fun `should return correct telemetry data for multiple strategies`() {
    val originals =
        mapOf(
            Neo4jConfiguration.URI to "bolt://neo4j:7687",
            Neo4jConfiguration.AUTHENTICATION_TYPE to "NONE",
            SinkConnector.TOPICS_CONFIG to "foo,bar,baz",
            SinkConfiguration.CUD_TOPICS to "baz",
            SinkConfiguration.CDC_SOURCE_ID_TOPICS to "foo",
            SinkConfiguration.PATTERN_TOPIC_PREFIX + "bar" to
                "LabelA{!id} REL_TYPE{id} LabelB{!targetId}")
    val config = SinkConfiguration(originals, Renderer.getDefaultRenderer())

    config.userAgentComment() shouldBe "cdc-source-id; cud; relationship-pattern"
    config.txConfig() shouldBe
        TransactionConfig.builder().withMetadata(mapOf("app" to "kafka-sink")).build()
  }
}
