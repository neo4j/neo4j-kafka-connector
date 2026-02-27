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
import kotlin.reflect.KClass
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.sink.SinkConnector
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.EnumSource
import org.junit.jupiter.params.provider.MethodSource
import org.mockito.kotlin.mock
import org.neo4j.caniuse.Neo4j
import org.neo4j.caniuse.Neo4jDeploymentType
import org.neo4j.caniuse.Neo4jEdition
import org.neo4j.caniuse.Neo4jVersion
import org.neo4j.connectors.kafka.configuration.Neo4jConfiguration
import org.neo4j.connectors.kafka.metrics.Metrics
import org.neo4j.connectors.kafka.sink.strategy.CypherHandler
import org.neo4j.connectors.kafka.sink.strategy.NodePatternHandler
import org.neo4j.connectors.kafka.sink.strategy.SinkHandler
import org.neo4j.connectors.kafka.sink.strategy.cud.CudEventTransformer
import org.neo4j.connectors.kafka.sink.strategy.pattern.NodePattern
import org.neo4j.connectors.kafka.sink.strategy.pattern.PropertyMapping
import org.neo4j.cypherdsl.core.renderer.Renderer
import org.neo4j.driver.TransactionConfig

class SinkConfigurationTest {

  val metricsMock: Metrics = mock()

  @Test
  fun `should throw a ConfigException because of mismatch`() {
    shouldThrow<ConfigException> {
      val originals =
          mapOf(
              Neo4jConfiguration.URI to "bolt://neo4j:7687",
              Neo4jConfiguration.AUTHENTICATION_TYPE to "NONE",
              SinkConnector.TOPICS_CONFIG to "foo, bar",
              "${SinkConfiguration.CYPHER_TOPIC_PREFIX}foo" to
                  "CREATE (p:Person{name: event.firstName})",
          )
      val config = SinkConfiguration(originals, Renderer.getDefaultRenderer())
      val topicHandlers = SinkStrategyHandler.createFrom(config, metricsMock)
      config.validateAllTopics(topicHandlers)
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
              SinkConfiguration.CDC_SOURCE_ID_TOPICS to "foo",
          )

      val config = SinkConfiguration(originals, Renderer.getDefaultRenderer())
      val topicHandlers = SinkStrategyHandler.createFrom(config, metricsMock)
      config.validateAllTopics(topicHandlers)
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
            Neo4jConfiguration.DATABASE to "customers",
        )
    val config = SinkConfiguration(originals, Renderer.getDefaultRenderer())

    config.batchSize shouldBe 10

    val topicHandlers = SinkStrategyHandler.createFrom(config, metricsMock)
    topicHandlers shouldHaveKey "foo"
    topicHandlers["foo"] shouldBe instanceOf<CypherHandler>()
    (topicHandlers["foo"] as CypherHandler).query shouldBe
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
            SinkConfiguration.BATCH_SIZE to "10",
        )
    val config = SinkConfiguration(originals, Renderer.getDefaultRenderer())

    config.batchSize shouldBe 10

    val topicHandlers = SinkStrategyHandler.createFrom(config, metricsMock)
    topicHandlers shouldHaveKey "foo"
    topicHandlers["foo"] shouldBe instanceOf<NodePatternHandler>()
    (topicHandlers["foo"] as NodePatternHandler).pattern shouldBe
        NodePattern(
            setOf("Foo"),
            false,
            setOf(PropertyMapping("fooId", "fooId")),
            setOf(PropertyMapping("fooName", "fooName")),
            emptySet(),
        )

    topicHandlers shouldHaveKey "bar"
    topicHandlers["bar"] shouldBe instanceOf<NodePatternHandler>()
    (topicHandlers["bar"] as NodePatternHandler).pattern shouldBe
        NodePattern(
            setOf("Bar"),
            false,
            setOf(PropertyMapping("barId", "barId")),
            setOf(PropertyMapping("barName", "barName")),
            emptySet(),
        )
  }

  @Test
  fun `should default to empty exactly once offset label`() {
    val originals =
        mapOf(
            Neo4jConfiguration.URI to "bolt://neo4j:7687",
            Neo4jConfiguration.AUTHENTICATION_TYPE to "NONE",
            SinkConnector.TOPICS_CONFIG to "bar,foo",
            SinkConfiguration.CDC_SOURCE_ID_TOPICS to "bar,foo",
        )
    val config =
        SinkConfiguration(
            originals,
            Renderer.getDefaultRenderer(),
            apocCypherDoItAvailable = false,
            neo4j = neo4j5_26,
        )

    config.eosOffsetLabel shouldBe ""
  }

  @Test
  fun `should return configured exactly once offset label escaped`() {
    val originals =
        mapOf(
            Neo4jConfiguration.URI to "bolt://neo4j:7687",
            Neo4jConfiguration.AUTHENTICATION_TYPE to "NONE",
            SinkConnector.TOPICS_CONFIG to "bar,foo",
            SinkConfiguration.EOS_OFFSET_LABEL to "__MyKafkaOffset",
            SinkConfiguration.CDC_SCHEMA_TOPICS to "bar,foo",
        )
    val config =
        SinkConfiguration(
            originals,
            Renderer.getDefaultRenderer(),
            apocCypherDoItAvailable = false,
            neo4j = neo4j5_26,
        )

    config.eosOffsetLabel shouldBe "`__MyKafkaOffset`"
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
            SinkConfiguration.CDC_SOURCE_ID_PROPERTY_NAME to testId,
        )
    val config =
        SinkConfiguration(
            originals,
            Renderer.getDefaultRenderer(),
            apocCypherDoItAvailable = false,
            neo4j = neo4j5_26,
        )

    val topicHandlers = SinkStrategyHandler.createFrom(config, metricsMock)
    topicHandlers shouldHaveKey "foo"
    topicHandlers["foo"] shouldBe instanceOf<SinkHandler>()

    topicHandlers shouldHaveKey "bar"
    topicHandlers["bar"] shouldBe instanceOf<SinkHandler>()
  }

  @ParameterizedTest
  @MethodSource("cdcHandlersTypes")
  fun `should return multiple CDC schema topics`(
      apocDoItAvailable: Boolean,
      neo4jTarget: Neo4j?,
      clazz: KClass<SinkHandler>,
  ) {
    val originals =
        mapOf(
            Neo4jConfiguration.URI to "bolt://neo4j:7687",
            Neo4jConfiguration.AUTHENTICATION_TYPE to "NONE",
            SinkConnector.TOPICS_CONFIG to "bar,foo",
            SinkConfiguration.CDC_SCHEMA_TOPICS to "bar,foo",
        )
    val config =
        SinkConfiguration(
            originals,
            Renderer.getDefaultRenderer(),
            neo4j = neo4jTarget,
            apocCypherDoItAvailable = apocDoItAvailable,
        )

    val topicHandlers = SinkStrategyHandler.createFrom(config, metricsMock)
    topicHandlers shouldHaveKey "foo"
    topicHandlers["foo"] shouldBe instanceOf(clazz)

    topicHandlers shouldHaveKey "bar"
    topicHandlers["bar"] shouldBe instanceOf(clazz)
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

    val topicHandlers = SinkStrategyHandler.createFrom(config, metricsMock)
    topicHandlers shouldHaveKey "foo"
    topicHandlers["foo"] shouldBe instanceOf<SinkHandler>()
    val fooHandler = topicHandlers["foo"] as SinkHandler
    fooHandler.eventTransformer shouldBe instanceOf<CudEventTransformer>()

    topicHandlers shouldHaveKey "bar"
    topicHandlers["bar"] shouldBe instanceOf<SinkHandler>()
    val barHandler = topicHandlers["bar"] as SinkHandler
    barHandler.eventTransformer shouldBe instanceOf<CudEventTransformer>()
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
    val config =
        SinkConfiguration(
            originals,
            Renderer.getDefaultRenderer(),
            apocCypherDoItAvailable = false,
            neo4j = neo4j5_26,
        )

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
            SinkConfiguration.CYPHER_TOPIC_PREFIX + "bar" to "RETURN 1",
        )
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
            SinkConfiguration.PATTERN_TOPIC_PREFIX + "bar" to "Label{!id}",
        )
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
                "LabelA{!id} REL_TYPE{id} LabelB{!targetId}",
        )
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
                "LabelA{!id} REL_TYPE{id} LabelB{!targetId}",
        )
    val config =
        SinkConfiguration(
            originals,
            Renderer.getDefaultRenderer(),
            apocCypherDoItAvailable = false,
            neo4j = neo4j5_26,
        )

    config.userAgentComment() shouldBe "cdc-source-id; cud; relationship-pattern"
    config.txConfig() shouldBe
        TransactionConfig.builder().withMetadata(mapOf("app" to "kafka-sink")).build()
  }

  companion object {
    private val neo4j2026_01 =
        Neo4j(Neo4jVersion(2026, 1), Neo4jEdition.ENTERPRISE, Neo4jDeploymentType.SELF_MANAGED)
    private val neo4j5_26 =
        Neo4j(Neo4jVersion(5, 26), Neo4jEdition.ENTERPRISE, Neo4jDeploymentType.SELF_MANAGED)
    private val neo4j4_4 =
        Neo4j(Neo4jVersion(4, 4), Neo4jEdition.ENTERPRISE, Neo4jDeploymentType.SELF_MANAGED)

    @JvmStatic
    fun cdcHandlersTypes() =
        listOf(
            Arguments.argumentSet(
                "APOC DoIt not available 4.4",
                false,
                neo4j4_4,
                SinkHandler::class,
            ),
            Arguments.argumentSet(
                "APOC DoIt not available 5.26",
                false,
                neo4j5_26,
                SinkHandler::class,
            ),
            Arguments.argumentSet(
                "APOC DoIt not available 2026.01",
                false,
                neo4j2026_01,
                SinkHandler::class,
            ),
            Arguments.argumentSet("APOC DoIt available 4.4", true, neo4j4_4, SinkHandler::class),
            Arguments.argumentSet("APOC DoIt available 5.26", true, neo4j5_26, SinkHandler::class),
            Arguments.argumentSet(
                "APOC DoIt available 2026.01",
                true,
                neo4j2026_01,
                SinkHandler::class,
            ),
        )
  }
}
