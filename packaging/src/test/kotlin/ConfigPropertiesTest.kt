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
import io.kotest.assertions.throwables.shouldNotThrowAny
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import java.io.File
import java.io.FileInputStream
import java.util.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.mockito.Mockito.mock
import org.neo4j.caniuse.Neo4j
import org.neo4j.caniuse.Neo4jDeploymentType
import org.neo4j.caniuse.Neo4jEdition
import org.neo4j.caniuse.Neo4jVersion
import org.neo4j.cdc.client.selector.NodeSelector
import org.neo4j.cdc.client.selector.RelationshipSelector
import org.neo4j.connectors.kafka.metrics.Metrics
import org.neo4j.connectors.kafka.sink.SinkConfiguration
import org.neo4j.connectors.kafka.sink.SinkStrategy
import org.neo4j.connectors.kafka.sink.SinkStrategyHandler
import org.neo4j.connectors.kafka.sink.strategy.CypherHandler
import org.neo4j.connectors.kafka.sink.strategy.SinkHandler
import org.neo4j.connectors.kafka.source.SourceConfiguration
import org.neo4j.connectors.kafka.source.SourceType
import org.neo4j.cypherdsl.core.renderer.Renderer

class ConfigPropertiesTest {

  private val metricsMock: Metrics = mock()

  // This test checks that the number of config files is fixed.
  // If a new file is added, the test will fail, reminding the developer to update this test and add
  // unit tests for the new file.
  @Test
  fun `should be specific amount of config files`() {
    val configDirectory = File(BASE_CONFIG_PATH)
    configDirectory.listFiles()!!.size shouldBe 8
  }

  @ParameterizedTest
  @MethodSource("sinkHandlers")
  fun `sink cdc schema quick start config should be valid`(
      apocDoITAvailable: Boolean,
      neo4j: Neo4j?,
  ) {
    val properties = loadConfigProperties("sink-cdc-schema-quickstart.properties")

    properties["connector.class"] shouldBe "org.neo4j.connectors.kafka.sink.Neo4jConnector"

    val config = shouldNotThrowAny {
      SinkConfiguration(properties, Renderer.getDefaultRenderer(), neo4j, apocDoITAvailable)
    }

    val topicHandlers = SinkStrategyHandler.createFrom(config, metricsMock)
    topicHandlers.keys shouldBe setOf("creates", "updates", "deletes")
    sequenceOf("creates", "updates", "deletes").forEach { topic ->
      topicHandlers[topic].shouldBeInstanceOf<SinkHandler> {
        it.strategy() shouldBe SinkStrategy.CDC_SCHEMA
      }
    }
  }

  @ParameterizedTest
  @MethodSource("sinkHandlers")
  fun `sink cdc source id quick start config should be valid`(
      apocDoITAvailable: Boolean,
      neo4j: Neo4j?,
  ) {
    val properties = loadConfigProperties("sink-cdc-source-id-quickstart.properties")

    properties["connector.class"] shouldBe "org.neo4j.connectors.kafka.sink.Neo4jConnector"

    val config = shouldNotThrowAny {
      SinkConfiguration(properties, Renderer.getDefaultRenderer(), neo4j, apocDoITAvailable)
    }

    val topicHandlers = SinkStrategyHandler.createFrom(config, metricsMock)
    topicHandlers.keys shouldBe setOf("creates", "updates", "deletes")
    sequenceOf("creates", "updates", "deletes").forEach { topic ->
      topicHandlers[topic].shouldBeInstanceOf<SinkHandler> {
        it.strategy() shouldBe SinkStrategy.CDC_SOURCE_ID
      }
    }
  }

  @ParameterizedTest
  @MethodSource("sinkHandlers")
  fun `sink cud quick start config should be valid`(apocDoITAvailable: Boolean, neo4j: Neo4j?) {
    val properties = loadConfigProperties("sink-cud-quickstart.properties")

    properties["connector.class"] shouldBe "org.neo4j.connectors.kafka.sink.Neo4jConnector"

    val config = shouldNotThrowAny {
      SinkConfiguration(properties, Renderer.getDefaultRenderer(), neo4j, apocDoITAvailable)
    }

    val topicHandlers = SinkStrategyHandler.createFrom(config, metricsMock)
    topicHandlers.keys shouldBe setOf("people")
    topicHandlers["people"].shouldBeInstanceOf<SinkHandler>().should {
      it.strategy() shouldBe SinkStrategy.CUD
    }
  }

  @Test
  fun `sink cypher quick start config should be valid`() {
    val properties = loadConfigProperties("sink-cypher-quickstart.properties")

    properties["connector.class"] shouldBe "org.neo4j.connectors.kafka.sink.Neo4jConnector"

    val config = shouldNotThrowAny { SinkConfiguration(properties, Renderer.getDefaultRenderer()) }

    val topicHandlers = SinkStrategyHandler.createFrom(config, metricsMock)
    topicHandlers.keys shouldBe setOf("people")
    topicHandlers["people"].shouldBeInstanceOf<CypherHandler>()
  }

  @ParameterizedTest
  @MethodSource("sinkHandlers")
  fun `sink pattern node quick start config should be valid`(
      apocDoITAvailable: Boolean,
      neo4j: Neo4j?,
  ) {
    val properties = loadConfigProperties("sink-pattern-node-quickstart.properties")

    properties["connector.class"] shouldBe "org.neo4j.connectors.kafka.sink.Neo4jConnector"

    val config = shouldNotThrowAny {
      SinkConfiguration(properties, Renderer.getDefaultRenderer(), neo4j, apocDoITAvailable)
    }

    val topicHandlers = SinkStrategyHandler.createFrom(config, metricsMock)
    topicHandlers["people"].shouldBeInstanceOf<SinkHandler>().should {
      it.strategy() shouldBe SinkStrategy.NODE_PATTERN
    }
  }

  @ParameterizedTest
  @MethodSource("sinkHandlers")
  fun `sink pattern relationship quick start config should be valid`(
      apocDoITAvailable: Boolean,
      neo4j: Neo4j?,
  ) {
    val properties = loadConfigProperties("sink-pattern-relationship-quickstart.properties")

    properties["connector.class"] shouldBe "org.neo4j.connectors.kafka.sink.Neo4jConnector"

    val config = shouldNotThrowAny {
      SinkConfiguration(properties, Renderer.getDefaultRenderer(), neo4j, apocDoITAvailable)
    }

    val topicHandlers = SinkStrategyHandler.createFrom(config, metricsMock)
    topicHandlers["knows"].shouldBeInstanceOf<SinkHandler>().should {
      it.strategy() shouldBe SinkStrategy.RELATIONSHIP_PATTERN
    }
  }

  @Test
  fun `source cdc quick start config should be valid`() {
    val properties = loadConfigProperties("source-cdc-quickstart.properties")

    properties["connector.class"] shouldBe "org.neo4j.connectors.kafka.source.Neo4jConnector"

    val config = shouldNotThrowAny { SourceConfiguration(properties) }

    config.strategy shouldBe SourceType.CDC
    config.cdcSelectorsToTopics.forEach { (selector, topics) ->
      when (topics) {
        listOf("company"),
        listOf("person") -> {
          selector.shouldBeInstanceOf<NodeSelector>()
        }

        listOf("works_for") -> {
          selector.shouldBeInstanceOf<RelationshipSelector>()
        }
      }
    }
    config.query shouldBe ""
  }

  @Test
  fun `source query quick start config should be valid`() {
    val properties = loadConfigProperties("source-query-quickstart.properties")

    properties["connector.class"] shouldBe "org.neo4j.connectors.kafka.source.Neo4jConnector"

    val config = shouldNotThrowAny { SourceConfiguration(properties) }

    config.strategy shouldBe SourceType.QUERY
    config.topic shouldBe "my-topic"
    config.query shouldBe
        "MATCH (ts:TestSource) WHERE ts.timestamp > ${"$"}lastCheck RETURN ts.name AS name, ts.surname AS surname, ts.timestamp AS timestamp"
  }

  private fun loadConfigProperties(fileName: String): Map<String, Any> {
    val properties = Properties()
    FileInputStream("$BASE_CONFIG_PATH/$fileName").use { properties.load(it) }
    val originals = mutableMapOf<String, Any>()
    properties.map { (k, v) -> originals[k as String] = v }
    return originals
  }

  companion object {
    var BASE_CONFIG_PATH: String

    init {
      val properties = Properties()
      ConfigPropertiesTest::class.java.getResourceAsStream("/test.properties").use {
        properties.load(it)
      }
      BASE_CONFIG_PATH = properties.getProperty("quickstart.config.properties.path")
    }

    @JvmStatic
    fun sinkHandlers(): List<Arguments> {
      return listOf(
          Arguments.argumentSet("5.26 & APOC DoIT available", true, neo4j5_26, SinkHandler::class),
          Arguments.argumentSet("2026.01 & APOC DoIT available", true, neo4j2026_1),
          Arguments.argumentSet("4.4 & APOC DoIT available", true, neo4j4_4, SinkHandler::class),
          Arguments.argumentSet("5.26 & APOC DoIT not available", false, neo4j5_26),
          Arguments.argumentSet("2026.01 & APOC DoIT not available", false, neo4j2026_1),
          Arguments.argumentSet("4.4 & APOC DoIT not available", false, neo4j4_4),
      )
    }

    private val neo4j4_4 =
        Neo4j(Neo4jVersion(4, 4), Neo4jEdition.ENTERPRISE, Neo4jDeploymentType.SELF_MANAGED)
    private val neo4j5_26 =
        Neo4j(Neo4jVersion(5, 26), Neo4jEdition.ENTERPRISE, Neo4jDeploymentType.SELF_MANAGED)
    private val neo4j2026_1 =
        Neo4j(Neo4jVersion(2026, 1), Neo4jEdition.ENTERPRISE, Neo4jDeploymentType.SELF_MANAGED)
  }
}
