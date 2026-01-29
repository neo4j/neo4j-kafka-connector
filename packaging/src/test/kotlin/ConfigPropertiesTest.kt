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
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.instanceOf
import io.kotest.matchers.types.shouldBeInstanceOf
import java.io.File
import java.io.FileInputStream
import java.util.*
import kotlin.reflect.KClass
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.neo4j.caniuse.Neo4j
import org.neo4j.caniuse.Neo4jDeploymentType
import org.neo4j.caniuse.Neo4jEdition
import org.neo4j.caniuse.Neo4jVersion
import org.neo4j.cdc.client.selector.NodeSelector
import org.neo4j.cdc.client.selector.RelationshipSelector
import org.neo4j.connectors.kafka.sink.SinkConfiguration
import org.neo4j.connectors.kafka.sink.SinkStrategyHandler
import org.neo4j.connectors.kafka.sink.strategy.CdcSchemaHandler
import org.neo4j.connectors.kafka.sink.strategy.CdcSourceIdHandler
import org.neo4j.connectors.kafka.sink.strategy.CudHandler
import org.neo4j.connectors.kafka.sink.strategy.CypherHandler
import org.neo4j.connectors.kafka.sink.strategy.NodePatternHandler
import org.neo4j.connectors.kafka.sink.strategy.RelationshipPatternHandler
import org.neo4j.connectors.kafka.sink.strategy.cdc.Cypher25CdcSchemaHandler
import org.neo4j.connectors.kafka.sink.strategy.cdc.Cypher25CdcSourceIdHandler
import org.neo4j.connectors.kafka.source.SourceConfiguration
import org.neo4j.connectors.kafka.source.SourceType
import org.neo4j.cypherdsl.core.renderer.Renderer

class ConfigPropertiesTest {

  // This test checks that the number of config files is fixed.
  // If a new file is added, the test will fail, reminding the developer to update this test and add
  // unit tests for the new file.
  @Test
  fun `should be specific amount of config files`() {
    val configDirectory = File(BASE_CONFIG_PATH)
    configDirectory.listFiles()!!.size shouldBe 8
  }

  @ParameterizedTest
  @MethodSource("cdcSchemaHandlers")
  fun `sink cdc schema quick start config should be valid`(
      neo4j: Neo4j,
      expectedHandlerType: KClass<SinkStrategyHandler>,
  ) {
    val properties = loadConfigProperties("sink-cdc-schema-quickstart.properties")

    properties["connector.class"] shouldBe "org.neo4j.connectors.kafka.sink.Neo4jConnector"

    val config = shouldNotThrowAny {
      SinkConfiguration(properties, Renderer.getDefaultRenderer(), neo4j)
    }

    config.topicHandlers.keys shouldBe setOf("creates", "updates", "deletes")
    config.topicHandlers["creates"] shouldBe instanceOf(expectedHandlerType)
    config.topicHandlers["updates"] shouldBe instanceOf(expectedHandlerType)
    config.topicHandlers["deletes"] shouldBe instanceOf(expectedHandlerType)
  }

  @ParameterizedTest
  @MethodSource("cdcSourceHandlers")
  fun `sink cdc source id quick start config should be valid`(
      neo4j: Neo4j,
      expectedHandlerType: KClass<SinkStrategyHandler>,
  ) {
    val properties = loadConfigProperties("sink-cdc-source-id-quickstart.properties")

    properties["connector.class"] shouldBe "org.neo4j.connectors.kafka.sink.Neo4jConnector"

    val config = shouldNotThrowAny {
      SinkConfiguration(properties, Renderer.getDefaultRenderer(), neo4j)
    }

    config.topicHandlers.keys shouldBe setOf("creates", "updates", "deletes")
    config.topicHandlers["creates"] shouldBe instanceOf(expectedHandlerType)
    config.topicHandlers["updates"] shouldBe instanceOf(expectedHandlerType)
    config.topicHandlers["deletes"] shouldBe instanceOf(expectedHandlerType)
  }

  @Test
  fun `sink cud quick start config should be valid`() {
    val properties = loadConfigProperties("sink-cud-quickstart.properties")

    properties["connector.class"] shouldBe "org.neo4j.connectors.kafka.sink.Neo4jConnector"

    val config = shouldNotThrowAny { SinkConfiguration(properties, Renderer.getDefaultRenderer()) }

    config.topicHandlers.keys shouldBe setOf("people")
    config.topicHandlers["people"].shouldBeInstanceOf<CudHandler>()
  }

  @Test
  fun `sink cypher quick start config should be valid`() {
    val properties = loadConfigProperties("sink-cypher-quickstart.properties")

    properties["connector.class"] shouldBe "org.neo4j.connectors.kafka.sink.Neo4jConnector"

    val config = shouldNotThrowAny { SinkConfiguration(properties, Renderer.getDefaultRenderer()) }

    config.topicHandlers.keys shouldBe setOf("people")
    config.topicHandlers["people"].shouldBeInstanceOf<CypherHandler>()
  }

  @Test
  fun `sink pattern node quick start config should be valid`() {
    val properties = loadConfigProperties("sink-pattern-node-quickstart.properties")

    properties["connector.class"] shouldBe "org.neo4j.connectors.kafka.sink.Neo4jConnector"

    val config = shouldNotThrowAny { SinkConfiguration(properties, Renderer.getDefaultRenderer()) }

    config.topicHandlers.keys shouldBe setOf("people")
    config.topicHandlers["people"].shouldBeInstanceOf<NodePatternHandler>()
  }

  @Test
  fun `sink pattern relationship quick start config should be valid`() {
    val properties = loadConfigProperties("sink-pattern-relationship-quickstart.properties")

    properties["connector.class"] shouldBe "org.neo4j.connectors.kafka.sink.Neo4jConnector"

    val config = shouldNotThrowAny { SinkConfiguration(properties, Renderer.getDefaultRenderer()) }

    config.topicHandlers.keys shouldBe setOf("knows")
    config.topicHandlers["knows"].shouldBeInstanceOf<RelationshipPatternHandler>()
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
    fun cdcSourceHandlers(): List<Arguments> {
      return listOf(
          Arguments.argumentSet("Cypher 5", neo4j5, CdcSourceIdHandler::class),
          Arguments.argumentSet("Cypher 25", neo4j2025, Cypher25CdcSourceIdHandler::class),
      )
    }

    @JvmStatic
    fun cdcSchemaHandlers(): List<Arguments> {
      return listOf(
          Arguments.argumentSet("Cypher 5", neo4j5, CdcSchemaHandler::class),
          Arguments.argumentSet("Cypher 25", neo4j2025, Cypher25CdcSchemaHandler::class),
      )
    }

    private val neo4j5 =
        Neo4j(Neo4jVersion(5, 26), Neo4jEdition.ENTERPRISE, Neo4jDeploymentType.SELF_MANAGED)

    private val neo4j2025 =
        Neo4j(Neo4jVersion(2025, 12), Neo4jEdition.ENTERPRISE, Neo4jDeploymentType.SELF_MANAGED)
  }
}
