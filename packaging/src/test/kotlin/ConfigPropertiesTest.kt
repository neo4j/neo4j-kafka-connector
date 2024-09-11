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
import io.kotest.matchers.shouldNotBe
import io.kotest.matchers.types.shouldBeInstanceOf
import java.io.File
import java.io.FileInputStream
import java.util.*
import org.junit.jupiter.api.Test
import org.neo4j.connectors.kafka.sink.SinkConfiguration
import org.neo4j.connectors.kafka.sink.strategy.pattern.NodePattern
import org.neo4j.connectors.kafka.sink.strategy.pattern.Pattern
import org.neo4j.connectors.kafka.sink.strategy.pattern.RelationshipPattern
import org.neo4j.connectors.kafka.source.SourceConfiguration
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

  @Test
  fun `sink cdc schema quick start config should be valid`() {
    val properties = loadConfigProperties("sink-cdc-schema-quickstart.properties")

    properties["connector.class"] shouldBe "org.neo4j.connectors.kafka.sink.Neo4jConnector"
    properties["neo4j.cdc.schema.topics"] shouldNotBe null

    shouldNotThrowAny { SinkConfiguration(properties, Renderer.getDefaultRenderer()) }
  }

  @Test
  fun `sink cdc source id quick start config should be valid`() {
    val properties = loadConfigProperties("sink-cdc-source-id-quickstart.properties")

    properties["connector.class"] shouldBe "org.neo4j.connectors.kafka.sink.Neo4jConnector"
    properties["neo4j.cdc.source-id.topics"] shouldNotBe null

    shouldNotThrowAny { SinkConfiguration(properties, Renderer.getDefaultRenderer()) }
  }

  @Test
  fun `sink cud quick start config should be valid`() {
    val properties = loadConfigProperties("sink-cud-quickstart.properties")

    properties["connector.class"] shouldBe "org.neo4j.connectors.kafka.sink.Neo4jConnector"
    properties["neo4j.cud.topics"] shouldNotBe null

    shouldNotThrowAny { SinkConfiguration(properties, Renderer.getDefaultRenderer()) }
  }

  @Test
  fun `sink cypher quick start config should be valid`() {
    val properties = loadConfigProperties("sink-cypher-quickstart.properties")

    properties["connector.class"] shouldBe "org.neo4j.connectors.kafka.sink.Neo4jConnector"
    properties.keys.any { it.startsWith("neo4j.cypher.topic") } shouldBe true

    shouldNotThrowAny { SinkConfiguration(properties, Renderer.getDefaultRenderer()) }
  }

  @Test
  fun `sink pattern node quick start config should be valid`() {
    val properties = loadConfigProperties("sink-pattern-node-quickstart.properties")

    properties["connector.class"] shouldBe "org.neo4j.connectors.kafka.sink.Neo4jConnector"

    val patternKey = properties.keys.first { it.startsWith("neo4j.pattern.topic") }
    patternKey shouldNotBe null

    val parsedPattern = Pattern.parse(properties[patternKey] as String)
    parsedPattern.shouldBeInstanceOf<NodePattern>()

    shouldNotThrowAny { SinkConfiguration(properties, Renderer.getDefaultRenderer()) }
  }

  @Test
  fun `sink pattern relationship quick start config should be valid`() {
    val properties = loadConfigProperties("sink-pattern-relationship-quickstart.properties")

    properties["connector.class"] shouldBe "org.neo4j.connectors.kafka.sink.Neo4jConnector"

    val patternKey = properties.keys.first { it.startsWith("neo4j.pattern.topic") }
    patternKey shouldNotBe null

    val parsedPattern = Pattern.parse(properties[patternKey] as String)
    parsedPattern.shouldBeInstanceOf<RelationshipPattern>()

    shouldNotThrowAny { SinkConfiguration(properties, Renderer.getDefaultRenderer()) }
  }

  @Test
  fun `source cdc quick start config should be valid`() {
    val properties = loadConfigProperties("source-cdc-quickstart.properties")

    properties["connector.class"] shouldBe "org.neo4j.connectors.kafka.source.Neo4jConnector"
    properties["neo4j.source-strategy"] shouldBe "CDC"
    properties.keys.any { it.startsWith("neo4j.cdc.topic") } shouldBe true

    shouldNotThrowAny { SourceConfiguration(properties) }
  }

  @Test
  fun `source query quick start config should be valid`() {
    val properties = loadConfigProperties("source-query-quickstart.properties")

    properties["connector.class"] shouldBe "org.neo4j.connectors.kafka.source.Neo4jConnector"
    properties["neo4j.source-strategy"] shouldBe "QUERY"
    properties["neo4j.query.topic"] shouldNotBe null
    properties["neo4j.query"] shouldNotBe null

    shouldNotThrowAny { SourceConfiguration(properties) }
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
  }
}
