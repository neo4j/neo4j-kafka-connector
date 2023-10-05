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
package org.neo4j.connectors.kafka.sink

import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.sink.SinkConnector
import org.junit.jupiter.api.Test
import org.neo4j.connectors.kafka.configuration.Neo4jConfiguration
import streams.kafka.connect.sink.DeprecatedNeo4jSinkConfiguration

class Neo4jSinkConnectorConfigTest {

  @Test
  fun `should throw a ConfigException because of mismatch`() {
    val exception =
        assertFailsWith(ConfigException::class) {
          val originals =
              mapOf(
                  Neo4jConfiguration.URI to "bolt://neo4j:7687",
                  Neo4jConfiguration.AUTHENTICATION_TYPE to "NONE",
                  SinkConnector.TOPICS_CONFIG to "foo, bar",
                  "${SinkConfiguration.TOPIC_CYPHER_PREFIX}foo" to
                      "CREATE (p:Person{name: event.firstName})")
          SinkConfiguration(originals)
        }

    assertEquals(
        "There is a mismatch between topics defined into the property `topics` ([bar, foo]) and configured topics ([foo])",
        exception.message)
  }

  @Test
  fun `should throw a ConfigException because of cross defined topics`() {
    val exception =
        assertFailsWith(ConfigException::class) {
          val originals =
              mapOf(
                  Neo4jConfiguration.URI to "bolt://neo4j:7687",
                  Neo4jConfiguration.AUTHENTICATION_TYPE to "NONE",
                  SinkConnector.TOPICS_CONFIG to "foo, bar",
                  "${SinkConfiguration.TOPIC_CYPHER_PREFIX}foo" to
                      "CREATE (p:Person{name: event.firstName})",
                  "${SinkConfiguration.TOPIC_CYPHER_PREFIX}bar" to
                      "CREATE (p:Person{name: event.firstName})",
                  SinkConfiguration.TOPIC_CDC_SOURCE_ID to "foo")
          SinkConfiguration(originals)
        }

    assertEquals("The following topics are cross defined: [foo]", exception.message)
  }

  @Test
  fun `should return the configuration`() {
    val originals =
        mapOf(
            Neo4jConfiguration.URI to "bolt://neo4j:7687",
            Neo4jConfiguration.AUTHENTICATION_TYPE to "NONE",
            SinkConnector.TOPICS_CONFIG to "foo",
            "${DeprecatedNeo4jSinkConfiguration.TOPIC_CYPHER_PREFIX}foo" to
                "CREATE (p:Person{name: event.firstName})",
            SinkConfiguration.BATCH_SIZE to 10,
            "kafka.${CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG}" to "broker:9093",
            "kafka.${ProducerConfig.ACKS_CONFIG}" to 1,
            Neo4jConfiguration.DATABASE to "customers")
    val config = SinkConfiguration(originals)

    assertEquals(
        mapOf(
            CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG to "broker:9093",
            ProducerConfig.ACKS_CONFIG to 1),
        config.kafkaBrokerProperties)
    assertEquals(
        originals["${SinkConfiguration.TOPIC_CYPHER_PREFIX}foo"], config.topics.cypherTopics["foo"])
    assertEquals(10, config.batchSize)
  }

  @Test
  fun `should return the configuration with shuffled topic order`() {
    val originals =
        mapOf(
            Neo4jConfiguration.URI to "bolt://neo4j:7687",
            Neo4jConfiguration.AUTHENTICATION_TYPE to "NONE",
            SinkConnector.TOPICS_CONFIG to "bar,foo",
            "${SinkConfiguration.TOPIC_PATTERN_NODE_PREFIX}foo" to "(:Foo{!fooId,fooName})",
            "${SinkConfiguration.TOPIC_PATTERN_NODE_PREFIX}bar" to "(:Bar{!barId,barName})",
            SinkConfiguration.BATCH_SIZE to 10)
    val config = SinkConfiguration(originals)

    assertEquals(
        originals["${SinkConfiguration.TOPIC_CYPHER_PREFIX}foo"], config.topics.cypherTopics["foo"])
    assertEquals(
        originals["${SinkConfiguration.TOPIC_CYPHER_PREFIX}bar"], config.topics.cypherTopics["bar"])
    assertEquals(10, config.batchSize)
    assertEquals(SinkConfiguration.DEFAULT_BATCH_TIMEOUT, config.batchTimeout)
  }
}
