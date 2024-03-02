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

import io.kotest.matchers.shouldBe
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.sink.SinkConnector
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.neo4j.connectors.kafka.configuration.DeprecatedNeo4jConfiguration
import org.neo4j.connectors.kafka.configuration.Neo4jConfiguration
import org.neo4j.connectors.kafka.service.sink.strategy.SourceIdIngestionStrategyConfig
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.Session
import org.neo4j.driver.TransactionConfig
import org.testcontainers.containers.Neo4jContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

@Testcontainers
class SinkConfigurationTest {
  companion object {
    @Container
    val neo4j: Neo4jContainer<*> =
        Neo4jContainer("neo4j:5-enterprise")
            .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
            .withoutAuthentication()

    private lateinit var driver: Driver
    private lateinit var session: Session

    @BeforeAll
    @JvmStatic
    fun setUpContainer() {
      driver = GraphDatabase.driver(neo4j.boltUrl, AuthTokens.none())
      session = driver.session()
    }

    @AfterAll
    @JvmStatic
    fun tearDownContainer() {
      session.close()
      driver.close()
    }
  }

  @Test
  fun `should throw a ConfigException because of mismatch`() {
    val exception =
        assertFailsWith(ConfigException::class) {
          val originals =
              mapOf(
                  Neo4jConfiguration.URI to "bolt://neo4j:7687",
                  Neo4jConfiguration.AUTHENTICATION_TYPE to "NONE",
                  SinkConnector.TOPICS_CONFIG to "foo, bar",
                  "${SinkConfiguration.CYPHER_TOPIC_PREFIX}foo" to
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
                  "${SinkConfiguration.CYPHER_TOPIC_PREFIX}foo" to
                      "CREATE (p:Person{name: event.firstName})",
                  "${SinkConfiguration.CYPHER_TOPIC_PREFIX}bar" to
                      "CREATE (p:Person{name: event.firstName})",
                  SinkConfiguration.CDC_SOURCE_ID_TOPICS to "foo")
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
            "${SinkConfiguration.CYPHER_TOPIC_PREFIX}foo" to
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
        originals["${SinkConfiguration.CYPHER_TOPIC_PREFIX}foo"], config.topics.cypherTopics["foo"])
    assertEquals(10, config.batchSize)
  }

  @Test
  fun `should return the configuration with shuffled topic order`() {
    val originals =
        mapOf(
            Neo4jConfiguration.URI to "bolt://neo4j:7687",
            Neo4jConfiguration.AUTHENTICATION_TYPE to "NONE",
            SinkConnector.TOPICS_CONFIG to "bar,foo",
            "${SinkConfiguration.PATTERN_NODE_TOPIC_PREFIX}foo" to "(:Foo{!fooId,fooName})",
            "${SinkConfiguration.PATTERN_NODE_TOPIC_PREFIX}bar" to "(:Bar{!barId,barName})",
            SinkConfiguration.BATCH_SIZE to 10)
    val config = SinkConfiguration(originals)

    assertEquals(
        originals["${SinkConfiguration.CYPHER_TOPIC_PREFIX}foo"], config.topics.cypherTopics["foo"])
    assertEquals(
        originals["${SinkConfiguration.CYPHER_TOPIC_PREFIX}bar"], config.topics.cypherTopics["bar"])
    assertEquals(10, config.batchSize)
    assertEquals(SinkConfiguration.DEFAULT_BATCH_TIMEOUT, config.batchTimeout)
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
    val config = SinkConfiguration(originals)

    assertEquals(
        setOf("bar", "foo") to
            SourceIdIngestionStrategyConfig(labelName = testLabel, idName = testId),
        config.topics.cdcSourceIdTopics)
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
    val config = SinkConfiguration(originals)

    assertEquals(setOf("foo", "bar"), config.topics.cdcSchemaTopics)
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
    val config = SinkConfiguration(originals)

    assertEquals(setOf("foo", "bar"), config.topics.cudTopics)
  }

  @Suppress("DEPRECATION")
  @Test
  fun `migrateSettings should replace deprecated settings with up-to-date equivalent`() {
    val originals =
        mapOf(
            DeprecatedNeo4jConfiguration.SERVER_URI to "bolt://neo4j:7687",
            DeprecatedNeo4jConfiguration.CONNECTION_LIVENESS_CHECK_TIMEOUT_MSECS to "456",
            DeprecatedNeo4jConfiguration.CONNECTION_MAX_CONNECTION_LIFETIME_MSECS to "678",
            DeprecatedNeo4jConfiguration.CONNECTION_POOL_MAX_SIZE to 2,
            DeprecatedNeo4jConfiguration.RETRY_MAX_ATTEMPTS to 5,
            DeprecatedNeo4jConfiguration.RETRY_BACKOFF_MSECS to "890",
            DeprecatedNeo4jConfiguration.CONNECTION_MAX_CONNECTION_ACQUISITION_TIMEOUT_MSECS to
                "234",
            DeprecatedNeo4jConfiguration.ENCRYPTION_ENABLED to true,
            DeprecatedNeo4jConfiguration.ENCRYPTION_CA_CERTIFICATE_PATH to "/path/to/cert",
            DeprecatedNeo4jConfiguration.ENCRYPTION_TRUST_STRATEGY to
                "TRUST_CUSTOM_CA_SIGNED_CERTIFICATES",
            DeprecatedNeo4jConfiguration.AUTHENTICATION_TYPE to "NONE",
            DeprecatedNeo4jConfiguration.BATCH_SIZE to 20,
            DeprecatedNeo4jConfiguration.BATCH_TIMEOUT_MSECS to 468,
            DeprecatedNeo4jSinkConfiguration.BATCH_PARALLELIZE to false,
            DeprecatedNeo4jSinkConfiguration.TOPIC_PATTERN_MERGE_NODE_PROPERTIES_ENABLED to true,
            DeprecatedNeo4jSinkConfiguration.TOPIC_PATTERN_MERGE_RELATIONSHIP_PROPERTIES_ENABLED to
                false,
            DeprecatedNeo4jSinkConfiguration.TOPIC_CDC_SOURCE_ID to "foo;bar",
            DeprecatedNeo4jSinkConfiguration.TOPIC_CDC_SOURCE_ID_LABEL_NAME to "Custom",
            DeprecatedNeo4jSinkConfiguration.TOPIC_CDC_SOURCE_ID_ID_NAME to "c_id",
            DeprecatedNeo4jSinkConfiguration.TOPIC_CDC_SCHEMA to "foo; bar",
            DeprecatedNeo4jSinkConfiguration.TOPIC_CUD to "foo;bar",
            "${DeprecatedNeo4jSinkConfiguration.TOPIC_CYPHER_PREFIX}foo" to "MERGE (c: Source)",
            "${DeprecatedNeo4jSinkConfiguration.TOPIC_PATTERN_NODE_PREFIX}bar" to "Source(!id)",
            "${DeprecatedNeo4jSinkConfiguration.TOPIC_PATTERN_RELATIONSHIP_PREFIX}bar" to "TYPED",
            SinkConnector.TOPICS_CONFIG to "bar,foo")
    val actual = SinkConfiguration.migrateSettings(originals)

    val expected =
        mapOf(
            Neo4jConfiguration.URI to "bolt://neo4j:7687",
            Neo4jConfiguration.POOL_IDLE_TIME_BEFORE_TEST to "456ms",
            Neo4jConfiguration.POOL_MAX_CONNECTION_LIFETIME to "678ms",
            Neo4jConfiguration.POOL_MAX_CONNECTION_POOL_SIZE to "2",
            Neo4jConfiguration.MAX_TRANSACTION_RETRY_ATTEMPTS to "5",
            Neo4jConfiguration.MAX_TRANSACTION_RETRY_TIMEOUT to "890ms",
            Neo4jConfiguration.POOL_CONNECTION_ACQUISITION_TIMEOUT to "234ms",
            Neo4jConfiguration.SECURITY_ENCRYPTED to "true",
            Neo4jConfiguration.SECURITY_CERT_FILES to "/path/to/cert",
            Neo4jConfiguration.SECURITY_TRUST_STRATEGY to "TRUST_CUSTOM_CA_SIGNED_CERTIFICATES",
            Neo4jConfiguration.AUTHENTICATION_TYPE to "NONE",
            SinkConfiguration.BATCH_SIZE to "20",
            SinkConfiguration.BATCH_TIMEOUT to "468ms",
            SinkConfiguration.BATCH_PARALLELIZE to "false",
            SinkConfiguration.PATTERN_NODE_MERGE_PROPERTIES to "true",
            SinkConfiguration.PATTERN_RELATIONSHIP_MERGE_PROPERTIES to "false",
            SinkConfiguration.CDC_SOURCE_ID_TOPICS to "foo,bar",
            SinkConfiguration.CDC_SOURCE_ID_LABEL_NAME to "Custom",
            SinkConfiguration.CDC_SOURCE_ID_PROPERTY_NAME to "c_id",
            SinkConfiguration.CDC_SCHEMA_TOPICS to "foo, bar",
            SinkConfiguration.CUD_TOPICS to "foo,bar",
            "${SinkConfiguration.CYPHER_TOPIC_PREFIX}foo" to "MERGE (c: Source)",
            "${SinkConfiguration.PATTERN_NODE_TOPIC_PREFIX}bar" to "Source(!id)",
            "${SinkConfiguration.PATTERN_RELATIONSHIP_TOPIC_PREFIX}bar" to "TYPED",
            SinkConnector.TOPICS_CONFIG to "bar,foo")

    assertEquals(expected, actual)
  }

  @ParameterizedTest
  @EnumSource(SinkStrategy::class, names = ["CDC_SOURCE_ID", "CDC_SCHEMA", "CUD"])
  fun `should return correct telemetry data for cdc and cud strategies`(strategy: SinkStrategy) {
    val originals =
        mapOf(
            Neo4jConfiguration.URI to neo4j.boltUrl,
            Neo4jConfiguration.AUTHENTICATION_TYPE to "NONE",
            SinkConnector.TOPICS_CONFIG to "bar",
            when (strategy) {
              SinkStrategy.CDC_SCHEMA -> SinkConfiguration.CDC_SCHEMA_TOPICS
              SinkStrategy.CDC_SOURCE_ID -> SinkConfiguration.CDC_SOURCE_ID_TOPICS
              SinkStrategy.CUD -> SinkConfiguration.CUD_TOPICS
              else -> throw IllegalArgumentException(strategy.name)
            } to "bar",
        )
    val config = SinkConfiguration(originals)

    config.txConfig() shouldBe
        TransactionConfig.builder()
            .withMetadata(
                mapOf(
                    "app" to "kafka-sink",
                    "metadata" to mapOf("strategies" to strategy.description)))
            .build()
  }

  @Test
  fun `should return correct telemetry data for cypher strategy`() {
    val originals =
        mapOf(
            Neo4jConfiguration.URI to neo4j.boltUrl,
            Neo4jConfiguration.AUTHENTICATION_TYPE to "NONE",
            SinkConnector.TOPICS_CONFIG to "bar",
            SinkConfiguration.CYPHER_TOPIC_PREFIX + "bar" to "RETURN 1")
    val config = SinkConfiguration(originals)

    config.txConfig() shouldBe
        TransactionConfig.builder()
            .withMetadata(
                mapOf("app" to "kafka-sink", "metadata" to mapOf("strategies" to "cypher")))
            .build()
  }

  @Test
  fun `should return correct telemetry data for node pattern strategy`() {
    val originals =
        mapOf(
            Neo4jConfiguration.URI to neo4j.boltUrl,
            Neo4jConfiguration.AUTHENTICATION_TYPE to "NONE",
            SinkConnector.TOPICS_CONFIG to "bar",
            SinkConfiguration.PATTERN_NODE_TOPIC_PREFIX + "bar" to "Label{!id}")
    val config = SinkConfiguration(originals)

    config.txConfig() shouldBe
        TransactionConfig.builder()
            .withMetadata(
                mapOf("app" to "kafka-sink", "metadata" to mapOf("strategies" to "node-pattern")))
            .build()
  }

  @Test
  fun `should return correct telemetry data for relationship pattern strategy`() {
    val originals =
        mapOf(
            Neo4jConfiguration.URI to neo4j.boltUrl,
            Neo4jConfiguration.AUTHENTICATION_TYPE to "NONE",
            SinkConnector.TOPICS_CONFIG to "bar",
            SinkConfiguration.PATTERN_RELATIONSHIP_TOPIC_PREFIX + "bar" to
                "LabelA{!id} REL_TYPE{id} LabelB{!targetId}")
    val config = SinkConfiguration(originals)

    config.txConfig() shouldBe
        TransactionConfig.builder()
            .withMetadata(
                mapOf(
                    "app" to "kafka-sink",
                    "metadata" to mapOf("strategies" to "relationship-pattern")))
            .build()
  }

  @Test
  fun `should return correct telemetry data for multiple strategies`() {
    val originals =
        mapOf(
            Neo4jConfiguration.URI to neo4j.boltUrl,
            Neo4jConfiguration.AUTHENTICATION_TYPE to "NONE",
            SinkConnector.TOPICS_CONFIG to "foo,bar,baz",
            SinkConfiguration.CUD_TOPICS to "baz",
            SinkConfiguration.CDC_SOURCE_ID_TOPICS to "foo",
            SinkConfiguration.PATTERN_RELATIONSHIP_TOPIC_PREFIX + "bar" to
                "LabelA{!id} REL_TYPE{id} LabelB{!targetId}")
    val config = SinkConfiguration(originals)

    config.txConfig() shouldBe
        TransactionConfig.builder()
            .withMetadata(
                mapOf(
                    "app" to "kafka-sink",
                    "metadata" to mapOf("strategies" to "cdc-source-id,cud,relationship-pattern")))
            .build()
  }
}
