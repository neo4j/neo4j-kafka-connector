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

import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.nulls.shouldNotBeNull
import java.time.Duration
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import org.neo4j.cdc.client.model.ChangeEvent
import org.neo4j.cdc.client.model.EntityOperation
import org.neo4j.cdc.client.model.EventType
import org.neo4j.connectors.kafka.testing.assertions.ChangeEventAssert
import org.neo4j.connectors.kafka.testing.assertions.TopicVerifier
import org.neo4j.connectors.kafka.testing.format.KafkaConverter.AVRO
import org.neo4j.connectors.kafka.testing.format.KafkaConverter.JSON_SCHEMA
import org.neo4j.connectors.kafka.testing.format.KafkaConverter.PROTOBUF
import org.neo4j.connectors.kafka.testing.format.KeyValueConverter
import org.neo4j.connectors.kafka.testing.kafka.ConvertingKafkaConsumer
import org.neo4j.connectors.kafka.testing.source.CdcSource
import org.neo4j.connectors.kafka.testing.source.CdcSourceParam
import org.neo4j.connectors.kafka.testing.source.CdcSourceTopic
import org.neo4j.connectors.kafka.testing.source.Neo4jSource
import org.neo4j.connectors.kafka.testing.source.SourceStrategy
import org.neo4j.connectors.kafka.testing.source.TopicConsumer
import org.neo4j.driver.Session

abstract class Neo4jCdcSourceKeyStrategyIT {

  @Neo4jSource(
      startFrom = "EARLIEST",
      strategy = SourceStrategy.CDC,
      cdc =
          CdcSource(
              topics =
                  arrayOf(
                      CdcSourceTopic(
                          topic = "neo4j-cdc-topic-key-serialization-none",
                          patterns = arrayOf(CdcSourceParam("(:TestSource{name,+execId})")),
                          keySerialization = "SKIP"),
                  ),
          ),
  )
  @Test
  fun `supports skipping serialization of keys`(
      testInfo: TestInfo,
      @TopicConsumer(topic = "neo4j-cdc-topic-key-serialization-none", offset = "earliest")
      consumer: ConvertingKafkaConsumer,
      session: Session
  ) {
    val executionId = testInfo.displayName + System.currentTimeMillis()
    session
        .run(
            "CREATE (:TestSource {name: 'Jane', execId: \$execId})",
            mapOf("execId" to executionId),
        )
        .consume()

    TopicVerifier.createForMap(consumer)
        .assertMessage { it.raw.key().shouldBeNull() }
        .verifyWithin(Duration.ofSeconds(30))
  }

  @Neo4jSource(
      startFrom = "EARLIEST",
      strategy = SourceStrategy.CDC,
      cdc =
          CdcSource(
              topics =
                  arrayOf(
                      CdcSourceTopic(
                          topic = "neo4j-cdc-topic-key-serialization-whole",
                          patterns = arrayOf(CdcSourceParam("(:TestSource{name,+execId})")),
                          keySerialization = "WHOLE_VALUE"),
                  ),
          ),
  )
  @Test
  fun `supports serialization of keys as whole values`(
      testInfo: TestInfo,
      @TopicConsumer(topic = "neo4j-cdc-topic-key-serialization-whole", offset = "earliest")
      consumer: ConvertingKafkaConsumer,
      session: Session
  ) {
    val executionId = testInfo.displayName + System.currentTimeMillis()
    session
        .run(
            "CREATE (:TestSource {name: 'Jane', execId: \$execId})",
            mapOf("execId" to executionId),
        )
        .consume()

    TopicVerifier.create<ChangeEvent, ChangeEvent>(consumer)
        .assertMessageKey { key ->
          ChangeEventAssert.assertThat(key)
              .isNotNull()
              .hasEventType(EventType.NODE)
              .hasOperation(EntityOperation.CREATE)
              .labelledAs("TestSource")
              .hasNoBeforeState()
              .hasAfterStateProperties(mapOf("name" to "Jane", "execId" to executionId))
        }
        .verifyWithin(Duration.ofSeconds(30))
  }

  @Neo4jSource(
      startFrom = "EARLIEST",
      strategy = SourceStrategy.CDC,
      cdc =
          CdcSource(
              topics =
                  arrayOf(
                      CdcSourceTopic(
                          topic = "neo4j-cdc-topic-key-serialization-element-ids",
                          patterns = arrayOf(CdcSourceParam("(:TestSource{name,+execId})")),
                          keySerialization = "ELEMENT_ID"),
                  ),
          ),
  )
  @Test
  fun `supports serialization of keys as element IDs`(
      testInfo: TestInfo,
      @TopicConsumer(topic = "neo4j-cdc-topic-key-serialization-element-ids", offset = "earliest")
      consumer: ConvertingKafkaConsumer,
      session: Session
  ) {
    val executionId = testInfo.displayName + System.currentTimeMillis()
    session
        .run(
            "CREATE (:TestSource {name: 'Jane', execId: \$execId})",
            mapOf("execId" to executionId),
        )
        .consume()

    TopicVerifier.create<String, Map<String, Any>>(consumer)
        .assertMessageKey { it.shouldNotBeNull() }
        .verifyWithin(Duration.ofSeconds(30))
  }

  @Neo4jSource(
      startFrom = "EARLIEST",
      strategy = SourceStrategy.CDC,
      cdc =
          CdcSource(
              topics =
                  arrayOf(
                      CdcSourceTopic(
                          topic = "neo4j-cdc-topic-key-serialization-missing-node-keys",
                          patterns = arrayOf(CdcSourceParam("(:TestSource{name,+execId})")),
                          keySerialization = "ENTITY_KEYS"),
                  ),
          ),
  )
  @Test
  fun `supports serialization of keys as (missing) node keys`(
      testInfo: TestInfo,
      @TopicConsumer(
          topic = "neo4j-cdc-topic-key-serialization-missing-node-keys", offset = "earliest")
      consumer: ConvertingKafkaConsumer,
      session: Session
  ) {
    val executionId = testInfo.displayName + System.currentTimeMillis()
    session
        .run(
            "CREATE (:TestSource {name: 'Jane', execId: \$execId})",
            mapOf("execId" to executionId),
        )
        .consume()

    TopicVerifier.create<Map<String, Any>, Map<String, Any>>(consumer)
        .assertMessage { it.raw.key().shouldBeNull() }
        .verifyWithin(Duration.ofSeconds(30))
  }

  @Neo4jSource(
      startFrom = "EARLIEST",
      strategy = SourceStrategy.CDC,
      cdc =
          CdcSource(
              topics =
                  arrayOf(
                      CdcSourceTopic(
                          topic = "neo4j-cdc-topic-key-serialization-node-keys",
                          patterns = arrayOf(CdcSourceParam("(:TestSource{name,+execId})")),
                          keySerialization = "ENTITY_KEYS"),
                  ),
          ),
  )
  @Test
  fun `supports serialization of keys as node keys`(
      testInfo: TestInfo,
      @TopicConsumer(topic = "neo4j-cdc-topic-key-serialization-node-keys", offset = "earliest")
      consumer: ConvertingKafkaConsumer,
      session: Session
  ) {
    val executionId = testInfo.displayName + System.currentTimeMillis()
    session
        .run(
            "CREATE CONSTRAINT FOR (ts:TestSource) REQUIRE ts.name IS NODE KEY",
            mapOf("execId" to executionId),
        )
        .consume()
    session
        .run(
            "CREATE (:TestSource {name: 'Jane', execId: \$execId})",
            mapOf("execId" to executionId),
        )
        .consume()

    TopicVerifier.createForMap(consumer)
        .assertMessageKey {
          assertThat(it)
              .isNotNull
              .isEqualTo(mapOf("keys" to mapOf("TestSource" to listOf(mapOf("name" to "Jane")))))
        }
        .verifyWithin(Duration.ofSeconds(30))
  }

  @Neo4jSource(
      startFrom = "EARLIEST",
      strategy = SourceStrategy.CDC,
      cdc =
          CdcSource(
              topics =
                  arrayOf(
                      CdcSourceTopic(
                          topic = "neo4j-cdc-topic-key-serialization-missing-rel-keys",
                          patterns = arrayOf(CdcSourceParam("()-[:TO {name,+execId}]-()")),
                          keySerialization = "ENTITY_KEYS"),
                  ),
          ),
  )
  @Test
  fun `supports serialization of keys as (missing) rel keys`(
      testInfo: TestInfo,
      @TopicConsumer(
          topic = "neo4j-cdc-topic-key-serialization-missing-rel-keys", offset = "earliest")
      consumer: ConvertingKafkaConsumer,
      session: Session
  ) {
    val executionId = testInfo.displayName + System.currentTimeMillis()
    session
        .run(
            "CREATE (:Source)-[:TO {name: 'somewhere', execId: \$execId}]->(:Destination)",
            mapOf("execId" to executionId),
        )
        .consume()

    TopicVerifier.create<String, Map<String, Any>>(consumer)
        .assertMessage { it.raw.key().shouldBeNull() }
        .verifyWithin(Duration.ofSeconds(30))
  }

  @Neo4jSource(
      startFrom = "EARLIEST",
      strategy = SourceStrategy.CDC,
      cdc =
          CdcSource(
              topics =
                  arrayOf(
                      CdcSourceTopic(
                          topic = "neo4j-cdc-topic-key-serialization-rel-keys",
                          patterns = arrayOf(CdcSourceParam("()-[:TO {name,+execId}]-()")),
                          keySerialization = "ENTITY_KEYS"),
                  ),
          ),
  )
  @Test
  fun `supports serialization of keys as rel keys`(
      testInfo: TestInfo,
      @TopicConsumer(topic = "neo4j-cdc-topic-key-serialization-rel-keys", offset = "earliest")
      consumer: ConvertingKafkaConsumer,
      session: Session
  ) {
    val executionId = testInfo.displayName + System.currentTimeMillis()
    session
        .run(
            "CREATE CONSTRAINT FOR ()-[to:TO]-() REQUIRE to.name IS RELATIONSHIP KEY",
            mapOf("execId" to executionId),
        )
        .consume()
    session
        .run(
            "CREATE (:Source)-[:TO {name: 'somewhere', execId: \$execId}]->(:Destination)",
            mapOf("execId" to executionId),
        )
        .consume()

    TopicVerifier.createForMap(consumer)
        .assertMessageKey { key ->
          assertThat(key)
              .isNotNull()
              .isEqualTo(mapOf("keys" to listOf(mapOf("name" to "somewhere"))))
        }
        .verifyWithin(Duration.ofSeconds(30))
  }
}

@KeyValueConverter(key = AVRO, value = AVRO)
class Neo4jCdcSourceKeyStrategyAvroIT : Neo4jCdcSourceKeyStrategyIT()

@KeyValueConverter(key = JSON_SCHEMA, value = JSON_SCHEMA)
class Neo4jCdcSourceKeyStrategyJsonIT : Neo4jCdcSourceKeyStrategyIT()

@KeyValueConverter(key = PROTOBUF, value = PROTOBUF)
class Neo4jCdcSourceKeyStrategyProtobufIT : Neo4jCdcSourceKeyStrategyIT()
