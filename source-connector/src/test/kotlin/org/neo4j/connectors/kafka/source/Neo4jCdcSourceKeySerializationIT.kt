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

import io.confluent.kafka.serializers.KafkaAvroDeserializer
import java.time.Duration
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import org.neo4j.connectors.kafka.testing.assertions.AvroCdcRecordAssert
import org.neo4j.connectors.kafka.testing.assertions.EventType
import org.neo4j.connectors.kafka.testing.assertions.Operation
import org.neo4j.connectors.kafka.testing.assertions.TopicVerifier
import org.neo4j.connectors.kafka.testing.source.CdcSource
import org.neo4j.connectors.kafka.testing.source.CdcSourceParam
import org.neo4j.connectors.kafka.testing.source.CdcSourceTopic
import org.neo4j.connectors.kafka.testing.source.Neo4jSource
import org.neo4j.connectors.kafka.testing.source.SourceStrategy
import org.neo4j.connectors.kafka.testing.source.TopicConsumer
import org.neo4j.driver.Session

class Neo4jCdcSourceKeySerializationIT {

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
      @TopicConsumer(
          topic = "neo4j-cdc-topic-key-serialization-none",
          offset = "earliest",
          keyDeserializer = StringDeserializer::class)
      consumer: KafkaConsumer<Any, GenericRecord>,
      session: Session
  ) {
    val executionId = testInfo.displayName + System.currentTimeMillis()
    session
        .run(
            "CREATE (:TestSource {name: 'Jane', execId: \$execId})",
            mapOf("execId" to executionId),
        )
        .consume()

    TopicVerifier.create(consumer).assertNoMessageKey().verifyWithin(Duration.ofSeconds(30))
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
      @TopicConsumer(
          topic = "neo4j-cdc-topic-key-serialization-whole",
          offset = "earliest",
          keyDeserializer = KafkaAvroDeserializer::class)
      consumer: KafkaConsumer<GenericRecord, GenericRecord>,
      session: Session
  ) {
    val executionId = testInfo.displayName + System.currentTimeMillis()
    session
        .run(
            "CREATE (:TestSource {name: 'Jane', execId: \$execId})",
            mapOf("execId" to executionId),
        )
        .consume()

    TopicVerifier.create(consumer)
        .assertMessageKey { key ->
          AvroCdcRecordAssert.assertThat(key)
              .hasEventType(EventType.NODE)
              .hasOperation(Operation.CREATE)
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
      @TopicConsumer(
          topic = "neo4j-cdc-topic-key-serialization-element-ids",
          offset = "earliest",
          keyDeserializer = StringDeserializer::class)
      consumer: KafkaConsumer<String, GenericRecord>,
      session: Session
  ) {
    val executionId = testInfo.displayName + System.currentTimeMillis()
    session
        .run(
            "CREATE (:TestSource {name: 'Jane', execId: \$execId})",
            mapOf("execId" to executionId),
        )
        .consume()

    TopicVerifier.create(consumer)
        .assertMessageKey { key -> assertTrue(key.isNotEmpty()) }
        .verifyWithin(Duration.ofSeconds(30))
  }
}
