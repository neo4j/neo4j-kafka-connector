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

import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import org.neo4j.connectors.kafka.testing.assertions.AssertionUtils
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
import java.time.Duration

class Neo4jCdcSourceIT {

  @Neo4jSource(
      streamingProperty = "timestamp",
      startFrom = "EARLIEST",
      strategy = SourceStrategy.CDC,
      cdc =
      CdcSource(
          topics =
          arrayOf(
              CdcSourceTopic(
                  topic = "neo4j-cdc-topic",
                  patterns =
                  arrayOf(
                      CdcSourceParam("(:TestSource{name,+surname,+execId,-age})")
                  ))
          ))
  )
  @Test
  fun `reads changes specified by CDC pattern`(
    testInfo: TestInfo,
    @TopicConsumer(topic = "neo4j-cdc-topic", offset = "earliest")
    consumer: KafkaConsumer<String, GenericRecord>,
    session: Session
  ) {
    val executionId = testInfo.displayName + System.currentTimeMillis()
    session
        .run(
            "CREATE (:TestSource {name: 'Jane', surname: 'Doe', age: 42, execId: \$execId})",
            mapOf("execId" to executionId))
        .consume()
    session
        .run("MATCH (ts:TestSource {name: 'Jane'}) SET ts.surname = 'Smith'")
        .consume()
    session
        .run("MATCH (ts:TestSource {name: 'Jane'}) DELETE n")
        .consume()

    TopicVerifier.create(consumer)
        .expectMessageValueMatching { value ->
          AssertionUtils.assertionValid {
            AvroCdcRecordAssert.assertThat(value)
                .hasEventType(EventType.NODE)
                .hasOperation(Operation.CREATE)
                .labelledAs("TestSource")
                .hasNoBeforeState()
                .hasAfterStateProperties(
                    mapOf("name" to "Jane", "surname" to "Doe", "execId" to executionId)
                )
          }
        }
        .expectMessageValueMatching { value ->
          AssertionUtils.assertionValid {
            AvroCdcRecordAssert.assertThat(value)
                .hasEventType(EventType.NODE)
                .hasOperation(Operation.UPDATE)
                .labelledAs("TestSource")
                .hasBeforeStateProperties(
                    mapOf("name" to "Jane", "surname" to "Doe", "execId" to executionId)
                )
                .hasAfterStateProperties(
                    mapOf("name" to "Jane", "surname" to "Smith", "execId" to executionId)
                )
          }
        }
        .expectMessageValueMatching { value ->
          AssertionUtils.assertionValid {
            AvroCdcRecordAssert.assertThat(value)
                .hasEventType(EventType.NODE)
                .hasOperation(Operation.DELETE)
                .labelledAs("TestSource")
                .hasBeforeStateProperties(
                    mapOf("name" to "Jane", "surname" to "Smith", "execId" to executionId)
                )
                .hasNoAfterState()
          }
        }
        .verifyWithin(Duration.ofSeconds(30))
  }
}
