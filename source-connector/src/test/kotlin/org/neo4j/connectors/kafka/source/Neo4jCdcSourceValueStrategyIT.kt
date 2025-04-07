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

import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import java.time.Duration
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfSystemProperty
import org.neo4j.cdc.client.model.ChangeEvent
import org.neo4j.cdc.client.model.EntityOperation
import org.neo4j.cdc.client.model.EventType
import org.neo4j.connectors.kafka.configuration.PayloadMode
import org.neo4j.connectors.kafka.testing.MapSupport.excludingKeys
import org.neo4j.connectors.kafka.testing.assertions.ChangeEventAssert
import org.neo4j.connectors.kafka.testing.assertions.TopicVerifier
import org.neo4j.connectors.kafka.testing.format.KafkaConverter.AVRO
import org.neo4j.connectors.kafka.testing.format.KafkaConverter.JSON_EMBEDDED
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

@EnabledIfSystemProperty(
    named = "neo4j.cdc",
    matches = "true",
    disabledReason = "CDC is not available with this version of Neo4j")
abstract class Neo4jCdcSourceValueStrategyIT {
  @Neo4jSource(
      startFrom = "EARLIEST",
      strategy = SourceStrategy.CDC,
      cdc =
          CdcSource(
              topics =
                  arrayOf(
                      CdcSourceTopic(
                          topic = "neo4j-cdc-topic-value-serialization-change-event",
                          patterns = arrayOf(CdcSourceParam("(:TestSource{name,+execId})")),
                          valueSerializationStrategy = "CHANGE_EVENT"),
                  ),
          ),
  )
  @Test
  fun `supports serialization of values as whole change event`(
      @TopicConsumer(
          topic = "neo4j-cdc-topic-value-serialization-change-event", offset = "earliest")
      consumer: ConvertingKafkaConsumer,
      session: Session
  ) {

    session.run("CREATE (:TestSource {name: 'Jane'})").consume()

    TopicVerifier.create<ChangeEvent, ChangeEvent>(consumer)
        .assertMessageValue {
          ChangeEventAssert.assertThat(it)
              .isNotNull()
              .hasOperation(EntityOperation.CREATE)
              .labelledAs("TestSource")
              .hasNoBeforeState()
              .hasAfterStateProperties(mapOf("name" to "Jane"))
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
                          topic = "neo4j-cdc-topic-value-serialization-entity-event",
                          patterns = arrayOf(CdcSourceParam("(:TestSource{name,+execId})")),
                          valueSerializationStrategy = "ENTITY_EVENT"),
                  ),
          ),
  )
  @Test
  fun `supports serialization of values as only event entity`(
      @TopicConsumer(
          topic = "neo4j-cdc-topic-value-serialization-entity-event", offset = "earliest")
      consumer: ConvertingKafkaConsumer,
      session: Session
  ) {
    session.run("CREATE (:TestSource {name: 'Jane'})").consume()

    TopicVerifier.createForMap(consumer)
        .assertMessageValue {
          it["elementId"] shouldNotBe null
          it.excludingKeys("elementId") shouldBe
              mapOf(
                  "eventType" to EventType.NODE.name,
                  "operation" to EntityOperation.CREATE.name,
                  "labels" to listOf("TestSource"),
                  "keys" to emptyMap<Any, Any>(),
                  "state" to
                      mapOf(
                          "after" to
                              mapOf(
                                  "labels" to listOf("TestSource"),
                                  "properties" to mapOf("name" to "Jane"))))
        }
        .verifyWithin(Duration.ofSeconds(30))
  }
}

@KeyValueConverter(key = AVRO, value = AVRO, payloadMode = PayloadMode.EXTENDED)
class Neo4jCdcSourceValueStrategyAvroExtendedIT : Neo4jCdcSourceValueStrategyIT()

@KeyValueConverter(key = AVRO, value = AVRO, payloadMode = PayloadMode.COMPACT)
class Neo4jCdcSourceValueStrategyAvroCompactIT : Neo4jCdcSourceValueStrategyIT()

@KeyValueConverter(key = JSON_SCHEMA, value = JSON_SCHEMA, payloadMode = PayloadMode.EXTENDED)
class Neo4jCdcSourceValueStrategyJsonSchemaExtendedIT : Neo4jCdcSourceValueStrategyIT()

@KeyValueConverter(key = JSON_SCHEMA, value = JSON_SCHEMA, payloadMode = PayloadMode.COMPACT)
class Neo4jCdcSourceValueStrategyJsonSchemaCompactIT : Neo4jCdcSourceValueStrategyIT()

@KeyValueConverter(key = JSON_EMBEDDED, value = JSON_EMBEDDED, payloadMode = PayloadMode.EXTENDED)
class Neo4jCdcSourceValueStrategyJsonEmbeddedExtendedIT : Neo4jCdcSourceValueStrategyIT()

@KeyValueConverter(key = JSON_EMBEDDED, value = JSON_EMBEDDED, payloadMode = PayloadMode.COMPACT)
class Neo4jCdcSourceValueStrategyJsonEmbeddedCompactIT : Neo4jCdcSourceValueStrategyIT()

@KeyValueConverter(key = PROTOBUF, value = PROTOBUF, payloadMode = PayloadMode.EXTENDED)
class Neo4jCdcSourceValueStrategyProtobufExtendedIT : Neo4jCdcSourceValueStrategyIT()
