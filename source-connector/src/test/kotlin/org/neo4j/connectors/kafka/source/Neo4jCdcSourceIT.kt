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

import io.kotest.matchers.collections.shouldHaveSingleElement
import io.kotest.matchers.collections.shouldHaveSize
import java.time.Duration
import java.time.LocalDate
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.storage.SimpleHeaderConverter
import org.junit.jupiter.api.Test
import org.neo4j.cdc.client.model.ChangeEvent
import org.neo4j.connectors.kafka.configuration.PayloadMode
import org.neo4j.connectors.kafka.connect.ConnectHeader
import org.neo4j.connectors.kafka.data.Headers
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
import org.neo4j.connectors.kafka.testing.source.SourceStrategy.CDC
import org.neo4j.connectors.kafka.testing.source.TopicConsumer
import org.neo4j.driver.Session

abstract class Neo4jCdcSourceIT {

  @Neo4jSource(
      startFrom = "EARLIEST",
      strategy = CDC,
      cdc =
          CdcSource(
              topics =
                  arrayOf(
                      CdcSourceTopic(
                          topic = "neo4j-cdc-nodes-topic",
                          patterns = arrayOf(CdcSourceParam("(:Person)")),
                      ),
                      CdcSourceTopic(
                          topic = "neo4j-cdc-rels-topic",
                          patterns = arrayOf(CdcSourceParam("()-[:KNOWS]-()")),
                      ),
                  )
          ),
  )
  @Test
  fun `should place cdc related information into headers`(
      @TopicConsumer(topic = "neo4j-cdc-nodes-topic", offset = "earliest")
      nodesConsumer: ConvertingKafkaConsumer,
      @TopicConsumer(topic = "neo4j-cdc-rels-topic", offset = "earliest")
      relsConsumer: ConvertingKafkaConsumer,
      session: Session,
  ) {
    session
        .run(
            """
              CREATE (p1:Person) SET p1 = ${'$'}person1
              CREATE (p2:Person) SET p2 = ${'$'}person2
              CREATE (p1)-[:KNOWS]->(p2)
            """
                .trimIndent(),
            mapOf(
                "person1" to
                    mapOf(
                        "id" to 1L,
                        "name" to "Jane",
                        "surname" to "Doe",
                        "dob" to LocalDate.of(2000, 1, 1),
                    ),
                "person2" to
                    mapOf(
                        "id" to 2L,
                        "name" to "John",
                        "surname" to "Doe",
                        "dob" to LocalDate.of(1999, 1, 1),
                    ),
            ),
        )
        .consume()

    TopicVerifier.create<ChangeEvent, ChangeEvent>(nodesConsumer)
        .assertMessage { msg ->
          msg.raw
              .headers()
              .map {
                ConnectHeader(
                    it.key(),
                    SimpleHeaderConverter().toConnectHeader("", it.key(), it.value()),
                )
              }
              .asIterable()
              .shouldHaveSize(3)
              .shouldHaveSingleElement {
                it.key() == Headers.KEY_CDC_ID && it.schema() == Schema.STRING_SCHEMA
              }
              .shouldHaveSingleElement {
                it.key() == Headers.KEY_CDC_TX_ID &&
                    listOf(
                            Schema.INT8_SCHEMA,
                            Schema.INT16_SCHEMA,
                            Schema.INT32_SCHEMA,
                            Schema.INT64_SCHEMA,
                        )
                        .contains(it.schema())
              }
              .shouldHaveSingleElement {
                it.key() == Headers.KEY_CDC_TX_SEQ &&
                    listOf(
                            Schema.INT8_SCHEMA,
                            Schema.INT16_SCHEMA,
                            Schema.INT32_SCHEMA,
                            Schema.INT64_SCHEMA,
                        )
                        .contains(it.schema())
              }
        }
        .assertMessage { msg ->
          msg.raw
              .headers()
              .map {
                ConnectHeader(
                    it.key(),
                    SimpleHeaderConverter().toConnectHeader("", it.key(), it.value()),
                )
              }
              .asIterable()
              .shouldHaveSize(3)
              .shouldHaveSingleElement {
                it.key() == Headers.KEY_CDC_ID && it.schema() == Schema.STRING_SCHEMA
              }
              .shouldHaveSingleElement {
                it.key() == Headers.KEY_CDC_TX_ID &&
                    listOf(
                            Schema.INT8_SCHEMA,
                            Schema.INT16_SCHEMA,
                            Schema.INT32_SCHEMA,
                            Schema.INT64_SCHEMA,
                        )
                        .contains(it.schema())
              }
              .shouldHaveSingleElement {
                it.key() == Headers.KEY_CDC_TX_SEQ &&
                    listOf(
                            Schema.INT8_SCHEMA,
                            Schema.INT16_SCHEMA,
                            Schema.INT32_SCHEMA,
                            Schema.INT64_SCHEMA,
                        )
                        .contains(it.schema())
              }
        }
        .verifyWithin(Duration.ofSeconds(30))

    TopicVerifier.create<ChangeEvent, ChangeEvent>(relsConsumer)
        .assertMessage { msg ->
          msg.raw
              .headers()
              .map {
                ConnectHeader(
                    it.key(),
                    SimpleHeaderConverter().toConnectHeader("", it.key(), it.value()),
                )
              }
              .asIterable()
              .shouldHaveSize(3)
              .shouldHaveSingleElement {
                it.key() == Headers.KEY_CDC_ID && it.schema() == Schema.STRING_SCHEMA
              }
              .shouldHaveSingleElement {
                it.key() == Headers.KEY_CDC_TX_ID &&
                    listOf(
                            Schema.INT8_SCHEMA,
                            Schema.INT16_SCHEMA,
                            Schema.INT32_SCHEMA,
                            Schema.INT64_SCHEMA,
                        )
                        .contains(it.schema())
              }
              .shouldHaveSingleElement {
                it.key() == Headers.KEY_CDC_TX_SEQ &&
                    listOf(
                            Schema.INT8_SCHEMA,
                            Schema.INT16_SCHEMA,
                            Schema.INT32_SCHEMA,
                            Schema.INT64_SCHEMA,
                        )
                        .contains(it.schema())
              }
        }
        .verifyWithin(Duration.ofSeconds(30))
  }
}

@KeyValueConverter(key = AVRO, value = AVRO, payloadMode = PayloadMode.EXTENDED)
class Neo4jCdcSourceAvroExtendedIT : Neo4jCdcSourceIT()

@KeyValueConverter(key = AVRO, value = AVRO, payloadMode = PayloadMode.COMPACT)
class Neo4jCdcSourceAvroCompactIT : Neo4jCdcSourceIT()

@KeyValueConverter(key = JSON_SCHEMA, value = JSON_SCHEMA, payloadMode = PayloadMode.EXTENDED)
class Neo4jCdcSourceJsonSchemaExtendedIT : Neo4jCdcSourceIT()

@KeyValueConverter(key = JSON_SCHEMA, value = JSON_SCHEMA, payloadMode = PayloadMode.COMPACT)
class Neo4jCdcSourceJsonSchemaCompactIT : Neo4jCdcSourceIT()

@KeyValueConverter(key = JSON_EMBEDDED, value = JSON_EMBEDDED, payloadMode = PayloadMode.EXTENDED)
class Neo4jCdcSourceJsonEmbeddedExtendedIT : Neo4jCdcSourceIT()

@KeyValueConverter(key = JSON_EMBEDDED, value = JSON_EMBEDDED, payloadMode = PayloadMode.COMPACT)
class Neo4jCdcSourceJsonEmbeddedCompactIT : Neo4jCdcSourceIT()

@KeyValueConverter(key = PROTOBUF, value = PROTOBUF, payloadMode = PayloadMode.EXTENDED)
class Neo4jCdcSourceProtobufExtendedIT : Neo4jCdcSourceIT()
