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
import io.kotest.matchers.equality.shouldBeEqualToComparingFields
import io.kotest.matchers.shouldBe
import java.time.Duration
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.OffsetTime
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.storage.SimpleHeaderConverter
import org.junit.jupiter.api.Test
import org.neo4j.cdc.client.model.ChangeEvent
import org.neo4j.connectors.kafka.connect.ConnectHeader
import org.neo4j.connectors.kafka.data.DynamicTypes
import org.neo4j.connectors.kafka.data.Headers
import org.neo4j.connectors.kafka.data.SimpleTypes
import org.neo4j.connectors.kafka.data.TemporalDataSchemaType
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
                          patterns = arrayOf(CdcSourceParam("(:Person)"))),
                      CdcSourceTopic(
                          topic = "neo4j-cdc-rels-topic",
                          patterns = arrayOf(CdcSourceParam("()-[:KNOWS]-()"))))))
  @Test
  fun `should place cdc related information into headers`(
      @TopicConsumer(topic = "neo4j-cdc-nodes-topic", offset = "earliest")
      nodesConsumer: ConvertingKafkaConsumer,
      @TopicConsumer(topic = "neo4j-cdc-rels-topic", offset = "earliest")
      relsConsumer: ConvertingKafkaConsumer,
      session: Session
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
                        "dob" to LocalDate.of(2000, 1, 1)),
                "person2" to
                    mapOf(
                        "id" to 2L,
                        "name" to "John",
                        "surname" to "Doe",
                        "dob" to LocalDate.of(1999, 1, 1))))
        .consume()

    TopicVerifier.create<ChangeEvent, ChangeEvent>(nodesConsumer)
        .assertMessage { msg ->
          msg.raw
              .headers()
              .map {
                ConnectHeader(
                    it.key(), SimpleHeaderConverter().toConnectHeader("", it.key(), it.value()))
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
                            Schema.INT64_SCHEMA)
                        .contains(it.schema())
              }
              .shouldHaveSingleElement {
                it.key() == Headers.KEY_CDC_TX_SEQ &&
                    listOf(
                            Schema.INT8_SCHEMA,
                            Schema.INT16_SCHEMA,
                            Schema.INT32_SCHEMA,
                            Schema.INT64_SCHEMA)
                        .contains(it.schema())
              }
        }
        .assertMessage { msg ->
          msg.raw
              .headers()
              .map {
                ConnectHeader(
                    it.key(), SimpleHeaderConverter().toConnectHeader("", it.key(), it.value()))
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
                            Schema.INT64_SCHEMA)
                        .contains(it.schema())
              }
              .shouldHaveSingleElement {
                it.key() == Headers.KEY_CDC_TX_SEQ &&
                    listOf(
                            Schema.INT8_SCHEMA,
                            Schema.INT16_SCHEMA,
                            Schema.INT32_SCHEMA,
                            Schema.INT64_SCHEMA)
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
                    it.key(), SimpleHeaderConverter().toConnectHeader("", it.key(), it.value()))
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
                            Schema.INT64_SCHEMA)
                        .contains(it.schema())
              }
              .shouldHaveSingleElement {
                it.key() == Headers.KEY_CDC_TX_SEQ &&
                    listOf(
                            Schema.INT8_SCHEMA,
                            Schema.INT16_SCHEMA,
                            Schema.INT32_SCHEMA,
                            Schema.INT64_SCHEMA)
                        .contains(it.schema())
              }
        }
        .verifyWithin(Duration.ofSeconds(30))
  }

  @Neo4jSource(
      startFrom = "EARLIEST",
      strategy = CDC,
      cdc =
          CdcSource(
              topics =
                  arrayOf(
                      CdcSourceTopic(
                          topic = "neo4j-cdc-topic",
                          patterns =
                              arrayOf(
                                  CdcSourceParam(
                                      "(:TestSource{localDate, localDatetime, localTime, zonedDatetime, offsetDatetime, offsetTime})"))))),
      temporalDataSchemaType = TemporalDataSchemaType.STRUCT)
  @Test
  fun `should return struct temporal types`(
      @TopicConsumer(topic = "neo4j-cdc-topic", offset = "earliest")
      consumer: ConvertingKafkaConsumer,
      session: Session
  ) {
    session
        .run(
            "CREATE (:TestSource {" +
                "localDate: date('2024-01-01'), " +
                "localDatetime: localdatetime('2024-01-01T12:00:00'), " +
                "localTime: localtime('12:00:00'), " +
                "zonedDatetime: datetime('2024-01-01T12:00:00[Europe/Stockholm]'), " +
                "offsetDatetime: datetime('2024-01-01T12:00:00Z'), " +
                "offsetTime: time('12:00:00Z'), " +
                "timestamp: 0})")
        .consume()

    TopicVerifier.create<Struct, Struct>(consumer)
        .assertMessageValue { value ->
          val properties =
              value.getStruct("event").getStruct("state").getStruct("after").getStruct("properties")

          properties.getStruct("localDate") shouldBeEqualToComparingFields
              DynamicTypes.toConnectValue(
                  SimpleTypes.LOCALDATE_STRUCT.schema,
                  LocalDate.of(2024, 1, 1),
              ) as Struct

          properties.getStruct("localDatetime") shouldBeEqualToComparingFields
              DynamicTypes.toConnectValue(
                  SimpleTypes.LOCALDATETIME_STRUCT.schema,
                  LocalDateTime.of(2024, 1, 1, 12, 0, 0),
              ) as Struct

          properties.getStruct("localTime") shouldBeEqualToComparingFields
              DynamicTypes.toConnectValue(
                  SimpleTypes.LOCALTIME_STRUCT.schema,
                  LocalTime.of(12, 0, 0),
              ) as Struct

          properties.getStruct("zonedDatetime") shouldBeEqualToComparingFields
              DynamicTypes.toConnectValue(
                  SimpleTypes.ZONEDDATETIME_STRUCT.schema,
                  ZonedDateTime.of(2024, 1, 1, 12, 0, 0, 0, ZoneId.of("Europe/Stockholm")),
              ) as Struct

          properties.getStruct("offsetDatetime") shouldBeEqualToComparingFields
              DynamicTypes.toConnectValue(
                  SimpleTypes.ZONEDDATETIME_STRUCT.schema,
                  OffsetDateTime.of(2024, 1, 1, 12, 0, 0, 0, ZoneOffset.UTC),
              ) as Struct

          properties.getStruct("offsetTime") shouldBeEqualToComparingFields
              DynamicTypes.toConnectValue(
                  SimpleTypes.OFFSETTIME_STRUCT.schema,
                  OffsetTime.of(12, 0, 0, 0, ZoneOffset.UTC),
              ) as Struct
        }
        .verifyWithin(Duration.ofSeconds(30))
  }

  @Neo4jSource(
      startFrom = "EARLIEST",
      strategy = CDC,
      cdc =
          CdcSource(
              topics =
                  arrayOf(
                      CdcSourceTopic(
                          topic = "neo4j-cdc-topic",
                          patterns =
                              arrayOf(
                                  CdcSourceParam(
                                      "(:TestSource{localDate, localDatetime, localTime, zonedDatetime, offsetDatetime, offsetTime})"))))),
      temporalDataSchemaType = TemporalDataSchemaType.STRING)
  @Test
  fun `should return string temporal types`(
      @TopicConsumer(topic = "neo4j-cdc-topic", offset = "earliest")
      consumer: ConvertingKafkaConsumer,
      session: Session
  ) {
    session
        .run(
            "CREATE (:TestSource {" +
                "localDate: date('2024-01-01'), " +
                "localDatetime: localdatetime('2024-01-01T12:00:00'), " +
                "localTime: localtime('12:00:00'), " +
                "zonedDatetime: datetime('2024-01-01T12:00:00[Europe/Stockholm]'), " +
                "offsetDatetime: datetime('2024-01-01T12:00:00Z'), " +
                "offsetTime: time('12:00:00Z'), " +
                "timestamp: 0})")
        .consume()

    TopicVerifier.create<Struct, Struct>(consumer)
        .assertMessageValue { value ->
          val properties =
              value.getStruct("event").getStruct("state").getStruct("after").getStruct("properties")

          properties.getString("localDate") shouldBe
              DynamicTypes.toConnectValue(
                  SimpleTypes.LOCALDATE.schema,
                  LocalDate.of(2024, 1, 1),
              )

          properties.getString("localDatetime") shouldBe
              DynamicTypes.toConnectValue(
                  SimpleTypes.LOCALDATETIME.schema,
                  LocalDateTime.of(2024, 1, 1, 12, 0, 0),
              )

          properties.getString("localTime") shouldBe
              DynamicTypes.toConnectValue(
                  SimpleTypes.LOCALTIME.schema,
                  LocalTime.of(12, 0, 0),
              )

          properties.getString("zonedDatetime") shouldBe
              DynamicTypes.toConnectValue(
                  SimpleTypes.ZONEDDATETIME.schema,
                  ZonedDateTime.of(2024, 1, 1, 12, 0, 0, 0, ZoneId.of("Europe/Stockholm")),
              )

          properties.getString("offsetDatetime") shouldBe
              DynamicTypes.toConnectValue(
                  SimpleTypes.ZONEDDATETIME.schema,
                  OffsetDateTime.of(2024, 1, 1, 12, 0, 0, 0, ZoneOffset.UTC),
              )

          properties.getString("offsetTime") shouldBe
              DynamicTypes.toConnectValue(
                  SimpleTypes.OFFSETTIME.schema,
                  OffsetTime.of(12, 0, 0, 0, ZoneOffset.UTC),
              )
        }
        .verifyWithin(Duration.ofSeconds(30))
  }
}

@KeyValueConverter(key = AVRO, value = AVRO) class Neo4jCdcSourceAvroIT : Neo4jCdcSourceIT()

@KeyValueConverter(key = JSON_SCHEMA, value = JSON_SCHEMA)
class Neo4jCdcSourceJsonIT : Neo4jCdcSourceIT()

@KeyValueConverter(key = PROTOBUF, value = PROTOBUF)
class Neo4jCdcSourceProtobufIT : Neo4jCdcSourceIT()
