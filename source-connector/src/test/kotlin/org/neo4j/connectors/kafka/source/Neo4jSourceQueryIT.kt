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
import org.apache.kafka.connect.data.Struct
import org.junit.jupiter.api.Test
import org.neo4j.connectors.kafka.data.DynamicTypes
import org.neo4j.connectors.kafka.data.SimpleTypes
import org.neo4j.connectors.kafka.data.TemporalDataSchemaType
import org.neo4j.connectors.kafka.testing.MapSupport.excludingKeys
import org.neo4j.connectors.kafka.testing.TestSupport.runTest
import org.neo4j.connectors.kafka.testing.assertions.TopicVerifier
import org.neo4j.connectors.kafka.testing.format.KafkaConverter.AVRO
import org.neo4j.connectors.kafka.testing.format.KafkaConverter.JSON_SCHEMA
import org.neo4j.connectors.kafka.testing.format.KafkaConverter.PROTOBUF
import org.neo4j.connectors.kafka.testing.format.KeyValueConverter
import org.neo4j.connectors.kafka.testing.kafka.ConvertingKafkaConsumer
import org.neo4j.connectors.kafka.testing.source.Neo4jSource
import org.neo4j.connectors.kafka.testing.source.SourceStrategy
import org.neo4j.connectors.kafka.testing.source.TopicConsumer
import org.neo4j.driver.Session

abstract class Neo4jSourceQueryIT {

  companion object {
    private const val TOPIC = "neo4j-source-topic"
  }

  @Neo4jSource(
      topic = TOPIC,
      strategy = SourceStrategy.QUERY,
      streamingProperty = "timestamp",
      startFrom = "EARLIEST",
      query =
          "MATCH (ts:TestSource) WHERE ts.timestamp > \$lastCheck RETURN ts.name AS name, ts.surname AS surname, ts.timestamp AS timestamp")
  @Test
  fun `reads latest changes from Neo4j source starting from EARLIEST`(
      @TopicConsumer(topic = TOPIC, offset = "earliest") consumer: ConvertingKafkaConsumer,
      session: Session
  ) = runTest {
    session.run("CREATE (:TestSource {name: 'jane', surname: 'doe', timestamp: 0})").consume()
    session
        .run(
            "CREATE (:TestSource {name: 'john', surname: 'doe', timestamp: datetime().epochMillis})")
        .consume()
    session
        .run(
            "CREATE (:TestSource {name: 'mary', surname: 'doe', timestamp: datetime().epochMillis})")
        .consume()

    TopicVerifier.createForMap(consumer)
        .assertMessageValue { value ->
          value.excludingKeys("timestamp") shouldBe mapOf("name" to "jane", "surname" to "doe")
        }
        .assertMessageValue { value ->
          value.excludingKeys("timestamp") shouldBe mapOf("name" to "john", "surname" to "doe")
        }
        .assertMessageValue { value ->
          value.excludingKeys("timestamp") shouldBe mapOf("name" to "mary", "surname" to "doe")
        }
        .verifyWithin(Duration.ofSeconds(30))
  }

  @Neo4jSource(
      topic = TOPIC,
      strategy = SourceStrategy.QUERY,
      streamingProperty = "timestamp",
      startFrom = "NOW",
      query =
          "MATCH (ts:TestSource) WHERE ts.timestamp > \$lastCheck RETURN ts.name AS name, ts.surname AS surname, ts.timestamp AS timestamp")
  @Test
  fun `reads latest changes from Neo4j source starting from NOW`(
      @TopicConsumer(topic = TOPIC, offset = "earliest") consumer: ConvertingKafkaConsumer,
      session: Session
  ) = runTest {
    session
        .run(
            "CREATE (:TestSource {name: 'jane', surname: 'doe', timestamp: (datetime() - duration('PT10S')).epochMillis})")
        .consume()
    session
        .run(
            "CREATE (:TestSource {name: 'john', surname: 'doe', timestamp: (datetime() + duration('PT10S')).epochMillis})")
        .consume()
    session
        .run(
            "CREATE (:TestSource {name: 'mary', surname: 'doe', timestamp: (datetime() + duration('PT10S')).epochMillis})")
        .consume()

    TopicVerifier.createForMap(consumer)
        .assertMessageValue { value ->
          value.excludingKeys("timestamp") shouldBe mapOf("name" to "john", "surname" to "doe")
        }
        .assertMessageValue { value ->
          value.excludingKeys("timestamp") shouldBe mapOf("name" to "mary", "surname" to "doe")
        }
        .verifyWithin(Duration.ofSeconds(30))
  }

  @Neo4jSource(
      topic = TOPIC,
      strategy = SourceStrategy.QUERY,
      streamingProperty = "timestamp",
      startFrom = "USER_PROVIDED",
      startFromValue = "1704067200000", // 2024-01-01T00:00:00
      query =
          "MATCH (ts:TestSource) WHERE ts.timestamp > \$lastCheck RETURN ts.name AS name, ts.surname AS surname, ts.timestamp AS timestamp")
  @Test
  fun `reads latest changes from Neo4j source starting from USER_PROVIDED`(
      @TopicConsumer(topic = TOPIC, offset = "earliest") consumer: ConvertingKafkaConsumer,
      session: Session
  ) = runTest {
    session
        .run(
            "CREATE (:TestSource {name: 'jane', surname: 'doe', timestamp: datetime('2023-12-31T23:59:59Z').epochMillis})")
        .consume()
    session
        .run(
            "CREATE (:TestSource {name: 'john', surname: 'doe', timestamp: datetime('2024-01-01T12:00:00Z').epochMillis})")
        .consume()
    session
        .run(
            "CREATE (:TestSource {name: 'mary', surname: 'doe', timestamp: datetime('2024-01-03T00:00:00Z').epochMillis})")
        .consume()
    session
        .run(
            "CREATE (:TestSource {name: 'mary', surname: 'doe', timestamp: datetime().epochMillis})")
        .consume()

    TopicVerifier.createForMap(consumer)
        .assertMessageValue { value ->
          value shouldBe
              mapOf(
                  "name" to "john",
                  "surname" to "doe",
                  "timestamp" to
                      OffsetDateTime.of(2024, 1, 1, 12, 0, 0, 0, ZoneOffset.UTC)
                          .toInstant()
                          .toEpochMilli())
        }
        .assertMessageValue { value ->
          value shouldBe
              mapOf(
                  "name" to "mary",
                  "surname" to "doe",
                  "timestamp" to
                      OffsetDateTime.of(2024, 1, 3, 0, 0, 0, 0, ZoneOffset.UTC)
                          .toInstant()
                          .toEpochMilli())
        }
        .assertMessageValue { value ->
          value.excludingKeys("timestamp") shouldBe mapOf("name" to "mary", "surname" to "doe")
        }
        .verifyWithin(Duration.ofSeconds(30))
  }

  @Neo4jSource(
      topic = TOPIC,
      strategy = SourceStrategy.QUERY,
      streamingProperty = "timestamp",
      startFrom = "EARLIEST",
      query =
          "MATCH (ts:TestSource) WHERE ts.timestamp > \$lastCheck RETURN " +
              "ts.localDate AS localDate, " +
              "ts.localDatetime AS localDatetime, " +
              "ts.localTime AS localTime, " +
              "ts.zonedDatetime AS zonedDatetime, " +
              "ts.offsetDatetime AS offsetDatetime, " +
              "ts.offsetTime AS offsetTime, " +
              "ts.timestamp AS timestamp",
      temporalDataSchemaType = TemporalDataSchemaType.STRUCT,
  )
  @Test
  fun `should return struct temporal types`(
      @TopicConsumer(topic = TOPIC, offset = "earliest") consumer: ConvertingKafkaConsumer,
      session: Session
  ) = runTest {
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
          value.getStruct("localDate") shouldBeEqualToComparingFields
              DynamicTypes.toConnectValue(
                  SimpleTypes.LOCALDATE_STRUCT.schema,
                  LocalDate.of(2024, 1, 1),
              ) as Struct

          value.getStruct("localDatetime") shouldBeEqualToComparingFields
              DynamicTypes.toConnectValue(
                  SimpleTypes.LOCALDATETIME_STRUCT.schema,
                  LocalDateTime.of(2024, 1, 1, 12, 0, 0),
              ) as Struct

          value.getStruct("localTime") shouldBeEqualToComparingFields
              DynamicTypes.toConnectValue(
                  SimpleTypes.LOCALTIME_STRUCT.schema,
                  LocalTime.of(12, 0, 0),
              ) as Struct

          value.getStruct("zonedDatetime") shouldBeEqualToComparingFields
              DynamicTypes.toConnectValue(
                  SimpleTypes.ZONEDDATETIME_STRUCT.schema,
                  ZonedDateTime.of(2024, 1, 1, 12, 0, 0, 0, ZoneId.of("Europe/Stockholm")),
              ) as Struct

          value.getStruct("offsetDatetime") shouldBeEqualToComparingFields
              DynamicTypes.toConnectValue(
                  SimpleTypes.ZONEDDATETIME_STRUCT.schema,
                  OffsetDateTime.of(2024, 1, 1, 12, 0, 0, 0, ZoneOffset.UTC),
              ) as Struct

          value.getStruct("offsetTime") shouldBeEqualToComparingFields
              DynamicTypes.toConnectValue(
                  SimpleTypes.OFFSETTIME_STRUCT.schema,
                  OffsetTime.of(12, 0, 0, 0, ZoneOffset.UTC),
              ) as Struct
        }
        .verifyWithin(Duration.ofSeconds(300))
  }

  @Neo4jSource(
      topic = TOPIC,
      strategy = SourceStrategy.QUERY,
      streamingProperty = "timestamp",
      startFrom = "EARLIEST",
      query =
          "MATCH (ts:TestSource) WHERE ts.timestamp > \$lastCheck RETURN " +
              "ts.localDate AS localDate, " +
              "ts.localDatetime AS localDatetime, " +
              "ts.localTime AS localTime, " +
              "ts.zonedDatetime AS zonedDatetime, " +
              "ts.offsetDatetime AS offsetDatetime, " +
              "ts.offsetTime AS offsetTime, " +
              "ts.timestamp AS timestamp",
      temporalDataSchemaType = TemporalDataSchemaType.STRING)
  @Test
  fun `should return string temporal types`(
      @TopicConsumer(topic = TOPIC, offset = "earliest") consumer: ConvertingKafkaConsumer,
      session: Session
  ) = runTest {
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
          value.getString("localDate") shouldBe
              DynamicTypes.toConnectValue(
                  SimpleTypes.LOCALDATE.schema,
                  LocalDate.of(2024, 1, 1),
              )

          value.getString("localDatetime") shouldBe
              DynamicTypes.toConnectValue(
                  SimpleTypes.LOCALDATETIME.schema,
                  LocalDateTime.of(2024, 1, 1, 12, 0, 0),
              )

          value.getString("localTime") shouldBe
              DynamicTypes.toConnectValue(
                  SimpleTypes.LOCALTIME.schema,
                  LocalTime.of(12, 0, 0),
              )

          value.getString("zonedDatetime") shouldBe
              DynamicTypes.toConnectValue(
                  SimpleTypes.ZONEDDATETIME.schema,
                  ZonedDateTime.of(2024, 1, 1, 12, 0, 0, 0, ZoneId.of("Europe/Stockholm")),
              )

          value.getString("offsetDatetime") shouldBe
              DynamicTypes.toConnectValue(
                  SimpleTypes.ZONEDDATETIME.schema,
                  OffsetDateTime.of(2024, 1, 1, 12, 0, 0, 0, ZoneOffset.UTC),
              )

          value.getString("offsetTime") shouldBe
              DynamicTypes.toConnectValue(
                  SimpleTypes.OFFSETTIME.schema,
                  OffsetTime.of(12, 0, 0, 0, ZoneOffset.UTC),
              )
        }
        .verifyWithin(Duration.ofSeconds(300))
  }
}

@KeyValueConverter(key = AVRO, value = AVRO) class Neo4jSourceAvroIT : Neo4jSourceQueryIT()

@KeyValueConverter(key = JSON_SCHEMA, value = JSON_SCHEMA)
class Neo4jSourceJsonIT : Neo4jSourceQueryIT()

@KeyValueConverter(key = PROTOBUF, value = PROTOBUF)
class Neo4jSourceProtobufIT : Neo4jSourceQueryIT()
