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
import java.time.Duration
import java.time.OffsetDateTime
import java.time.ZoneOffset
import org.junit.jupiter.api.Test
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
}

@KeyValueConverter(key = AVRO, value = AVRO) class Neo4jSourceAvroIT : Neo4jSourceQueryIT()

@KeyValueConverter(key = JSON_SCHEMA, value = JSON_SCHEMA)
class Neo4jSourceJsonIT : Neo4jSourceQueryIT()

@KeyValueConverter(key = PROTOBUF, value = PROTOBUF)
class Neo4jSourceProtobufIT : Neo4jSourceQueryIT()
