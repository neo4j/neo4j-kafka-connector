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
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.neo4j.connectors.kafka.configuration.MapEncoding
import org.neo4j.connectors.kafka.configuration.PayloadMode
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

/**
 * What `neo4j.query.map-encoding` puts on the topic, once a schema has been through a converter and
 * a schema registry and back. These assertions read the schema of the message rather than its
 * values, because a map and a struct carry the same values and only their schemas differ.
 */
abstract class Neo4jSourceMapEncodingIT {

  companion object {
    const val TOPIC = "neo4j-source-map-encoding"

    const val LIST_OF_MAPS_QUERY =
        "WITH {list: [{ property1: 'value1' }, { property2: 'value2' }]} AS data " +
            "RETURN data, dateTime().epochMillis AS timestamp"
  }

  @Neo4jSource(
      topic = TOPIC,
      strategy = SourceStrategy.QUERY,
      streamingProperty = "timestamp",
      startFrom = "USER_PROVIDED",
      startFromValue = "1704067200000", // 2024-01-01T00:00:00
      query = "WITH {b: 1, a: 2} AS data RETURN data, dateTime().epochMillis AS timestamp",
  )
  @Test
  fun `struct encoding should describe a map as a struct with its keys sorted`(
      @TopicConsumer(topic = TOPIC, offset = "earliest") consumer: ConvertingKafkaConsumer
  ) = runTest {
    TopicVerifier.create<Any, Struct>(consumer)
        .assertMessageValue { value ->
          val data = value.schema().field("data").schema()

          data.type() shouldBe Schema.Type.STRUCT
          data.fields().map { it.name() } shouldBe listOf("a", "b")
        }
        .verifyWithin(Duration.ofSeconds(30))
  }

  @Neo4jSource(
      topic = TOPIC,
      strategy = SourceStrategy.QUERY,
      streamingProperty = "timestamp",
      startFrom = "USER_PROVIDED",
      startFromValue = "1704067200000",
      mapEncoding = MapEncoding.MAP,
      query = "WITH {b: 1, a: 2} AS data RETURN data, dateTime().epochMillis AS timestamp",
  )
  @Test
  fun `map encoding should describe a map as a map`(
      @TopicConsumer(topic = TOPIC, offset = "earliest") consumer: ConvertingKafkaConsumer
  ) = runTest {
    TopicVerifier.create<Any, Struct>(consumer)
        .assertMessageValue { value ->
          value.schema().field("data").schema().type() shouldBe Schema.Type.MAP
          value.getMap<String, Any>("data").keys shouldBe setOf("a", "b")
        }
        .verifyWithin(Duration.ofSeconds(30))
  }

  @Neo4jSource(
      topic = TOPIC,
      strategy = SourceStrategy.QUERY,
      streamingProperty = "timestamp",
      startFrom = "USER_PROVIDED",
      startFromValue = "1704067200000",
      mapEncoding = MapEncoding.LEGACY,
      query =
          "WITH {uniform: {a: 1, b: 2}, mixed: {a: 1, b: 'text'}} AS data " +
              "RETURN data, dateTime().epochMillis AS timestamp",
  )
  @Test
  fun `legacy encoding should describe a map as a map only when its values share a type`(
      @TopicConsumer(topic = TOPIC, offset = "earliest") consumer: ConvertingKafkaConsumer,
      payloadMode: PayloadMode,
  ) = runTest {
    // in EXTENDED mode every property value has the same schema, so both maps are uniform
    assumeTrue(payloadMode == PayloadMode.COMPACT)

    TopicVerifier.create<Any, Struct>(consumer)
        .assertMessageValue { value ->
          val data = value.schema().field("data").schema()

          data.type() shouldBe Schema.Type.STRUCT
          data.field("uniform").schema().type() shouldBe Schema.Type.MAP
          data.field("mixed").schema().type() shouldBe Schema.Type.STRUCT
        }
        .verifyWithin(Duration.ofSeconds(30))
  }

  @Neo4jSource(
      topic = TOPIC,
      strategy = SourceStrategy.QUERY,
      streamingProperty = "timestamp",
      startFrom = "USER_PROVIDED",
      startFromValue = "1704067200000",
      mapEncoding = MapEncoding.MAP,
      query =
          "WITH {`a key`: 1, `b.key`: 2, `3rd key`: 3} AS data " +
              "RETURN data, dateTime().epochMillis AS timestamp",
  )
  @Test
  fun `map encoding should carry keys that are not valid field names`(
      @TopicConsumer(topic = TOPIC, offset = "earliest") consumer: ConvertingKafkaConsumer
  ) = runTest {
    TopicVerifier.create<Any, Struct>(consumer)
        .assertMessageValue { value ->
          // spaces, dots and a leading digit are fine as map keys, and would not be as field names
          value.getMap<String, Any>("data").keys shouldBe setOf("a key", "b.key", "3rd key")
        }
        .verifyWithin(Duration.ofSeconds(30))
  }

  @Neo4jSource(
      topic = TOPIC,
      strategy = SourceStrategy.QUERY,
      streamingProperty = "timestamp",
      startFrom = "USER_PROVIDED",
      startFromValue = "1704067200000",
      mapEncoding = MapEncoding.MAP,
      query =
          "WITH {b: 'second', a: 'first'} AS data " +
              "RETURN data.a AS text, 1 AS number, data, dateTime().epochMillis AS timestamp",
  )
  @Test
  fun `map encoding should leave the row as a struct holding every column`(
      @TopicConsumer(topic = TOPIC, offset = "earliest") consumer: ConvertingKafkaConsumer
  ) = runTest {
    TopicVerifier.create<Any, Struct>(consumer)
        .assertMessageValue { value ->
          // the columns are text, a number and a map, which have no shared type: a message only
          // arrives at all because the row is a struct whatever the map encoding says
          value.schema().type() shouldBe Schema.Type.STRUCT
          value.schema().fields().map { it.name() } shouldBe
              listOf("text", "number", "data", "timestamp")
        }
        .verifyWithin(Duration.ofSeconds(30))
  }

  @Neo4jSource(
      topic = TOPIC,
      strategy = SourceStrategy.QUERY,
      streamingProperty = "timestamp",
      startFrom = "USER_PROVIDED",
      startFromValue = "1704067200000",
      query =
          "WITH {list: [{ property1: null }, { property2: 'value2' }]} AS data " +
              "RETURN data, dateTime().epochMillis AS timestamp",
  )
  @Test
  fun `struct encoding should keep the key of a list element whose values are all null`(
      @TopicConsumer(topic = TOPIC, offset = "earliest") consumer: ConvertingKafkaConsumer
  ) = runTest {
    TopicVerifier.create<Any, Struct>(consumer)
        .assertMessageValue { value ->
          val element = value.schema().field("data").schema().field("list").schema().valueSchema()

          // property1 used to disappear from the schema, and its value with it
          element.fields().map { it.name() } shouldBe listOf("property1", "property2")
        }
        .verifyWithin(Duration.ofSeconds(30))
  }

  @Neo4jSource(
      topic = TOPIC,
      strategy = SourceStrategy.QUERY,
      streamingProperty = "timestamp",
      startFrom = "USER_PROVIDED",
      startFromValue = "1704067200000",
      mapEncoding = MapEncoding.MAP,
      query = LIST_OF_MAPS_QUERY,
  )
  @Test
  open fun `map encoding should describe the elements of a list as maps`(
      @TopicConsumer(topic = TOPIC, offset = "earliest") consumer: ConvertingKafkaConsumer
  ) = runTest {
    TopicVerifier.create<Any, Struct>(consumer)
        .assertMessageValue { value ->
          val list = value.schema().field("data").schema().valueSchema()

          list.type() shouldBe Schema.Type.ARRAY
          list.valueSchema().type() shouldBe Schema.Type.MAP
        }
        .verifyWithin(Duration.ofSeconds(30))
  }
}

@KeyValueConverter(key = AVRO, value = AVRO, payloadMode = PayloadMode.COMPACT)
class Neo4jSourceMapEncodingAvroCompactIT : Neo4jSourceMapEncodingIT()

@KeyValueConverter(key = AVRO, value = AVRO, payloadMode = PayloadMode.EXTENDED)
class Neo4jSourceMapEncodingAvroExtendedIT : Neo4jSourceMapEncodingIT()

@KeyValueConverter(key = JSON_SCHEMA, value = JSON_SCHEMA, payloadMode = PayloadMode.COMPACT)
class Neo4jSourceMapEncodingJsonSchemaCompactIT : Neo4jSourceMapEncodingIT()

@KeyValueConverter(key = PROTOBUF, value = PROTOBUF, payloadMode = PayloadMode.COMPACT)
class Neo4jSourceMapEncodingProtobufCompactIT : Neo4jSourceMapEncodingIT() {

  @Disabled(
      "the Confluent Protobuf converter cannot serialise a list of maps, because Protobuf has no " +
          "repeated map field"
  )
  @Test
  override fun `map encoding should describe the elements of a list as maps`(
      consumer: ConvertingKafkaConsumer
  ) = Unit
}
