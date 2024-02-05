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
package streams.kafka.connect.source

import java.time.Duration
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import org.neo4j.connectors.kafka.testing.MapSupport.asGeneric
import org.neo4j.connectors.kafka.testing.MapSupport.excludingKeys
import org.neo4j.connectors.kafka.testing.assertions.TopicVerifier
import org.neo4j.connectors.kafka.testing.format.KafkaConverter.AVRO
import org.neo4j.connectors.kafka.testing.format.KafkaConverter.JSON_SCHEMA
import org.neo4j.connectors.kafka.testing.format.KafkaConverter.PROTOBUF
import org.neo4j.connectors.kafka.testing.format.KeyValueConverter
import org.neo4j.connectors.kafka.testing.kafka.GenericKafkaConsumer
import org.neo4j.connectors.kafka.testing.source.LegacyNeo4jSource
import org.neo4j.connectors.kafka.testing.source.TopicConsumer
import org.neo4j.driver.Session

abstract class LegacyNeo4jSourceIT {

  companion object {
    const val TOPIC = "neo4j-source-topic"
  }

  @LegacyNeo4jSource(
      topic = TOPIC,
      streamingProperty = "timestamp",
      streamingFrom = "ALL",
      streamingQuery =
          "MATCH (ts:TestSource) WHERE ts.timestamp > \$lastCheck RETURN ts.name AS name, ts.surname AS surname, ts.timestamp AS timestamp, ts.execId AS execId")
  @Test
  fun `reads latest changes from legacy Neo4j source`(
      testInfo: TestInfo,
      @TopicConsumer(topic = TOPIC, offset = "earliest") consumer: GenericKafkaConsumer,
      session: Session
  ) {
    val executionId = testInfo.displayName + System.currentTimeMillis()

    session
        .run(
            "CREATE (:TestSource {name: 'jane', surname: 'doe', timestamp: datetime().epochMillis, execId: \$execId})",
            mapOf("execId" to executionId))
        .consume()
    session
        .run(
            "CREATE (:TestSource {name: 'john', surname: 'doe', timestamp: datetime().epochMillis, execId: \$execId})",
            mapOf("execId" to executionId))
        .consume()
    session
        .run(
            "CREATE (:TestSource {name: 'mary', surname: 'doe', timestamp: datetime().epochMillis, execId: \$execId})",
            mapOf("execId" to executionId))
        .consume()

    TopicVerifier.create(consumer, Map::class.java)
        .assertMessageValue { value ->
          assertThat(value.asGeneric().excludingKeys("timestamp"))
              .isEqualTo(mapOf("name" to "jane", "surname" to "doe", "execId" to executionId))
        }
        .assertMessageValue { value ->
          assertThat(value.asGeneric().excludingKeys("timestamp"))
              .isEqualTo(mapOf("name" to "john", "surname" to "doe", "execId" to executionId))
        }
        .assertMessageValue { value ->
          assertThat(value.asGeneric().excludingKeys("timestamp"))
              .isEqualTo(mapOf("name" to "mary", "surname" to "doe", "execId" to executionId))
        }
        .verifyWithin(Duration.ofSeconds(30))
  }

  @KeyValueConverter(key = AVRO, value = AVRO)
  class LegacyNeo4jSourceAvroIT : LegacyNeo4jSourceIT()

  @KeyValueConverter(key = JSON_SCHEMA, value = JSON_SCHEMA)
  class LegacyNeo4jSourceJsonIT : LegacyNeo4jSourceIT()

  @KeyValueConverter(key = PROTOBUF, value = PROTOBUF)
  class LegacyNeo4jSourceProtobufIT : LegacyNeo4jSourceIT()
}
