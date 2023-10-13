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

import java.time.Duration
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import org.neo4j.connectors.kafka.source.testing.Neo4jSource
import org.neo4j.connectors.kafka.source.testing.TopicVerifier
import org.neo4j.driver.Session

class Neo4jSourceIT {

  companion object {
    const val TOPIC = "neo4j-source-topic"
  }

  @Neo4jSource(
      topic = TOPIC,
      streamingProperty = "timestamp",
      streamingFrom = StreamingFrom.ALL,
      streamingQuery =
          "MATCH (ts:TestSource) WHERE ts.timestamp > \$lastCheck RETURN ts.name AS name, ts.surname AS surname, ts.timestamp AS timestamp, ts.execId AS execId",
      consumerOffset = "latest",
  )
  @Test
  fun `reads latest changes from Neo4j source`(
      testInfo: TestInfo,
      consumer: KafkaConsumer<String, GenericRecord>,
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

    TopicVerifier.create(consumer)
        .expectMessageValueMatching { value ->
          value.asMap().excludingKeys("timestamp") ==
              mapOf("name" to "jane", "surname" to "doe", "execId" to executionId)
        }
        .expectMessageValueMatching { value ->
          value.asMap().excludingKeys("timestamp") ==
              mapOf("name" to "john", "surname" to "doe", "execId" to executionId)
        }
        .expectMessageValueMatching { value ->
          value.asMap().excludingKeys("timestamp") ==
              mapOf("name" to "mary", "surname" to "doe", "execId" to executionId)
        }
        .verifyWithin(Duration.ofSeconds(300))
  }
}

fun GenericRecord.asMap(): Map<String, String> {
  // FIXME: properly convert values
  return this.schema.fields.associate { field -> field.name() to this.get(field.name()).toString() }
}

/**
 * Filters out all specified keys from map
 *
 * @throws IllegalArgumentException if any of the specified keys are not part of this map set of
 *   keys
 */
fun <K, V> Map<K, V>.excludingKeys(vararg keys: K): Map<K, V> {
  val missing = keys.filter { !this.keys.contains(it) }
  if (missing.isNotEmpty()) {
    throw IllegalArgumentException(
        "Cannot exclude keys ${missing.joinToString()}: they are missing from map $this")
  }
  val exclusions = setOf(*keys)
  return this.filterKeys { !exclusions.contains(it) }
}
