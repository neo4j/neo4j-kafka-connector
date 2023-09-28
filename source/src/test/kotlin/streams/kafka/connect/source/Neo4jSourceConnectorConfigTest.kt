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

import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import org.apache.kafka.common.config.ConfigException
import org.junit.jupiter.api.Test

class Neo4jSourceConnectorConfigTest {

  @Test
  fun `should throw a ConfigException because of unsupported streaming type`() {
    val exception =
        assertFailsWith(ConfigException::class) {
          val originals =
              mapOf(
                  Neo4jSourceConnectorConfig.SOURCE_TYPE to "labels",
                  Neo4jSourceConnectorConfig.TOPIC to "topic",
                  Neo4jSourceConnectorConfig.STREAMING_FROM to StreamingFrom.NOW.toString(),
                  Neo4jSourceConnectorConfig.STREAMING_PROPERTY to "timestamp")
          Neo4jSourceConnectorConfig(originals)
        }

    assertEquals("Supported source query types are: ${SourceType.QUERY}", exception.message)
  }

  @Test
  fun `should throw a ConfigException because of empty query`() {
    val exception =
        assertFailsWith(ConfigException::class) {
          val originals =
              mapOf(
                  Neo4jSourceConnectorConfig.SOURCE_TYPE to SourceType.QUERY.toString(),
                  Neo4jSourceConnectorConfig.TOPIC to "topic",
                  Neo4jSourceConnectorConfig.STREAMING_FROM to StreamingFrom.NOW.toString(),
                  Neo4jSourceConnectorConfig.STREAMING_PROPERTY to "timestamp")
          Neo4jSourceConnectorConfig(originals)
        }

    assertEquals(
        "You need to define: ${Neo4jSourceConnectorConfig.SOURCE_TYPE_QUERY}", exception.message)
  }

  @Test
  fun `should return config`() {
    val originals =
        mapOf(
            Neo4jSourceConnectorConfig.SOURCE_TYPE to SourceType.QUERY.toString(),
            Neo4jSourceConnectorConfig.SOURCE_TYPE_QUERY to "MATCH (n) RETURN n",
            Neo4jSourceConnectorConfig.TOPIC to "topic",
            Neo4jSourceConnectorConfig.STREAMING_POLL_INTERVAL to "10",
            Neo4jSourceConnectorConfig.STREAMING_FROM to StreamingFrom.NOW.toString(),
            Neo4jSourceConnectorConfig.STREAMING_PROPERTY to "timestamp")
    val config = Neo4jSourceConnectorConfig(originals)
    assertEquals(originals[Neo4jSourceConnectorConfig.TOPIC], config.topic)
    assertEquals(originals[Neo4jSourceConnectorConfig.SOURCE_TYPE_QUERY], config.query)
    assertEquals(originals[Neo4jSourceConnectorConfig.STREAMING_PROPERTY], config.streamingProperty)
    assertEquals(
        originals[Neo4jSourceConnectorConfig.STREAMING_FROM], config.streamingFrom.toString())
    assertEquals(
        originals[Neo4jSourceConnectorConfig.STREAMING_POLL_INTERVAL]?.toInt(), config.pollInterval)
  }

  @Test
  fun `should return config null streaming property`() {
    val originals =
        mapOf(
            Neo4jSourceConnectorConfig.SOURCE_TYPE to SourceType.QUERY.toString(),
            Neo4jSourceConnectorConfig.SOURCE_TYPE_QUERY to "MATCH (n) RETURN n",
            Neo4jSourceConnectorConfig.TOPIC to "topic",
            Neo4jSourceConnectorConfig.STREAMING_POLL_INTERVAL to "10",
            Neo4jSourceConnectorConfig.STREAMING_FROM to StreamingFrom.NOW.toString())
    val config = Neo4jSourceConnectorConfig(originals)
    assertEquals("", config.streamingProperty)
  }
}
