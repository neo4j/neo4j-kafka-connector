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
package org.neo4j.connectors.kafka.source.legacy

import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import org.apache.kafka.common.config.ConfigException
import org.junit.jupiter.api.Test
import org.neo4j.connectors.kafka.configuration.DeprecatedNeo4jConfiguration

class DeprecatedNeo4jSourceConfigurationTest {

  @Suppress("DEPRECATION")
  @Test
  fun `should not allow cdc as source type`() {
    assertFailsWith(ConfigException::class) {
          DeprecatedNeo4jSourceConfiguration(
              mapOf(
                  DeprecatedNeo4jConfiguration.SERVER_URI to "bolt://localhost",
                  DeprecatedNeo4jConfiguration.AUTHENTICATION_TYPE to "NONE",
                  DeprecatedNeo4jSourceConfiguration.TOPIC to "topic",
                  DeprecatedNeo4jSourceConfiguration.SOURCE_TYPE to "CDC"))
        }
        .also {
          assertEquals(
              "Invalid value CDC for configuration neo4j.source.type: Must be one of: 'QUERY'.",
              it.message)
        }
  }
}
