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

import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import org.junit.jupiter.api.Test
import org.neo4j.connectors.kafka.source.DeprecatedNeo4jSourceConfiguration

class Neo4jSourceConnectorTest {

  @Test
  fun `should not allow cdc as source type during validation`() {
    val connector = Neo4jSourceConnector()
    val config = connector.validate(mapOf(DeprecatedNeo4jSourceConfiguration.SOURCE_TYPE to "CDC"))

    val entry =
        config.configValues().first { it.name() == DeprecatedNeo4jSourceConfiguration.SOURCE_TYPE }
    assertNotNull(entry)
    assertEquals(listOf("QUERY"), entry.recommendedValues())
    assertTrue(entry.errorMessages().isNotEmpty())
    assertContains(
        entry.errorMessages(),
        "Invalid value CDC for configuration neo4j.source.type: Must be one of: 'QUERY'.")
  }
}
