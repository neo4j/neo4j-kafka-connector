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
package org.neo4j.connectors.kafka.configuration

import kotlin.test.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class Neo4jVersionTest {

  @Test
  fun `should parse version string`() {
    assertEquals(Neo4jVersion.of("4.4"), Neo4jVersion(4, 4))
    assertEquals(Neo4jVersion.of("4.4-aura"), Neo4jVersion(4, 4))
    assertEquals(Neo4jVersion.of("4.4.13"), Neo4jVersion(4, 4, 13))
    assertEquals(Neo4jVersion.of("2025.01"), Neo4jVersion(2025, 1))
    assertEquals(Neo4jVersion.of("2025.01.0"), Neo4jVersion(2025, 1, 0))
    assertEquals(Neo4jVersion.of("2025.01-aura"), Neo4jVersion(2025, 1))
  }

  @Test
  fun `should fail invalid version format`() {
    assertThrows<IllegalArgumentException> { Neo4jVersion.of("") }
    assertThrows<IllegalArgumentException> { Neo4jVersion.of("5") }
    assertThrows<IllegalArgumentException> { Neo4jVersion.of("5.5.3.1") }
    assertThrows<NumberFormatException> { Neo4jVersion.of("..") }
    assertThrows<NumberFormatException> { Neo4jVersion.of("5..3") }
    assertThrows<NumberFormatException> { Neo4jVersion.of(".2025.6") }
  }
}
