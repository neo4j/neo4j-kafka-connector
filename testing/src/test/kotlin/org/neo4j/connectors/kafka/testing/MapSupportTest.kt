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
package org.neo4j.connectors.kafka.testing

import java.lang.IllegalStateException
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.neo4j.connectors.kafka.testing.MapSupport.nestUnder

class MapSupportTest {

  @Test
  fun `nests map under specified key`() {
    val initialMap = mutableMapOf<String, Any>("foo" to mutableMapOf("bar" to "baz"))

    val result = initialMap.nestUnder("foo", mapOf("fighters" to "rocks"))

    assertEquals(mutableMapOf("foo" to mutableMapOf("bar" to "baz", "fighters" to "rocks")), result)
  }

  @Test
  fun `fails to nest map if key's value is not a map`() {
    val initialMap = mutableMapOf<String, Any>("foo" to "bar")

    val exception =
        assertThrows<IllegalStateException> {
          initialMap.nestUnder("foo", mapOf("fighters" to "rocks"))
        }
    assertEquals(exception.message, "entry at key foo is not a mutable map")
  }
}
