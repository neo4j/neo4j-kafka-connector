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
package org.neo4j.connectors.kafka.utils

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import io.kotest.matchers.throwable.shouldHaveMessage
import org.junit.jupiter.api.Test
import org.neo4j.connectors.kafka.exceptions.InvalidDataException
import org.neo4j.connectors.kafka.utils.MapUtils.getMap

class MapUtilsTest {

  @Test
  fun `should keep null values`() {
    val message = mapOf("properties" to mapOf("name" to null))

    message.getMap<String, Any?>("properties") shouldBe mapOf("name" to null)
  }

  @Test
  fun `should keep a mix of null and non-null values`() {
    val message = mapOf("properties" to mapOf("name" to null, "age" to 21))

    message.getMap<String, Any?>("properties") shouldBe mapOf("name" to null, "age" to 21)
  }

  @Test
  fun `should reject a wrong value type`() {
    val message = mapOf("properties" to mapOf("name" to 21))

    shouldThrow<InvalidDataException> { message.getMap<String, String>("properties") }
        .shouldHaveMessage("Values of 'properties' is not an instance of String")
  }

  @Test
  fun `should reject a wrong key type`() {
    val message = mapOf("properties" to mapOf(1 to "name"))

    shouldThrow<InvalidDataException> { message.getMap<String, String>("properties") }
        .shouldHaveMessage("Keys of 'properties' is not an instance of String")
  }

  @Test
  fun `should reject a value that is not a map`() {
    val message = mapOf("properties" to "not a map")

    shouldThrow<InvalidDataException> { message.getMap<String, Any?>("properties") }
        .shouldHaveMessage("Map element 'properties' is not an instance of Map")
  }
}
