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
package org.neo4j.connectors.kafka.configuration.helpers

import java.util.function.Predicate
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import org.junit.jupiter.api.Test
import org.neo4j.connectors.kafka.configuration.AuthenticationType
import org.neo4j.connectors.kafka.configuration.ConnectorType

class RecommendersTest {

  @Test
  fun `should and chained recommenders`() {
    Recommenders.and(
            Recommenders.enum(ConnectorType::class.java),
            Recommenders.visibleIf("test.1", Predicate.isEqual("a")),
            Recommenders.visibleIf("test.2", Predicate.isEqual("b")))
        .apply {
          assertEquals(listOf("SINK", "SOURCE"), this.validValues("my.property", mapOf()))

          assertFalse(this.visible("my.property", mapOf()))
          assertFalse(this.visible("my.property", mapOf("test.1" to "a")))
          assertFalse(this.visible("my.property", mapOf("test.1" to "a", "test.2" to "c")))
          assertTrue(this.visible("my.property", mapOf("test.1" to "a", "test.2" to "b")))
        }
  }

  @Test
  fun `should return enum entries`() {
    Recommenders.enum(AuthenticationType::class.java).apply {
      assertTrue(this.visible("my.property", emptyMap()))
      assertEquals(
          listOf("NONE", "BASIC", "KERBEROS", "BEARER", "CUSTOM"),
          this.validValues("my.property", mapOf()))
    }

    Recommenders.enum(
            AuthenticationType::class.java, AuthenticationType.NONE, AuthenticationType.CUSTOM)
        .apply {
          assertTrue(this.visible("my.property", emptyMap()))
          assertEquals(
              listOf("BASIC", "KERBEROS", "BEARER"), this.validValues("my.property", mapOf()))
        }
  }

  @Test
  fun `should return visible if predicate matches`() {
    Recommenders.visibleIf("test.1", Predicate.isEqual("value")).apply {
      assertFalse(this.visible("my.property", emptyMap()))
      assertFalse(this.visible("my.property", mapOf("test.2" to "value")))
      assertFalse(this.visible("my.property", mapOf("test.1" to "")))
      assertFalse(this.visible("my.property", mapOf("test.1" to "another value")))
      assertTrue(this.visible("my.property", mapOf("test.1" to "value")))
    }
  }

  @Test
  fun `should return visible if matching dependent configuration values are not empty`() {
    Recommenders.visibleIfNotEmpty { k -> k.startsWith("test") }
        .apply {

          // no dependent config that starts with "test"
          assertFalse(this.visible("my.property", emptyMap()))
          // dependent is empty string
          assertFalse(this.visible("my.property", mapOf("test.1" to "")))
          // dependent is empty list
          assertFalse(this.visible("my.property", mapOf("test.1" to listOf<Any>())))
          // dependent is provided as a string
          assertTrue(this.visible("my.property", mapOf("test.1" to "value")))
          // dependent is provided as a list
          assertTrue(this.visible("my.property", mapOf("test.1" to listOf(1))))
          // dependent is multiple values
          assertTrue(this.visible("my.property", mapOf("test.1" to "value", "test.2" to "value")))
          // dependent is multiple values, one empty
          assertTrue(
              this.visible(
                  "my.property", mapOf("test.1" to "value", "test.2" to "value", "test.3" to "")))
        }
  }
}
