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
package org.neo4j.connectors.kafka.sink.strategy.cdc

import io.kotest.matchers.ints.negative
import io.kotest.matchers.ints.positive
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test
import org.neo4j.connectors.kafka.events.EntityType

class GroupingKeyTest {

  private fun key(
      entityType: EntityType = EntityType.node,
      operationType: String = "merge",
      vararg propertyKeys: Set<String>,
  ): GroupingKey =
      GroupingKey(
          entityType = entityType,
          operationType = operationType,
          propertyKeys = propertyKeys.toList(),
      )

  @Test
  fun `orders nodes before relationships`() {
    key(entityType = EntityType.node).compareTo(key(entityType = EntityType.relationship)) shouldBe
        negative()
  }

  @Test
  fun `orders by operation type lexicographically`() {
    key(operationType = "delete").compareTo(key(operationType = "merge")) shouldBe negative()
  }

  @Test
  fun `orders by property key set size`() {
    key(propertyKeys = arrayOf(setOf("a")))
        .compareTo(key(propertyKeys = arrayOf(setOf("a", "b")))) shouldBe negative()
  }

  @Test
  fun `orders by property key set contents when sizes match`() {
    key(propertyKeys = arrayOf(setOf("a", "c")))
        .compareTo(key(propertyKeys = arrayOf(setOf("a", "b")))) shouldBe positive()
  }

  @Test
  fun `orders by property key list length when prefix matches`() {
    key(propertyKeys = arrayOf(setOf("a"), setOf("b")))
        .compareTo(key(propertyKeys = arrayOf(setOf("a")))) shouldBe positive()
  }

  @Test
  fun `orders by later property key sets when earlier sets match`() {
    key(propertyKeys = arrayOf(setOf("a"), setOf("b")))
        .compareTo(key(propertyKeys = arrayOf(setOf("a"), setOf("c")))) shouldBe negative()
  }

  @Test
  fun `orders by set size inside later property key sets`() {
    key(propertyKeys = arrayOf(setOf("a"), setOf("b")))
        .compareTo(key(propertyKeys = arrayOf(setOf("a"), setOf("b", "c")))) shouldBe negative()
  }

  @Test
  fun `returns zero for identical property key set`() {
    key(propertyKeys = arrayOf(setOf("a")))
        .compareTo(key(propertyKeys = arrayOf(setOf("a")))) shouldBe 0
  }

  @Test
  fun `returns zero for identical property key sets`() {
    key(propertyKeys = arrayOf(setOf("a"), setOf("b", "c")))
        .compareTo(key(propertyKeys = arrayOf(setOf("a"), setOf("b", "c")))) shouldBe 0
  }
}
