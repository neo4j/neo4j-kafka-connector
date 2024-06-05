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
package org.neo4j.connectors.kafka.sink.strategy.cud

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import io.kotest.matchers.throwable.shouldHaveMessage
import org.junit.jupiter.api.Test
import org.neo4j.connectors.kafka.exceptions.InvalidDataException
import org.neo4j.driver.Query

class MergeNodeTest {
  @Test
  fun `should create correct statement`() {
    val operation =
        MergeNode(setOf("Person"), mapOf("name" to "john", "surname" to "doe"), mapOf("age" to 18))

    operation.toQuery() shouldBe
        Query(
            "MERGE (n:`Person` {name: ${'$'}keys.name, surname: ${'$'}keys.surname}) SET n += ${'$'}properties",
            mapOf(
                "keys" to mapOf("name" to "john", "surname" to "doe"),
                "properties" to mapOf("age" to 18)))
  }

  @Test
  fun `should create correct statement with _id`() {
    val operation = MergeNode(setOf("Person"), mapOf("_id" to 1), mapOf("age" to 18))

    operation.toQuery() shouldBe
        Query(
            "MATCH (n) WHERE id(n) = ${'$'}keys._id SET n += ${'$'}properties",
            mapOf("keys" to mapOf("_id" to 1), "properties" to mapOf("age" to 18)))
  }

  @Test
  fun `should create correct statement with _elementId`() {
    val operation = MergeNode(setOf("Person"), mapOf("_elementId" to "db:1"), mapOf("age" to 18))

    operation.toQuery() shouldBe
        Query(
            "MATCH (n) WHERE elementId(n) = ${'$'}keys._elementId SET n += ${'$'}properties",
            mapOf("keys" to mapOf("_elementId" to "db:1"), "properties" to mapOf("age" to 18)))
  }

  @Test
  fun `should create correct statement with multiple labels`() {
    val operation =
        MergeNode(
            setOf("Person", "Employee"),
            mapOf("name" to "john", "surname" to "doe"),
            mapOf("age" to 18))

    operation.toQuery() shouldBe
        Query(
            "MERGE (n:`Person`:`Employee` {name: ${'$'}keys.name, surname: ${'$'}keys.surname}) SET n += ${'$'}properties",
            mapOf(
                "keys" to mapOf("name" to "john", "surname" to "doe"),
                "properties" to mapOf("age" to 18)))
  }

  @Test
  fun `should throw if no keys specified`() {
    val operation = MergeNode(setOf("Person"), emptyMap(), mapOf("age" to 18))

    shouldThrow<InvalidDataException> { operation.toQuery() } shouldHaveMessage
        "Node must contain at least one ID property."
  }
}
