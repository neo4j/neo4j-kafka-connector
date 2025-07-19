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

import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test
import org.neo4j.driver.Query

class CreateNodeTest {

  @Test
  fun `should create correct statement`() {
    val operation =
        CreateNode(setOf("Person"), mapOf("id" to 1, "name" to "john", "surname" to "doe"))

    operation.toQuery() shouldBe
        Query(
            "CREATE (n:`Person` {}) SET n = ${'$'}properties",
            mapOf("properties" to mapOf("id" to 1, "name" to "john", "surname" to "doe")),
        )
  }

  @Test
  fun `should create correct statement with multiple labels`() {
    val operation =
        CreateNode(
            setOf("Person", "Employee"),
            mapOf("id" to 1, "name" to "john", "surname" to "doe"),
        )

    operation.toQuery() shouldBe
        Query(
            "CREATE (n:`Person`:`Employee` {}) SET n = ${'$'}properties",
            mapOf("properties" to mapOf("id" to 1, "name" to "john", "surname" to "doe")),
        )
  }

  @Test
  fun `should create correct statement without labels`() {
    val operation = CreateNode(emptySet(), mapOf("id" to 1, "name" to "john", "surname" to "doe"))

    operation.toQuery() shouldBe
        Query(
            "CREATE (n {}) SET n = ${'$'}properties",
            mapOf("properties" to mapOf("id" to 1, "name" to "john", "surname" to "doe")),
        )
  }
}
