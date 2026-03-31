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
import org.neo4j.connectors.kafka.sink.strategy.DeleteNodeSinkAction
import org.neo4j.connectors.kafka.sink.strategy.NodeMatcher

class DeleteNodeTest {

  @Test
  fun `should create correct statement`() {
    val operation = DeleteNode(setOf("Person"), mapOf("name" to "john", "surname" to "doe"), false)

    operation.toAction() shouldBe
        DeleteNodeSinkAction(
            NodeMatcher.ByLabelsAndProperties(
                setOf("Person"),
                mapOf("name" to "john", "surname" to "doe"),
            ),
            false,
        )
  }

  @Test
  fun `should create correct statement with _id`() {
    val operation = DeleteNode(setOf("Person"), mapOf("_id" to 1), false)

    operation.toAction() shouldBe DeleteNodeSinkAction(NodeMatcher.ById(1), false)
  }

  @Test
  fun `should create correct statement with _elementId`() {
    val operation = DeleteNode(setOf("Person"), mapOf("_elementId" to "db:1"), false)

    operation.toAction() shouldBe DeleteNodeSinkAction(NodeMatcher.ByElementId("db:1"), false)
  }

  @Test
  fun `should create correct statement with detach delete`() {
    val operation = DeleteNode(setOf("Person"), mapOf("name" to "john", "surname" to "doe"), true)

    operation.toAction() shouldBe
        DeleteNodeSinkAction(
            NodeMatcher.ByLabelsAndProperties(
                setOf("Person"),
                mapOf("name" to "john", "surname" to "doe"),
            ),
            true,
        )
  }

  @Test
  fun `should create correct statement with multiple labels`() {
    val operation =
        DeleteNode(setOf("Person", "Employee"), mapOf("name" to "john", "surname" to "doe"), true)

    operation.toAction() shouldBe
        DeleteNodeSinkAction(
            NodeMatcher.ByLabelsAndProperties(
                setOf("Person", "Employee"),
                mapOf("name" to "john", "surname" to "doe"),
            ),
            true,
        )
  }

  @Test
  fun `should throw if no keys specified`() {
    val operation = DeleteNode(setOf("Person"), emptyMap())

    shouldThrow<InvalidDataException> { operation.toAction() } shouldHaveMessage
        "Node must contain at least one ID property."
  }
}
