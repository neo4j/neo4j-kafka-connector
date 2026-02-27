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

import io.kotest.assertions.withClue
import io.kotest.matchers.shouldBe
import io.kotest.matchers.throwable.shouldHaveMessage
import org.junit.jupiter.api.Test
import org.neo4j.connectors.kafka.exceptions.InvalidDataException
import org.neo4j.connectors.kafka.sink.strategy.DeleteRelationshipSinkAction
import org.neo4j.connectors.kafka.sink.strategy.LookupMode
import org.neo4j.connectors.kafka.sink.strategy.NodeMatcher
import org.neo4j.connectors.kafka.sink.strategy.RelationshipMatcher
import org.neo4j.connectors.kafka.sink.strategy.SinkActionNodeReference

class DeleteRelationshipTest {
  @Test
  fun `should create correct statement`() {
    val operation =
        DeleteRelationship(
            "RELATED",
            NodeReference(setOf("LabelA"), mapOf("id" to 1), LookupMode.MATCH),
            NodeReference(setOf("LabelB"), mapOf("id" to 2), LookupMode.MATCH),
            emptyMap(),
        )

    operation.toAction() shouldBe
        DeleteRelationshipSinkAction(
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(setOf("LabelA"), mapOf("id" to 1)),
                LookupMode.MATCH,
            ),
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(setOf("LabelB"), mapOf("id" to 2)),
                LookupMode.MATCH,
            ),
            RelationshipMatcher.ByTypeAndProperties("RELATED", emptyMap(), false),
        )
  }

  @Test
  fun `should create correct statement with keys`() {
    val operation =
        DeleteRelationship(
            "RELATED",
            NodeReference(setOf("LabelA"), mapOf("id" to 1), LookupMode.MATCH),
            NodeReference(setOf("LabelB"), mapOf("id" to 2), LookupMode.MATCH),
            mapOf("id" to 3),
        )

    operation.toAction() shouldBe
        DeleteRelationshipSinkAction(
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(setOf("LabelA"), mapOf("id" to 1)),
                LookupMode.MATCH,
            ),
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(setOf("LabelB"), mapOf("id" to 2)),
                LookupMode.MATCH,
            ),
            RelationshipMatcher.ByTypeAndProperties("RELATED", mapOf("id" to 3), true),
        )
  }

  @Test
  fun `should create correct statement with _id`() {
    val operation =
        DeleteRelationship(
            "RELATED",
            NodeReference(setOf("LabelA"), mapOf("_id" to 1), LookupMode.MATCH),
            NodeReference(setOf("LabelB"), mapOf("_id" to 2), LookupMode.MATCH),
            emptyMap(),
        )

    operation.toAction() shouldBe
        DeleteRelationshipSinkAction(
            SinkActionNodeReference(NodeMatcher.ById(1), LookupMode.MATCH),
            SinkActionNodeReference(NodeMatcher.ById(2), LookupMode.MATCH),
            RelationshipMatcher.ByTypeAndProperties("RELATED", emptyMap(), false),
        )
  }

  @Test
  fun `should create correct statement with _elementId`() {
    val operation =
        DeleteRelationship(
            "RELATED",
            NodeReference(setOf("LabelA"), mapOf("_elementId" to "db:1"), LookupMode.MATCH),
            NodeReference(setOf("LabelB"), mapOf("_elementId" to "db:2"), LookupMode.MATCH),
            emptyMap(),
        )

    operation.toAction() shouldBe
        DeleteRelationshipSinkAction(
            SinkActionNodeReference(NodeMatcher.ByElementId("db:1"), LookupMode.MATCH),
            SinkActionNodeReference(NodeMatcher.ByElementId("db:2"), LookupMode.MATCH),
            RelationshipMatcher.ByTypeAndProperties("RELATED", emptyMap(), false),
        )
  }

  @Test
  fun `should create correct statement with multiple labels`() {
    val operation =
        DeleteRelationship(
            "RELATED",
            NodeReference(setOf("LabelA", "LabelC"), mapOf("id" to 1), LookupMode.MATCH),
            NodeReference(setOf("LabelB", "LabelD"), mapOf("id" to 2), LookupMode.MATCH),
            emptyMap(),
        )

    operation.toAction() shouldBe
        DeleteRelationshipSinkAction(
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(setOf("LabelA", "LabelC"), mapOf("id" to 1)),
                LookupMode.MATCH,
            ),
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(setOf("LabelB", "LabelD"), mapOf("id" to 2)),
                LookupMode.MATCH,
            ),
            RelationshipMatcher.ByTypeAndProperties("RELATED", emptyMap(), false),
        )
  }

  @Test
  fun `should throw when start and end nodes don't specify ids`() {
    val node1 = NodeReference(setOf("LabelA"), emptyMap())
    val node2 = NodeReference(setOf("LabelB"), mapOf("id" to 2))

    listOf(Pair(node1, node2), Pair(node2, node1), Pair(node1, node1)).forEach { (from, to) ->
      withClue("from: $from, to: $to") {
        val operation = DeleteRelationship("RELATED", from, to, emptyMap())

        org.junit.jupiter.api.assertThrows<InvalidDataException> {
          operation.toAction()
        } shouldHaveMessage "'from' and 'to' must contain at least one ID property."
      }
    }
  }

  @Test
  fun `should throw when rel_type is missing`() {
    val operation =
        DeleteRelationship(
            "",
            NodeReference(setOf("LabelA", "LabelC"), mapOf("id" to 1), LookupMode.MATCH),
            NodeReference(setOf("LabelB", "LabelD"), mapOf("id" to 2), LookupMode.MERGE),
            emptyMap(),
        )

    org.junit.jupiter.api.assertThrows<InvalidDataException> {
      operation.toAction()
    } shouldHaveMessage "'rel_type' must be specified."
  }

  @Test
  fun `should throw when lookup mode is not match`() {
    val node1 = NodeReference(setOf("LabelA"), mapOf("id" to 1), LookupMode.MERGE)
    val node2 = NodeReference(setOf("LabelB"), mapOf("id" to 2), LookupMode.MATCH)

    listOf(Pair(node1, node2), Pair(node2, node1), Pair(node1, node1)).forEach { (from, to) ->
      withClue("from: $from, to: $to") {
        val operation = DeleteRelationship("RELATED", from, to, emptyMap())

        org.junit.jupiter.api.assertThrows<InvalidDataException> {
          operation.toAction()
        } shouldHaveMessage
            "'from' and 'to' must have 'op' as 'MATCH' for relationship deletion operations."
      }
    }
  }
}
