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
import org.neo4j.connectors.kafka.sink.strategy.InvalidDataException
import org.neo4j.cypherdsl.parser.CypherParser
import org.neo4j.driver.Query

class DeleteRelationshipTest {
  @Test
  fun `should create correct statement`() {
    val operation =
        DeleteRelationship(
            "RELATED",
            NodeReference(setOf("LabelA"), mapOf("id" to 1), LookupMode.MATCH),
            NodeReference(setOf("LabelB"), mapOf("id" to 2), LookupMode.MATCH),
            emptyMap())

    operation.toQuery() shouldBe
        Query(
            CypherParser.parse(
                    """
                      MATCH (start:`LabelA` {id: ${'$'}start.keys.id})
                      WITH start 
                      MATCH (end:`LabelB` {id: ${'$'}end.keys.id}) 
                      WITH start, end 
                      MATCH (start)-[r:`RELATED` {}]->(end)
                      DELETE r
                    """
                        .trimIndent())
                .cypher,
            mapOf(
                "start" to mapOf("keys" to mapOf("id" to 1)),
                "end" to mapOf("keys" to mapOf("id" to 2)),
                "keys" to emptyMap()))
  }

  @Test
  fun `should create correct statement with keys`() {
    val operation =
        DeleteRelationship(
            "RELATED",
            NodeReference(setOf("LabelA"), mapOf("id" to 1), LookupMode.MATCH),
            NodeReference(setOf("LabelB"), mapOf("id" to 2), LookupMode.MATCH),
            mapOf("id" to 3))

    operation.toQuery() shouldBe
        Query(
            CypherParser.parse(
                    """
                      MATCH (start:`LabelA` {id: ${'$'}start.keys.id})
                      WITH start 
                      MATCH (end:`LabelB` {id: ${'$'}end.keys.id}) 
                      WITH start, end 
                      MATCH (start)-[r:`RELATED` {id: ${'$'}keys.id}]->(end)
                      DELETE r
                    """
                        .trimIndent())
                .cypher,
            mapOf(
                "start" to mapOf("keys" to mapOf("id" to 1)),
                "end" to mapOf("keys" to mapOf("id" to 2)),
                "keys" to mapOf("id" to 3)))
  }

  @Test
  fun `should create correct statement with multiple labels`() {
    val operation =
        DeleteRelationship(
            "RELATED",
            NodeReference(setOf("LabelA", "LabelC"), mapOf("id" to 1), LookupMode.MATCH),
            NodeReference(setOf("LabelB", "LabelD"), mapOf("id" to 2), LookupMode.MATCH),
            emptyMap())

    operation.toQuery() shouldBe
        Query(
            CypherParser.parse(
                    """
                      MATCH (start:`LabelA`:`LabelC` {id: ${'$'}start.keys.id})
                      WITH start 
                      MATCH (end:`LabelB`:`LabelD` {id: ${'$'}end.keys.id}) 
                      WITH start, end 
                      MATCH (start)-[r:`RELATED` {}]->(end) DELETE r
                    """
                        .trimIndent())
                .cypher,
            mapOf(
                "start" to mapOf("keys" to mapOf("id" to 1)),
                "end" to mapOf("keys" to mapOf("id" to 2)),
                "keys" to emptyMap()))
  }

  @Test
  fun `should throw when start and end nodes don't specify ids`() {
    val node1 = NodeReference(setOf("LabelA"), emptyMap())
    val node2 = NodeReference(setOf("LabelB"), mapOf("id" to 2))

    listOf(Pair(node1, node2), Pair(node2, node1), Pair(node1, node1)).forEach { (from, to) ->
      withClue("from: $from, to: $to") {
        val operation = DeleteRelationship("RELATED", from, to, emptyMap())

        org.junit.jupiter.api.assertThrows<InvalidDataException> {
          operation.toQuery()
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
            emptyMap())

    org.junit.jupiter.api.assertThrows<InvalidDataException> {
      operation.toQuery()
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
          operation.toQuery()
        } shouldHaveMessage
            "'from' and 'to' must have 'op' as 'MATCH' for relationship deletion operations."
      }
    }
  }
}
