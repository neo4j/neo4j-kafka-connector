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
import org.neo4j.cypherdsl.parser.CypherParser
import org.neo4j.driver.Query

class MergeRelationshipTest {
  @Test
  fun `should create correct statement`() {
    val operation =
        MergeRelationship(
            "RELATED",
            NodeReference(setOf("LabelA"), mapOf("id" to 1), LookupMode.MATCH),
            NodeReference(setOf("LabelB"), mapOf("id" to 2), LookupMode.MATCH),
            emptyMap(),
            mapOf("prop1" to 1, "prop2" to "test", "prop3" to true),
        )

    operation.toQuery() shouldBe
        Query(
            CypherParser.parse(
                    """
                      MATCH (start:`LabelA` {id: ${'$'}start.keys.id})
                      WITH start 
                      MATCH (end:`LabelB` {id: ${'$'}end.keys.id}) 
                      WITH start, end 
                      MERGE (start)-[r:`RELATED` {}]->(end)
                      SET r += ${'$'}properties
                    """
                        .trimIndent()
                )
                .cypher,
            mapOf(
                "start" to mapOf("keys" to mapOf("id" to 1)),
                "end" to mapOf("keys" to mapOf("id" to 2)),
                "keys" to emptyMap(),
                "properties" to mapOf("prop1" to 1, "prop2" to "test", "prop3" to true),
            ),
        )
  }

  @Test
  fun `should create correct statement with specified lookup modes`() {
    val operation =
        MergeRelationship(
            "RELATED",
            NodeReference(setOf("LabelA"), mapOf("id" to 1), LookupMode.MATCH),
            NodeReference(setOf("LabelB"), mapOf("id" to 2), LookupMode.MERGE),
            emptyMap(),
            mapOf("prop1" to 1, "prop2" to "test", "prop3" to true),
        )

    operation.toQuery() shouldBe
        Query(
            CypherParser.parse(
                    """
                      MATCH (start:`LabelA` {id: ${'$'}start.keys.id})
                      WITH start 
                      MERGE (end:`LabelB` {id: ${'$'}end.keys.id}) 
                      WITH start, end 
                      MERGE (start)-[r:`RELATED` {}]->(end)
                      SET r += ${'$'}properties
                    """
                        .trimIndent()
                )
                .cypher,
            mapOf(
                "start" to mapOf("keys" to mapOf("id" to 1)),
                "end" to mapOf("keys" to mapOf("id" to 2)),
                "keys" to emptyMap(),
                "properties" to mapOf("prop1" to 1, "prop2" to "test", "prop3" to true),
            ),
        )
  }

  @Test
  fun `should create correct statement with keys`() {
    val operation =
        MergeRelationship(
            "RELATED",
            NodeReference(setOf("LabelA"), mapOf("id" to 1), LookupMode.MATCH),
            NodeReference(setOf("LabelB"), mapOf("id" to 2), LookupMode.MATCH),
            mapOf("id" to 3),
            mapOf("prop1" to 1, "prop2" to "test", "prop3" to true),
        )

    operation.toQuery() shouldBe
        Query(
            CypherParser.parse(
                    """
                      MATCH (start:`LabelA` {id: ${'$'}start.keys.id})
                      WITH start 
                      MATCH (end:`LabelB` {id: ${'$'}end.keys.id}) 
                      WITH start, end 
                      MERGE (start)-[r:`RELATED` {id: ${'$'}keys.id}]->(end)
                      SET r += ${'$'}properties
                    """
                        .trimIndent()
                )
                .cypher,
            mapOf(
                "start" to mapOf("keys" to mapOf("id" to 1)),
                "end" to mapOf("keys" to mapOf("id" to 2)),
                "keys" to mapOf("id" to 3),
                "properties" to mapOf("prop1" to 1, "prop2" to "test", "prop3" to true),
            ),
        )
  }

  @Test
  fun `should create correct statement with _id`() {
    val operation =
        MergeRelationship(
            "RELATED",
            NodeReference(setOf("LabelA"), mapOf("_id" to 1), LookupMode.MATCH),
            NodeReference(setOf("LabelB"), mapOf("_id" to 2), LookupMode.MATCH),
            emptyMap(),
            mapOf("prop1" to 1, "prop2" to "test", "prop3" to true),
        )

    operation.toQuery() shouldBe
        Query(
            CypherParser.parse(
                    """
                      MATCH (start) WHERE id(start) = ${'$'}start.keys._id
                      WITH start 
                      MATCH (end) WHERE id(end) = ${'$'}end.keys._id 
                      WITH start, end 
                      MERGE (start)-[r:`RELATED` {}]->(end) 
                      SET r += ${'$'}properties
                    """
                        .trimIndent()
                )
                .cypher,
            mapOf(
                "start" to mapOf("keys" to mapOf("_id" to 1)),
                "end" to mapOf("keys" to mapOf("_id" to 2)),
                "keys" to emptyMap(),
                "properties" to mapOf("prop1" to 1, "prop2" to "test", "prop3" to true),
            ),
        )
  }

  @Test
  fun `should create correct statement with _elementId`() {
    val operation =
        MergeRelationship(
            "RELATED",
            NodeReference(setOf("LabelA"), mapOf("_elementId" to "db:1"), LookupMode.MATCH),
            NodeReference(setOf("LabelB"), mapOf("_elementId" to "db:2"), LookupMode.MATCH),
            emptyMap(),
            mapOf("prop1" to 1, "prop2" to "test", "prop3" to true),
        )

    operation.toQuery() shouldBe
        Query(
            """
            MATCH (start) WHERE elementId(start) = ${'$'}start.keys._elementId 
            WITH start 
            MATCH (end) WHERE elementId(end) = ${'$'}end.keys._elementId 
            WITH start, end 
            MERGE (start)-[r:`RELATED` {}]->(end) 
            SET r += ${'$'}properties
            """
                .trimIndent()
                .replace(System.lineSeparator(), ""),
            mapOf(
                "start" to mapOf("keys" to mapOf("_elementId" to "db:1")),
                "end" to mapOf("keys" to mapOf("_elementId" to "db:2")),
                "keys" to emptyMap(),
                "properties" to mapOf("prop1" to 1, "prop2" to "test", "prop3" to true),
            ),
        )
  }

  @Test
  fun `should create correct statement with multiple labels`() {
    val operation =
        MergeRelationship(
            "RELATED",
            NodeReference(setOf("LabelA", "LabelC"), mapOf("id" to 1), LookupMode.MATCH),
            NodeReference(setOf("LabelB", "LabelD"), mapOf("id" to 2), LookupMode.MATCH),
            emptyMap(),
            mapOf("prop1" to 1, "prop2" to "test", "prop3" to true),
        )

    operation.toQuery() shouldBe
        Query(
            CypherParser.parse(
                    """
                      MATCH (start:`LabelA`:`LabelC` {id: ${'$'}start.keys.id})
                      WITH start 
                      MATCH (end:`LabelB`:`LabelD` {id: ${'$'}end.keys.id}) 
                      WITH start, end 
                      MERGE (start)-[r:`RELATED` {}]->(end)
                      SET r += ${'$'}properties
                    """
                        .trimIndent()
                )
                .cypher,
            mapOf(
                "start" to mapOf("keys" to mapOf("id" to 1)),
                "end" to mapOf("keys" to mapOf("id" to 2)),
                "keys" to emptyMap(),
                "properties" to mapOf("prop1" to 1, "prop2" to "test", "prop3" to true),
            ),
        )
  }

  @Test
  fun `should throw when start and end nodes don't specify ids`() {
    val node1 = NodeReference(setOf("LabelA"), emptyMap())
    val node2 = NodeReference(setOf("LabelB"), mapOf("id" to 2))

    listOf(Pair(node1, node2), Pair(node2, node1), Pair(node1, node1)).forEach { (from, to) ->
      withClue("from: $from, to: $to") {
        val operation = MergeRelationship("RELATED", from, to, emptyMap(), mapOf("prop1" to 1))

        org.junit.jupiter.api.assertThrows<InvalidDataException> {
          operation.toQuery()
        } shouldHaveMessage "'from' and 'to' must contain at least one ID property."
      }
    }
  }

  @Test
  fun `should throw when rel_type is missing`() {
    val operation =
        MergeRelationship(
            "",
            NodeReference(setOf("LabelA", "LabelC"), mapOf("id" to 1), LookupMode.MATCH),
            NodeReference(setOf("LabelB", "LabelD"), mapOf("id" to 2), LookupMode.MERGE),
            emptyMap(),
            mapOf("prop1" to 1),
        )

    org.junit.jupiter.api.assertThrows<InvalidDataException> {
      operation.toQuery()
    } shouldHaveMessage "'rel_type' must be specified."
  }
}
