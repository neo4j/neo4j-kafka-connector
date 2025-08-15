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
package org.neo4j.connectors.kafka.sink.strategy

import io.kotest.matchers.shouldBe
import io.kotest.matchers.throwable.shouldHaveMessage
import java.time.Instant
import java.time.ZoneOffset
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.neo4j.connectors.kafka.data.ConstraintData
import org.neo4j.connectors.kafka.data.ConstraintEntityType
import org.neo4j.connectors.kafka.data.ConstraintType
import org.neo4j.connectors.kafka.exceptions.InvalidDataException
import org.neo4j.connectors.kafka.sink.ChangeQuery
import org.neo4j.cypherdsl.core.renderer.Renderer
import org.neo4j.cypherdsl.parser.CypherParser
import org.neo4j.driver.Query

class RelationshipPatternHandlerTest : HandlerTest() {

  @Test
  fun `should construct correct query for simple relationship pattern`() {
    val handler =
        RelationshipPatternHandler(
            "my-topic",
            "(:LabelA{!idStart})-[:REL_TYPE]->(:LabelB{!idEnd})",
            mergeNodeProperties = true,
            mergeRelationshipProperties = true,
            renderer = Renderer.getDefaultRenderer(),
            batchSize = 1,
        )

    handler.query shouldBe
        CypherParser.parse(
                """
                  UNWIND ${'$'}events AS event 
                  CALL { WITH event 
                    CALL { WITH event
                      WITH event WHERE event[0] = 'C'
                      WITH event[1] AS event 
                      MERGE (start:`LabelA` {idStart: event.start.keys.idStart})
                      SET start += event.start.properties
                      MERGE (end:`LabelB` {idEnd: event.end.keys.idEnd})
                      SET end += event.end.properties
                      MERGE (start)-[relationship:`REL_TYPE` {}]->(end)
                      SET relationship += event.properties 
                      RETURN count(relationship) AS created
                    } 
                    CALL { WITH event
                      WITH event WHERE event[0] = 'D' 
                      WITH event[1] AS event 
                      MATCH (start:`LabelA` {idStart: event.start.keys.idStart})-[relationship:`REL_TYPE` {}]->(end:`LabelB` {idEnd: event.end.keys.idEnd})
                      DELETE relationship
                      RETURN count(relationship) AS deleted
                    } 
                    RETURN created, deleted
                  }
                  RETURN sum(created) AS created, sum(deleted) AS deleted
                  """
                    .trimIndent()
            )
            .cypher
  }

  @Test
  fun `should construct correct query for relationship pattern with aliased properties`() {
    val handler =
        RelationshipPatternHandler(
            "my-topic",
            "(:LabelA{!id: idStart})-[:REL_TYPE]->(:LabelB{!id: idEnd})",
            mergeNodeProperties = true,
            mergeRelationshipProperties = true,
            renderer = Renderer.getDefaultRenderer(),
            batchSize = 1,
        )

    handler.query shouldBe
        CypherParser.parse(
                """
                  UNWIND ${'$'}events AS event 
                  CALL { WITH event 
                    CALL { WITH event
                      WITH event WHERE event[0] = 'C'
                      WITH event[1] AS event 
                      MERGE (start:`LabelA` {id: event.start.keys.id})
                      SET start += event.start.properties
                      MERGE (end:`LabelB` {id: event.end.keys.id})
                      SET end += event.end.properties
                      MERGE (start)-[relationship:`REL_TYPE` {}]->(end)
                      SET relationship += event.properties 
                      RETURN count(relationship) AS created
                    } 
                    CALL { WITH event
                      WITH event WHERE event[0] = 'D' 
                      WITH event[1] AS event 
                      MATCH (start:`LabelA` {id: event.start.keys.id})-[relationship:`REL_TYPE` {}]->(end:`LabelB` {id: event.end.keys.id})
                      DELETE relationship
                      RETURN count(relationship) AS deleted
                    } 
                    RETURN created, deleted
                  }
                  RETURN sum(created) AS created, sum(deleted) AS deleted
                  """
                    .trimIndent()
            )
            .cypher
  }

  @Test
  fun `should construct correct query for relationship pattern with relationship keys`() {
    val handler =
        RelationshipPatternHandler(
            "my-topic",
            "(:LabelA{!id: idStart})-[:REL_TYPE{!rel_id: rel_id}]->(:LabelB{!id: idEnd})",
            mergeNodeProperties = true,
            mergeRelationshipProperties = true,
            renderer = Renderer.getDefaultRenderer(),
            batchSize = 1,
        )

    handler.query shouldBe
        CypherParser.parse(
                """
                  UNWIND ${'$'}events AS event 
                  CALL { WITH event 
                    CALL { WITH event
                      WITH event WHERE event[0] = 'C'
                      WITH event[1] AS event 
                      MERGE (start:`LabelA` {id: event.start.keys.id})
                      SET start += event.start.properties
                      MERGE (end:`LabelB` {id: event.end.keys.id})
                      SET end += event.end.properties
                      MERGE (start)-[relationship:`REL_TYPE` {rel_id: event.keys.rel_id}]->(end)
                      SET relationship += event.properties 
                      RETURN count(relationship) AS created
                    } 
                    CALL { WITH event
                      WITH event WHERE event[0] = 'D' 
                      WITH event[1] AS event 
                      MATCH (start:`LabelA` {id: event.start.keys.id})-[relationship:`REL_TYPE` {rel_id: event.keys.rel_id}]->(end:`LabelB` {id: event.end.keys.id})
                      DELETE relationship
                      RETURN count(relationship) AS deleted
                    } 
                    RETURN created, deleted
                  }
                  RETURN sum(created) AS created, sum(deleted) AS deleted
                  """
                    .trimIndent()
            )
            .cypher
  }

  @Test
  fun `should construct correct query for relationship pattern without merging node properties`() {
    val handler =
        RelationshipPatternHandler(
            "my-topic",
            "(:LabelA{!id: idStart})-[:REL_TYPE{!rel_id: rel_id}]->(:LabelB{!id: idEnd})",
            mergeNodeProperties = false,
            mergeRelationshipProperties = true,
            renderer = Renderer.getDefaultRenderer(),
            batchSize = 1,
        )

    handler.query shouldBe
        CypherParser.parse(
                """
                  UNWIND ${'$'}events AS event 
                  CALL { WITH event 
                    CALL { WITH event
                      WITH event WHERE event[0] = 'C'
                      WITH event[1] AS event 
                      MERGE (start:`LabelA` {id: event.start.keys.id})
                      SET start = event.start.properties
                      SET start += event.start.keys
                      MERGE (end:`LabelB` {id: event.end.keys.id})
                      SET end = event.end.properties
                      SET end += event.end.keys
                      MERGE (start)-[relationship:`REL_TYPE` {rel_id: event.keys.rel_id}]->(end)
                      SET relationship += event.properties 
                      RETURN count(relationship) AS created
                    } 
                    CALL { WITH event
                      WITH event WHERE event[0] = 'D' 
                      WITH event[1] AS event 
                      MATCH (start:`LabelA` {id: event.start.keys.id})-[relationship:`REL_TYPE` {rel_id: event.keys.rel_id}]->(end:`LabelB` {id: event.end.keys.id})
                      DELETE relationship
                      RETURN count(relationship) AS deleted
                    } 
                    RETURN created, deleted
                  }
                  RETURN sum(created) AS created, sum(deleted) AS deleted
                  """
                    .trimIndent()
            )
            .cypher
  }

  @Test
  fun `should construct correct query for relationship pattern without merging relationship properties`() {
    val handler =
        RelationshipPatternHandler(
            "my-topic",
            "(:LabelA{!id: idStart})-[:REL_TYPE{!rel_id: rel_id}]->(:LabelB{!id: idEnd})",
            mergeNodeProperties = true,
            mergeRelationshipProperties = false,
            renderer = Renderer.getDefaultRenderer(),
            batchSize = 1,
        )

    handler.query shouldBe
        CypherParser.parse(
                """
                  UNWIND ${'$'}events AS event 
                  CALL { WITH event 
                    CALL { WITH event
                      WITH event WHERE event[0] = 'C'
                      WITH event[1] AS event 
                      MERGE (start:`LabelA` {id: event.start.keys.id})
                      SET start += event.start.properties
                      MERGE (end:`LabelB` {id: event.end.keys.id})
                      SET end += event.end.properties
                      MERGE (start)-[relationship:`REL_TYPE` {rel_id: event.keys.rel_id}]->(end)
                      SET relationship = event.properties
                      SET relationship += event.keys 
                      RETURN count(relationship) AS created
                    } 
                    CALL { WITH event
                      WITH event WHERE event[0] = 'D' 
                      WITH event[1] AS event 
                      MATCH (start:`LabelA` {id: event.start.keys.id})-[relationship:`REL_TYPE` {rel_id: event.keys.rel_id}]->(end:`LabelB` {id: event.end.keys.id})
                      DELETE relationship
                      RETURN count(relationship) AS deleted
                    } 
                    RETURN created, deleted
                  }
                  RETURN sum(created) AS created, sum(deleted) AS deleted
                  """
                    .trimIndent()
            )
            .cypher
  }

  @Test
  fun `should construct correct query for relationship pattern without merging node and relationship properties`() {
    val handler =
        RelationshipPatternHandler(
            "my-topic",
            "(:LabelA{!id: idStart})-[:REL_TYPE{!rel_id: rel_id}]->(:LabelB{!id: idEnd})",
            mergeNodeProperties = false,
            mergeRelationshipProperties = false,
            renderer = Renderer.getDefaultRenderer(),
            batchSize = 1,
        )

    handler.query shouldBe
        CypherParser.parse(
                """
                  UNWIND ${'$'}events AS event 
                  CALL { WITH event 
                    CALL { WITH event
                      WITH event WHERE event[0] = 'C'
                      WITH event[1] AS event 
                      MERGE (start:`LabelA` {id: event.start.keys.id})
                      SET start = event.start.properties
                      SET start += event.start.keys
                      MERGE (end:`LabelB` {id: event.end.keys.id})
                      SET end = event.end.properties
                      SET end += event.end.keys
                      MERGE (start)-[relationship:`REL_TYPE` {rel_id: event.keys.rel_id}]->(end)
                      SET relationship = event.properties
                      SET relationship += event.keys 
                      RETURN count(relationship) AS created
                    } 
                    CALL { WITH event
                      WITH event WHERE event[0] = 'D' 
                      WITH event[1] AS event 
                      MATCH (start:`LabelA` {id: event.start.keys.id})-[relationship:`REL_TYPE` {rel_id: event.keys.rel_id}]->(end:`LabelB` {id: event.end.keys.id})
                      DELETE relationship
                      RETURN count(relationship) AS deleted
                    } 
                    RETURN created, deleted
                  }
                  RETURN sum(created) AS created, sum(deleted) AS deleted
                  """
                    .trimIndent()
            )
            .cypher
  }

  @Test
  fun `should construct correct query for composite keys`() {
    val handler =
        RelationshipPatternHandler(
            "my-topic",
            "(:LabelA{!id1: idStart1, !id2: idStart2})-[:REL_TYPE{!rel_id1: rel_id1, !rel_id2: rel_id2}]->(:LabelB{!id1: idEnd1, !id2: idEnd2})",
            mergeNodeProperties = true,
            mergeRelationshipProperties = true,
            renderer = Renderer.getDefaultRenderer(),
            batchSize = 1,
        )

    handler.query shouldBe
        CypherParser.parse(
                """
                  UNWIND ${'$'}events AS event 
                  CALL { WITH event 
                    CALL { WITH event
                      WITH event WHERE event[0] = 'C'
                      WITH event[1] AS event 
                      MERGE (start:`LabelA` {id1: event.start.keys.id1, id2: event.start.keys.id2})
                      SET start += event.start.properties
                      MERGE (end:`LabelB` {id1: event.end.keys.id1, id2: event.end.keys.id2})
                      SET end += event.end.properties
                      MERGE (start)-[relationship:`REL_TYPE` {rel_id1: event.keys.rel_id1, rel_id2: event.keys.rel_id2}]->(end)
                      SET relationship += event.properties
                      RETURN count(relationship) AS created
                    } 
                    CALL { WITH event
                      WITH event WHERE event[0] = 'D' 
                      WITH event[1] AS event 
                      MATCH (start:`LabelA` {id1: event.start.keys.id1, id2: event.start.keys.id2})-[relationship:`REL_TYPE` {rel_id1: event.keys.rel_id1, rel_id2: event.keys.rel_id2}]->(end:`LabelB` {id1: event.end.keys.id1, id2: event.end.keys.id2})
                      DELETE relationship
                      RETURN count(relationship) AS deleted
                    } 
                    RETURN created, deleted
                  }
                  RETURN sum(created) AS created, sum(deleted) AS deleted
                  """
                    .trimIndent()
            )
            .cypher
  }

  @Test
  fun `should construct correct query for relationship pattern with multiple labels`() {
    val handler =
        RelationshipPatternHandler(
            "my-topic",
            "(:LabelA:LabelC{!id: idStart})-[:REL_TYPE{!rel_id: rel_id}]->(:LabelB:LabelD{!id: idEnd})",
            mergeNodeProperties = true,
            mergeRelationshipProperties = true,
            renderer = Renderer.getDefaultRenderer(),
            batchSize = 1,
        )

    handler.query shouldBe
        CypherParser.parse(
                """
                  UNWIND ${'$'}events AS event 
                  CALL { WITH event 
                    CALL { WITH event
                      WITH event WHERE event[0] = 'C'
                      WITH event[1] AS event 
                      MERGE (start:`LabelA`:`LabelC` {id: event.start.keys.id})
                      SET start += event.start.properties
                      MERGE (end:`LabelB`:`LabelD` {id: event.end.keys.id})
                      SET end += event.end.properties
                      MERGE (start)-[relationship:`REL_TYPE` {rel_id: event.keys.rel_id}]->(end)
                      SET relationship += event.properties 
                      RETURN count(relationship) AS created
                    } 
                    CALL { WITH event
                      WITH event WHERE event[0] = 'D' 
                      WITH event[1] AS event 
                      MATCH (start:`LabelA`:`LabelC` {id: event.start.keys.id})-[relationship:`REL_TYPE` {rel_id: event.keys.rel_id}]->(end:`LabelB`:`LabelD` {id: event.end.keys.id})
                      DELETE relationship
                      RETURN count(relationship) AS deleted
                    } 
                    RETURN created, deleted
                  }
                  RETURN sum(created) AS created, sum(deleted) AS deleted
                  """
                    .trimIndent()
            )
            .cypher
  }

  @Test
  fun `should construct correct query for relationship pattern with escaped labels and properties`() {
    val handler =
        RelationshipPatternHandler(
            "my-topic",
            "(:`Label A`:`Label C`{!`a-id`: `id start`})-[:`REL TYPE`{!`rel-id`: `rel id`}]->(:`Label B`:`Label D`{!`b-id`: `id end`})",
            mergeNodeProperties = true,
            mergeRelationshipProperties = true,
            renderer = Renderer.getDefaultRenderer(),
            batchSize = 1,
        )

    handler.query shouldBe
        CypherParser.parse(
                """
                  UNWIND ${'$'}events AS event 
                  CALL { WITH event 
                    CALL { WITH event
                      WITH event WHERE event[0] = 'C'
                      WITH event[1] AS event 
                      MERGE (start:`Label A`:`Label C` {`a-id`: event.start.keys.`a-id`})
                      SET start += event.start.properties
                      MERGE (end:`Label B`:`Label D` {`b-id`: event.end.keys.`b-id`})
                      SET end += event.end.properties
                      MERGE (start)-[relationship:`REL TYPE` {`rel-id`: event.keys.`rel-id`}]->(end)
                      SET relationship += event.properties 
                      RETURN count(relationship) AS created
                    } 
                    CALL { WITH event
                      WITH event WHERE event[0] = 'D' 
                      WITH event[1] AS event 
                      MATCH (start:`Label A`:`Label C` {`a-id`: event.start.keys.`a-id`})-[relationship:`REL TYPE` {`rel-id`: event.keys.`rel-id`}]->(end:`Label B`:`Label D` {`b-id`: event.end.keys.`b-id`})
                      DELETE relationship
                      RETURN count(relationship) AS deleted
                    } 
                    RETURN created, deleted
                  }
                  RETURN sum(created) AS created, sum(deleted) AS deleted
                  """
                    .trimIndent()
            )
            .cypher
  }

  @Test
  fun `should include all properties`() {
    assertQueryAndParameters(
        "(:LabelA{!id: idStart})-[:REL_TYPE{!id: idRelationship}]->(:LabelB{!id: idEnd})",
        value = """{"idStart": 1, "idEnd": 2, "idRelationship": 3, "foo": "foo", "bar": "bar"}""",
        expected =
            listOf(
                listOf(
                    "C",
                    mapOf(
                        "start" to
                            mapOf(
                                "keys" to mapOf("id" to 1),
                                "properties" to emptyMap<String, Any?>(),
                            ),
                        "end" to
                            mapOf(
                                "keys" to mapOf("id" to 2),
                                "properties" to emptyMap<String, Any?>(),
                            ),
                        "keys" to mapOf<String, Any?>("id" to 3),
                        "properties" to mapOf<String, Any?>("foo" to "foo", "bar" to "bar"),
                    ),
                )
            ),
    )
  }

  @Test
  fun `should include nested properties`() {
    assertQueryAndParameters(
        "(:LabelA{!id: idStart})-[:REL_TYPE{!id: idRelationship}]->(:LabelB{!id: idEnd})",
        value =
            """{"idStart": 1, "idEnd": 2, "idRelationship": 3, "foo": "foo", "bar": "bar", "nested": {"baz": "baz", "bak": "bak"}}""",
        expected =
            listOf(
                listOf(
                    "C",
                    mapOf(
                        "start" to
                            mapOf(
                                "keys" to mapOf("id" to 1),
                                "properties" to emptyMap<String, Any?>(),
                            ),
                        "end" to
                            mapOf(
                                "keys" to mapOf("id" to 2),
                                "properties" to emptyMap<String, Any?>(),
                            ),
                        "keys" to mapOf<String, Any?>("id" to 3),
                        "properties" to
                            mapOf<String, Any?>(
                                "foo" to "foo",
                                "bar" to "bar",
                                "nested.baz" to "baz",
                                "nested.bak" to "bak",
                            ),
                    ),
                )
            ),
    )
  }

  @Test
  fun `should include nested properties by hierarchy`() {
    assertQueryAndParameters(
        "(:LabelA{!id: idStart})-[:REL_TYPE{!id: idRelationship,nested}]->(:LabelB{!id: idEnd})",
        value =
            """{"idStart": 1, "idEnd": 2, "idRelationship": 3, "foo": "foo", "bar": "bar", "nested": {"baz": "baz", "bak": "bak"}}""",
        expected =
            listOf(
                listOf(
                    "C",
                    mapOf(
                        "start" to
                            mapOf(
                                "keys" to mapOf("id" to 1),
                                "properties" to emptyMap<String, Any?>(),
                            ),
                        "end" to
                            mapOf(
                                "keys" to mapOf("id" to 2),
                                "properties" to emptyMap<String, Any?>(),
                            ),
                        "keys" to mapOf<String, Any?>("id" to 3),
                        "properties" to
                            mapOf<String, Any?>("nested.baz" to "baz", "nested.bak" to "bak"),
                    ),
                )
            ),
    )
  }

  @Test
  fun `should exclude properties`() {
    assertQueryAndParameters(
        "(:LabelA{!id: idStart})-[:REL_TYPE{!id: idRelationship,-foo,-nested.bak}]->(:LabelB{!id: idEnd})",
        value =
            """{"idStart": 1, "idEnd": 2, "idRelationship": 3, "foo": "foo", "bar": "bar", "nested": {"baz": "baz", "bak": "bak"}}""",
        expected =
            listOf(
                listOf(
                    "C",
                    mapOf(
                        "start" to
                            mapOf(
                                "keys" to mapOf("id" to 1),
                                "properties" to emptyMap<String, Any?>(),
                            ),
                        "end" to
                            mapOf(
                                "keys" to mapOf("id" to 2),
                                "properties" to emptyMap<String, Any?>(),
                            ),
                        "keys" to mapOf<String, Any?>("id" to 3),
                        "properties" to mapOf<String, Any?>("bar" to "bar", "nested.baz" to "baz"),
                    ),
                )
            ),
    )
  }

  @Test
  fun `should exclude nested properties by hierarchy`() {
    assertQueryAndParameters(
        "(:LabelA{!id: idStart})-[:REL_TYPE{!id: idRelationship,-nested}]->(:LabelB{!id: idEnd})",
        value =
            """{"idStart": 1, "idEnd": 2, "idRelationship": 3, "foo": "foo", "bar": "bar", "nested": {"baz": "baz", "bak": "bak"}}""",
        expected =
            listOf(
                listOf(
                    "C",
                    mapOf(
                        "start" to
                            mapOf(
                                "keys" to mapOf("id" to 1),
                                "properties" to emptyMap<String, Any?>(),
                            ),
                        "end" to
                            mapOf(
                                "keys" to mapOf("id" to 2),
                                "properties" to emptyMap<String, Any?>(),
                            ),
                        "keys" to mapOf<String, Any?>("id" to 3),
                        "properties" to mapOf<String, Any?>("foo" to "foo", "bar" to "bar"),
                    ),
                )
            ),
    )
  }

  @Test
  fun `should alias properties`() {
    assertQueryAndParameters(
        "(:LabelA{!id: idStart})-[:REL_TYPE{!id: idRelationship,new_foo: foo, new_bar: bar}]->(:LabelB{!id: idEnd})",
        value =
            """{"idStart": 1, "idEnd": 2, "idRelationship": 3, "foo": "foo", "bar": "bar", "nested": {"baz": "baz", "bak": "bak"}}""",
        expected =
            listOf(
                listOf(
                    "C",
                    mapOf(
                        "start" to
                            mapOf(
                                "keys" to mapOf("id" to 1),
                                "properties" to emptyMap<String, Any?>(),
                            ),
                        "end" to
                            mapOf(
                                "keys" to mapOf("id" to 2),
                                "properties" to emptyMap<String, Any?>(),
                            ),
                        "keys" to mapOf<String, Any?>("id" to 3),
                        "properties" to mapOf<String, Any?>("new_foo" to "foo", "new_bar" to "bar"),
                    ),
                )
            ),
    )
  }

  @Test
  fun `should alias nested properties`() {
    assertQueryAndParameters(
        "(:LabelA{!id: idStart})-[:REL_TYPE{!id: idRelationship,new_foo: foo, new_bar: bar, new_nested_baz: nested.baz}]->(:LabelB{!id: idEnd})",
        value =
            """{"idStart": 1, "idEnd": 2, "idRelationship": 3, "foo": "foo", "bar": "bar", "nested": {"baz": "baz", "bak": "bak"}}""",
        expected =
            listOf(
                listOf(
                    "C",
                    mapOf(
                        "start" to
                            mapOf(
                                "keys" to mapOf("id" to 1),
                                "properties" to emptyMap<String, Any?>(),
                            ),
                        "end" to
                            mapOf(
                                "keys" to mapOf("id" to 2),
                                "properties" to emptyMap<String, Any?>(),
                            ),
                        "keys" to mapOf<String, Any?>("id" to 3),
                        "properties" to
                            mapOf<String, Any?>(
                                "new_foo" to "foo",
                                "new_bar" to "bar",
                                "new_nested_baz" to "baz",
                            ),
                    ),
                )
            ),
    )
  }

  @Test
  fun `should alias nested properties by hierarchy`() {
    assertQueryAndParameters(
        "(:LabelA{!id: idStart})-[:REL_TYPE{!id: idRelationship,new_foo: foo, new_bar: bar, new_nested: nested}]->(:LabelB{!id: idEnd})",
        value =
            """{"idStart": 1, "idEnd": 2, "idRelationship": 3, "foo": "foo", "bar": "bar", "nested": {"baz": "baz", "bak": "bak"}}""",
        expected =
            listOf(
                listOf(
                    "C",
                    mapOf(
                        "start" to
                            mapOf(
                                "keys" to mapOf("id" to 1),
                                "properties" to emptyMap<String, Any?>(),
                            ),
                        "end" to
                            mapOf(
                                "keys" to mapOf("id" to 2),
                                "properties" to emptyMap<String, Any?>(),
                            ),
                        "keys" to mapOf<String, Any?>("id" to 3),
                        "properties" to
                            mapOf<String, Any?>(
                                "new_foo" to "foo",
                                "new_bar" to "bar",
                                "new_nested.baz" to "baz",
                                "new_nested.bak" to "bak",
                            ),
                    ),
                )
            ),
    )
  }

  @Test
  fun `should include explicit properties`() {
    assertQueryAndParameters(
        "(:LabelA{!id: idStart})-[:REL_TYPE{!id: idRelationship, foo: __value.foo, bar: __value.bar}]->(:LabelB{!id: idEnd})",
        value = """{"idStart": 1, "idEnd": 2, "idRelationship": 3, "foo": "foo", "bar": "bar"}""",
        expected =
            listOf(
                listOf(
                    "C",
                    mapOf(
                        "start" to
                            mapOf(
                                "keys" to mapOf("id" to 1),
                                "properties" to emptyMap<String, Any?>(),
                            ),
                        "end" to
                            mapOf(
                                "keys" to mapOf("id" to 2),
                                "properties" to emptyMap<String, Any?>(),
                            ),
                        "keys" to mapOf<String, Any?>("id" to 3),
                        "properties" to mapOf<String, Any?>("foo" to "foo", "bar" to "bar"),
                    ),
                )
            ),
    )
  }

  @Test
  fun `should include explicit nested properties`() {
    assertQueryAndParameters(
        "(:LabelA{!id: idStart})-[:REL_TYPE{!id: idRelationship, foo: __value.foo, bar: __value.bar, baz: __value.nested.baz}]->(:LabelB{!id: idEnd})",
        value =
            """{"idStart": 1, "idEnd": 2, "idRelationship": 3, "foo": "foo", "bar": "bar", "nested": {"baz": "baz", "bak": "bak"}}""",
        expected =
            listOf(
                listOf(
                    "C",
                    mapOf(
                        "start" to
                            mapOf(
                                "keys" to mapOf("id" to 1),
                                "properties" to emptyMap<String, Any?>(),
                            ),
                        "end" to
                            mapOf(
                                "keys" to mapOf("id" to 2),
                                "properties" to emptyMap<String, Any?>(),
                            ),
                        "keys" to mapOf<String, Any?>("id" to 3),
                        "properties" to
                            mapOf<String, Any?>("foo" to "foo", "bar" to "bar", "baz" to "baz"),
                    ),
                )
            ),
    )
  }

  @Test
  fun `should include explicit nested properties by hierarchy`() {
    assertQueryAndParameters(
        "(:LabelA{!id: idStart})-[:REL_TYPE{!id: idRelationship, foo: __value.foo, bar: __value.bar, new_nested: __value.nested}]->(:LabelB{!id: idEnd})",
        value =
            """{"idStart": 1, "idEnd": 2, "idRelationship": 3, "foo": "foo", "bar": "bar", "nested": {"baz": "baz", "bak": "bak"}}""",
        expected =
            listOf(
                listOf(
                    "C",
                    mapOf(
                        "start" to
                            mapOf(
                                "keys" to mapOf("id" to 1),
                                "properties" to emptyMap<String, Any?>(),
                            ),
                        "end" to
                            mapOf(
                                "keys" to mapOf("id" to 2),
                                "properties" to emptyMap<String, Any?>(),
                            ),
                        "keys" to mapOf<String, Any?>("id" to 3),
                        "properties" to
                            mapOf<String, Any?>(
                                "foo" to "foo",
                                "bar" to "bar",
                                "new_nested.baz" to "baz",
                                "new_nested.bak" to "bak",
                            ),
                    ),
                )
            ),
    )
  }

  @Test
  fun `should be able to mix implicit and explicit properties`() {
    assertQueryAndParameters(
        "(:LabelA{!id: idStart})-[:REL_TYPE{!id: idRelationship, foo, bar: __value.bar, baz: __value.nested.baz}]->(:LabelB{!id: idEnd})",
        value =
            """{"idStart": 1, "idEnd": 2, "idRelationship": 3, "foo": "foo", "bar": "bar", "nested": {"baz": "baz", "bak": "bak"}}""",
        expected =
            listOf(
                listOf(
                    "C",
                    mapOf(
                        "start" to
                            mapOf(
                                "keys" to mapOf("id" to 1),
                                "properties" to emptyMap<String, Any?>(),
                            ),
                        "end" to
                            mapOf(
                                "keys" to mapOf("id" to 2),
                                "properties" to emptyMap<String, Any?>(),
                            ),
                        "keys" to mapOf<String, Any?>("id" to 3),
                        "properties" to
                            mapOf<String, Any?>("foo" to "foo", "bar" to "bar", "baz" to "baz"),
                    ),
                )
            ),
    )
  }

  @Test
  fun `should be able to use other message fields`() {
    assertQueryAndParameters(
        "(:LabelA{!id: __key.start})-[:REL_TYPE{!id: __key.id, foo, bar: __value.bar, created_at: __timestamp}]->(:LabelB{!id: __key.end})",
        key = """{"start": 1, "end": 2, "id": 3}""",
        value =
            """{"idStart": 1, "idEnd": 2, "idRelationship": 3, "foo": "foo", "bar": "bar", "nested": {"baz": "baz", "bak": "bak"}}""",
        expected =
            listOf(
                listOf(
                    "C",
                    mapOf(
                        "start" to
                            mapOf(
                                "keys" to mapOf("id" to 1),
                                "properties" to emptyMap<String, Any?>(),
                            ),
                        "end" to
                            mapOf(
                                "keys" to mapOf("id" to 2),
                                "properties" to emptyMap<String, Any?>(),
                            ),
                        "keys" to mapOf<String, Any?>("id" to 3),
                        "properties" to
                            mapOf<String, Any?>(
                                "foo" to "foo",
                                "bar" to "bar",
                                "created_at" to
                                    Instant.ofEpochMilli(TIMESTAMP).atOffset(ZoneOffset.UTC),
                            ),
                    ),
                )
            ),
    )
  }

  @Test
  fun `should be able to use inclusion in node patterns`() {
    assertQueryAndParameters(
        "(:LabelA{!id: __key.start, foo, propA, nested.baz})-[:REL_TYPE{!id: __key.id, bar}]->(:LabelB{!id: __key.end, foo, propB, nested.bak})",
        key = """{"start": 1, "end": 2, "id": 3}""",
        value =
            """{"idStart": 1, "idEnd": 2, "idRelationship": 3, "propA": "a", "propB": "b", "foo": "foo", "bar": "bar", "nested": {"baz": "baz", "bak": "bak"}}""",
        expected =
            listOf(
                listOf(
                    "C",
                    mapOf(
                        "start" to
                            mapOf(
                                "keys" to mapOf("id" to 1),
                                "properties" to
                                    mapOf<String, Any?>(
                                        "propA" to "a",
                                        "foo" to "foo",
                                        "nested.baz" to "baz",
                                    ),
                            ),
                        "end" to
                            mapOf(
                                "keys" to mapOf("id" to 2),
                                "properties" to
                                    mapOf<String, Any?>(
                                        "propB" to "b",
                                        "foo" to "foo",
                                        "nested.bak" to "bak",
                                    ),
                            ),
                        "keys" to mapOf<String, Any?>("id" to 3),
                        "properties" to mapOf<String, Any?>("bar" to "bar"),
                    ),
                )
            ),
    )
  }

  @Test
  fun `should be able to delete`() {
    assertQueryAndParameters(
        "(:LabelA{!id: start})-[:REL_TYPE{!id: id}]->(:LabelB{!id: end})",
        key = """{"start": 1, "end": 2, "id": 3}""",
        value = null,
        expected =
            listOf(
                listOf(
                    "D",
                    mapOf(
                        "start" to mapOf("keys" to mapOf("id" to 1)),
                        "end" to mapOf("keys" to mapOf("id" to 2)),
                        "keys" to mapOf<String, Any?>("id" to 3),
                    ),
                )
            ),
    )
  }

  @Test
  fun `should be able to delete with explicit`() {
    assertQueryAndParameters(
        "(:LabelA{!id: __key.start})-[:REL_TYPE{!id: __key.id}]->(:LabelB{!id: __key.end})",
        key = """{"start": 1, "end": 2, "id": 3}""",
        value = null,
        expected =
            listOf(
                listOf(
                    "D",
                    mapOf(
                        "start" to mapOf("keys" to mapOf("id" to 1)),
                        "end" to mapOf("keys" to mapOf("id" to 2)),
                        "keys" to mapOf<String, Any?>("id" to 3),
                    ),
                )
            ),
    )
  }

  @Test
  fun `should be able to delete with struct`() {
    val schema =
        SchemaBuilder.struct()
            .field("start", Schema.INT32_SCHEMA)
            .field("end", Schema.INT32_SCHEMA)
            .field("id", Schema.INT32_SCHEMA)
            .build()

    assertQueryAndParameters(
        "(:LabelA{!id: start})-[:REL_TYPE{!id}]->(:LabelB{!id: end})",
        keySchema = schema,
        key = Struct(schema).put("start", 1).put("end", 2).put("id", 3),
        value = null,
        expected =
            listOf(
                listOf(
                    "D",
                    mapOf(
                        "start" to mapOf("keys" to mapOf("id" to 1)),
                        "end" to mapOf("keys" to mapOf("id" to 2)),
                        "keys" to mapOf<String, Any?>("id" to 3),
                    ),
                )
            ),
    )
  }

  @Test
  fun `should fail when the source node key is not located in the message`() {
    assertThrowsHandler<InvalidDataException>(
        pattern = "(:LabelA{!id: start})-[:REL_TYPE{!id: rel_id}]->(:LabelB{!id: end})",
        key = """{"rel_id": 1, "end": 1}""",
        message = "Key 'start' could not be located in the message.",
    )
  }

  @Test
  fun `should fail when the relationship key is not located in the message`() {
    assertThrowsHandler<InvalidDataException>(
        pattern = "(:LabelA{!id: start})-[:REL_TYPE{!id: rel_id}]->(:LabelB{!id: end})",
        key = """{"start": 1, "end": 1}""",
        message = "Key 'rel_id' could not be located in the message.",
    )
  }

  @Test
  fun `should fail when the target node key is not located in the message`() {
    assertThrowsHandler<InvalidDataException>(
        pattern = "(:LabelA{!id: start})-[:REL_TYPE{!id: rel_id}]->(:LabelB{!id: end})",
        key = """{"start": 1, "rel_id": 1}""",
        message = "Key 'end' could not be located in the message.",
    )
  }

  @Test
  fun `should fail when the explicit source node key is not located in the keys`() {
    assertThrowsHandler<InvalidDataException>(
        pattern = "(:LabelA{!id: __key.start})-[:REL_TYPE{!id: rel_id}]->(:LabelB{!id: end})",
        key = """{"rel_id": 1, "end": 1}""",
        message = "Key 'start' could not be located in the keys.",
    )
  }

  @Test
  fun `should fail when the explicit relationship key is not located in the keys`() {
    assertThrowsHandler<InvalidDataException>(
        pattern = "(:LabelA{!id: __key.start})-[:REL_TYPE{!id: __key.rel_id}]->(:LabelB{!id: end})",
        key = """{"start": 1, "end": 1}""",
        message = "Key 'rel_id' could not be located in the keys.",
    )
  }

  @Test
  fun `should fail when the explicit target node key is not located in the keys`() {
    assertThrowsHandler<InvalidDataException>(
        pattern = "(:LabelA{!id: __key.start})-[:REL_TYPE{!id: rel_id}]->(:LabelB{!id: __key.end})",
        key = """{"start": 1, "rel_id": 1}""",
        message = "Key 'end' could not be located in the keys.",
    )
  }

  @Test
  fun `should fail when the explicit source node key is not located in the values`() {
    assertThrowsHandler<InvalidDataException>(
        pattern = "(:LabelA{!id: __value.start})-[:REL_TYPE{!id: rel_id}]->(:LabelB{!id: end})",
        value = """{"rel_id": 1, "end": 1}""",
        message = "Key 'start' could not be located in the values.",
    )
  }

  @Test
  fun `should fail when the explicit relationship key is not located in the values`() {
    assertThrowsHandler<InvalidDataException>(
        pattern = "(:LabelA{!id: start})-[:REL_TYPE{!id: __value.rel_id}]->(:LabelB{!id: end})",
        value = """{"start": 1, "end": 1}""",
        message = "Key 'rel_id' could not be located in the values.",
    )
  }

  @Test
  fun `should fail when the explicit target node key is not located in the values`() {
    assertThrowsHandler<InvalidDataException>(
        pattern = "(:LabelA{!id: start})-[:REL_TYPE{!id: rel_id}]->(:LabelB{!id: __value.end})",
        value = """{"start": 1, "rel_id": 1}""",
        message = "Key 'end' could not be located in the values.",
    )
  }

  @Test
  fun `checkConstraints should not return warning messages if relationship key constraint provided with all keys`() {
    val handler =
        RelationshipPatternHandler(
            "my-topic",
            "(:LabelA{!idStart})-[:REL_TYPE{!id, !second_id}]->(:LabelB{!idEnd})",
            mergeNodeProperties = true,
            mergeRelationshipProperties = true,
            renderer = Renderer.getDefaultRenderer(),
            batchSize = 1,
        )

    val constraints =
        listOf(
            ConstraintData(
                entityType = ConstraintEntityType.NODE.value,
                constraintType = ConstraintType.NODE_KEY.value,
                labelOrType = "LabelA",
                properties = listOf("idStart"),
            ),
            ConstraintData(
                entityType = ConstraintEntityType.RELATIONSHIP.value,
                constraintType = ConstraintType.RELATIONSHIP_KEY.value,
                labelOrType = "REL_TYPE",
                properties = listOf("id", "second_id"),
            ),
            ConstraintData(
                entityType = ConstraintEntityType.NODE.value,
                constraintType = ConstraintType.NODE_KEY.value,
                labelOrType = "LabelB",
                properties = listOf("idEnd"),
            ),
        )

    val warningMessages = handler.checkConstraints(constraints)

    warningMessages shouldBe emptyList()
  }

  @Test
  fun `checkConstraints should not return warning messages if relationship uniqueness and existence constraint provided with all keys`() {
    val handler =
        RelationshipPatternHandler(
            "my-topic",
            "(:LabelA{!idStart})-[:REL_TYPE{!id, !second_id}]->(:LabelB{!idEnd})",
            mergeNodeProperties = true,
            mergeRelationshipProperties = true,
            renderer = Renderer.getDefaultRenderer(),
            batchSize = 1,
        )

    val constraints =
        listOf(
            ConstraintData(
                entityType = ConstraintEntityType.NODE.value,
                constraintType = ConstraintType.NODE_KEY.value,
                labelOrType = "LabelA",
                properties = listOf("idStart"),
            ),
            ConstraintData(
                entityType = ConstraintEntityType.RELATIONSHIP.value,
                constraintType = ConstraintType.RELATIONSHIP_UNIQUENESS.value,
                labelOrType = "REL_TYPE",
                properties = listOf("id", "second_id"),
            ),
            ConstraintData(
                entityType = ConstraintEntityType.RELATIONSHIP.value,
                constraintType = ConstraintType.RELATIONSHIP_EXISTENCE.value,
                labelOrType = "REL_TYPE",
                properties = listOf("id"),
            ),
            ConstraintData(
                entityType = ConstraintEntityType.RELATIONSHIP.value,
                constraintType = ConstraintType.RELATIONSHIP_EXISTENCE.value,
                labelOrType = "REL_TYPE",
                properties = listOf("second_id"),
            ),
            ConstraintData(
                entityType = ConstraintEntityType.NODE.value,
                constraintType = ConstraintType.NODE_KEY.value,
                labelOrType = "LabelB",
                properties = listOf("idEnd"),
            ),
        )

    val warningMessages = handler.checkConstraints(constraints)

    warningMessages shouldBe emptyList()
  }

  @Test
  fun `checkConstraints should return warning messages with empty list of constraints`() {
    val handler =
        RelationshipPatternHandler(
            "my-topic",
            "(:LabelA{!idStart})-[:REL_TYPE{!id}]->(:LabelB{!idEnd})",
            mergeNodeProperties = true,
            mergeRelationshipProperties = true,
            renderer = Renderer.getDefaultRenderer(),
            batchSize = 1,
        )

    val constraints = emptyList<ConstraintData>()

    val warningMessages = handler.checkConstraints(constraints)

    val startNode =
        "Label 'LabelA' does not match the key(s) defined by the pattern (:LabelA {!idStart: idStart})-[:REL_TYPE {!id: id, *}]->(:LabelB {!idEnd: idEnd})." +
            "\nPlease fix the label constraint:" +
            "\n\t'LabelA' has no key constraints" +
            "\nExpected constraints:" +
            "\n\t- NODE_KEY (idStart)" +
            "\nor:" +
            "\n\t- UNIQUENESS (idStart)" +
            "\n\t- NODE_PROPERTY_EXISTENCE (idStart)"

    warningMessages[0] shouldBe startNode

    val relationship =
        "Relationship 'REL_TYPE' does not match the key(s) defined by the pattern (:LabelA {!idStart: idStart})-[:REL_TYPE {!id: id, *}]->(:LabelB {!idEnd: idEnd})." +
            "\nPlease fix the relationship constraints:" +
            "\n\t'REL_TYPE' has no key constraints" +
            "\nExpected constraints:" +
            "\n\t- RELATIONSHIP_KEY (id)" +
            "\nor:" +
            "\n\t- RELATIONSHIP_UNIQUENESS (id)" +
            "\n\t- RELATIONSHIP_PROPERTY_EXISTENCE (id)"

    warningMessages[1] shouldBe relationship

    val endNode =
        "Label 'LabelB' does not match the key(s) defined by the pattern (:LabelA {!idStart: idStart})-[:REL_TYPE {!id: id, *}]->(:LabelB {!idEnd: idEnd})." +
            "\nPlease fix the label constraint:" +
            "\n\t'LabelB' has no key constraints" +
            "\nExpected constraints:" +
            "\n\t- NODE_KEY (idEnd)" +
            "\nor:" +
            "\n\t- UNIQUENESS (idEnd)" +
            "\n\t- NODE_PROPERTY_EXISTENCE (idEnd)"

    warningMessages[2] shouldBe endNode
  }

  @Test
  fun `checkConstraints should return warning messages with existing key constraints`() {
    val handler =
        RelationshipPatternHandler(
            "my-topic",
            "(:LabelA{!idStart})-[:REL_TYPE{!id, !second_id}]->(:LabelB{!idEnd})",
            mergeNodeProperties = true,
            mergeRelationshipProperties = true,
            renderer = Renderer.getDefaultRenderer(),
            batchSize = 1,
        )

    val constraints =
        listOf(
            ConstraintData(
                entityType = ConstraintEntityType.NODE.value,
                constraintType = ConstraintType.NODE_KEY.value,
                labelOrType = "LabelA",
                properties = listOf("idStart"),
            ),
            ConstraintData(
                entityType = ConstraintEntityType.RELATIONSHIP.value,
                constraintType = ConstraintType.RELATIONSHIP_KEY.value,
                labelOrType = "REL_TYPE",
                properties = listOf("id"),
            ),
            ConstraintData(
                entityType = ConstraintEntityType.NODE.value,
                constraintType = ConstraintType.NODE_KEY.value,
                labelOrType = "LabelB",
                properties = listOf("idEnd"),
            ),
        )

    val warningMessages = handler.checkConstraints(constraints)

    val relationship =
        "Relationship 'REL_TYPE' does not match the key(s) defined by the pattern (:LabelA {!idStart: idStart})-[:REL_TYPE {!id: id, !second_id: second_id, *}]->(:LabelB {!idEnd: idEnd})." +
            "\nPlease fix the relationship constraints:" +
            "\n\t'REL_TYPE' has:" +
            "\n\t\t- RELATIONSHIP_KEY (id)" +
            "\nExpected constraints:" +
            "\n\t- RELATIONSHIP_KEY (id, second_id)" +
            "\nor:" +
            "\n\t- RELATIONSHIP_UNIQUENESS (id, second_id)" +
            "\n\t- RELATIONSHIP_PROPERTY_EXISTENCE (id)" +
            "\n\t- RELATIONSHIP_PROPERTY_EXISTENCE (second_id)"

    warningMessages shouldBe listOf(relationship)
  }

  @Test
  fun `checkConstraints should return warning messages with existing uniqueness and existence constraints`() {
    val handler =
        RelationshipPatternHandler(
            "my-topic",
            "(:LabelA{!idStart})-[:REL_TYPE{!id, !second_id}]->(:LabelB{!idEnd})",
            mergeNodeProperties = true,
            mergeRelationshipProperties = true,
            renderer = Renderer.getDefaultRenderer(),
            batchSize = 1,
        )

    val constraints =
        listOf(
            ConstraintData(
                entityType = ConstraintEntityType.NODE.value,
                constraintType = ConstraintType.NODE_KEY.value,
                labelOrType = "LabelA",
                properties = listOf("idStart"),
            ),
            ConstraintData(
                entityType = ConstraintEntityType.RELATIONSHIP.value,
                constraintType = ConstraintType.RELATIONSHIP_UNIQUENESS.value,
                labelOrType = "REL_TYPE",
                properties = listOf("id", "second_id"),
            ),
            ConstraintData(
                entityType = ConstraintEntityType.RELATIONSHIP.value,
                constraintType = ConstraintType.RELATIONSHIP_EXISTENCE.value,
                labelOrType = "REL_TYPE",
                properties = listOf("id"),
            ),
            ConstraintData(
                entityType = ConstraintEntityType.NODE.value,
                constraintType = ConstraintType.NODE_KEY.value,
                labelOrType = "LabelB",
                properties = listOf("idEnd"),
            ),
        )

    val warningMessages = handler.checkConstraints(constraints)

    val relationship =
        "Relationship 'REL_TYPE' does not match the key(s) defined by the pattern (:LabelA {!idStart: idStart})-[:REL_TYPE {!id: id, !second_id: second_id, *}]->(:LabelB {!idEnd: idEnd})." +
            "\nPlease fix the relationship constraints:" +
            "\n\t'REL_TYPE' has:" +
            "\n\t\t- RELATIONSHIP_UNIQUENESS (id, second_id)" +
            "\n\t\t- RELATIONSHIP_PROPERTY_EXISTENCE (id)" +
            "\nExpected constraints:" +
            "\n\t- RELATIONSHIP_KEY (id, second_id)" +
            "\nor:" +
            "\n\t- RELATIONSHIP_UNIQUENESS (id, second_id)" +
            "\n\t- RELATIONSHIP_PROPERTY_EXISTENCE (id)" +
            "\n\t- RELATIONSHIP_PROPERTY_EXISTENCE (second_id)"

    warningMessages shouldBe listOf(relationship)
  }

  private fun assertQueryAndParameters(
      pattern: String,
      keySchema: Schema = Schema.STRING_SCHEMA,
      key: Any? = null,
      valueSchema: Schema = Schema.STRING_SCHEMA,
      value: Any? = null,
      expected: List<List<Any>> = emptyList(),
      mergeNodeProperties: Boolean = true,
      mergeRelationshipProperties: Boolean = true,
  ) {
    val handler =
        RelationshipPatternHandler(
            "my-topic",
            pattern,
            mergeNodeProperties = mergeNodeProperties,
            mergeRelationshipProperties = mergeRelationshipProperties,
            renderer = Renderer.getDefaultRenderer(),
            batchSize = 1,
        )
    val sinkMessage = newMessage(valueSchema, value, keySchema = keySchema, key = key)
    handler.handle(listOf(sinkMessage)) shouldBe
        listOf(
            listOf(
                ChangeQuery(
                    null,
                    null,
                    listOf(sinkMessage),
                    Query(
                        CypherParser.parse(
                                """
                                UNWIND ${'$'}events AS event 
                                CALL { WITH event 
                                  CALL { WITH event
                                    WITH event WHERE event[0] = 'C'
                                    WITH event[1] AS event 
                                    MERGE (start:`LabelA` {id: event.start.keys.id})
                                    SET start ${if (mergeNodeProperties) "+" else "" }= event.start.properties
                                    ${if (!mergeNodeProperties) "SET start += event.start.keys" else ""}
                                    MERGE (end:`LabelB` {id: event.end.keys.id})
                                    SET end ${if (mergeNodeProperties) "+" else "" }= event.end.properties
                                    ${if (!mergeNodeProperties) "SET end += event.end.keys" else ""}
                                    MERGE (start)-[relationship:`REL_TYPE` {id: event.keys.id}]->(end)
                                    SET relationship ${if (mergeRelationshipProperties) "+" else "" }= event.properties 
                                    ${if (!mergeRelationshipProperties) "SET relationship += event.keys" else ""}
                                    RETURN count(relationship) AS created
                                  } 
                                  CALL { WITH event
                                    WITH event WHERE event[0] = 'D' 
                                    WITH event[1] AS event 
                                    MATCH (start:`LabelA` {id: event.start.keys.id})-[relationship:`REL_TYPE` {id: event.keys.id}]->(end:`LabelB` {id: event.end.keys.id})
                                    DELETE relationship
                                    RETURN count(relationship) AS deleted
                                  } 
                                  RETURN created, deleted
                                }
                                RETURN sum(created) AS created, sum(deleted) AS deleted
                                """
                                    .trimIndent()
                            )
                            .cypher,
                        mapOf("events" to expected),
                    ),
                )
            )
        )
  }

  private inline fun <reified T : Throwable> assertThrowsHandler(
      pattern: String,
      keySchema: Schema = Schema.STRING_SCHEMA,
      key: Any? = null,
      valueSchema: Schema = Schema.STRING_SCHEMA,
      value: Any? = null,
      message: String? = null,
  ) {
    val handler =
        RelationshipPatternHandler(
            "my-topic",
            pattern,
            mergeNodeProperties = false,
            mergeRelationshipProperties = false,
            renderer = Renderer.getDefaultRenderer(),
            batchSize = 1,
        )
    val sinkMessage = newMessage(valueSchema, value, keySchema = keySchema, key = key)

    if (message != null) {
      assertThrows<T> { handler.handle(listOf(sinkMessage)) } shouldHaveMessage message
    } else {
      assertThrows<T> { handler.handle(listOf(sinkMessage)) }
    }
  }
}
