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
import java.time.Instant
import java.time.ZoneOffset
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.junit.jupiter.api.Test
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
            batchSize = 1)

    handler.query shouldBe
        CypherParser.parse(
                """
                  UNWIND ${'$'}messages AS event 
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
                  RETURN sum(created) AS created, sum(deleted) AS deleted
                  """
                    .trimIndent())
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
            batchSize = 1)

    handler.query shouldBe
        CypherParser.parse(
                """
                  UNWIND ${'$'}messages AS event 
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
                  RETURN sum(created) AS created, sum(deleted) AS deleted
                  """
                    .trimIndent())
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
            batchSize = 1)

    handler.query shouldBe
        CypherParser.parse(
                """
                  UNWIND ${'$'}messages AS event 
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
                  RETURN sum(created) AS created, sum(deleted) AS deleted
                  """
                    .trimIndent())
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
            batchSize = 1)

    handler.query shouldBe
        CypherParser.parse(
                """
                  UNWIND ${'$'}messages AS event 
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
                  RETURN sum(created) AS created, sum(deleted) AS deleted
                  """
                    .trimIndent())
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
            batchSize = 1)

    handler.query shouldBe
        CypherParser.parse(
                """
                  UNWIND ${'$'}messages AS event 
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
                  RETURN sum(created) AS created, sum(deleted) AS deleted
                  """
                    .trimIndent())
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
            batchSize = 1)

    handler.query shouldBe
        CypherParser.parse(
                """
                  UNWIND ${'$'}messages AS event 
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
                  RETURN sum(created) AS created, sum(deleted) AS deleted
                  """
                    .trimIndent())
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
            batchSize = 1)

    handler.query shouldBe
        CypherParser.parse(
                """
                  UNWIND ${'$'}messages AS event 
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
                  RETURN sum(created) AS created, sum(deleted) AS deleted
                  """
                    .trimIndent())
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
            batchSize = 1)

    handler.query shouldBe
        CypherParser.parse(
                """
                  UNWIND ${'$'}messages AS event 
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
                  RETURN sum(created) AS created, sum(deleted) AS deleted
                  """
                    .trimIndent())
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
                                "properties" to emptyMap<String, Any?>()),
                        "end" to
                            mapOf(
                                "keys" to mapOf("id" to 2),
                                "properties" to emptyMap<String, Any?>()),
                        "keys" to mapOf<String, Any?>("id" to 3),
                        "properties" to mapOf<String, Any?>("foo" to "foo", "bar" to "bar")))))
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
                                "properties" to emptyMap<String, Any?>()),
                        "end" to
                            mapOf(
                                "keys" to mapOf("id" to 2),
                                "properties" to emptyMap<String, Any?>()),
                        "keys" to mapOf<String, Any?>("id" to 3),
                        "properties" to
                            mapOf<String, Any?>(
                                "foo" to "foo",
                                "bar" to "bar",
                                "nested.baz" to "baz",
                                "nested.bak" to "bak")))))
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
                                "properties" to emptyMap<String, Any?>()),
                        "end" to
                            mapOf(
                                "keys" to mapOf("id" to 2),
                                "properties" to emptyMap<String, Any?>()),
                        "keys" to mapOf<String, Any?>("id" to 3),
                        "properties" to
                            mapOf<String, Any?>("nested.baz" to "baz", "nested.bak" to "bak")))))
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
                                "properties" to emptyMap<String, Any?>()),
                        "end" to
                            mapOf(
                                "keys" to mapOf("id" to 2),
                                "properties" to emptyMap<String, Any?>()),
                        "keys" to mapOf<String, Any?>("id" to 3),
                        "properties" to
                            mapOf<String, Any?>("bar" to "bar", "nested.baz" to "baz")))))
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
                                "properties" to emptyMap<String, Any?>()),
                        "end" to
                            mapOf(
                                "keys" to mapOf("id" to 2),
                                "properties" to emptyMap<String, Any?>()),
                        "keys" to mapOf<String, Any?>("id" to 3),
                        "properties" to mapOf<String, Any?>("foo" to "foo", "bar" to "bar")))))
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
                                "properties" to emptyMap<String, Any?>()),
                        "end" to
                            mapOf(
                                "keys" to mapOf("id" to 2),
                                "properties" to emptyMap<String, Any?>()),
                        "keys" to mapOf<String, Any?>("id" to 3),
                        "properties" to
                            mapOf<String, Any?>("new_foo" to "foo", "new_bar" to "bar")))))
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
                                "properties" to emptyMap<String, Any?>()),
                        "end" to
                            mapOf(
                                "keys" to mapOf("id" to 2),
                                "properties" to emptyMap<String, Any?>()),
                        "keys" to mapOf<String, Any?>("id" to 3),
                        "properties" to
                            mapOf<String, Any?>(
                                "new_foo" to "foo",
                                "new_bar" to "bar",
                                "new_nested_baz" to "baz")))))
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
                                "properties" to emptyMap<String, Any?>()),
                        "end" to
                            mapOf(
                                "keys" to mapOf("id" to 2),
                                "properties" to emptyMap<String, Any?>()),
                        "keys" to mapOf<String, Any?>("id" to 3),
                        "properties" to
                            mapOf<String, Any?>(
                                "new_foo" to "foo",
                                "new_bar" to "bar",
                                "new_nested.baz" to "baz",
                                "new_nested.bak" to "bak")))))
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
                                "properties" to emptyMap<String, Any?>()),
                        "end" to
                            mapOf(
                                "keys" to mapOf("id" to 2),
                                "properties" to emptyMap<String, Any?>()),
                        "keys" to mapOf<String, Any?>("id" to 3),
                        "properties" to mapOf<String, Any?>("foo" to "foo", "bar" to "bar")))))
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
                                "properties" to emptyMap<String, Any?>()),
                        "end" to
                            mapOf(
                                "keys" to mapOf("id" to 2),
                                "properties" to emptyMap<String, Any?>()),
                        "keys" to mapOf<String, Any?>("id" to 3),
                        "properties" to
                            mapOf<String, Any?>("foo" to "foo", "bar" to "bar", "baz" to "baz")))))
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
                                "properties" to emptyMap<String, Any?>()),
                        "end" to
                            mapOf(
                                "keys" to mapOf("id" to 2),
                                "properties" to emptyMap<String, Any?>()),
                        "keys" to mapOf<String, Any?>("id" to 3),
                        "properties" to
                            mapOf<String, Any?>(
                                "foo" to "foo",
                                "bar" to "bar",
                                "new_nested.baz" to "baz",
                                "new_nested.bak" to "bak")))))
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
                                "properties" to emptyMap<String, Any?>()),
                        "end" to
                            mapOf(
                                "keys" to mapOf("id" to 2),
                                "properties" to emptyMap<String, Any?>()),
                        "keys" to mapOf<String, Any?>("id" to 3),
                        "properties" to
                            mapOf<String, Any?>("foo" to "foo", "bar" to "bar", "baz" to "baz")))))
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
                                "properties" to emptyMap<String, Any?>()),
                        "end" to
                            mapOf(
                                "keys" to mapOf("id" to 2),
                                "properties" to emptyMap<String, Any?>()),
                        "keys" to mapOf<String, Any?>("id" to 3),
                        "properties" to
                            mapOf<String, Any?>(
                                "foo" to "foo",
                                "bar" to "bar",
                                "created_at" to
                                    Instant.ofEpochMilli(TIMESTAMP).atOffset(ZoneOffset.UTC))))))
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
                                        "propA" to "a", "foo" to "foo", "nested.baz" to "baz")),
                        "end" to
                            mapOf(
                                "keys" to mapOf("id" to 2),
                                "properties" to
                                    mapOf<String, Any?>(
                                        "propB" to "b", "foo" to "foo", "nested.bak" to "bak")),
                        "keys" to mapOf<String, Any?>("id" to 3),
                        "properties" to mapOf<String, Any?>("bar" to "bar")))))
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
                        "keys" to mapOf<String, Any?>("id" to 3)))))
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
                        "keys" to mapOf<String, Any?>("id" to 3)))))
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
                        "keys" to mapOf<String, Any?>("id" to 3)))))
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
            batchSize = 1)
    handler.handle(
        listOf(
            newMessage(valueSchema, value, keySchema = keySchema, key = key),
        ),
    ) shouldBe
        listOf(
            listOf(
                ChangeQuery(
                    null,
                    null,
                    Query(
                        CypherParser.parse(
                                """
                                UNWIND ${'$'}messages AS event 
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
                                RETURN sum(created) AS created, sum(deleted) AS deleted
                                """
                                    .trimIndent())
                            .cypher,
                        mapOf("events" to expected),
                    ),
                ),
            ),
        )
  }
}
