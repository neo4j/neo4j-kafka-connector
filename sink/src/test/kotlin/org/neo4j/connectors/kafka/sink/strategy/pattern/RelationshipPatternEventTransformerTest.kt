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
package org.neo4j.connectors.kafka.sink.strategy.pattern

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
import org.neo4j.connectors.kafka.sink.strategy.DeleteRelationshipSinkAction
import org.neo4j.connectors.kafka.sink.strategy.HandlerTest
import org.neo4j.connectors.kafka.sink.strategy.LookupMode
import org.neo4j.connectors.kafka.sink.strategy.MergeRelationshipSinkAction
import org.neo4j.connectors.kafka.sink.strategy.NodeMatcher
import org.neo4j.connectors.kafka.sink.strategy.RelationshipMatcher
import org.neo4j.connectors.kafka.sink.strategy.SinkAction
import org.neo4j.connectors.kafka.sink.strategy.SinkActionNodeReference

class RelationshipPatternEventTransformerTest : HandlerTest() {

  @Test
  fun `should include all properties`() {
    assertQueryAndParameters(
        "(:LabelA{!id: idStart})-[:REL_TYPE{!id: idRelationship}]->(:LabelB{!id: idEnd})",
        value = """{"idStart": 1, "idEnd": 2, "idRelationship": 3, "foo": "foo", "bar": "bar"}""",
        expected =
            MergeRelationshipSinkAction(
                SinkActionNodeReference(
                    NodeMatcher.ByLabelsAndProperties(setOf("LabelA"), mapOf("id" to 1)),
                    LookupMode.MERGE,
                    null,
                    emptyMap(),
                ),
                SinkActionNodeReference(
                    NodeMatcher.ByLabelsAndProperties(setOf("LabelB"), mapOf("id" to 2)),
                    LookupMode.MERGE,
                    null,
                    emptyMap(),
                ),
                RelationshipMatcher.ByTypeAndProperties("REL_TYPE", mapOf("id" to 3), true),
                mutateProperties = mapOf("foo" to "foo", "bar" to "bar"),
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
            MergeRelationshipSinkAction(
                SinkActionNodeReference(
                    NodeMatcher.ByLabelsAndProperties(setOf("LabelA"), mapOf("id" to 1)),
                    LookupMode.MERGE,
                    null,
                    emptyMap(),
                ),
                SinkActionNodeReference(
                    NodeMatcher.ByLabelsAndProperties(setOf("LabelB"), mapOf("id" to 2)),
                    LookupMode.MERGE,
                    null,
                    emptyMap(),
                ),
                RelationshipMatcher.ByTypeAndProperties("REL_TYPE", mapOf("id" to 3), true),
                mutateProperties =
                    mapOf(
                        "foo" to "foo",
                        "bar" to "bar",
                        "nested.baz" to "baz",
                        "nested.bak" to "bak",
                    ),
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
            MergeRelationshipSinkAction(
                SinkActionNodeReference(
                    NodeMatcher.ByLabelsAndProperties(setOf("LabelA"), mapOf("id" to 1)),
                    LookupMode.MERGE,
                    null,
                    emptyMap(),
                ),
                SinkActionNodeReference(
                    NodeMatcher.ByLabelsAndProperties(setOf("LabelB"), mapOf("id" to 2)),
                    LookupMode.MERGE,
                    null,
                    emptyMap(),
                ),
                RelationshipMatcher.ByTypeAndProperties("REL_TYPE", mapOf("id" to 3), true),
                mutateProperties = mapOf("nested.baz" to "baz", "nested.bak" to "bak"),
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
            MergeRelationshipSinkAction(
                SinkActionNodeReference(
                    NodeMatcher.ByLabelsAndProperties(setOf("LabelA"), mapOf("id" to 1)),
                    LookupMode.MERGE,
                    null,
                    emptyMap(),
                ),
                SinkActionNodeReference(
                    NodeMatcher.ByLabelsAndProperties(setOf("LabelB"), mapOf("id" to 2)),
                    LookupMode.MERGE,
                    null,
                    emptyMap(),
                ),
                RelationshipMatcher.ByTypeAndProperties("REL_TYPE", mapOf("id" to 3), true),
                mutateProperties = mapOf("bar" to "bar", "nested.baz" to "baz"),
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
            MergeRelationshipSinkAction(
                SinkActionNodeReference(
                    NodeMatcher.ByLabelsAndProperties(setOf("LabelA"), mapOf("id" to 1)),
                    LookupMode.MERGE,
                    null,
                    emptyMap(),
                ),
                SinkActionNodeReference(
                    NodeMatcher.ByLabelsAndProperties(setOf("LabelB"), mapOf("id" to 2)),
                    LookupMode.MERGE,
                    null,
                    emptyMap(),
                ),
                RelationshipMatcher.ByTypeAndProperties("REL_TYPE", mapOf("id" to 3), true),
                mutateProperties = mapOf("foo" to "foo", "bar" to "bar"),
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
            MergeRelationshipSinkAction(
                SinkActionNodeReference(
                    NodeMatcher.ByLabelsAndProperties(setOf("LabelA"), mapOf("id" to 1)),
                    LookupMode.MERGE,
                    null,
                    emptyMap(),
                ),
                SinkActionNodeReference(
                    NodeMatcher.ByLabelsAndProperties(setOf("LabelB"), mapOf("id" to 2)),
                    LookupMode.MERGE,
                    null,
                    emptyMap(),
                ),
                RelationshipMatcher.ByTypeAndProperties("REL_TYPE", mapOf("id" to 3), true),
                mutateProperties = mapOf("new_foo" to "foo", "new_bar" to "bar"),
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
            MergeRelationshipSinkAction(
                SinkActionNodeReference(
                    NodeMatcher.ByLabelsAndProperties(setOf("LabelA"), mapOf("id" to 1)),
                    LookupMode.MERGE,
                    null,
                    emptyMap(),
                ),
                SinkActionNodeReference(
                    NodeMatcher.ByLabelsAndProperties(setOf("LabelB"), mapOf("id" to 2)),
                    LookupMode.MERGE,
                    null,
                    emptyMap(),
                ),
                RelationshipMatcher.ByTypeAndProperties("REL_TYPE", mapOf("id" to 3), true),
                mutateProperties =
                    mapOf("new_foo" to "foo", "new_bar" to "bar", "new_nested_baz" to "baz"),
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
            MergeRelationshipSinkAction(
                SinkActionNodeReference(
                    NodeMatcher.ByLabelsAndProperties(setOf("LabelA"), mapOf("id" to 1)),
                    LookupMode.MERGE,
                    null,
                    emptyMap(),
                ),
                SinkActionNodeReference(
                    NodeMatcher.ByLabelsAndProperties(setOf("LabelB"), mapOf("id" to 2)),
                    LookupMode.MERGE,
                    null,
                    emptyMap(),
                ),
                RelationshipMatcher.ByTypeAndProperties("REL_TYPE", mapOf("id" to 3), true),
                mutateProperties =
                    mapOf(
                        "new_foo" to "foo",
                        "new_bar" to "bar",
                        "new_nested.baz" to "baz",
                        "new_nested.bak" to "bak",
                    ),
            ),
    )
  }

  @Test
  fun `should include explicit properties`() {
    assertQueryAndParameters(
        "(:LabelA{!id: idStart})-[:REL_TYPE{!id: idRelationship, foo: __value.foo, bar: __value.bar}]->(:LabelB{!id: idEnd})",
        value = """{"idStart": 1, "idEnd": 2, "idRelationship": 3, "foo": "foo", "bar": "bar"}""",
        expected =
            MergeRelationshipSinkAction(
                SinkActionNodeReference(
                    NodeMatcher.ByLabelsAndProperties(setOf("LabelA"), mapOf("id" to 1)),
                    LookupMode.MERGE,
                    null,
                    emptyMap(),
                ),
                SinkActionNodeReference(
                    NodeMatcher.ByLabelsAndProperties(setOf("LabelB"), mapOf("id" to 2)),
                    LookupMode.MERGE,
                    null,
                    emptyMap(),
                ),
                RelationshipMatcher.ByTypeAndProperties("REL_TYPE", mapOf("id" to 3), true),
                mutateProperties = mapOf("foo" to "foo", "bar" to "bar"),
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
            MergeRelationshipSinkAction(
                SinkActionNodeReference(
                    NodeMatcher.ByLabelsAndProperties(setOf("LabelA"), mapOf("id" to 1)),
                    LookupMode.MERGE,
                    null,
                    emptyMap(),
                ),
                SinkActionNodeReference(
                    NodeMatcher.ByLabelsAndProperties(setOf("LabelB"), mapOf("id" to 2)),
                    LookupMode.MERGE,
                    null,
                    emptyMap(),
                ),
                RelationshipMatcher.ByTypeAndProperties("REL_TYPE", mapOf("id" to 3), true),
                mutateProperties = mapOf("foo" to "foo", "bar" to "bar", "baz" to "baz"),
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
            MergeRelationshipSinkAction(
                SinkActionNodeReference(
                    NodeMatcher.ByLabelsAndProperties(setOf("LabelA"), mapOf("id" to 1)),
                    LookupMode.MERGE,
                    null,
                    emptyMap(),
                ),
                SinkActionNodeReference(
                    NodeMatcher.ByLabelsAndProperties(setOf("LabelB"), mapOf("id" to 2)),
                    LookupMode.MERGE,
                    null,
                    emptyMap(),
                ),
                RelationshipMatcher.ByTypeAndProperties("REL_TYPE", mapOf("id" to 3), true),
                mutateProperties =
                    mapOf(
                        "foo" to "foo",
                        "bar" to "bar",
                        "new_nested.baz" to "baz",
                        "new_nested.bak" to "bak",
                    ),
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
            MergeRelationshipSinkAction(
                SinkActionNodeReference(
                    NodeMatcher.ByLabelsAndProperties(setOf("LabelA"), mapOf("id" to 1)),
                    LookupMode.MERGE,
                    null,
                    emptyMap(),
                ),
                SinkActionNodeReference(
                    NodeMatcher.ByLabelsAndProperties(setOf("LabelB"), mapOf("id" to 2)),
                    LookupMode.MERGE,
                    null,
                    emptyMap(),
                ),
                RelationshipMatcher.ByTypeAndProperties("REL_TYPE", mapOf("id" to 3), true),
                mutateProperties = mapOf("foo" to "foo", "bar" to "bar", "baz" to "baz"),
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
            MergeRelationshipSinkAction(
                SinkActionNodeReference(
                    NodeMatcher.ByLabelsAndProperties(setOf("LabelA"), mapOf("id" to 1)),
                    LookupMode.MERGE,
                    null,
                    emptyMap(),
                ),
                SinkActionNodeReference(
                    NodeMatcher.ByLabelsAndProperties(setOf("LabelB"), mapOf("id" to 2)),
                    LookupMode.MERGE,
                    null,
                    emptyMap(),
                ),
                RelationshipMatcher.ByTypeAndProperties("REL_TYPE", mapOf("id" to 3), true),
                mutateProperties =
                    mapOf(
                        "foo" to "foo",
                        "bar" to "bar",
                        "created_at" to Instant.ofEpochMilli(TIMESTAMP).atOffset(ZoneOffset.UTC),
                    ),
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
            MergeRelationshipSinkAction(
                SinkActionNodeReference(
                    NodeMatcher.ByLabelsAndProperties(setOf("LabelA"), mapOf("id" to 1)),
                    LookupMode.MERGE,
                    null,
                    mapOf("foo" to "foo", "propA" to "a", "nested.baz" to "baz"),
                ),
                SinkActionNodeReference(
                    NodeMatcher.ByLabelsAndProperties(setOf("LabelB"), mapOf("id" to 2)),
                    LookupMode.MERGE,
                    null,
                    mapOf("foo" to "foo", "propB" to "b", "nested.bak" to "bak"),
                ),
                RelationshipMatcher.ByTypeAndProperties("REL_TYPE", mapOf("id" to 3), true),
                mutateProperties = mapOf("bar" to "bar"),
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
            DeleteRelationshipSinkAction(
                SinkActionNodeReference(
                    NodeMatcher.ByLabelsAndProperties(setOf("LabelA"), mapOf("id" to 1)),
                    LookupMode.MATCH,
                ),
                SinkActionNodeReference(
                    NodeMatcher.ByLabelsAndProperties(setOf("LabelB"), mapOf("id" to 2)),
                    LookupMode.MATCH,
                ),
                RelationshipMatcher.ByTypeAndProperties("REL_TYPE", mapOf("id" to 3), true),
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
            DeleteRelationshipSinkAction(
                SinkActionNodeReference(
                    NodeMatcher.ByLabelsAndProperties(setOf("LabelA"), mapOf("id" to 1)),
                    LookupMode.MATCH,
                ),
                SinkActionNodeReference(
                    NodeMatcher.ByLabelsAndProperties(setOf("LabelB"), mapOf("id" to 2)),
                    LookupMode.MATCH,
                ),
                RelationshipMatcher.ByTypeAndProperties("REL_TYPE", mapOf("id" to 3), true),
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
            DeleteRelationshipSinkAction(
                SinkActionNodeReference(
                    NodeMatcher.ByLabelsAndProperties(setOf("LabelA"), mapOf("id" to 1)),
                    LookupMode.MATCH,
                ),
                SinkActionNodeReference(
                    NodeMatcher.ByLabelsAndProperties(setOf("LabelB"), mapOf("id" to 2)),
                    LookupMode.MATCH,
                ),
                RelationshipMatcher.ByTypeAndProperties("REL_TYPE", mapOf("id" to 3), true),
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
        RelationshipPatternEventTransformer(
            "my-topic",
            "(:LabelA{!idStart})-[:REL_TYPE{!id, !second_id}]->(:LabelB{!idEnd})",
            mergeNodeProperties = true,
            mergeRelationshipProperties = true,
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
        RelationshipPatternEventTransformer(
            "my-topic",
            "(:LabelA{!idStart})-[:REL_TYPE{!id, !second_id}]->(:LabelB{!idEnd})",
            mergeNodeProperties = true,
            mergeRelationshipProperties = true,
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
        RelationshipPatternEventTransformer(
            "my-topic",
            "(:LabelA{!idStart})-[:REL_TYPE{!id}]->(:LabelB{!idEnd})",
            mergeNodeProperties = true,
            mergeRelationshipProperties = true,
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
        RelationshipPatternEventTransformer(
            "my-topic",
            "(:LabelA{!idStart})-[:REL_TYPE{!id, !second_id}]->(:LabelB{!idEnd})",
            mergeNodeProperties = true,
            mergeRelationshipProperties = true,
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
        RelationshipPatternEventTransformer(
            "my-topic",
            "(:LabelA{!idStart})-[:REL_TYPE{!id, !second_id}]->(:LabelB{!idEnd})",
            mergeNodeProperties = true,
            mergeRelationshipProperties = true,
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
      expected: SinkAction,
      mergeNodeProperties: Boolean = true,
      mergeRelationshipProperties: Boolean = true,
  ) {
    val transformer =
        RelationshipPatternEventTransformer(
            "my-topic",
            pattern,
            mergeNodeProperties = mergeNodeProperties,
            mergeRelationshipProperties = mergeRelationshipProperties,
        )
    val sinkMessage = newMessage(valueSchema, value, keySchema = keySchema, key = key)
    val transformed = transformer.transform(sinkMessage)
    transformed shouldBe expected
  }

  private inline fun <reified T : Throwable> assertThrowsHandler(
      pattern: String,
      keySchema: Schema = Schema.STRING_SCHEMA,
      key: Any? = null,
      valueSchema: Schema = Schema.STRING_SCHEMA,
      value: Any? = null,
      message: String? = null,
  ) {
    val transformer =
        RelationshipPatternEventTransformer(
            "my-topic",
            pattern,
            mergeNodeProperties = false,
            mergeRelationshipProperties = false,
        )
    val sinkMessage = newMessage(valueSchema, value, keySchema = keySchema, key = key)

    if (message != null) {
      assertThrows<T> { transformer.transform(sinkMessage) } shouldHaveMessage message
    } else {
      assertThrows<T> { transformer.transform(sinkMessage) }
    }
  }
}
