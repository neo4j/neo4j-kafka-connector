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
package org.neo4j.connectors.kafka.data

import io.kotest.assertions.withClue
import io.kotest.matchers.shouldBe
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.junit.jupiter.api.Test

class SchemasTest {

  private val unknown = Schemas.UNKNOWN

  @Test
  fun `identical schemas should combine to themselves`() {
    listOf(
            Schema.INT64_SCHEMA,
            Schema.OPTIONAL_STRING_SCHEMA,
            SimpleTypes.LOCALDATE.schema(),
            SimpleTypes.POINT.schema(),
            PropertyType.schema,
            SchemaBuilder.array(Schema.INT64_SCHEMA).build(),
            SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT64_SCHEMA).build(),
            SchemaBuilder.struct().field("a", Schema.INT64_SCHEMA).build(),
            unknown,
        )
        .forEach { withClue(it) { Schemas.combine(it, it) shouldBe it } }
  }

  @Test
  fun `the unknown placeholder should combine with anything and give it back as optional`() {
    Schemas.combine(unknown, Schema.INT64_SCHEMA) shouldBe Schema.OPTIONAL_INT64_SCHEMA
    Schemas.combine(Schema.INT64_SCHEMA, unknown) shouldBe Schema.OPTIONAL_INT64_SCHEMA
    Schemas.combine(unknown, SimpleTypes.POINT.schema()) shouldBe SimpleTypes.POINT.schema(true)
    Schemas.combine(unknown, unknown) shouldBe unknown
  }

  @Test
  fun `schemas differing only in optionality should combine to the optional one`() {
    Schemas.combine(Schema.INT64_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA) shouldBe
        Schema.OPTIONAL_INT64_SCHEMA
    Schemas.combine(SimpleTypes.LOCALDATE.schema(), SimpleTypes.LOCALDATE.schema(true)) shouldBe
        SimpleTypes.LOCALDATE.schema(true)
  }

  @Test
  fun `structs should combine over the union of their fields`() {
    val combined =
        Schemas.combine(
            SchemaBuilder.struct().field("a", Schema.INT64_SCHEMA).build(),
            SchemaBuilder.struct()
                .field("a", Schema.INT64_SCHEMA)
                .field("b", Schema.STRING_SCHEMA)
                .build(),
        )

    combined shouldBe
        SchemaBuilder.struct()
            .field("a", Schema.INT64_SCHEMA)
            .field("b", Schema.OPTIONAL_STRING_SCHEMA)
            .build()
  }

  @Test
  fun `containers should combine their element schema`() {
    Schemas.combine(
        SchemaBuilder.array(Schema.INT64_SCHEMA).build(),
        SchemaBuilder.array(Schema.OPTIONAL_INT64_SCHEMA).build(),
    ) shouldBe SchemaBuilder.array(Schema.OPTIONAL_INT64_SCHEMA).build()

    Schemas.combine(
        SchemaBuilder.map(Schema.STRING_SCHEMA, unknown).build(),
        SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT64_SCHEMA).build(),
    ) shouldBe SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA).build()

    Schemas.combine(
        SchemaBuilder.array(SchemaBuilder.struct().field("a", Schema.INT64_SCHEMA).build()).build(),
        SchemaBuilder.array(SchemaBuilder.struct().field("b", Schema.INT64_SCHEMA).build()).build(),
    ) shouldBe
        SchemaBuilder.array(
                SchemaBuilder.struct()
                    .field("a", Schema.OPTIONAL_INT64_SCHEMA)
                    .field("b", Schema.OPTIONAL_INT64_SCHEMA)
                    .build()
            )
            .build()
  }

  @Test
  fun `values of different types should have no shared schema`() {
    listOf(
            Schema.BOOLEAN_SCHEMA to Schema.INT64_SCHEMA,
            Schema.INT64_SCHEMA to Schema.FLOAT64_SCHEMA,
            Schema.INT64_SCHEMA to Schema.STRING_SCHEMA,
            Schema.BYTES_SCHEMA to Schema.STRING_SCHEMA,
            SimpleTypes.LOCALDATE.schema() to Schema.STRING_SCHEMA,
            SimpleTypes.LOCALDATE.schema() to SimpleTypes.LOCALDATETIME.schema(),
            SimpleTypes.POINT.schema() to SimpleTypes.DURATION.schema(),
            PropertyType.schema to SimpleTypes.POINT.schema(),
            SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT64_SCHEMA).build() to
                SchemaBuilder.struct().field("a", Schema.INT64_SCHEMA).build(),
            SchemaBuilder.array(Schema.INT64_SCHEMA).build() to Schema.INT64_SCHEMA,
            SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT64_SCHEMA).build() to
                SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build(),
        )
        .forEach { (a, b) ->
          withClue("$a and $b") {
            Schemas.combine(a, b) shouldBe null
            Schemas.combine(b, a) shouldBe null
          }
        }
  }

  @Test
  fun `a point should not combine with a struct holding the same fields`() {
    val lookalike =
        SchemaBuilder.struct()
            .apply { SimpleTypes.POINT.schema().fields().forEach { field(it.name(), it.schema()) } }
            .build()

    Schemas.combine(SimpleTypes.POINT.schema(), lookalike) shouldBe null
  }

  @Test
  fun `entity structs should combine only with the same kind of entity and the same fields`() {
    val node = nodeSchema("name" to Schema.OPTIONAL_STRING_SCHEMA)
    val otherNode = nodeSchema("name" to unknown)
    val nodeWithMoreKeys =
        nodeSchema("name" to Schema.OPTIONAL_STRING_SCHEMA, "age" to Schema.OPTIONAL_INT64_SCHEMA)
    val relationship = relationshipSchema("name" to Schema.OPTIONAL_STRING_SCHEMA)
    val plainMap = SchemaBuilder.struct().field("name", Schema.OPTIONAL_STRING_SCHEMA).build()

    Schemas.combine(node, otherNode) shouldBe node
    Schemas.combine(node, nodeWithMoreKeys) shouldBe null
    Schemas.combine(node, relationship) shouldBe null
    Schemas.combine(node, plainMap) shouldBe null
  }

  @Test
  fun `the indexed struct a list falls back to should not combine`() {
    val two =
        SchemaBuilder.struct()
            .field("e0", Schema.OPTIONAL_INT64_SCHEMA)
            .field("e1", Schema.OPTIONAL_INT64_SCHEMA)
            .build()
    val three =
        SchemaBuilder.struct()
            .field("e0", Schema.OPTIONAL_INT64_SCHEMA)
            .field("e1", Schema.OPTIONAL_INT64_SCHEMA)
            .field("e2", Schema.OPTIONAL_INT64_SCHEMA)
            .build()

    Schemas.combine(two, three) shouldBe null
    // an identical one is safe, because both lists then hold the same positions
    Schemas.combine(two, two) shouldBe two
  }

  @Test
  fun `combining a group should not depend on its order`() {
    val schemas =
        listOf(
            SchemaBuilder.struct().field("a", Schema.OPTIONAL_INT64_SCHEMA).build(),
            SchemaBuilder.struct().field("b", Schema.OPTIONAL_STRING_SCHEMA).build(),
            unknown,
            SchemaBuilder.struct().field("a", unknown).build(),
        )

    val combined = Schemas.combineAll(schemas)
    combined shouldBe Schemas.combineAll(schemas.reversed())
    combined shouldBe Schemas.combineAll(schemas + schemas)
  }

  @Test
  fun `an empty group should combine to the unknown placeholder`() {
    Schemas.combineAll(emptyList()) shouldBe unknown
  }

  @Test
  fun `a group holding values with no shared schema should combine to nothing`() {
    Schemas.combineAll(
        listOf(Schema.INT64_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA, Schema.STRING_SCHEMA)
    ) shouldBe null
  }

  @Test
  fun `normalize should sort the fields of a map's struct and make them optional at every level`() {
    val nested = SchemaBuilder.struct().field("z", Schema.INT64_SCHEMA).build()
    val schema =
        SchemaBuilder.struct()
            .field("b", SchemaBuilder.array(nested).build())
            .field("a", Schema.INT64_SCHEMA)
            .optional()
            .build()

    Schemas.normalize(schema) shouldBe
        SchemaBuilder.struct()
            .field("a", Schema.OPTIONAL_INT64_SCHEMA)
            .field(
                "b",
                SchemaBuilder.array(
                        SchemaBuilder.struct().field("z", Schema.OPTIONAL_INT64_SCHEMA).build()
                    )
                    .optional()
                    .build(),
            )
            .optional()
            .build()
  }

  @Test
  fun `normalize should keep the field order of an entity struct and of the indexed struct`() {
    val node = nodeSchema("name" to Schema.STRING_SCHEMA, "age" to Schema.INT64_SCHEMA)
    Schemas.normalize(node).fields().map { it.name() } shouldBe
        listOf("<elementId>", "<labels>", "name", "age")
    Schemas.normalize(node).fields().all { it.schema().isOptional } shouldBe true

    val indexed =
        SchemaBuilder.struct()
            .apply { (0..10).forEach { field("e$it", Schema.OPTIONAL_INT64_SCHEMA) } }
            .build()
    Schemas.normalize(indexed) shouldBe indexed
  }

  @Test
  fun `normalize should leave a schema carrying a type name alone`() {
    Schemas.normalize(SimpleTypes.POINT.schema()) shouldBe SimpleTypes.POINT.schema()
    Schemas.normalize(PropertyType.schema) shouldBe PropertyType.schema
  }

  private fun nodeSchema(vararg properties: Pair<String, Schema>): Schema =
      SchemaBuilder.struct()
          .field("<elementId>", Schema.STRING_SCHEMA)
          .field("<labels>", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
          .apply { properties.forEach { field(it.first, it.second) } }
          .build()

  private fun relationshipSchema(vararg properties: Pair<String, Schema>): Schema =
      SchemaBuilder.struct()
          .field("<elementId>", Schema.STRING_SCHEMA)
          .field("<type>", Schema.STRING_SCHEMA)
          .field("<start.elementId>", Schema.STRING_SCHEMA)
          .field("<end.elementId>", Schema.STRING_SCHEMA)
          .apply { properties.forEach { field(it.first, it.second) } }
          .build()
}
