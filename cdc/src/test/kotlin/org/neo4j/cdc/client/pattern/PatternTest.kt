/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cdc.client.pattern

import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import org.junit.jupiter.api.Test

class PatternTest {

  @Test
  fun `should parse numeric literals correctly`() {
    assertContentEquals(
        listOf(
            NodePattern(
                emptySet(),
                mapOf(
                    "id1" to 0L,
                    "id2" to 125L,
                    "id3" to -24278L,
                    "id4" to -2147483648L,
                    "id5" to 2147483647L,
                    "id6" to -9223372036854775807L,
                    "id7" to 9223372036854775807L,
                ),
                emptySet(),
                emptySet())),
        Pattern.parse(
            "({id1: 0, id2: 125, id3: -24278, id4: -2147483648, id5: 2147483647, id6: -9223372036854775807, id7: 9223372036854775807})"))

    assertContentEquals(
        listOf(
            NodePattern(
                emptySet(),
                mapOf("id1" to 0x00L, "id2" to -0x0fL, "id3" to 0x246FFL),
                emptySet(),
                emptySet())),
        Pattern.parse("({id1: 0x00, id2: -0x0f, id3: 0x246FF})"))

    assertContentEquals(
        listOf(
            NodePattern(
                emptySet(),
                mapOf("id1" to 0L, "id2" to -15L, "id3" to 5349L),
                emptySet(),
                emptySet())),
        Pattern.parse("({id1: 0o0, id2: -0o17, id3: 0o12345})"))

    assertContentEquals(
        listOf(
            NodePattern(
                emptySet(),
                mapOf(
                    "id1" to 0.0,
                    "id2" to -125.25,
                    "id3" to 1.2e20,
                    "id4" to -1.2e20,
                    "id5" to 2.7182818284),
                emptySet(),
                emptySet())),
        Pattern.parse("({id1: 0.0, id2: -125.25, id3: 1.2e20, id4: -1.2e20, id5: 2.7182818284})"))

    assertContentEquals(
        listOf(
            NodePattern(
                emptySet(),
                mapOf(
                    "id1" to Double.NaN,
                    "id2" to Double.POSITIVE_INFINITY,
                    "id3" to Double.POSITIVE_INFINITY),
                emptySet(),
                emptySet())),
        Pattern.parse("({id1: NaN, id2: INF, id3: INFINITY})"))
  }

  @Test
  fun `should parse string literals`() {
    assertContentEquals(
        listOf(
            NodePattern(
                emptySet(),
                mapOf("id1" to "", "id2" to "", "id3" to "a string", "id4" to "another string"),
                emptySet(),
                emptySet())),
        Pattern.parse("({id1: '', id2: \"\", id3: 'a string',  id4: \"another string\" })"))
  }

  @Test
  fun `should parse bool literals`() {
    assertContentEquals(
        listOf(
            NodePattern(emptySet(), mapOf("id1" to true, "id2" to false), emptySet(), emptySet())),
        Pattern.parse("({id1: true, id2: false })"))
  }

  @Test
  fun `should parse node patterns`() {
    assertContentEquals(emptySet(), Pattern.parse(""))
    assertContentEquals(
        listOf(NodePattern(emptySet(), emptyMap(), emptySet(), emptySet())), Pattern.parse("()"))
    assertContentEquals(
        listOf(NodePattern(setOf("Person"), emptyMap(), emptySet(), emptySet())),
        Pattern.parse("(:Person)"))
    assertContentEquals(
        listOf(NodePattern(setOf("Person", "Employee"), emptyMap(), emptySet(), emptySet())),
        Pattern.parse("(:Person:Employee)"))
    assertContentEquals(
        listOf(NodePattern(setOf("Person"), emptyMap(), emptySet(), emptySet())),
        Pattern.parse("(:Person {})"))
    assertContentEquals(
        listOf(NodePattern(emptySet(), emptyMap(), emptySet(), emptySet())), Pattern.parse("({})"))
    assertContentEquals(
        listOf(NodePattern(emptySet(), emptyMap(), setOf("*"), emptySet())), Pattern.parse("({*})"))
    assertContentEquals(
        listOf(NodePattern(setOf("Person"), emptyMap(), setOf("*"), emptySet())),
        Pattern.parse("(:Person {*})"))
    assertContentEquals(
        listOf(NodePattern(setOf("Person", "Employee"), emptyMap(), setOf("*"), emptySet())),
        Pattern.parse("(:Person:Employee {*})"))

    assertContentEquals(
        listOf(NodePattern(emptySet(), mapOf("id" to 5L), emptySet(), emptySet())),
        Pattern.parse("({id: 5})"))
    assertContentEquals(
        listOf(NodePattern(emptySet(), mapOf("id" to 5L), setOf("*"), emptySet())),
        Pattern.parse("({id: 5, *})"))
    assertContentEquals(
        listOf(
            NodePattern(emptySet(), mapOf("id" to 5L, "name" to "john"), setOf("*"), emptySet())),
        Pattern.parse("({id: 5, *, name: 'john'})"))
    assertContentEquals(
        listOf(
            NodePattern(
                emptySet(),
                mapOf("id" to 5L, "name" to "john"),
                setOf("select1", "select2", "*"),
                emptySet())),
        Pattern.parse("({id: 5, *, name: 'john', select1, +select2})"))
    assertContentEquals(
        listOf(
            NodePattern(
                emptySet(),
                mapOf("id" to 5L, "name" to "john"),
                setOf("*"),
                setOf("skip1", "skip2"))),
        Pattern.parse("({id: 5, *, name: 'john', -skip1, -skip2})"))
    assertContentEquals(
        listOf(
            NodePattern(
                emptySet(),
                mapOf("id" to 5L, "name" to "john"),
                setOf("select1", "select2", "*"),
                setOf("skip1", "skip2"))),
        Pattern.parse("({id: 5, *, name: 'john', -skip1, select1, +select2, -skip2})"))
    assertContentEquals(
        listOf(
            NodePattern(
                emptySet(),
                mapOf("id" to 5L, "name" to "john", "surname" to "doe"),
                setOf("select1", "select2", "*"),
                setOf("skip1", "skip2"))),
        Pattern.parse(
            "({id: 5, *, name: 'john', -skip1, select1, +select2, -skip2, surname: 'doe'})"))
    assertContentEquals(
        listOf(
            NodePattern(
                emptySet(), emptyMap(), setOf("select1", "select2", "*"), setOf("skip1", "skip2"))),
        Pattern.parse("({*, -skip1, select1, +select2, -skip2})"))
  }

  @Test
  fun `should parse relationship directions correctly`() {
    assertContentEquals(
        listOf(
            RelationshipPattern(
                "KNOWS",
                NodePattern(emptySet(), emptyMap(), emptySet(), emptySet()),
                NodePattern(emptySet(), emptyMap(), emptySet(), emptySet()),
                false,
                emptyMap(),
                emptySet(),
                emptySet())),
        Pattern.parse("()-[:KNOWS]->()"))
    assertContentEquals(
        listOf(
            RelationshipPattern(
                "KNOWS",
                NodePattern(emptySet(), emptyMap(), emptySet(), emptySet()),
                NodePattern(emptySet(), emptyMap(), emptySet(), emptySet()),
                false,
                emptyMap(),
                emptySet(),
                emptySet())),
        Pattern.parse("()<-[:KNOWS]-()"))
    assertContentEquals(
        listOf(
            RelationshipPattern(
                "KNOWS",
                NodePattern(emptySet(), emptyMap(), emptySet(), emptySet()),
                NodePattern(emptySet(), emptyMap(), emptySet(), emptySet()),
                true,
                emptyMap(),
                emptySet(),
                emptySet())),
        Pattern.parse("()-[:KNOWS]-()"))
    assertContentEquals(
        listOf(
            RelationshipPattern(
                "KNOWS",
                NodePattern(emptySet(), emptyMap(), emptySet(), emptySet()),
                NodePattern(emptySet(), emptyMap(), emptySet(), emptySet()),
                true,
                emptyMap(),
                emptySet(),
                emptySet())),
        Pattern.parse("()<-[:KNOWS]->()"))

    assertContentEquals(
        listOf(
            RelationshipPattern(
                "WORKS_FOR",
                NodePattern(setOf("Person"), emptyMap(), emptySet(), emptySet()),
                NodePattern(setOf("Company"), emptyMap(), emptySet(), emptySet()),
                false,
                emptyMap(),
                emptySet(),
                emptySet())),
        Pattern.parse("(:Person)-[:WORKS_FOR]->(:Company)"))
    assertContentEquals(
        listOf(
            RelationshipPattern(
                "WORKS_FOR",
                NodePattern(setOf("Person"), emptyMap(), emptySet(), emptySet()),
                NodePattern(setOf("Company"), emptyMap(), emptySet(), emptySet()),
                false,
                emptyMap(),
                emptySet(),
                emptySet())),
        Pattern.parse("(:Company)<-[:WORKS_FOR]-(:Person)"))
  }

  @Test
  fun `should parse relationship patterns`() {
    listOf("()-[:WORKS_FOR]->()", "()-[:WORKS_FOR{}]->()").forEach { pattern ->
      assertContentEquals(
          listOf(
              RelationshipPattern(
                  "WORKS_FOR",
                  NodePattern(emptySet(), emptyMap(), emptySet(), emptySet()),
                  NodePattern(emptySet(), emptyMap(), emptySet(), emptySet()),
                  false,
                  emptyMap(),
                  emptySet(),
                  emptySet())),
          Pattern.parse(pattern))
    }

    assertContentEquals(
        listOf(
            RelationshipPattern(
                "WORKS_FOR",
                NodePattern(emptySet(), emptyMap(), emptySet(), emptySet()),
                NodePattern(emptySet(), emptyMap(), emptySet(), emptySet()),
                false,
                mapOf("id" to 5L),
                emptySet(),
                emptySet())),
        Pattern.parse("()-[:WORKS_FOR {id: 5}]->()"))
    assertContentEquals(
        listOf(
            RelationshipPattern(
                "WORKS_FOR",
                NodePattern(emptySet(), emptyMap(), emptySet(), emptySet()),
                NodePattern(emptySet(), emptyMap(), emptySet(), emptySet()),
                false,
                mapOf("id" to 5L, "name" to "john"),
                setOf("*"),
                emptySet())),
        Pattern.parse("()-[:WORKS_FOR {id: 5,*,name: 'john'}]->()"))
    assertContentEquals(
        listOf(
            RelationshipPattern(
                "WORKS_FOR",
                NodePattern(emptySet(), emptyMap(), emptySet(), emptySet()),
                NodePattern(emptySet(), emptyMap(), emptySet(), emptySet()),
                false,
                mapOf("id" to 5L, "name" to "john"),
                setOf("select1", "select2", "*"),
                setOf("skip1", "skip2"))),
        Pattern.parse(
            "()-[:WORKS_FOR {id: 5,*,name: 'john', select1, -skip1, +select2, -skip2}]->()"))

    assertContentEquals(
        listOf(
            RelationshipPattern(
                "WORKS_FOR",
                NodePattern(emptySet(), mapOf("id" to 5L), emptySet(), emptySet()),
                NodePattern(emptySet(), mapOf("name" to "john"), emptySet(), emptySet()),
                false,
                mapOf("id" to 5L),
                emptySet(),
                emptySet())),
        Pattern.parse("({id:5})-[:WORKS_FOR {id: 5}]->({name:'john'})"))
    assertContentEquals(
        listOf(
            RelationshipPattern(
                "WORKS_FOR",
                NodePattern(setOf("Person", "User"), mapOf("id" to 5L), emptySet(), emptySet()),
                NodePattern(setOf("Person"), mapOf("name" to "john"), emptySet(), emptySet()),
                false,
                mapOf("id" to 5L),
                emptySet(),
                emptySet())),
        Pattern.parse("(:Person:User{id:5})-[:WORKS_FOR {id: 5}]->(:Person{name:'john'})"))
  }

  @Test
  fun `relationship node patterns cannot contain property selectors`() {
    listOf(
            "({id})-[:KNOWS]-()",
            "()-[:KNOWS]-({id})",
            "(:Person {+id})-[:KNOWS]->(:Person)",
            "(:Person {*})-[:KNOWS]->(:Person)",
            "(:Person {-id})-[:KNOWS]->(:Person)",
        )
        .forEach {
          val exception = assertFailsWith(PatternException::class) { Pattern.parse(it) }

          assertEquals(
              "property selectors are not allowed in node part of relationship patterns",
              exception.message)
        }
  }
}
