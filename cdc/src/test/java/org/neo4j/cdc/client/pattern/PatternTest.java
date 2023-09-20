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
package org.neo4j.cdc.client.pattern;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.assertj.core.api.Assertions.*;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

public class PatternTest {
    @Test
    void shouldParseNumericLiteralsCorrectly() {
        assertThat(
                        Pattern.parse(
                                "({id1: 0, id2: 125, id3: -24278, id4: -2147483648, id5: 2147483647, id6: -9223372036854775807, id7: 9223372036854775807})"))
                .containsExactly(new NodePattern(
                        emptySet(),
                        Map.of(
                                "id1",
                                0L,
                                "id2",
                                125L,
                                "id3",
                                -24278L,
                                "id4",
                                -2147483648L,
                                "id5",
                                2147483647L,
                                "id6",
                                -9223372036854775807L,
                                "id7",
                                9223372036854775807L),
                        emptySet(),
                        emptySet()));

        assertThat(Pattern.parse("({id1: 0x00, id2: -0x0f, id3: 0x246FF})"))
                .containsExactly(new NodePattern(
                        emptySet(), Map.of("id1", 0x00L, "id2", -0x0fL, "id3", 0x246FFL), emptySet(), emptySet()));

        assertThat(Pattern.parse("({id1: 0o0, id2: -0o17, id3: 0o12345})"))
                .containsExactly(new NodePattern(
                        emptySet(), Map.of("id1", 0L, "id2", -15L, "id3", 5349L), emptySet(), emptySet()));

        assertThat(Pattern.parse("({id1: 0.0, id2: -125.25, id3: 1.2e20, id4: -1.2e20, id5: 2.7182818284})"))
                .containsExactly(new NodePattern(
                        emptySet(),
                        Map.of(
                                "id1", 0.0,
                                "id2", -125.25,
                                "id3", 1.2e20,
                                "id4", -1.2e20,
                                "id5", 2.7182818284),
                        emptySet(),
                        emptySet()));

        assertThat(Pattern.parse("({id1: NaN, id2: INF, id3: INFINITY})"))
                .containsExactly(new NodePattern(
                        emptySet(),
                        Map.of(
                                "id1", Double.NaN,
                                "id2", Double.POSITIVE_INFINITY,
                                "id3", Double.POSITIVE_INFINITY),
                        emptySet(),
                        emptySet()));
    }

    @Test
    void shouldParseStringLiterals() {
        assertThat(Pattern.parse("({id1: '', id2: \"\", id3: 'a string',  id4: \"another string\" })"))
                .containsExactly(new NodePattern(
                        emptySet(),
                        Map.of("id1", "", "id2", "", "id3", "a string", "id4", "another string"),
                        emptySet(),
                        emptySet()));
    }

    @Test
    void shouldParseBoolLiterals() {
        assertThat(Pattern.parse("({id1: true, id2: false })"))
                .containsExactly(
                        new NodePattern(emptySet(), Map.of("id1", true, "id2", false), emptySet(), emptySet()));
    }

    @Test
    void shouldParseNodePatterns() {
        assertThat(Pattern.parse("")).isEmpty();
        assertThat(Pattern.parse("()"))
                .containsExactly(new NodePattern(emptySet(), emptyMap(), emptySet(), emptySet()));
        assertThat(Pattern.parse("(:Person)"))
                .containsExactly(new NodePattern(Set.of("Person"), emptyMap(), emptySet(), emptySet()));
        assertThat(Pattern.parse("(:Person:Employee)"))
                .containsExactly(new NodePattern(Set.of("Person", "Employee"), emptyMap(), emptySet(), emptySet()));
        assertThat(Pattern.parse("(:Person {})"))
                .containsExactly(new NodePattern(Set.of("Person"), emptyMap(), emptySet(), emptySet()));
        assertThat(Pattern.parse("({})"))
                .containsExactly(new NodePattern(emptySet(), emptyMap(), emptySet(), emptySet()));
        assertThat(Pattern.parse("({*})"))
                .containsExactly(new NodePattern(emptySet(), emptyMap(), Set.of("*"), emptySet()));
        assertThat(Pattern.parse("(:Person {*})"))
                .containsExactly(new NodePattern(Set.of("Person"), emptyMap(), Set.of("*"), emptySet()));
        assertThat(Pattern.parse("(:Person:Employee {*})"))
                .containsExactly(new NodePattern(Set.of("Person", "Employee"), emptyMap(), Set.of("*"), emptySet()));
        assertThat(Pattern.parse("({id: 5})"))
                .containsExactly(new NodePattern(emptySet(), Map.of("id", 5L), emptySet(), emptySet()));
        assertThat(Pattern.parse("({id: 5, *})"))
                .containsExactly(new NodePattern(emptySet(), Map.of("id", 5L), Set.of("*"), emptySet()));
        assertThat(Pattern.parse("({id: 5, *, name: 'john'})"))
                .containsExactly(
                        new NodePattern(emptySet(), Map.of("id", 5L, "name", "john"), Set.of("*"), emptySet()));
        assertThat(Pattern.parse("({id: 5, *, name: 'john', select1, +select2})"))
                .containsExactly(new NodePattern(
                        emptySet(), Map.of("id", 5L, "name", "john"), Set.of("select1", "select2", "*"), emptySet()));
        assertThat(Pattern.parse("({id: 5, *, name: 'john', -skip1, -skip2})"))
                .containsExactly(new NodePattern(
                        emptySet(), Map.of("id", 5L, "name", "john"), Set.of("*"), Set.of("skip1", "skip2")));
        assertThat(Pattern.parse("({id: 5, *, name: 'john', -skip1, select1, +select2, -skip2})"))
                .containsExactly(new NodePattern(
                        emptySet(),
                        Map.of("id", 5L, "name", "john"),
                        Set.of("select1", "select2", "*"),
                        Set.of("skip1", "skip2")));
        assertThat(Pattern.parse("({id: 5, *, name: 'john', -skip1, select1, +select2, -skip2, surname: 'doe'})"))
                .containsExactly(new NodePattern(
                        emptySet(),
                        Map.of("id", 5L, "name", "john", "surname", "doe"),
                        Set.of("select1", "select2", "*"),
                        Set.of("skip1", "skip2")));

        assertThat(Pattern.parse("({*, -skip1, select1, +select2, -skip2})"))
                .containsExactly(new NodePattern(
                        emptySet(), emptyMap(), Set.of("select1", "select2", "*"), Set.of("skip1", "skip2")));
    }

    @Test
    void shouldParseRelationshipDirectionsCorrectly() {
        assertThat(Pattern.parse("()-[]->()"))
                .containsExactly(new RelationshipPattern(
                        null,
                        new NodePattern(emptySet(), emptyMap(), emptySet(), emptySet()),
                        new NodePattern(emptySet(), emptyMap(), emptySet(), emptySet()),
                        false,
                        emptyMap(),
                        emptySet(),
                        emptySet()));
        assertThat(Pattern.parse("()-[]-()"))
                .containsExactly(new RelationshipPattern(
                        null,
                        new NodePattern(emptySet(), emptyMap(), emptySet(), emptySet()),
                        new NodePattern(emptySet(), emptyMap(), emptySet(), emptySet()),
                        true,
                        emptyMap(),
                        emptySet(),
                        emptySet()));
        assertThat(Pattern.parse("()-[:KNOWS]->()"))
                .containsExactly(new RelationshipPattern(
                        "KNOWS",
                        new NodePattern(emptySet(), emptyMap(), emptySet(), emptySet()),
                        new NodePattern(emptySet(), emptyMap(), emptySet(), emptySet()),
                        false,
                        emptyMap(),
                        emptySet(),
                        emptySet()));
        assertThat(Pattern.parse("()<-[:KNOWS]-()"))
                .containsExactly(new RelationshipPattern(
                        "KNOWS",
                        new NodePattern(emptySet(), emptyMap(), emptySet(), emptySet()),
                        new NodePattern(emptySet(), emptyMap(), emptySet(), emptySet()),
                        false,
                        emptyMap(),
                        emptySet(),
                        emptySet()));

        assertThat(Pattern.parse("()-[:KNOWS]-()"))
                .containsExactly(new RelationshipPattern(
                        "KNOWS",
                        new NodePattern(emptySet(), emptyMap(), emptySet(), emptySet()),
                        new NodePattern(emptySet(), emptyMap(), emptySet(), emptySet()),
                        true,
                        emptyMap(),
                        emptySet(),
                        emptySet()));

        assertThat(Pattern.parse("()<-[:KNOWS]->()"))
                .containsExactly(new RelationshipPattern(
                        "KNOWS",
                        new NodePattern(emptySet(), emptyMap(), emptySet(), emptySet()),
                        new NodePattern(emptySet(), emptyMap(), emptySet(), emptySet()),
                        true,
                        emptyMap(),
                        emptySet(),
                        emptySet()));

        assertThat(Pattern.parse("(:Person)-[:WORKS_FOR]->(:Company)"))
                .containsExactly(new RelationshipPattern(
                        "WORKS_FOR",
                        new NodePattern(Set.of("Person"), emptyMap(), emptySet(), emptySet()),
                        new NodePattern(Set.of("Company"), emptyMap(), emptySet(), emptySet()),
                        false,
                        emptyMap(),
                        emptySet(),
                        emptySet()));

        assertThat(Pattern.parse("(:Company)<-[:WORKS_FOR]-(:Person)"))
                .containsExactly(new RelationshipPattern(
                        "WORKS_FOR",
                        new NodePattern(Set.of("Person"), emptyMap(), emptySet(), emptySet()),
                        new NodePattern(Set.of("Company"), emptyMap(), emptySet(), emptySet()),
                        false,
                        emptyMap(),
                        emptySet(),
                        emptySet()));
    }

    @Test
    void shouldParseRelationshipPatterns() {
        List.of("()-[:WORKS_FOR]->()", "()-[:WORKS_FOR{}]->()").forEach(pattern -> assertThat(Pattern.parse(pattern))
                .containsExactly(new RelationshipPattern(
                        "WORKS_FOR",
                        new NodePattern(emptySet(), emptyMap(), emptySet(), emptySet()),
                        new NodePattern(emptySet(), emptyMap(), emptySet(), emptySet()),
                        false,
                        emptyMap(),
                        emptySet(),
                        emptySet())));

        assertThat(Pattern.parse("()-[:WORKS_FOR {id: 5}]->()"))
                .containsExactly(new RelationshipPattern(
                        "WORKS_FOR",
                        new NodePattern(emptySet(), emptyMap(), emptySet(), emptySet()),
                        new NodePattern(emptySet(), emptyMap(), emptySet(), emptySet()),
                        false,
                        Map.of("id", 5L),
                        emptySet(),
                        emptySet()));

        assertThat(Pattern.parse("()-[:WORKS_FOR {id: 5,*,name: 'john'}]->()"))
                .containsExactly(new RelationshipPattern(
                        "WORKS_FOR",
                        new NodePattern(emptySet(), emptyMap(), emptySet(), emptySet()),
                        new NodePattern(emptySet(), emptyMap(), emptySet(), emptySet()),
                        false,
                        Map.of("id", 5L, "name", "john"),
                        Set.of("*"),
                        emptySet()));

        assertThat(Pattern.parse("()-[:WORKS_FOR {id: 5,*,name: 'john', select1, -skip1, +select2, -skip2}]->()"))
                .containsExactly(new RelationshipPattern(
                        "WORKS_FOR",
                        new NodePattern(emptySet(), emptyMap(), emptySet(), emptySet()),
                        new NodePattern(emptySet(), emptyMap(), emptySet(), emptySet()),
                        false,
                        Map.of("id", 5L, "name", "john"),
                        Set.of("select1", "select2", "*"),
                        Set.of("skip1", "skip2")));

        assertThat(Pattern.parse("({id:5})-[:WORKS_FOR {id: 5}]->({name:'john'})"))
                .containsExactly(new RelationshipPattern(
                        "WORKS_FOR",
                        new NodePattern(emptySet(), Map.of("id", 5L), emptySet(), emptySet()),
                        new NodePattern(emptySet(), Map.of("name", "john"), emptySet(), emptySet()),
                        false,
                        Map.of("id", 5L),
                        emptySet(),
                        emptySet()));

        assertThat(Pattern.parse("(:Person:User{id:5})-[:WORKS_FOR {id: 5}]->(:Person{name:'john'})"))
                .containsExactly(new RelationshipPattern(
                        "WORKS_FOR",
                        new NodePattern(Set.of("Person", "User"), Map.of("id", 5L), emptySet(), emptySet()),
                        new NodePattern(Set.of("Person"), Map.of("name", "john"), emptySet(), emptySet()),
                        false,
                        Map.of("id", 5L),
                        emptySet(),
                        emptySet()));
    }

    @Test
    void relationshipNodePatternsCannotContainPropertySelectors() {
        List.of(
                        "({id})-[:KNOWS]-()",
                        "()-[:KNOWS]-({id})",
                        "(:Person {+id})-[:KNOWS]->(:Person)",
                        "(:Person {*})-[:KNOWS]->(:Person)",
                        "(:Person {-id})-[:KNOWS]->(:Person)")
                .forEach(pattern -> assertThatThrownBy((() -> Pattern.parse(pattern)))
                        .isInstanceOf(PatternException.class)
                        .hasMessage("property selectors are not allowed in node part of relationship patterns"));
    }
}
