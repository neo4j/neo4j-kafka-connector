package org.neo4j.cdc.pattern;

import static org.assertj.core.api.Assertions.*;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.neo4j.cdc.client.pattern.NodePattern;
import org.neo4j.cdc.client.pattern.Pattern;
import org.neo4j.cdc.client.pattern.PatternException;
import org.neo4j.cdc.client.pattern.RelationshipPattern;

public class PatternTest {
    @Test
    void shouldParseNumericLiteralsCorrectly() {
        assertThat(
                        Pattern.parse(
                                "({id1: 0, id2: 125, id3: -24278, id4: -2147483648, id5: 2147483647, id6: -9223372036854775807, id7: 9223372036854775807})"))
                .containsExactly(new NodePattern(
                        Set.of(),
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
                        Set.of(),
                        Set.of()));

        assertThat(Pattern.parse("({id1: 0x00, id2: -0x0f, id3: 0x246FF})"))
                .containsExactly(new NodePattern(
                        Set.of(), Map.of("id1", 0x00L, "id2", -0x0fL, "id3", 0x246FFL), Set.of(), Set.of()));

        assertThat(Pattern.parse("({id1: 0o0, id2: -0o17, id3: 0o12345})"))
                .containsExactly(
                        new NodePattern(Set.of(), Map.of("id1", 0L, "id2", -15L, "id3", 5349L), Set.of(), Set.of()));

        assertThat(Pattern.parse("({id1: 0.0, id2: -125.25, id3: 1.2e20, id4: -1.2e20, id5: 2.7182818284})"))
                .containsExactly(new NodePattern(
                        Set.of(),
                        Map.of(
                                "id1", 0.0,
                                "id2", -125.25,
                                "id3", 1.2e20,
                                "id4", -1.2e20,
                                "id5", 2.7182818284),
                        Set.of(),
                        Set.of()));

        assertThat(Pattern.parse("({id1: NaN, id2: INF, id3: INFINITY})"))
                .containsExactly(new NodePattern(
                        Set.of(),
                        Map.of(
                                "id1", Double.NaN,
                                "id2", Double.POSITIVE_INFINITY,
                                "id3", Double.POSITIVE_INFINITY),
                        Set.of(),
                        Set.of()));
    }

    @Test
    void shouldParseStringLiterals() {
        assertThat(Pattern.parse("({id1: '', id2: \"\", id3: 'a string',  id4: \"another string\" })"))
                .containsExactly(new NodePattern(
                        Set.of(),
                        Map.of("id1", "", "id2", "", "id3", "a string", "id4", "another string"),
                        Set.of(),
                        Set.of()));
    }

    @Test
    void shouldParseBoolLiterals() {
        assertThat(Pattern.parse("({id1: true, id2: false })"))
                .containsExactly(new NodePattern(Set.of(), Map.of("id1", true, "id2", false), Set.of(), Set.of()));
    }

    @Test
    void shouldParseNodePatterns() {
        assertThat(Pattern.parse("")).isEmpty();
        assertThat(Pattern.parse("()")).containsExactly(new NodePattern(Set.of(), Map.of(), Set.of(), Set.of()));
        assertThat(Pattern.parse("(:Person)"))
                .containsExactly(new NodePattern(Set.of("Person"), Map.of(), Set.of(), Set.of()));
        assertThat(Pattern.parse("(:Person:Employee)"))
                .containsExactly(new NodePattern(Set.of("Person", "Employee"), Map.of(), Set.of(), Set.of()));
        assertThat(Pattern.parse("(:Person {})"))
                .containsExactly(new NodePattern(Set.of("Person"), Map.of(), Set.of(), Set.of()));
        assertThat(Pattern.parse("({})")).containsExactly(new NodePattern(Set.of(), Map.of(), Set.of(), Set.of()));
        assertThat(Pattern.parse("({*})")).containsExactly(new NodePattern(Set.of(), Map.of(), Set.of("*"), Set.of()));
        assertThat(Pattern.parse("(:Person {*})"))
                .containsExactly(new NodePattern(Set.of("Person"), Map.of(), Set.of("*"), Set.of()));
        assertThat(Pattern.parse("(:Person:Employee {*})"))
                .containsExactly(new NodePattern(Set.of("Person", "Employee"), Map.of(), Set.of("*"), Set.of()));
        assertThat(Pattern.parse("({id: 5})"))
                .containsExactly(new NodePattern(Set.of(), Map.of("id", 5L), Set.of(), Set.of()));
        assertThat(Pattern.parse("({id: 5, *})"))
                .containsExactly(new NodePattern(Set.of(), Map.of("id", 5L), Set.of("*"), Set.of()));
        assertThat(Pattern.parse("({id: 5, *, name: 'john'})"))
                .containsExactly(new NodePattern(Set.of(), Map.of("id", 5L, "name", "john"), Set.of("*"), Set.of()));
        assertThat(Pattern.parse("({id: 5, *, name: 'john', select1, +select2})"))
                .containsExactly(new NodePattern(
                        Set.of(), Map.of("id", 5L, "name", "john"), Set.of("select1", "select2", "*"), Set.of()));
        assertThat(Pattern.parse("({id: 5, *, name: 'john', -skip1, -skip2})"))
                .containsExactly(new NodePattern(
                        Set.of(), Map.of("id", 5L, "name", "john"), Set.of("*"), Set.of("skip1", "skip2")));
        assertThat(Pattern.parse("({id: 5, *, name: 'john', -skip1, select1, +select2, -skip2})"))
                .containsExactly(new NodePattern(
                        Set.of(),
                        Map.of("id", 5L, "name", "john"),
                        Set.of("select1", "select2", "*"),
                        Set.of("skip1", "skip2")));
        assertThat(Pattern.parse("({id: 5, *, name: 'john', -skip1, select1, +select2, -skip2, surname: 'doe'})"))
                .containsExactly(new NodePattern(
                        Set.of(),
                        Map.of("id", 5L, "name", "john", "surname", "doe"),
                        Set.of("select1", "select2", "*"),
                        Set.of("skip1", "skip2")));

        assertThat(Pattern.parse("({*, -skip1, select1, +select2, -skip2})"))
                .containsExactly(new NodePattern(
                        Set.of(), Map.of(), Set.of("select1", "select2", "*"), Set.of("skip1", "skip2")));
    }

    @Test
    void shouldParseRelationshipDirectionsCorrectly() {
        assertThat(Pattern.parse("()-[:KNOWS]->()"))
                .containsExactly(new RelationshipPattern(
                        "KNOWS",
                        new NodePattern(Set.of(), Map.of(), Set.of(), Set.of()),
                        new NodePattern(Set.of(), Map.of(), Set.of(), Set.of()),
                        false,
                        Map.of(),
                        Set.of(),
                        Set.of()));
        assertThat(Pattern.parse("()<-[:KNOWS]-()"))
                .containsExactly(new RelationshipPattern(
                        "KNOWS",
                        new NodePattern(Set.of(), Map.of(), Set.of(), Set.of()),
                        new NodePattern(Set.of(), Map.of(), Set.of(), Set.of()),
                        false,
                        Map.of(),
                        Set.of(),
                        Set.of()));

        assertThat(Pattern.parse("()-[:KNOWS]-()"))
                .containsExactly(new RelationshipPattern(
                        "KNOWS",
                        new NodePattern(Set.of(), Map.of(), Set.of(), Set.of()),
                        new NodePattern(Set.of(), Map.of(), Set.of(), Set.of()),
                        true,
                        Map.of(),
                        Set.of(),
                        Set.of()));

        assertThat(Pattern.parse("()<-[:KNOWS]->()"))
                .containsExactly(new RelationshipPattern(
                        "KNOWS",
                        new NodePattern(Set.of(), Map.of(), Set.of(), Set.of()),
                        new NodePattern(Set.of(), Map.of(), Set.of(), Set.of()),
                        true,
                        Map.of(),
                        Set.of(),
                        Set.of()));

        assertThat(Pattern.parse("(:Person)-[:WORKS_FOR]->(:Company)"))
                .containsExactly(new RelationshipPattern(
                        "WORKS_FOR",
                        new NodePattern(Set.of("Person"), Map.of(), Set.of(), Set.of()),
                        new NodePattern(Set.of("Company"), Map.of(), Set.of(), Set.of()),
                        false,
                        Map.of(),
                        Set.of(),
                        Set.of()));

        assertThat(Pattern.parse("(:Company)<-[:WORKS_FOR]-(:Person)"))
                .containsExactly(new RelationshipPattern(
                        "WORKS_FOR",
                        new NodePattern(Set.of("Person"), Map.of(), Set.of(), Set.of()),
                        new NodePattern(Set.of("Company"), Map.of(), Set.of(), Set.of()),
                        false,
                        Map.of(),
                        Set.of(),
                        Set.of()));
    }

    @Test
    void shouldParseRelationshipPatterns() {
        List.of("()-[:WORKS_FOR]->()", "()-[:WORKS_FOR{}]->()").forEach(pattern -> assertThat(Pattern.parse(pattern))
                .containsExactly(new RelationshipPattern(
                        "WORKS_FOR",
                        new NodePattern(Set.of(), Map.of(), Set.of(), Set.of()),
                        new NodePattern(Set.of(), Map.of(), Set.of(), Set.of()),
                        false,
                        Map.of(),
                        Set.of(),
                        Set.of())));

        assertThat(Pattern.parse("()-[:WORKS_FOR {id: 5}]->()"))
                .containsExactly(new RelationshipPattern(
                        "WORKS_FOR",
                        new NodePattern(Set.of(), Map.of(), Set.of(), Set.of()),
                        new NodePattern(Set.of(), Map.of(), Set.of(), Set.of()),
                        false,
                        Map.of("id", 5L),
                        Set.of(),
                        Set.of()));

        assertThat(Pattern.parse("()-[:WORKS_FOR {id: 5,*,name: 'john'}]->()"))
                .containsExactly(new RelationshipPattern(
                        "WORKS_FOR",
                        new NodePattern(Set.of(), Map.of(), Set.of(), Set.of()),
                        new NodePattern(Set.of(), Map.of(), Set.of(), Set.of()),
                        false,
                        Map.of("id", 5L, "name", "john"),
                        Set.of("*"),
                        Set.of()));

        assertThat(Pattern.parse("()-[:WORKS_FOR {id: 5,*,name: 'john', select1, -skip1, +select2, -skip2}]->()"))
                .containsExactly(new RelationshipPattern(
                        "WORKS_FOR",
                        new NodePattern(Set.of(), Map.of(), Set.of(), Set.of()),
                        new NodePattern(Set.of(), Map.of(), Set.of(), Set.of()),
                        false,
                        Map.of("id", 5L, "name", "john"),
                        Set.of("select1", "select2", "*"),
                        Set.of("skip1", "skip2")));

        assertThat(Pattern.parse("({id:5})-[:WORKS_FOR {id: 5}]->({name:'john'})"))
                .containsExactly(new RelationshipPattern(
                        "WORKS_FOR",
                        new NodePattern(Set.of(), Map.of("id", 5L), Set.of(), Set.of()),
                        new NodePattern(Set.of(), Map.of("name", "john"), Set.of(), Set.of()),
                        false,
                        Map.of("id", 5L),
                        Set.of(),
                        Set.of()));

        assertThat(Pattern.parse("(:Person:User{id:5})-[:WORKS_FOR {id: 5}]->(:Person{name:'john'})"))
                .containsExactly(new RelationshipPattern(
                        "WORKS_FOR",
                        new NodePattern(Set.of("Person", "User"), Map.of("id", 5L), Set.of(), Set.of()),
                        new NodePattern(Set.of("Person"), Map.of("name", "john"), Set.of(), Set.of()),
                        false,
                        Map.of("id", 5L),
                        Set.of(),
                        Set.of()));
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
