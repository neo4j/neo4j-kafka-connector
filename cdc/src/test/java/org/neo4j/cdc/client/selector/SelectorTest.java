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
package org.neo4j.cdc.client.selector;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.neo4j.cdc.client.model.*;

class SelectorTest {

    @Test
    void entitySelectorMatchesEntityEvents() {
        List.of(nodeCreateEvent(), relationshipCreateEvent()).forEach(event -> {
            assertThat(new EntitySelector().matches(event)).isTrue();
            assertThat(new EntitySelector(EntityOperation.CREATE).matches(event))
                    .isTrue();
            assertThat(new EntitySelector(EntityOperation.UPDATE).matches(event))
                    .isFalse();
            assertThat(new EntitySelector(null, Set.of("name")).matches(event)).isTrue();
            assertThat(new EntitySelector(EntityOperation.CREATE, Set.of("name")).matches(event))
                    .isTrue();
            assertThat(new EntitySelector(EntityOperation.UPDATE, Set.of("name")).matches(event))
                    .isFalse();
            assertThat(new EntitySelector(null, Set.of("name", "id")).matches(event))
                    .isTrue();
            assertThat(new EntitySelector(EntityOperation.CREATE, Set.of("name", "id")).matches(event))
                    .isTrue();
            assertThat(new EntitySelector(EntityOperation.UPDATE, Set.of("name", "id")).matches(event))
                    .isFalse();
            assertThat(new EntitySelector(null, Set.of("dob")).matches(event)).isFalse();
            assertThat(new EntitySelector(EntityOperation.CREATE, Set.of("dob")).matches(event))
                    .isFalse();
            assertThat(new EntitySelector(EntityOperation.UPDATE, Set.of("dob")).matches(event))
                    .isFalse();
            assertThat(new EntitySelector(EntityOperation.CREATE, Set.of("name", "id", "dob")).matches(event))
                    .isFalse();
            assertThat(new EntitySelector(EntityOperation.UPDATE, Set.of("name", "id", "dob")).matches(event))
                    .isFalse();
        });

        List.of(nodeDeleteEvent(), relationshipDeleteEvent()).forEach(event -> {
            assertThat(new EntitySelector().matches(event)).isTrue();
            assertThat(new EntitySelector(EntityOperation.DELETE).matches(event))
                    .isTrue();
            assertThat(new EntitySelector(EntityOperation.UPDATE).matches(event))
                    .isFalse();
            assertThat(new EntitySelector(null, Set.of("name")).matches(event)).isTrue();
            assertThat(new EntitySelector(EntityOperation.DELETE, Set.of("name")).matches(event))
                    .isTrue();
            assertThat(new EntitySelector(EntityOperation.UPDATE, Set.of("name")).matches(event))
                    .isFalse();
            assertThat(new EntitySelector(null, Set.of("name", "id")).matches(event))
                    .isTrue();
            assertThat(new EntitySelector(EntityOperation.DELETE, Set.of("name", "id")).matches(event))
                    .isTrue();
            assertThat(new EntitySelector(EntityOperation.UPDATE, Set.of("name", "id")).matches(event))
                    .isFalse();
            assertThat(new EntitySelector(null, Set.of("dob")).matches(event)).isFalse();
            assertThat(new EntitySelector(EntityOperation.DELETE, Set.of("dob")).matches(event))
                    .isFalse();
            assertThat(new EntitySelector(EntityOperation.UPDATE, Set.of("dob")).matches(event))
                    .isFalse();
            assertThat(new EntitySelector(EntityOperation.DELETE, Set.of("name", "id", "dob")).matches(event))
                    .isFalse();
            assertThat(new EntitySelector(EntityOperation.UPDATE, Set.of("name", "id", "dob")).matches(event))
                    .isFalse();
        });

        List.of(nodeUpdateEvent(), relationshipUpdateEvent()).forEach(event -> {
            assertThat(new EntitySelector().matches(event)).isTrue();
            assertThat(new EntitySelector(EntityOperation.UPDATE).matches(event))
                    .isTrue();
            assertThat(new EntitySelector(null, Set.of("name")).matches(event)).isTrue();
            assertThat(new EntitySelector(null, Set.of("surname")).matches(event))
                    .isTrue();
            assertThat(new EntitySelector(null, Set.of("dob")).matches(event)).isTrue();
            assertThat(new EntitySelector(null, Set.of("name", "surname", "dob")).matches(event))
                    .isTrue();
            assertThat(new EntitySelector(EntityOperation.CREATE, Set.of("name")).matches(event))
                    .isFalse();
            assertThat(new EntitySelector(null, Set.of("id")).matches(event)).isFalse();
            assertThat(new EntitySelector(null, Set.of("name", "id")).matches(event))
                    .isFalse();
            assertThat(new EntitySelector(EntityOperation.UPDATE, Set.of("dob")).matches(event))
                    .isTrue();
            assertThat(new EntitySelector(EntityOperation.UPDATE, Set.of("name", "id", "dob")).matches(event))
                    .isFalse();
        });
    }

    @Test
    void nodeSelectorMatchesNodeEvents() {
        var event = nodeCreateEvent();

        assertThat(new NodeSelector().matches(event)).isTrue();
        assertThat(new NodeSelector(EntityOperation.CREATE).matches(event)).isTrue();
        assertThat(new NodeSelector(EntityOperation.DELETE).matches(event)).isFalse();
        assertThat(new NodeSelector(EntityOperation.CREATE, Set.of("name")).matches(event))
                .isTrue();
        assertThat(new NodeSelector(EntityOperation.CREATE, Set.of("name", "id")).matches(event))
                .isTrue();
        assertThat(new NodeSelector(EntityOperation.CREATE, Set.of("name", "id", "dob")).matches(event))
                .isFalse();
        assertThat(new NodeSelector(null, emptySet(), Set.of("Person")).matches(event))
                .isTrue();
        assertThat(new NodeSelector(null, emptySet(), Set.of("Person", "Employee")).matches(event))
                .isTrue();
        assertThat(new NodeSelector(null, emptySet(), Set.of("Employee", "Person")).matches(event))
                .isTrue();
        assertThat(new NodeSelector(null, emptySet(), Set.of("Company")).matches(event))
                .isFalse();
        assertThat(new NodeSelector(null, emptySet(), Set.of("Person", "Company")).matches(event))
                .isFalse();
        assertThat(new NodeSelector(EntityOperation.CREATE, emptySet(), Set.of("Employee", "Person")).matches(event))
                .isTrue();
        assertThat(new NodeSelector(EntityOperation.UPDATE, emptySet(), Set.of("Employee", "Person")).matches(event))
                .isFalse();
        assertThat(new NodeSelector(null, emptySet(), emptySet(), Map.of("id", 1L)).matches(event))
                .isTrue();
        assertThat(new NodeSelector(null, emptySet(), emptySet(), Map.of("id", 1L, "dob", "1990")).matches(event))
                .isFalse();
        assertThat(new NodeSelector(null, emptySet(), emptySet(), Map.of("id", 5L, "role", "manager")).matches(event))
                .isTrue();
        assertThat(new NodeSelector(null, emptySet(), Set.of("Employee"), Map.of("id", 5L, "role", "manager"))
                        .matches(event))
                .isTrue();
        assertThat(new NodeSelector(null, emptySet(), Set.of("Employee", "Person"), Map.of("id", 5L, "role", "manager"))
                        .matches(event))
                .isTrue();
        assertThat(new NodeSelector(null, emptySet(), Set.of("Person"), Map.of("id", 5L, "role", "manager"))
                        .matches(event))
                .isTrue();
        assertThat(new NodeSelector(null, emptySet(), Set.of("Person", "Manager"), Map.of("id", 5L, "role", "manager"))
                        .matches(event))
                .isFalse();
        assertThat(new NodeSelector(null, emptySet(), emptySet(), Map.of("id", 5L, "name", "acme corp", "prop", false))
                        .matches(event))
                .isFalse();

        assertThat(new NodeSelector().matches(relationshipCreateEvent())).isFalse();
    }

    @Test
    void relationshipSelectorMatches() {
        var createEvent = relationshipCreateEvent();

        assertThat(new RelationshipSelector().matches(createEvent)).isTrue();
        assertThat(new RelationshipSelector(EntityOperation.CREATE).matches(createEvent))
                .isTrue();
        assertThat(new RelationshipSelector(EntityOperation.DELETE).matches(createEvent))
                .isFalse();

        assertThat(new RelationshipSelector(null, emptySet(), "WORKS_FOR").matches(createEvent))
                .isTrue();
        assertThat(new RelationshipSelector(null, emptySet(), "KNOWS").matches(createEvent))
                .isFalse();
        assertThat(new RelationshipSelector(
                                null,
                                emptySet(),
                                "WORKS_FOR",
                                new RelationshipNodeSelector(Set.of("Person"), emptyMap()))
                        .matches(createEvent))
                .isTrue();
        assertThat(new RelationshipSelector(
                                null,
                                emptySet(),
                                "WORKS_FOR",
                                new RelationshipNodeSelector(),
                                new RelationshipNodeSelector(Set.of("Company"), emptyMap()))
                        .matches(createEvent))
                .isTrue();
        assertThat(new RelationshipSelector(
                                null,
                                emptySet(),
                                "WORKS_FOR",
                                new RelationshipNodeSelector(Set.of("Person"), emptyMap()),
                                new RelationshipNodeSelector(Set.of("Company"), emptyMap()))
                        .matches(createEvent))
                .isTrue();
        assertThat(new RelationshipSelector(
                                null,
                                emptySet(),
                                "WORKS_FOR",
                                new RelationshipNodeSelector(Set.of("Person"), Map.of("id", 1L)),
                                new RelationshipNodeSelector(Set.of("Company"), Map.of("id", 5L)))
                        .matches(createEvent))
                .isTrue();
        assertThat(new RelationshipSelector(
                                null,
                                emptySet(),
                                "WORKS_FOR",
                                new RelationshipNodeSelector(Set.of("Person"), Map.of("id", 1L)),
                                new RelationshipNodeSelector(Set.of("Company"), Map.of("id", 5L)),
                                Map.of("year", 1990L))
                        .matches(createEvent))
                .isTrue();

        assertThat(new RelationshipSelector(
                                null,
                                emptySet(),
                                "WORKS_FOR",
                                new RelationshipNodeSelector(Set.of("Person", "Employee"), Map.of("id", 1L)),
                                new RelationshipNodeSelector(Set.of("Company"), Map.of("id", 5L)),
                                Map.of("year", 1990L))
                        .matches(createEvent))
                .isFalse();
        assertThat(new RelationshipSelector(
                                null,
                                emptySet(),
                                "WORKS_FOR",
                                new RelationshipNodeSelector(Set.of("Person"), Map.of("id", 1L)),
                                new RelationshipNodeSelector(Set.of("Company", "Corportation"), Map.of("id", 5L)),
                                Map.of("year", 1990L))
                        .matches(createEvent))
                .isFalse();
        assertThat(new RelationshipSelector(
                                null,
                                emptySet(),
                                "WORKS_FOR",
                                new RelationshipNodeSelector(Set.of("Person"), Map.of("id", true)),
                                new RelationshipNodeSelector(Set.of("Company"), Map.of("id", 5L)),
                                Map.of("year", 1990L))
                        .matches(createEvent))
                .isFalse();
        assertThat(new RelationshipSelector(
                                null,
                                emptySet(),
                                "WORKS_FOR",
                                new RelationshipNodeSelector(Set.of("Person"), Map.of("id", 1L)),
                                new RelationshipNodeSelector(Set.of("Company"), Map.of("id", "5")),
                                Map.of("year", 1990L))
                        .matches(createEvent))
                .isFalse();
        assertThat(new RelationshipSelector(
                                null,
                                emptySet(),
                                "WORKS_FOR",
                                new RelationshipNodeSelector(Set.of("Person"), Map.of("id", 1L)),
                                new RelationshipNodeSelector(Set.of("Company"), Map.of("id", "5")),
                                Map.of("year", "1990"))
                        .matches(createEvent))
                .isFalse();

        assertThat(new RelationshipSelector().matches(nodeCreateEvent())).isFalse();
    }

    @Test
    void applyFiltersShouldArrangeProperties() {
        List.of(nodeCreateEvent(), relationshipCreateEvent()).forEach(event -> {
            assertThat(new EntitySelector(null, emptySet(), emptySet(), emptySet()).applyProperties(event))
                    .extracting("event.after.properties", InstanceOfAssertFactories.map(String.class, Object.class))
                    .containsOnlyKeys("id", "name", "surname");
            assertThat(new EntitySelector(null, emptySet(), Set.of("*"), emptySet()).applyProperties(event))
                    .extracting("event.after.properties", InstanceOfAssertFactories.map(String.class, Object.class))
                    .containsOnlyKeys("id", "name", "surname");
            assertThat(new EntitySelector(null, emptySet(), Set.of("id"), emptySet()).applyProperties(event))
                    .extracting("event.after.properties", InstanceOfAssertFactories.map(String.class, Object.class))
                    .containsOnlyKeys("id");
            assertThat(new EntitySelector(null, emptySet(), Set.of("id", "name"), emptySet()).applyProperties(event))
                    .extracting("event.after.properties", InstanceOfAssertFactories.map(String.class, Object.class))
                    .containsOnlyKeys("id", "name");
            assertThat(new EntitySelector(null, emptySet(), emptySet(), Set.of("id")).applyProperties(event))
                    .extracting("event.after.properties", InstanceOfAssertFactories.map(String.class, Object.class))
                    .containsOnlyKeys("name", "surname")
                    .doesNotContainKey("id");
            assertThat(new EntitySelector(null, emptySet(), emptySet(), Set.of("id", "name")).applyProperties(event))
                    .extracting("event.after.properties", InstanceOfAssertFactories.map(String.class, Object.class))
                    .containsOnlyKeys("surname")
                    .doesNotContainKeys("id", "name");
        });

        List.of(nodeDeleteEvent(), relationshipDeleteEvent()).forEach(event -> {
            assertThat(new EntitySelector(null, emptySet(), emptySet(), emptySet()).applyProperties(event))
                    .extracting("event.before.properties", InstanceOfAssertFactories.map(String.class, Object.class))
                    .containsOnlyKeys("id", "name", "surname");
            assertThat(new EntitySelector(null, emptySet(), Set.of("*"), emptySet()).applyProperties(event))
                    .extracting("event.before.properties", InstanceOfAssertFactories.map(String.class, Object.class))
                    .containsOnlyKeys("id", "name", "surname");
            assertThat(new EntitySelector(null, emptySet(), Set.of("id"), emptySet()).applyProperties(event))
                    .extracting("event.before.properties", InstanceOfAssertFactories.map(String.class, Object.class))
                    .containsOnlyKeys("id");
            assertThat(new EntitySelector(null, emptySet(), Set.of("id", "name"), emptySet()).applyProperties(event))
                    .extracting("event.before.properties", InstanceOfAssertFactories.map(String.class, Object.class))
                    .containsOnlyKeys("id", "name");
            assertThat(new EntitySelector(null, emptySet(), emptySet(), Set.of("id")).applyProperties(event))
                    .extracting("event.before.properties", InstanceOfAssertFactories.map(String.class, Object.class))
                    .containsOnlyKeys("name", "surname")
                    .doesNotContainKey("id");
            assertThat(new EntitySelector(null, emptySet(), emptySet(), Set.of("id", "name")).applyProperties(event))
                    .extracting("event.before.properties", InstanceOfAssertFactories.map(String.class, Object.class))
                    .containsOnlyKeys("surname")
                    .doesNotContainKeys("id", "name");
        });

        List.of(nodeUpdateEvent(), relationshipUpdateEvent()).forEach(event -> {
            assertThat(new EntitySelector(null, emptySet(), emptySet(), emptySet()).applyProperties(event))
                    .satisfies(e -> assertThat(e)
                            .extracting(
                                    "event.before.properties",
                                    InstanceOfAssertFactories.map(String.class, Object.class))
                            .containsOnlyKeys("id", "name", "surname"))
                    .satisfies(e -> assertThat(e)
                            .extracting(
                                    "event.after.properties", InstanceOfAssertFactories.map(String.class, Object.class))
                            .containsOnlyKeys("id", "name", "dob"));
            assertThat(new EntitySelector(null, emptySet(), Set.of("*"), emptySet()).applyProperties(event))
                    .satisfies(e -> assertThat(e)
                            .extracting(
                                    "event.before.properties",
                                    InstanceOfAssertFactories.map(String.class, Object.class))
                            .containsOnlyKeys("id", "name", "surname"))
                    .satisfies(e -> assertThat(e)
                            .extracting(
                                    "event.after.properties", InstanceOfAssertFactories.map(String.class, Object.class))
                            .containsOnlyKeys("id", "name", "dob"));
            assertThat(new EntitySelector(null, emptySet(), Set.of("id"), emptySet()).applyProperties(event))
                    .satisfies(e -> assertThat(e)
                            .extracting(
                                    "event.before.properties",
                                    InstanceOfAssertFactories.map(String.class, Object.class))
                            .containsOnlyKeys("id"))
                    .satisfies(e -> assertThat(e)
                            .extracting(
                                    "event.after.properties", InstanceOfAssertFactories.map(String.class, Object.class))
                            .containsOnlyKeys("id"));
            assertThat(new EntitySelector(null, emptySet(), Set.of("id", "name"), emptySet()).applyProperties(event))
                    .satisfies(e -> assertThat(e)
                            .extracting(
                                    "event.before.properties",
                                    InstanceOfAssertFactories.map(String.class, Object.class))
                            .containsOnlyKeys("id", "name"))
                    .satisfies(e -> assertThat(e)
                            .extracting(
                                    "event.after.properties", InstanceOfAssertFactories.map(String.class, Object.class))
                            .containsOnlyKeys("id", "name"));
            assertThat(new EntitySelector(null, emptySet(), emptySet(), Set.of("id")).applyProperties(event))
                    .satisfies(e -> assertThat(e)
                            .extracting(
                                    "event.before.properties",
                                    InstanceOfAssertFactories.map(String.class, Object.class))
                            .containsOnlyKeys("name", "surname")
                            .doesNotContainKey("id"))
                    .satisfies(e -> assertThat(e)
                            .extracting(
                                    "event.after.properties", InstanceOfAssertFactories.map(String.class, Object.class))
                            .containsOnlyKeys("name", "dob")
                            .doesNotContainKey("id"));
            assertThat(new EntitySelector(null, emptySet(), emptySet(), Set.of("id", "name")).applyProperties(event))
                    .satisfies(e -> assertThat(e)
                            .extracting(
                                    "event.before.properties",
                                    InstanceOfAssertFactories.map(String.class, Object.class))
                            .containsOnlyKeys("surname")
                            .doesNotContainKeys("id", "name"))
                    .satisfies(e -> assertThat(e)
                            .extracting(
                                    "event.after.properties", InstanceOfAssertFactories.map(String.class, Object.class))
                            .containsOnlyKeys("dob")
                            .doesNotContainKeys("id", "name"));
        });
    }

    @NotNull
    private static ChangeEvent nodeCreateEvent() {
        return new ChangeEvent(
                new ChangeIdentifier("id-1"),
                0L,
                0,
                new Metadata(
                        "neo4j",
                        "neo4j",
                        "server-1",
                        CaptureMode.DIFF,
                        "bolt",
                        "127.0.0.1:50000",
                        "127.0.0.1:7687",
                        ZonedDateTime.now().minusSeconds(5),
                        ZonedDateTime.now(),
                        emptyMap()),
                new NodeEvent(
                        "db:1",
                        EntityOperation.CREATE,
                        List.of("Person", "Employee"),
                        Map.of("Person", Map.of("id", 1L), "Employee", Map.of("id", 5L, "role", "manager")),
                        null,
                        new NodeState(
                                List.of("Person", "Employee"), Map.of("id", 1L, "name", "John", "surname", "Doe"))));
    }

    @NotNull
    private static ChangeEvent nodeDeleteEvent() {
        return new ChangeEvent(
                new ChangeIdentifier("id-1"),
                0L,
                0,
                new Metadata(
                        "neo4j",
                        "neo4j",
                        "server-1",
                        CaptureMode.DIFF,
                        "bolt",
                        "127.0.0.1:50000",
                        "127.0.0.1:7687",
                        ZonedDateTime.now().minusSeconds(5),
                        ZonedDateTime.now(),
                        emptyMap()),
                new NodeEvent(
                        "db:1",
                        EntityOperation.DELETE,
                        List.of("Person", "Employee"),
                        Map.of("Person", Map.of("id", 1L), "Employee", Map.of("id", 5L, "role", "manager")),
                        new NodeState(
                                List.of("Person", "Employee"), Map.of("id", 1L, "name", "John", "surname", "Doe")),
                        null));
    }

    @NotNull
    private static ChangeEvent nodeUpdateEvent() {
        return new ChangeEvent(
                new ChangeIdentifier("id-1"),
                0L,
                0,
                new Metadata(
                        "neo4j",
                        "neo4j",
                        "server-1",
                        CaptureMode.DIFF,
                        "bolt",
                        "127.0.0.1:50000",
                        "127.0.0.1:7687",
                        ZonedDateTime.now().minusSeconds(5),
                        ZonedDateTime.now(),
                        emptyMap()),
                new NodeEvent(
                        "db:1",
                        EntityOperation.UPDATE,
                        List.of("Person", "Employee"),
                        Map.of("Person", Map.of("id", 1L), "Employee", Map.of("id", 5L, "role", "manager")),
                        new NodeState(
                                List.of("Person", "Employee"), Map.of("id", 1L, "name", "John", "surname", "Doe")),
                        new NodeState(
                                List.of("Person", "Employee"),
                                Map.of("id", 1L, "name", "Jack", "dob", LocalDate.of(1990, 1, 1)))));
    }

    @NotNull
    private static ChangeEvent relationshipCreateEvent() {
        return new ChangeEvent(
                new ChangeIdentifier("id-2"),
                0L,
                1,
                new Metadata(
                        "neo4j",
                        "neo4j",
                        "server-1",
                        CaptureMode.DIFF,
                        "bolt",
                        "127.0.0.1:50000",
                        "127.0.0.1:7687",
                        ZonedDateTime.now().minusSeconds(5),
                        ZonedDateTime.now(),
                        emptyMap()),
                new RelationshipEvent(
                        "db:2",
                        "WORKS_FOR",
                        new Node("db:1", List.of("Person"), Map.of("Person", Map.of("id", 1L))),
                        new Node("db:2", List.of("Company"), Map.of("Company", Map.of("id", 5L))),
                        Map.of("year", 1990L),
                        EntityOperation.CREATE,
                        null,
                        new RelationshipState(Map.of("id", 1L, "name", "John", "surname", "Doe"))));
    }

    @NotNull
    private static ChangeEvent relationshipDeleteEvent() {
        return new ChangeEvent(
                new ChangeIdentifier("id-2"),
                0L,
                1,
                new Metadata(
                        "neo4j",
                        "neo4j",
                        "server-1",
                        CaptureMode.DIFF,
                        "bolt",
                        "127.0.0.1:50000",
                        "127.0.0.1:7687",
                        ZonedDateTime.now().minusSeconds(5),
                        ZonedDateTime.now(),
                        emptyMap()),
                new RelationshipEvent(
                        "db:2",
                        "WORKS_FOR",
                        new Node("db:1", List.of("Person"), Map.of("Person", Map.of("id", 1L))),
                        new Node("db:2", List.of("Company"), Map.of("Company", Map.of("id", 5L))),
                        Map.of("year", 1990L),
                        EntityOperation.DELETE,
                        new RelationshipState(Map.of("id", 1L, "name", "John", "surname", "Doe")),
                        null));
    }

    @NotNull
    private static ChangeEvent relationshipUpdateEvent() {
        return new ChangeEvent(
                new ChangeIdentifier("id-2"),
                0L,
                1,
                new Metadata(
                        "neo4j",
                        "neo4j",
                        "server-1",
                        CaptureMode.DIFF,
                        "bolt",
                        "127.0.0.1:50000",
                        "127.0.0.1:7687",
                        ZonedDateTime.now().minusSeconds(5),
                        ZonedDateTime.now(),
                        emptyMap()),
                new RelationshipEvent(
                        "db:2",
                        "WORKS_FOR",
                        new Node("db:1", List.of("Person"), Map.of("Person", Map.of("id", 1L))),
                        new Node("db:2", List.of("Company"), Map.of("Company", Map.of("id", 5L))),
                        Map.of("year", 1990L),
                        EntityOperation.UPDATE,
                        new RelationshipState(Map.of("id", 1L, "name", "John", "surname", "Doe")),
                        new RelationshipState(Map.of("id", 1L, "name", "Jack", "dob", LocalDate.of(1990, 1, 1)))));
    }
}
