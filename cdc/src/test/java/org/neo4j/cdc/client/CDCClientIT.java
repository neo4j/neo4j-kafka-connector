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
package org.neo4j.cdc.client;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.time.*;
import java.util.*;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.cdc.client.model.*;
import org.neo4j.cdc.client.selector.NodeSelector;
import org.neo4j.cdc.client.selector.RelationshipNodeSelector;
import org.neo4j.cdc.client.selector.RelationshipSelector;
import org.neo4j.driver.*;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.test.StepVerifier;

/**
 * @author Gerrit Meier
 */
@Testcontainers
public class CDCClientIT {

    private static final String NEO4J_VERSION = "5.11";

    @SuppressWarnings("resource")
    @Container
    private static final Neo4jContainer<?> neo4j = new Neo4jContainer<>("neo4j:" + NEO4J_VERSION + "-enterprise")
            .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
            .withNeo4jConfig("internal.dbms.change_data_capture", "true")
            .withAdminPassword("passw0rd");

    private static Driver driver;
    private static ChangeIdentifier current;

    @BeforeAll
    static void setup() {
        driver = GraphDatabase.driver(neo4j.getBoltUrl(), AuthTokens.basic("neo4j", "passw0rd"));
    }

    @AfterAll
    static void cleanup() {
        driver.close();
    }

    @BeforeEach
    void reset() {
        try (var session = driver.session()) {
            session.run(
                            "CREATE OR REPLACE DATABASE $db OPTIONS {txLogEnrichment: $mode} WAIT",
                            Map.of("db", "neo4j", "mode", "FULL"))
                    .consume();

            current = currentChangeId(session);
        }
    }

    private static ChangeIdentifier currentChangeId(Session session) {
        return new ChangeIdentifier(
                session.run("CALL cdc.current()").single().get(0).asString());
    }

    @Test
    void earliest() {
        var client = new CDCClient(driver, Duration.ZERO);

        StepVerifier.create(client.earliest())
                .assertNext(cv -> assertNotNull(cv.getId()))
                .verifyComplete();
    }

    @Test
    void current() {
        var client = new CDCClient(driver, Duration.ZERO);

        StepVerifier.create(client.current())
                .assertNext(cv -> assertNotNull(cv.getId()))
                .verifyComplete();
    }

    @Test
    void changesCanBeQueried() {
        var client = new CDCClient(driver, Duration.ZERO);

        try (Session session = driver.session()) {
            session.run("CREATE ()").consume();
        }

        StepVerifier.create(client.query(current))
                .assertNext(n -> assertThat(n).extracting(ChangeEvent::getEvent).isInstanceOf(NodeEvent.class))
                .verifyComplete();
    }

    @Test
    void shouldReturnCypherTypesWithoutConversion() {
        var client = new CDCClient(driver, Duration.ZERO);

        var props = new HashMap<String, Object>();
        props.put("bool", true);
        props.put("date", LocalDate.of(1990, 5, 1));
        props.put("duration", Values.isoDuration(1, 0, 0, 0).asIsoDuration());
        props.put("float", 5.25);
        props.put("integer", 123L);
        props.put("list", List.of(1L, 2L, 3L));
        props.put("local_datetime", LocalDateTime.of(1990, 5, 1, 23, 59, 59, 0));
        props.put("local_time", LocalTime.of(23, 59, 59, 0));
        props.put("point2d", Values.point(4326, 1, 2).asPoint());
        props.put("point3d", Values.point(4979, 1, 2, 3).asPoint());
        props.put("string", "a string");
        props.put("zoned_datetime", ZonedDateTime.of(1990, 5, 1, 23, 59, 59, 0, ZoneId.of("UTC")));
        props.put("zoned_time", OffsetTime.of(23, 59, 59, 0, ZoneOffset.ofHours(1)));

        try (Session session = driver.session()) {
            session.run("CREATE (a) SET a = $props", Map.of("props", props)).consume();
        }

        StepVerifier.create(client.query(current))
                .assertNext(event -> assertThat(event)
                        .extracting(ChangeEvent::getEvent)
                        .asInstanceOf(InstanceOfAssertFactories.type(NodeEvent.class))
                        .satisfies(e -> assertThat(e.getBefore()).isNull())
                        .satisfies(e -> assertThat(e.getAfter())
                                .isNotNull()
                                .extracting(NodeState::getProperties)
                                .asInstanceOf(InstanceOfAssertFactories.MAP)
                                .containsAllEntriesOf(props)))
                .verifyComplete();
    }

    @Test
    void nodeChangesCanBeQueried() {
        CDCClient client = new CDCClient(driver, Duration.ZERO);

        try (var session = driver.session()) {
            session.run("CREATE CONSTRAINT FOR (p:Person) REQUIRE (p.first_name, p.last_name) IS NODE KEY")
                    .consume();

            final String elementId = session.run(
                            "CREATE (p:Person:Employee) SET p = $props RETURN elementId(p)",
                            Map.of(
                                    "props",
                                    Map.of(
                                            "first_name", "john",
                                            "last_name", "doe",
                                            "date_of_birth", LocalDate.of(1990, 5, 1))))
                    .single()
                    .get(0)
                    .asString();

            StepVerifier.create(client.query(current))
                    .assertNext(event -> assertThat(event)
                            .satisfies(c -> {
                                assertThat(c.getId()).isNotNull();
                                assertThat(c.getTxId()).isNotNull();
                                assertThat(c.getSeq()).isNotNull();
                            })
                            .satisfies(e -> assertThat(e.getMetadata())
                                    .satisfies(m ->
                                            assertThat(m.getAdditionalEntries()).isEmpty())
                                    .satisfies(m ->
                                            assertThat(m.getAuthenticatedUser()).isEqualTo("neo4j"))
                                    .satisfies(m ->
                                            assertThat(m.getExecutingUser()).isEqualTo("neo4j"))
                                    .satisfies(
                                            m -> assertThat(m.getCaptureMode()).isEqualTo(CaptureMode.FULL))
                                    .satisfies(m ->
                                            assertThat(m.getConnectionType()).isEqualTo("bolt"))
                                    .satisfies(m ->
                                            assertThat(m.getConnectionClient()).isNotNull())
                                    .satisfies(m ->
                                            assertThat(m.getConnectionServer()).isNotNull())
                                    .satisfies(m -> assertThat(m.getServerId()).isNotNull())
                                    .satisfies(
                                            m -> assertThat(m.getTxStartTime()).isNotNull())
                                    .satisfies(
                                            m -> assertThat(m.getTxCommitTime()).isNotNull()))
                            .satisfies(e -> assertThat(e.getEvent())
                                    .isNotNull()
                                    .asInstanceOf(InstanceOfAssertFactories.type(NodeEvent.class))
                                    .hasFieldOrPropertyWithValue("eventType", EventType.NODE)
                                    .hasFieldOrPropertyWithValue("operation", EntityOperation.CREATE)
                                    .hasFieldOrPropertyWithValue("elementId", elementId)
                                    .hasFieldOrPropertyWithValue("labels", List.of("Person", "Employee"))
                                    .hasFieldOrPropertyWithValue(
                                            "keys", Map.of("Person", Map.of("first_name", "john", "last_name", "doe")))
                                    .hasFieldOrPropertyWithValue("before", null)
                                    .hasFieldOrPropertyWithValue(
                                            "after",
                                            new NodeState(
                                                    List.of("Person", "Employee"),
                                                    Map.of(
                                                            "first_name",
                                                            "john",
                                                            "last_name",
                                                            "doe",
                                                            "date_of_birth",
                                                            LocalDate.of(1990, 5, 1))))))
                    .verifyComplete();
        }
    }

    @Test
    void relationshipChangesCanBeQueried() {
        var client = new CDCClient(driver, Duration.ZERO);

        try (var session = driver.session()) {
            session.run("CREATE CONSTRAINT FOR (p:Person) REQUIRE (p.id) IS NODE KEY")
                    .consume();
            session.run("CREATE CONSTRAINT FOR (p:Place) REQUIRE (p.id) IS NODE KEY")
                    .consume();
            session.run("CREATE CONSTRAINT FOR ()-[b:BORN_IN]-() REQUIRE (b.on) IS RELATIONSHIP KEY")
                    .consume();

            var nodes = session.run(
                            "CREATE (p:Person), (a:Place) SET p = $person, a = $place RETURN elementId(p), elementId(a)",
                            Map.of("person", Map.of("id", 1L), "place", Map.of("id", 48)))
                    .single();
            final var startElementId = nodes.get(0).asString();
            final var endElementId = nodes.get(1).asString();

            current = currentChangeId(session);

            final var elementId = session.run(
                            "MATCH (p:Person {id: 1}), (a:Place {id:48}) CREATE (p)-[b:BORN_IN]->(a) SET b = $props RETURN elementId(b)",
                            Map.of("props", Map.of("on", LocalDate.of(1990, 5, 1))))
                    .single()
                    .get(0)
                    .asString();

            StepVerifier.create(client.query(current))
                    .assertNext(event -> assertThat(event)
                            .satisfies(c -> {
                                assertThat(c.getId()).isNotNull();
                                assertThat(c.getTxId()).isNotNull();
                                assertThat(c.getSeq()).isNotNull();
                            })
                            .satisfies(e -> assertThat(e.getMetadata())
                                    .satisfies(m ->
                                            assertThat(m.getAdditionalEntries()).isEmpty())
                                    .satisfies(m ->
                                            assertThat(m.getAuthenticatedUser()).isEqualTo("neo4j"))
                                    .satisfies(m ->
                                            assertThat(m.getExecutingUser()).isEqualTo("neo4j"))
                                    .satisfies(
                                            m -> assertThat(m.getCaptureMode()).isEqualTo(CaptureMode.FULL))
                                    .satisfies(m ->
                                            assertThat(m.getConnectionType()).isEqualTo("bolt"))
                                    .satisfies(m ->
                                            assertThat(m.getConnectionClient()).isNotNull())
                                    .satisfies(m ->
                                            assertThat(m.getConnectionServer()).isNotNull())
                                    .satisfies(m -> assertThat(m.getServerId()).isNotNull())
                                    .satisfies(
                                            m -> assertThat(m.getTxStartTime()).isNotNull())
                                    .satisfies(
                                            m -> assertThat(m.getTxCommitTime()).isNotNull()))
                            .satisfies(e -> assertThat(e.getEvent())
                                    .isNotNull()
                                    .asInstanceOf(InstanceOfAssertFactories.type(RelationshipEvent.class))
                                    .hasFieldOrPropertyWithValue("eventType", EventType.RELATIONSHIP)
                                    .hasFieldOrPropertyWithValue("operation", EntityOperation.CREATE)
                                    .hasFieldOrPropertyWithValue("elementId", elementId)
                                    .hasFieldOrPropertyWithValue("type", "BORN_IN")
                                    .hasFieldOrPropertyWithValue(
                                            "start",
                                            new Node(
                                                    startElementId,
                                                    List.of("Person"),
                                                    Map.of("Person", Map.of("id", 1L))))
                                    .hasFieldOrPropertyWithValue(
                                            "end",
                                            new Node(
                                                    endElementId, List.of("Place"), Map.of("Place", Map.of("id", 48L))))
                                    .hasFieldOrPropertyWithValue("key", Map.of("on", LocalDate.of(1990, 5, 1)))
                                    .hasFieldOrPropertyWithValue("before", null)
                                    .hasFieldOrPropertyWithValue(
                                            "after", new RelationshipState(Map.of("on", LocalDate.of(1990, 5, 1))))))
                    .verifyComplete();
        }
    }

    @Test
    void selectorsArePassedToServer() {

        try (var session = driver.session()) {
            session.run("CREATE CONSTRAINT FOR (p:Person) REQUIRE (p.id) IS NODE KEY")
                    .consume();
            session.run("CREATE CONSTRAINT FOR (p:Place) REQUIRE (p.id) IS NODE KEY")
                    .consume();
            session.run("CREATE CONSTRAINT FOR ()-[b:BORN_IN]-() REQUIRE (b.on) IS RELATIONSHIP KEY")
                    .consume();

            var person1 = session.run(
                            "CREATE (p:Person) SET p = $props RETURN elementId(p)", Map.of("props", Map.of("id", 1)))
                    .single()
                    .get(0)
                    .asString();
            var person2 = session.run(
                            "CREATE (p:Person) SET p = $props RETURN elementId(p)", Map.of("props", Map.of("id", 2)))
                    .single()
                    .get(0)
                    .asString();
            var place = session.run(
                            "CREATE (p:Place) SET p = $props RETURN elementId(p)", Map.of("props", Map.of("id", 48)))
                    .single()
                    .get(0)
                    .asString();
            var bornIn = session.run(
                            "MATCH (p:Person WHERE elementId(p) = $person) "
                                    + "MATCH (a:Place WHERE elementId(a) = $place) "
                                    + "CREATE (p)-[b:BORN_IN]->(a) SET b = $props RETURN elementId(b)",
                            Map.of("person", person1, "place", place, "props", Map.of("on", LocalDate.of(1990, 5, 1))))
                    .single()
                    .get(0)
                    .asString();

            StepVerifier.create(new CDCClient(driver, Duration.ZERO).query(current))
                    .assertNext(n -> assertThat(n).extracting("event.elementId").isEqualTo(person1))
                    .assertNext(n -> assertThat(n).extracting("event.elementId").isEqualTo(person2))
                    .assertNext(n -> assertThat(n).extracting("event.elementId").isEqualTo(place))
                    .assertNext(n -> assertThat(n).extracting("event.elementId").isEqualTo(bornIn))
                    .verifyComplete();

            StepVerifier.create(new CDCClient(
                                    driver,
                                    Duration.ZERO,
                                    new NodeSelector(EntityOperation.CREATE, emptySet(), Set.of("Place")))
                            .query(current))
                    .assertNext(n -> assertThat(n).extracting("event.elementId").isEqualTo(place))
                    .verifyComplete();

            StepVerifier.create(new CDCClient(
                                    driver,
                                    Duration.ZERO,
                                    new NodeSelector(EntityOperation.CREATE, emptySet(), Set.of("Place")),
                                    new NodeSelector(null, emptySet(), Set.of("Person")))
                            .query(current))
                    .assertNext(n -> assertThat(n).extracting("event.elementId").isEqualTo(person1))
                    .assertNext(n -> assertThat(n).extracting("event.elementId").isEqualTo(person2))
                    .assertNext(n -> assertThat(n).extracting("event.elementId").isEqualTo(place))
                    .verifyComplete();

            StepVerifier.create(
                            new CDCClient(driver, Duration.ZERO, new RelationshipSelector(null, emptySet(), "BORN_IN"))
                                    .query(current))
                    .assertNext(n -> assertThat(n).extracting("event.elementId").isEqualTo(bornIn))
                    .verifyComplete();

            StepVerifier.create(new CDCClient(
                                    driver,
                                    Duration.ZERO,
                                    new NodeSelector(null, emptySet(), Set.of("Place")),
                                    new RelationshipSelector(null, emptySet(), "BORN_IN"))
                            .query(current))
                    .assertNext(n -> assertThat(n).extracting("event.elementId").isEqualTo(place))
                    .assertNext(n -> assertThat(n).extracting("event.elementId").isEqualTo(bornIn))
                    .verifyComplete();
        }
    }

    @Test
    void selectorsDoFilteringCorrectly() {

        try (var session = driver.session()) {
            session.run("CREATE CONSTRAINT FOR (p:Person) REQUIRE (p.id) IS NODE KEY")
                    .consume();
            session.run("CREATE CONSTRAINT FOR (p:Place) REQUIRE (p.id) IS NODE KEY")
                    .consume();
            session.run("CREATE CONSTRAINT FOR ()-[b:BORN_IN]-() REQUIRE (b.on) IS RELATIONSHIP KEY")
                    .consume();

            var person1 = session.run(
                            "CREATE (p:Person) SET p = $props RETURN elementId(p)",
                            Map.of(
                                    "props",
                                    Map.of(
                                            "id",
                                            1,
                                            "name",
                                            "john",
                                            "surname",
                                            "doe",
                                            "dob",
                                            LocalDate.of(1990, 1, 5),
                                            "gender",
                                            "m")))
                    .single()
                    .get(0)
                    .asString();
            var person2 = session.run(
                            "CREATE (p:Person) SET p = $props RETURN elementId(p)",
                            Map.of(
                                    "props",
                                    Map.of(
                                            "id",
                                            2,
                                            "name",
                                            "jane",
                                            "surname",
                                            "doe",
                                            "dob",
                                            LocalDate.of(1995, 5, 1),
                                            "gender",
                                            "f")))
                    .single()
                    .get(0)
                    .asString();
            var place = session.run(
                            "CREATE (p:Place) SET p = $props RETURN elementId(p)",
                            Map.of("props", Map.of("id", 48, "name", "marmaris", "population", 50000)))
                    .single()
                    .get(0)
                    .asString();
            var bornIn = session.run(
                            "MATCH (p:Person WHERE elementId(p) = $person) "
                                    + "MATCH (a:Place WHERE elementId(a) = $place) "
                                    + "CREATE (p)-[b:BORN_IN]->(a) SET b = $props RETURN elementId(b)",
                            Map.of(
                                    "person",
                                    person1,
                                    "place",
                                    place,
                                    "props",
                                    Map.of("on", LocalDate.of(1990, 5, 1), "at", LocalTime.of(23, 39))))
                    .single()
                    .get(0)
                    .asString();

            session.run(
                            "MATCH ()-[r]->() WHERE elementId(r) = $bornIn SET r.hospital = $hospital, r.by = $doctor",
                            Map.of("bornIn", bornIn, "hospital", "state hospital", "doctor", "doctor who"))
                    .consume();

            StepVerifier.create(new CDCClient(driver, Duration.ZERO).query(current))
                    .assertNext(n -> assertThat(n).extracting("event.elementId").isEqualTo(person1))
                    .assertNext(n -> assertThat(n).extracting("event.elementId").isEqualTo(person2))
                    .assertNext(n -> assertThat(n).extracting("event.elementId").isEqualTo(place))
                    .assertNext(n -> assertThat(n).extracting("event.elementId").isEqualTo(bornIn))
                    .assertNext(n -> assertThat(n).extracting("event.elementId").isEqualTo(bornIn))
                    .verifyComplete();

            StepVerifier.create(new CDCClient(
                                    driver,
                                    Duration.ZERO,
                                    new NodeSelector(
                                            EntityOperation.CREATE,
                                            emptySet(),
                                            Set.of("Place"),
                                            emptyMap(),
                                            Set.of("*"),
                                            emptySet()))
                            .query(current))
                    .assertNext(n -> assertThat(n)
                            .satisfies(e ->
                                    assertThat(e).extracting("event.elementId").isEqualTo(place))
                            .satisfies(e -> assertThat(e)
                                    .extracting(
                                            "event.after.properties",
                                            InstanceOfAssertFactories.map(String.class, Object.class))
                                    .containsOnlyKeys("id", "name", "population")))
                    .verifyComplete();

            StepVerifier.create(new CDCClient(
                                    driver,
                                    Duration.ZERO,
                                    new NodeSelector(
                                            EntityOperation.CREATE,
                                            emptySet(),
                                            Set.of("Place"),
                                            emptyMap(),
                                            Set.of("id", "name"),
                                            emptySet()))
                            .query(current))
                    .assertNext(n -> assertThat(n)
                            .satisfies(e ->
                                    assertThat(e).extracting("event.elementId").isEqualTo(place))
                            .satisfies(e -> assertThat(e)
                                    .extracting(
                                            "event.after.properties",
                                            InstanceOfAssertFactories.map(String.class, Object.class))
                                    .containsOnlyKeys("id", "name")))
                    .verifyComplete();

            StepVerifier.create(new CDCClient(
                                    driver,
                                    Duration.ZERO,
                                    new NodeSelector(
                                            EntityOperation.CREATE,
                                            emptySet(),
                                            Set.of("Place"),
                                            emptyMap(),
                                            emptySet(),
                                            Set.of("population")))
                            .query(current))
                    .assertNext(n -> assertThat(n)
                            .satisfies(e ->
                                    assertThat(e).extracting("event.elementId").isEqualTo(place))
                            .satisfies(e -> assertThat(e)
                                    .extracting(
                                            "event.after.properties",
                                            InstanceOfAssertFactories.map(String.class, Object.class))
                                    .containsOnlyKeys("id", "name")))
                    .verifyComplete();

            StepVerifier.create(new CDCClient(
                                    driver,
                                    Duration.ZERO,
                                    new NodeSelector(
                                            null,
                                            emptySet(),
                                            Set.of("Place"),
                                            emptyMap(),
                                            emptySet(),
                                            Set.of("population")),
                                    new NodeSelector(
                                            null,
                                            emptySet(),
                                            Set.of("Person"),
                                            emptyMap(),
                                            Set.of("id", "name", "surname"),
                                            emptySet()))
                            .query(current))
                    .assertNext(n -> assertThat(n)
                            .satisfies(e ->
                                    assertThat(e).extracting("event.elementId").isEqualTo(person1))
                            .satisfies(e -> assertThat(e)
                                    .extracting(
                                            "event.after.properties",
                                            InstanceOfAssertFactories.map(String.class, Object.class))
                                    .containsOnlyKeys("id", "name", "surname")))
                    .assertNext(n -> assertThat(n)
                            .satisfies(e ->
                                    assertThat(e).extracting("event.elementId").isEqualTo(person2))
                            .satisfies(e -> assertThat(e)
                                    .extracting(
                                            "event.after.properties",
                                            InstanceOfAssertFactories.map(String.class, Object.class))
                                    .containsOnlyKeys("id", "name", "surname")))
                    .assertNext(n -> assertThat(n)
                            .satisfies(e ->
                                    assertThat(e).extracting("event.elementId").isEqualTo(place))
                            .satisfies(e -> assertThat(e)
                                    .extracting(
                                            "event.after.properties",
                                            InstanceOfAssertFactories.map(String.class, Object.class))
                                    .containsOnlyKeys("id", "name")))
                    .verifyComplete();

            StepVerifier.create(new CDCClient(
                                    driver,
                                    Duration.ZERO,
                                    new NodeSelector(
                                            null,
                                            emptySet(),
                                            Set.of("Person"),
                                            Map.of("id", 1L),
                                            emptySet(),
                                            Set.of("gender")),
                                    new NodeSelector(
                                            null,
                                            emptySet(),
                                            Set.of("Person"),
                                            Map.of("id", 2L),
                                            Set.of("id", "name", "surname"),
                                            emptySet()))
                            .query(current))
                    .assertNext(n -> assertThat(n)
                            .satisfies(e ->
                                    assertThat(e).extracting("event.elementId").isEqualTo(person1))
                            .satisfies(e -> assertThat(e)
                                    .extracting(
                                            "event.after.properties",
                                            InstanceOfAssertFactories.map(String.class, Object.class))
                                    .containsOnlyKeys("id", "name", "surname", "dob")))
                    .assertNext(n -> assertThat(n)
                            .satisfies(e ->
                                    assertThat(e).extracting("event.elementId").isEqualTo(person2))
                            .satisfies(e -> assertThat(e)
                                    .extracting(
                                            "event.after.properties",
                                            InstanceOfAssertFactories.map(String.class, Object.class))
                                    .containsOnlyKeys("id", "name", "surname")))
                    .verifyComplete();

            StepVerifier.create(new CDCClient(
                                    driver,
                                    Duration.ZERO,
                                    new RelationshipSelector(
                                            null,
                                            emptySet(),
                                            "BORN_IN",
                                            new RelationshipNodeSelector(),
                                            new RelationshipNodeSelector(),
                                            emptyMap(),
                                            Set.of("*"),
                                            emptySet()))
                            .query(current))
                    .assertNext(n -> assertThat(n)
                            .satisfies(e ->
                                    assertThat(e).extracting("event.elementId").isEqualTo(bornIn))
                            .satisfies(e -> assertThat(e)
                                    .extracting(
                                            "event.after.properties",
                                            InstanceOfAssertFactories.map(String.class, Object.class))
                                    .containsOnlyKeys("on", "at")))
                    .assertNext(n -> assertThat(n)
                            .satisfies(e ->
                                    assertThat(e).extracting("event.elementId").isEqualTo(bornIn))
                            .satisfies(e -> assertThat(e)
                                    .extracting(
                                            "event.before.properties",
                                            InstanceOfAssertFactories.map(String.class, Object.class))
                                    .containsOnlyKeys("on", "at"))
                            .satisfies(e -> assertThat(e)
                                    .extracting(
                                            "event.after.properties",
                                            InstanceOfAssertFactories.map(String.class, Object.class))
                                    .containsOnlyKeys("on", "at", "hospital", "by")))
                    .verifyComplete();

            StepVerifier.create(new CDCClient(
                                    driver,
                                    Duration.ZERO,
                                    new RelationshipSelector(
                                            null,
                                            emptySet(),
                                            "BORN_IN",
                                            new RelationshipNodeSelector(),
                                            new RelationshipNodeSelector(),
                                            emptyMap(),
                                            Set.of("on"),
                                            emptySet()))
                            .query(current))
                    .assertNext(n -> assertThat(n)
                            .satisfies(e ->
                                    assertThat(e).extracting("event.elementId").isEqualTo(bornIn))
                            .satisfies(e -> assertThat(e)
                                    .extracting(
                                            "event.after.properties",
                                            InstanceOfAssertFactories.map(String.class, Object.class))
                                    .containsOnlyKeys("on")))
                    .assertNext(n -> assertThat(n)
                            .satisfies(e ->
                                    assertThat(e).extracting("event.elementId").isEqualTo(bornIn))
                            .satisfies(e -> assertThat(e)
                                    .extracting(
                                            "event.before.properties",
                                            InstanceOfAssertFactories.map(String.class, Object.class))
                                    .containsOnlyKeys("on"))
                            .satisfies(e -> assertThat(e)
                                    .extracting(
                                            "event.after.properties",
                                            InstanceOfAssertFactories.map(String.class, Object.class))
                                    .containsOnlyKeys("on")))
                    .verifyComplete();

            // first matching selector wins
            StepVerifier.create(new CDCClient(
                                    driver,
                                    Duration.ZERO,
                                    new RelationshipSelector(
                                            EntityOperation.CREATE,
                                            emptySet(),
                                            "BORN_IN",
                                            new RelationshipNodeSelector(),
                                            new RelationshipNodeSelector(),
                                            emptyMap(),
                                            emptySet(),
                                            Set.of("on")),
                                    new RelationshipSelector(
                                            EntityOperation.CREATE,
                                            emptySet(),
                                            "BORN_IN",
                                            new RelationshipNodeSelector(),
                                            new RelationshipNodeSelector(),
                                            emptyMap(),
                                            emptySet(),
                                            Set.of("at")))
                            .query(current))
                    .assertNext(n -> assertThat(n)
                            .satisfies(e ->
                                    assertThat(e).extracting("event.elementId").isEqualTo(bornIn))
                            .satisfies(e -> assertThat(e)
                                    .extracting(
                                            "event.after.properties",
                                            InstanceOfAssertFactories.map(String.class, Object.class))
                                    .containsOnlyKeys("at")))
                    .verifyComplete();
        }
    }
}
