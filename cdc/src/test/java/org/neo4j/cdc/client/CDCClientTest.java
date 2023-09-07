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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.cdc.client.model.*;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.test.StepVerifier;

/**
 * @author Gerrit Meier
 */
@Testcontainers
public class CDCClientTest {

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

        try (Session session = driver.session()) {
            Map<String, Object> parameters = new HashMap<>();
            parameters.put("db", "neo4j");
            parameters.put("mode", CaptureMode.FULL.name());
            session.run("ALTER DATABASE $db SET OPTION txLogEnrichment $mode", parameters)
                    .consume();
        }
    }

    @AfterAll
    static void cleanup() {
        driver.close();
    }

    @BeforeEach
    void collectCurrent() {
        try (var session = driver.session()) {
            current = new ChangeIdentifier(
                    session.run("CALL cdc.current()").single().get(0).asString());
        }
    }

    @Test
    void earliest() {
        var client = new CDCClient(driver);

        StepVerifier.create(client.earliest())
                .assertNext(cv -> assertNotNull(cv.getId()))
                .verifyComplete();
    }

    @Test
    void current() {
        var client = new CDCClient(driver);

        StepVerifier.create(client.current())
                .assertNext(cv -> assertNotNull(cv.getId()))
                .verifyComplete();
    }

    @Test
    void changesCanBeQueried() {
        var client = new CDCClient(driver);

        try (Session session = driver.session()) {
            session.run("CREATE ()").consume();
        }

        StepVerifier.create(client.query(current))
                .assertNext(System.out::println)
                .verifyComplete();
    }

    @Test
    void nodeChangesCanBeQueried() {
        CDCClient client = new CDCClient(driver);

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
}
