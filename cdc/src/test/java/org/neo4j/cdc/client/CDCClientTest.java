package org.neo4j.cdc.client;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.cdc.client.model.CaptureMode;
import org.neo4j.cdc.client.model.ChangeIdentifier;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.test.StepVerifier;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author Gerrit Meier
 */
@Testcontainers
public class CDCClientTest {

	private static final String NEO4J_VERSION = "5.11";

	@Container
	private static final Neo4jContainer<?> neo4j = new Neo4jContainer<>("neo4j:" + NEO4J_VERSION + "-enterprise")
		.withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
		.withNeo4jConfig("internal.dbms.change_data_capture", "true")
		.withoutAuthentication();

	private static Driver driver;
	private static ChangeIdentifier current;

	private CDCClient client;

	@BeforeAll
	static void setup() {
		driver = GraphDatabase.driver(neo4j.getBoltUrl(), AuthTokens.none());

		try (Session session = driver.session()) {
			Map<String, Object> parameters = new HashMap<>();
			parameters.put("db", "neo4j");
			parameters.put("mode", CaptureMode.FULL.name());
			session.run("ALTER DATABASE $db SET OPTION txLogEnrichment $mode", parameters).consume();

			current = new ChangeIdentifier(session.run("CALL cdc.current()").single().get(0).asString());
		}
	}

	@AfterAll
	static void cleanup() {
		driver.close();
	}

	@BeforeEach
	void setupCDCClient() {
		client = new CDCClient(driver, null);
	}

	@Test
	void earliest() {
		StepVerifier.create(client.earliest())
			.assertNext(cv -> assertNotNull(cv.getId()))
			.verifyComplete();
	}

	@Test
	void current() {
		StepVerifier.create(client.current())
			.assertNext(cv -> assertNotNull(cv.getId()))
			.verifyComplete();
	}

	@Test
	void query() {
		try (Session session = driver.session()) {
			session.run("CREATE ()").consume();
		}

		StepVerifier.create(client.query(current))
			.assertNext(System.out::println)
			.verifyComplete();
	}
}
