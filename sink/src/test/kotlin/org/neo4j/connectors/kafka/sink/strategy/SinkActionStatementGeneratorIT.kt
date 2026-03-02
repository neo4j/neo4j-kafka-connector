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

import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import io.kotest.matchers.shouldBe
import java.time.LocalDate
import java.util.UUID
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.neo4j.caniuse.Neo4j
import org.neo4j.caniuse.Neo4jDetector
import org.neo4j.connectors.kafka.testing.DatabaseSupport.createDatabase
import org.neo4j.connectors.kafka.testing.DatabaseSupport.dropDatabase
import org.neo4j.connectors.kafka.testing.neo4jDatabase
import org.neo4j.connectors.kafka.testing.neo4jImage
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.Query
import org.neo4j.driver.Session
import org.neo4j.driver.SessionConfig
import org.neo4j.driver.summary.ResultSummary
import org.testcontainers.containers.Neo4jContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

@Testcontainers
class SinkActionStatementGeneratorIT {

  companion object {
    @Container
    val container: Neo4jContainer<*> =
        Neo4jContainer(neo4jImage())
            .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
            .withExposedPorts(7687)
            .withoutAuthentication()
            .waitingFor(neo4jDatabase())

    private lateinit var driver: Driver
    private lateinit var neo4j: Neo4j

    @BeforeAll
    @JvmStatic
    fun setUpContainer() {
      driver = GraphDatabase.driver(container.boltUrl, AuthTokens.none())
      neo4j = Neo4jDetector.detect(driver)
    }

    @AfterAll
    @JvmStatic
    fun tearDownContainer() {
      driver.close()
    }
  }

  private lateinit var db: String
  private lateinit var session: Session
  private lateinit var generator: DefaultSinkActionStatementGenerator

  @BeforeEach
  fun setUp() {
    db = "test-${UUID.randomUUID()}"
    driver.createDatabase(db)
    session = driver.session(SessionConfig.forDatabase(db))
    generator = DefaultSinkActionStatementGenerator(neo4j)
  }

  @AfterEach
  fun tearDown() {
    if (this::session.isInitialized) session.close()
    if (this::db.isInitialized) driver.dropDatabase(db)
  }

  // ===========================================
  // Node Create Tests
  // ===========================================

  @Test
  fun `should execute create node statement`() {
    val action =
        CreateNodeSinkAction(
            setOf("Person", "Employee"),
            mapOf("name" to "john", "surname" to "doe", "dob" to LocalDate.of(1990, 1, 1)),
        )

    val query = generator.buildStatement(action)
    executeAndVerifyNoDeprecations(query)

    val result = session.run("MATCH (n:Person:Employee) RETURN n").single()
    val node = result["n"].asNode()
    node["name"].asString() shouldBe "john"
    node["surname"].asString() shouldBe "doe"
    node["dob"].asLocalDate() shouldBe LocalDate.of(1990, 1, 1)
  }

  @Test
  fun `should execute create node statement with empty labels`() {
    val action = CreateNodeSinkAction(emptySet(), mapOf("name" to "anonymous"))

    val query = generator.buildStatement(action)
    executeAndVerifyNoDeprecations(query)

    val result = session.run("MATCH (n {name: 'anonymous'}) RETURN n").single()
    val node = result["n"].asNode()
    node["name"].asString() shouldBe "anonymous"
  }

  // ===========================================
  // Node Update Tests
  // ===========================================

  @Test
  fun `should execute update node statement with labels and properties matcher`() {
    // Setup: Create a node to update
    session.run("CREATE (:Person {id: 1, name: 'joe', surname: 'doe'})").consume()

    val action =
        UpdateNodeSinkAction(
            matcher = NodeMatcher.ByLabelsAndProperties(setOf("Person"), mapOf("id" to 1)),
            setProperties = mapOf("name" to "john", "updated" to true),
            addLabels = setOf("Employee"),
            removeLabels = setOf("Intern"),
        )

    val query = generator.buildStatement(action)
    executeAndVerifyNoDeprecations(query)

    val result = session.run("MATCH (n:Person:Employee {id: 1}) RETURN n").single()
    val node = result["n"].asNode()
    node["name"].asString() shouldBe "john"
    node["updated"].asBoolean() shouldBe true
    node.labels().toList() shouldContainExactlyInAnyOrder listOf("Person", "Employee")
  }

  @Test
  fun `should execute update node statement with id matcher`() {
    // Setup: Create a node and get its id
    val nodeId =
        session.run("CREATE (n:Person {name: 'joe'}) RETURN id(n) as id").single()["id"].asLong()

    val action =
        UpdateNodeSinkAction(
            matcher = NodeMatcher.ById(nodeId),
            setProperties = mapOf("name" to "john"),
            addLabels = setOf("Updated"),
            removeLabels = emptySet(),
        )

    val query = generator.buildStatement(action)
    executeAllowingIdDeprecation(query)

    val result = session.run("MATCH (n:Person:Updated) RETURN n").single()
    val node = result["n"].asNode()
    node["name"].asString() shouldBe "john"
  }

  @Test
  fun `should execute update node statement with element id matcher`() {
    // Setup: Create a node and get its element id
    val elementId =
        session
            .run("CREATE (n:Person {name: 'joe'}) RETURN elementId(n) as id")
            .single()["id"]
            .asString()

    val action =
        UpdateNodeSinkAction(
            matcher = NodeMatcher.ByElementId(elementId),
            setProperties = mapOf("name" to "john"),
            addLabels = setOf("Updated"),
            removeLabels = emptySet(),
        )

    val query = generator.buildStatement(action)
    executeAndVerifyNoDeprecations(query)

    val result = session.run("MATCH (n:Person:Updated) RETURN n").single()
    val node = result["n"].asNode()
    node["name"].asString() shouldBe "john"
  }

  // ===========================================
  // Node Merge Tests
  // ===========================================

  @Test
  fun `should execute merge node statement - creates new node`() {
    val action =
        MergeNodeSinkAction(
            matcher = NodeMatcher.ByLabelsAndProperties(setOf("Person"), mapOf("id" to 1)),
            setProperties = mapOf("name" to "john"),
            addLabels = setOf("Employee"),
            removeLabels = emptySet(),
        )

    val query = generator.buildStatement(action)
    executeAndVerifyNoDeprecations(query)

    val result = session.run("MATCH (n:Person:Employee {id: 1}) RETURN n").single()
    val node = result["n"].asNode()
    node["name"].asString() shouldBe "john"
  }

  @Test
  fun `should execute merge node statement - updates existing node`() {
    // Setup: Create existing node
    session.run("CREATE (:Person {id: 1, name: 'joe'})").consume()

    val action =
        MergeNodeSinkAction(
            matcher = NodeMatcher.ByLabelsAndProperties(setOf("Person"), mapOf("id" to 1)),
            setProperties = mapOf("name" to "john", "merged" to true),
            addLabels = setOf("Employee"),
            removeLabels = emptySet(),
        )

    val query = generator.buildStatement(action)
    executeAndVerifyNoDeprecations(query)

    val count = session.run("MATCH (n:Person {id: 1}) RETURN count(n) as c").single()["c"].asLong()
    count shouldBe 1

    val result = session.run("MATCH (n:Person:Employee {id: 1}) RETURN n").single()
    val node = result["n"].asNode()
    node["name"].asString() shouldBe "john"
    node["merged"].asBoolean() shouldBe true
  }

  // ===========================================
  // Node Delete Tests
  // ===========================================

  @Test
  fun `should execute delete node statement with labels and properties matcher`() {
    // Setup: Create a node to delete
    session.run("CREATE (:Person {id: 1, name: 'joe'})").consume()

    val action =
        DeleteNodeSinkAction(
            matcher = NodeMatcher.ByLabelsAndProperties(setOf("Person"), mapOf("id" to 1))
        )

    val query = generator.buildStatement(action)
    executeAndVerifyNoDeprecations(query)

    val count = session.run("MATCH (n:Person {id: 1}) RETURN count(n) as c").single()["c"].asLong()
    count shouldBe 0
  }

  @Test
  fun `should execute delete node statement with id matcher`() {
    // Setup: Create a node and get its id
    val nodeId =
        session.run("CREATE (n:Person {name: 'joe'}) RETURN id(n) as id").single()["id"].asLong()

    val action = DeleteNodeSinkAction(matcher = NodeMatcher.ById(nodeId))

    val query = generator.buildStatement(action)
    executeAllowingIdDeprecation(query)

    val count = session.run("MATCH (n:Person) RETURN count(n) as c").single()["c"].asLong()
    count shouldBe 0
  }

  @Test
  fun `should execute delete node statement with element id matcher`() {
    // Setup: Create a node and get its element id
    val elementId =
        session
            .run("CREATE (n:Person {name: 'joe'}) RETURN elementId(n) as id")
            .single()["id"]
            .asString()

    val action = DeleteNodeSinkAction(matcher = NodeMatcher.ByElementId(elementId))

    val query = generator.buildStatement(action)
    executeAndVerifyNoDeprecations(query)

    val count = session.run("MATCH (n:Person) RETURN count(n) as c").single()["c"].asLong()
    count shouldBe 0
  }

  // ===========================================
  // Relationship Create Tests
  // ===========================================

  @Test
  fun `should execute create relationship statement with labels and properties node matchers`() {
    // Setup: Create start and end nodes
    session.run("CREATE (:Person {id: 1}), (:Company {id: 2})").consume()

    val action =
        CreateRelationshipSinkAction(
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(setOf("Person"), mapOf("id" to 1)),
                LookupMode.MATCH,
            ),
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(setOf("Company"), mapOf("id" to 2)),
                LookupMode.MATCH,
            ),
            "WORKS_AT",
            mapOf("since" to 2020),
        )

    val query = generator.buildStatement(action)
    executeAndVerifyNoDeprecations(query)

    val result =
        session.run("MATCH (:Person {id: 1})-[r:WORKS_AT]->(:Company {id: 2}) RETURN r").single()
    val rel = result["r"].asRelationship()
    rel["since"].asInt() shouldBe 2020
  }

  @Test
  fun `should execute create relationship statement with node id matchers`() {
    // Setup: Create start and end nodes
    val ids =
        session
            .run(
                "CREATE (p:Person {name: 'joe'}), (c:Company {name: 'acme'}) " +
                    "RETURN id(p) as pid, id(c) as cid"
            )
            .single()
    val personId = ids["pid"].asLong()
    val companyId = ids["cid"].asLong()

    val action =
        CreateRelationshipSinkAction(
            SinkActionNodeReference(NodeMatcher.ById(personId), LookupMode.MATCH),
            SinkActionNodeReference(NodeMatcher.ById(companyId), LookupMode.MATCH),
            "WORKS_AT",
            mapOf("role" to "developer"),
        )

    val query = generator.buildStatement(action)
    executeAllowingIdDeprecation(query)

    val result = session.run("MATCH (:Person)-[r:WORKS_AT]->(:Company) RETURN r").single()
    val rel = result["r"].asRelationship()
    rel["role"].asString() shouldBe "developer"
  }

  @Test
  fun `should execute create relationship statement with node element id matchers`() {
    // Setup: Create start and end nodes
    val ids =
        session
            .run(
                "CREATE (p:Person {name: 'joe'}), (c:Company {name: 'acme'}) " +
                    "RETURN elementId(p) as pid, elementId(c) as cid"
            )
            .single()
    val personElementId = ids["pid"].asString()
    val companyElementId = ids["cid"].asString()

    val action =
        CreateRelationshipSinkAction(
            SinkActionNodeReference(NodeMatcher.ByElementId(personElementId), LookupMode.MATCH),
            SinkActionNodeReference(NodeMatcher.ByElementId(companyElementId), LookupMode.MATCH),
            "WORKS_AT",
            mapOf("role" to "developer"),
        )

    val query = generator.buildStatement(action)
    executeAndVerifyNoDeprecations(query)

    val result = session.run("MATCH (:Person)-[r:WORKS_AT]->(:Company) RETURN r").single()
    val rel = result["r"].asRelationship()
    rel["role"].asString() shouldBe "developer"
  }

  @Test
  fun `should execute create relationship statement with merge node matchers`() {
    // No pre-existing nodes - they should be created via MERGE
    val action =
        CreateRelationshipSinkAction(
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(setOf("Person"), mapOf("id" to 1)),
                LookupMode.MERGE,
            ),
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(setOf("Company"), mapOf("id" to 2)),
                LookupMode.MERGE,
            ),
            "WORKS_AT",
            mapOf("since" to 2020),
        )

    val query = generator.buildStatement(action)
    executeAndVerifyNoDeprecations(query)

    val result =
        session.run("MATCH (:Person {id: 1})-[r:WORKS_AT]->(:Company {id: 2}) RETURN r").single()
    val rel = result["r"].asRelationship()
    rel["since"].asInt() shouldBe 2020

    // Verify nodes were created
    val nodeCount =
        session
            .run("MATCH (n) WHERE n:Person OR n:Company RETURN count(n) as c")
            .single()["c"]
            .asLong()
    nodeCount shouldBe 2
  }

  // ===========================================
  // Relationship Update Tests
  // ===========================================

  @Test
  fun `should execute update relationship statement with type and properties matcher`() {
    // Setup: Create nodes and relationship
    session
        .run(
            "CREATE (:Person {id: 1})-[:WORKS_AT {empId: 100, role: 'junior'}]->(:Company {id: 2})"
        )
        .consume()

    val action =
        UpdateRelationshipSinkAction(
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(setOf("Person"), mapOf("id" to 1)),
                LookupMode.MATCH,
            ),
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(setOf("Company"), mapOf("id" to 2)),
                LookupMode.MATCH,
            ),
            RelationshipMatcher.ByTypeAndProperties("WORKS_AT", mapOf("empId" to 100), true),
            setProperties = mapOf("role" to "senior", "promoted" to true),
        )

    val query = generator.buildStatement(action)
    executeAndVerifyNoDeprecations(query)

    val result = session.run("MATCH (:Person)-[r:WORKS_AT]->(:Company) RETURN r").single()
    val rel = result["r"].asRelationship()
    rel["role"].asString() shouldBe "senior"
    rel["promoted"].asBoolean() shouldBe true
  }

  @Test
  fun `should execute update relationship statement with id matcher`() {
    // Setup: Create nodes and relationship, get relationship id
    val relId =
        session
            .run(
                "CREATE (:Person {id: 1})-[r:WORKS_AT {role: 'junior'}]->(:Company {id: 2}) " +
                    "RETURN id(r) as rid"
            )
            .single()["rid"]
            .asLong()

    val action =
        UpdateRelationshipSinkAction(
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(setOf("Person"), mapOf("id" to 1)),
                LookupMode.MATCH,
            ),
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(setOf("Company"), mapOf("id" to 2)),
                LookupMode.MATCH,
            ),
            RelationshipMatcher.ById(relId),
            setProperties = mapOf("role" to "senior"),
        )

    val query = generator.buildStatement(action)
    executeAllowingIdDeprecation(query)

    val result = session.run("MATCH (:Person)-[r:WORKS_AT]->(:Company) RETURN r").single()
    val rel = result["r"].asRelationship()
    rel["role"].asString() shouldBe "senior"
  }

  @Test
  fun `should execute update relationship statement with element id matcher`() {
    // Setup: Create nodes and relationship, get relationship element id
    val relElementId =
        session
            .run(
                "CREATE (:Person {id: 1})-[r:WORKS_AT {role: 'junior'}]->(:Company {id: 2}) " +
                    "RETURN elementId(r) as rid"
            )
            .single()["rid"]
            .asString()

    val action =
        UpdateRelationshipSinkAction(
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(setOf("Person"), mapOf("id" to 1)),
                LookupMode.MATCH,
            ),
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(setOf("Company"), mapOf("id" to 2)),
                LookupMode.MATCH,
            ),
            RelationshipMatcher.ByElementId(relElementId),
            setProperties = mapOf("role" to "senior"),
        )

    val query = generator.buildStatement(action)
    executeAndVerifyNoDeprecations(query)

    val result = session.run("MATCH (:Person)-[r:WORKS_AT]->(:Company) RETURN r").single()
    val rel = result["r"].asRelationship()
    rel["role"].asString() shouldBe "senior"
  }

  @Test
  fun `should execute update relationship statement with node id matchers`() {
    // Setup: Create nodes and relationship
    val ids =
        session
            .run(
                "CREATE (p:Person {name: 'joe'})-[r:WORKS_AT {role: 'junior'}]->(c:Company {name: 'acme'}) " +
                    "RETURN id(p) as pid, id(c) as cid"
            )
            .single()
    val personId = ids["pid"].asLong()
    val companyId = ids["cid"].asLong()

    val action =
        UpdateRelationshipSinkAction(
            SinkActionNodeReference(NodeMatcher.ById(personId), LookupMode.MATCH),
            SinkActionNodeReference(NodeMatcher.ById(companyId), LookupMode.MATCH),
            RelationshipMatcher.ByTypeAndProperties("WORKS_AT", mapOf("role" to "junior"), true),
            setProperties = mapOf("role" to "senior"),
        )

    val query = generator.buildStatement(action)
    executeAllowingIdDeprecation(query)

    val result = session.run("MATCH (:Person)-[r:WORKS_AT]->(:Company) RETURN r").single()
    val rel = result["r"].asRelationship()
    rel["role"].asString() shouldBe "senior"
  }

  // ===========================================
  // Relationship Merge Tests
  // ===========================================

  @Test
  fun `should execute merge relationship statement - creates new relationship`() {
    // Setup: Create nodes without relationship
    session.run("CREATE (:Person {id: 1}), (:Company {id: 2})").consume()

    val action =
        MergeRelationshipSinkAction(
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(setOf("Person"), mapOf("id" to 1)),
                LookupMode.MATCH,
            ),
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(setOf("Company"), mapOf("id" to 2)),
                LookupMode.MATCH,
            ),
            RelationshipMatcher.ByTypeAndProperties("WORKS_AT", mapOf("empId" to 100), true),
            setProperties = mapOf("role" to "developer"),
        )

    val query = generator.buildStatement(action)
    executeAndVerifyNoDeprecations(query)

    val result = session.run("MATCH (:Person)-[r:WORKS_AT]->(:Company) RETURN r").single()
    val rel = result["r"].asRelationship()
    rel["empId"].asInt() shouldBe 100
    rel["role"].asString() shouldBe "developer"
  }

  @Test
  fun `should execute merge relationship statement - updates existing relationship`() {
    // Setup: Create nodes with existing relationship
    session
        .run(
            "CREATE (:Person {id: 1})-[:WORKS_AT {empId: 100, role: 'junior'}]->(:Company {id: 2})"
        )
        .consume()

    val action =
        MergeRelationshipSinkAction(
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(setOf("Person"), mapOf("id" to 1)),
                LookupMode.MATCH,
            ),
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(setOf("Company"), mapOf("id" to 2)),
                LookupMode.MATCH,
            ),
            RelationshipMatcher.ByTypeAndProperties("WORKS_AT", mapOf("empId" to 100), true),
            setProperties = mapOf("role" to "senior"),
        )

    val query = generator.buildStatement(action)
    executeAndVerifyNoDeprecations(query)

    // Should only have one relationship
    val count =
        session
            .run("MATCH (:Person)-[r:WORKS_AT]->(:Company) RETURN count(r) as c")
            .single()["c"]
            .asLong()
    count shouldBe 1

    val result = session.run("MATCH (:Person)-[r:WORKS_AT]->(:Company) RETURN r").single()
    val rel = result["r"].asRelationship()
    rel["role"].asString() shouldBe "senior"
  }

  @Test
  fun `should execute merge relationship statement with merge node matchers`() {
    // No pre-existing nodes - they should be created via MERGE
    val action =
        MergeRelationshipSinkAction(
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(setOf("Person"), mapOf("id" to 1)),
                LookupMode.MERGE,
            ),
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(setOf("Company"), mapOf("id" to 2)),
                LookupMode.MERGE,
            ),
            RelationshipMatcher.ByTypeAndProperties("WORKS_AT", mapOf("empId" to 100), true),
            setProperties = mapOf("role" to "developer"),
        )

    val query = generator.buildStatement(action)
    executeAndVerifyNoDeprecations(query)

    val result =
        session.run("MATCH (:Person {id: 1})-[r:WORKS_AT]->(:Company {id: 2}) RETURN r").single()
    val rel = result["r"].asRelationship()
    rel["empId"].asInt() shouldBe 100
    rel["role"].asString() shouldBe "developer"

    // Verify nodes were created
    val nodeCount =
        session
            .run("MATCH (n) WHERE n:Person OR n:Company RETURN count(n) as c")
            .single()["c"]
            .asLong()
    nodeCount shouldBe 2
  }

  @Test
  fun `should execute merge relationship statement with node id matchers`() {
    // Setup: Create nodes
    val ids =
        session
            .run(
                "CREATE (p:Person {name: 'joe'}), (c:Company {name: 'acme'}) " +
                    "RETURN id(p) as pid, id(c) as cid"
            )
            .single()
    val personId = ids["pid"].asLong()
    val companyId = ids["cid"].asLong()

    val action =
        MergeRelationshipSinkAction(
            SinkActionNodeReference(NodeMatcher.ById(personId), LookupMode.MATCH),
            SinkActionNodeReference(NodeMatcher.ById(companyId), LookupMode.MATCH),
            RelationshipMatcher.ByTypeAndProperties("WORKS_AT", mapOf("empId" to 100), true),
            setProperties = mapOf("role" to "developer"),
        )

    val query = generator.buildStatement(action)
    executeAllowingIdDeprecation(query)

    val result = session.run("MATCH (:Person)-[r:WORKS_AT]->(:Company) RETURN r").single()
    val rel = result["r"].asRelationship()
    rel["role"].asString() shouldBe "developer"
  }

  // ===========================================
  // Relationship Delete Tests
  // ===========================================

  @Test
  fun `should execute delete relationship statement with type and properties matcher`() {
    // Setup: Create nodes and relationship
    session.run("CREATE (:Person {id: 1})-[:WORKS_AT {empId: 100}]->(:Company {id: 2})").consume()

    val action =
        DeleteRelationshipSinkAction(
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(setOf("Person"), mapOf("id" to 1)),
                LookupMode.MATCH,
            ),
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(setOf("Company"), mapOf("id" to 2)),
                LookupMode.MATCH,
            ),
            RelationshipMatcher.ByTypeAndProperties("WORKS_AT", mapOf("empId" to 100), true),
        )

    val query = generator.buildStatement(action)
    executeAndVerifyNoDeprecations(query)

    val count =
        session
            .run("MATCH (:Person)-[r:WORKS_AT]->(:Company) RETURN count(r) as c")
            .single()["c"]
            .asLong()
    count shouldBe 0

    // Nodes should still exist
    val nodeCount =
        session
            .run("MATCH (n) WHERE n:Person OR n:Company RETURN count(n) as c")
            .single()["c"]
            .asLong()
    nodeCount shouldBe 2
  }

  @Test
  fun `should execute delete relationship statement with id matcher`() {
    // Setup: Create nodes and relationship, get relationship id
    val relId =
        session
            .run(
                "CREATE (:Person {id: 1})-[r:WORKS_AT]->(:Company {id: 2}) " + "RETURN id(r) as rid"
            )
            .single()["rid"]
            .asLong()

    val action =
        DeleteRelationshipSinkAction(
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(setOf("Person"), mapOf("id" to 1)),
                LookupMode.MATCH,
            ),
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(setOf("Company"), mapOf("id" to 2)),
                LookupMode.MATCH,
            ),
            RelationshipMatcher.ById(relId),
        )

    val query = generator.buildStatement(action)
    executeAllowingIdDeprecation(query)

    val count =
        session
            .run("MATCH (:Person)-[r:WORKS_AT]->(:Company) RETURN count(r) as c")
            .single()["c"]
            .asLong()
    count shouldBe 0
  }

  @Test
  fun `should execute delete relationship statement with element id matcher`() {
    // Setup: Create nodes and relationship, get relationship element id
    val relElementId =
        session
            .run(
                "CREATE (:Person {id: 1})-[r:WORKS_AT]->(:Company {id: 2}) " +
                    "RETURN elementId(r) as rid"
            )
            .single()["rid"]
            .asString()

    val action =
        DeleteRelationshipSinkAction(
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(setOf("Person"), mapOf("id" to 1)),
                LookupMode.MATCH,
            ),
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(setOf("Company"), mapOf("id" to 2)),
                LookupMode.MATCH,
            ),
            RelationshipMatcher.ByElementId(relElementId),
        )

    val query = generator.buildStatement(action)
    executeAndVerifyNoDeprecations(query)

    val count =
        session
            .run("MATCH (:Person)-[r:WORKS_AT]->(:Company) RETURN count(r) as c")
            .single()["c"]
            .asLong()
    count shouldBe 0
  }

  @Test
  fun `should execute delete relationship statement with node id matchers`() {
    // Setup: Create nodes and relationship
    val ids =
        session
            .run(
                "CREATE (p:Person {name: 'joe'})-[:WORKS_AT]->(c:Company {name: 'acme'}) " +
                    "RETURN id(p) as pid, id(c) as cid"
            )
            .single()
    val personId = ids["pid"].asLong()
    val companyId = ids["cid"].asLong()

    val action =
        DeleteRelationshipSinkAction(
            SinkActionNodeReference(NodeMatcher.ById(personId), LookupMode.MATCH),
            SinkActionNodeReference(NodeMatcher.ById(companyId), LookupMode.MATCH),
            RelationshipMatcher.ByTypeAndProperties("WORKS_AT", emptyMap(), false),
        )

    val query = generator.buildStatement(action)
    executeAllowingIdDeprecation(query)

    val count =
        session
            .run("MATCH (:Person)-[r:WORKS_AT]->(:Company) RETURN count(r) as c")
            .single()["c"]
            .asLong()
    count shouldBe 0
  }

  private fun executeAndVerifyNoDeprecations(query: Query): ResultSummary {
    val summary = session.run(query.text(), query.parameters()).consume()
    summary
        .notifications()
        .filter {
          it.code()?.equals("Neo.ClientNotification.Statement.FeatureDeprecationWarning") ?: false
        }
        .shouldBeEmpty()
    return summary
  }

  /**
   * Executes the query and allows deprecation warnings for the legacy `id()` function. Use this for
   * tests that intentionally use `NodeMatcher.ById` or `RelationshipMatcher.ById` which generate
   * deprecated `id()` calls for backward compatibility.
   */
  private fun executeAllowingIdDeprecation(query: Query): ResultSummary {
    val summary = session.run(query.text(), query.parameters()).consume()
    summary
        .notifications()
        .filter {
          it.code()?.equals("Neo.ClientNotification.Statement.FeatureDeprecationWarning") ?: false
        }
        .filter {
          it.description()?.lowercase()?.contains("'id' has been replaced by 'elementId") ?: false
        }
        .shouldBeEmpty()
    return summary
  }
}
