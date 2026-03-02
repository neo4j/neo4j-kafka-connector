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

import io.kotest.matchers.shouldBe
import java.time.LocalDate
import java.util.stream.Stream
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.ArgumentsProvider
import org.junit.jupiter.params.provider.ArgumentsSource
import org.junit.jupiter.params.support.ParameterDeclarations
import org.neo4j.caniuse.Neo4j
import org.neo4j.caniuse.Neo4jDeploymentType
import org.neo4j.caniuse.Neo4jEdition
import org.neo4j.caniuse.Neo4jVersion
import org.neo4j.driver.Query

class SinkActionStatementGeneratorTest {

  @ParameterizedTest
  @ArgumentsSource(CreateNodeParams::class)
  fun `should build node create statements`(neo4j: Neo4j, expectedQuery: Query) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val data =
        CreateNodeSinkAction(
            setOf("Person", "Employee"),
            mapOf("name" to "john", "surname" to "doe", "dob" to LocalDate.of(1990, 1, 1)),
        )

    generator.buildStatement(data) shouldBe expectedQuery
  }

  object CreateNodeParams : ArgumentsProvider {
    private fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e CREATE (n:`Employee`:`Person`) " + "SET n += _e.properties",
          mapOf(
              "e" to
                  mapOf(
                      "properties" to
                          mapOf(
                              "name" to "john",
                              "surname" to "doe",
                              "dob" to LocalDate.of(1990, 1, 1),
                          )
                  )
          ),
      )
    }

    private fun setRemoveDynamicLabelsQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e CREATE (n:`Employee`:`Person`) " + "SET n += _e.properties",
          mapOf(
              "e" to
                  mapOf(
                      "properties" to
                          mapOf(
                              "name" to "john",
                              "surname" to "doe",
                              "dob" to LocalDate.of(1990, 1, 1),
                          )
                  )
          ),
      )
    }

    private fun dynamicLabelsQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e CREATE (n:${'$'}(_e.labels)) " + "SET n += _e.properties",
          mapOf(
              "e" to
                  mapOf(
                      "labels" to listOf("Person", "Employee"),
                      "properties" to
                          mapOf(
                              "name" to "john",
                              "surname" to "doe",
                              "dob" to LocalDate.of(1990, 1, 1),
                          ),
                  )
          ),
      )
    }

    override fun provideArguments(
        parameters: ParameterDeclarations?,
        context: ExtensionContext?,
    ): Stream<out Arguments?>? {
      return Stream.of(
          Arguments.of(neo4j4_4, standardCypherQuery()),
          Arguments.of(neo4j5_26, setRemoveDynamicLabelsQuery()),
          Arguments.of(neo4j2025_11, setRemoveDynamicLabelsQuery()),
          Arguments.of(neo4j2026_1, dynamicLabelsQuery()),
          Arguments.of(neo4jAura, dynamicLabelsQuery()),
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(UpdateNodeParams::class)
  fun `should build node update statements`(neo4j: Neo4j, expectedQuery: Query) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val data =
        UpdateNodeSinkAction(
            matchLabels = setOf("Person"),
            matchProperties = mapOf("name" to "joe", "surname" to "doe"),
            setProperties = mapOf("name" to "john"),
            addLabels = setOf("Employee"),
            removeLabels = setOf("Undergrad", "Intern"),
        )

    generator.buildStatement(data) shouldBe expectedQuery
  }

  object UpdateNodeParams : ArgumentsProvider {
    private fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (n:`Person` {`name`: _e.matchProperties.`name`, `surname`: _e.matchProperties.`surname`}) " +
              "SET n += _e.setProperties SET n:`Employee` REMOVE n:`Intern`:`Undergrad`",
          mapOf(
              "e" to
                  mapOf(
                      "matchProperties" to mapOf("name" to "joe", "surname" to "doe"),
                      "setProperties" to mapOf("name" to "john"),
                  )
          ),
      )
    }

    private fun setRemoveDynamicLabelsQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (n:`Person` {`name`: _e.matchProperties.`name`, `surname`: _e.matchProperties.`surname`}) " +
              "SET n += _e.setProperties SET n:${'$'}(_e.addLabels) REMOVE n:${'$'}(_e.removeLabels)",
          mapOf(
              "e" to
                  mapOf(
                      "addLabels" to listOf("Employee"),
                      "removeLabels" to listOf("Undergrad", "Intern"),
                      "matchProperties" to mapOf("name" to "joe", "surname" to "doe"),
                      "setProperties" to mapOf("name" to "john"),
                  )
          ),
      )
    }

    private fun dynamicLabelsQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (n:${'$'}(_e.matchLabels) {`name`: _e.matchProperties.`name`, `surname`: _e.matchProperties.`surname`}) " +
              "SET n += _e.setProperties SET n:${'$'}(_e.addLabels) REMOVE n:${'$'}(_e.removeLabels)",
          mapOf(
              "e" to
                  mapOf(
                      "matchLabels" to listOf("Person"),
                      "addLabels" to listOf("Employee"),
                      "removeLabels" to listOf("Undergrad", "Intern"),
                      "matchProperties" to mapOf("name" to "joe", "surname" to "doe"),
                      "setProperties" to mapOf("name" to "john"),
                  )
          ),
      )
    }

    override fun provideArguments(
        parameters: ParameterDeclarations?,
        context: ExtensionContext?,
    ): Stream<out Arguments?>? {
      return Stream.of(
          Arguments.of(neo4j4_4, standardCypherQuery()),
          Arguments.of(neo4j5_26, setRemoveDynamicLabelsQuery()),
          Arguments.of(neo4j2025_11, setRemoveDynamicLabelsQuery()),
          Arguments.of(neo4j2026_1, dynamicLabelsQuery()),
          Arguments.of(neo4jAura, dynamicLabelsQuery()),
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(MergeNodeParams::class)
  fun `should build node merge statements`(neo4j: Neo4j, expectedQuery: Query) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val data =
        MergeNodeSinkAction(
            matchLabels = setOf("Person"),
            matchProperties = mapOf("name" to "joe", "surname" to "doe"),
            setProperties = mapOf("name" to "john"),
            addLabels = setOf("Employee"),
            removeLabels = setOf("Undergrad", "Intern"),
        )

    generator.buildStatement(data) shouldBe expectedQuery
  }

  object MergeNodeParams : ArgumentsProvider {
    private fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MERGE (n:`Person` {`name`: _e.matchProperties.`name`, `surname`: _e.matchProperties.`surname`}) " +
              "SET n += _e.setProperties SET n:`Employee` REMOVE n:`Intern`:`Undergrad`",
          mapOf(
              "e" to
                  mapOf(
                      "matchProperties" to mapOf("name" to "joe", "surname" to "doe"),
                      "setProperties" to mapOf("name" to "john"),
                  )
          ),
      )
    }

    private fun setRemoveDynamicLabelsQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MERGE (n:`Person` {`name`: _e.matchProperties.`name`, `surname`: _e.matchProperties.`surname`}) " +
              "SET n += _e.setProperties SET n:${'$'}(_e.addLabels) REMOVE n:${'$'}(_e.removeLabels)",
          mapOf(
              "e" to
                  mapOf(
                      "addLabels" to listOf("Employee"),
                      "removeLabels" to listOf("Undergrad", "Intern"),
                      "matchProperties" to mapOf("name" to "joe", "surname" to "doe"),
                      "setProperties" to mapOf("name" to "john"),
                  )
          ),
      )
    }

    private fun dynamicLabelsQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MERGE (n:${'$'}(_e.matchLabels) {`name`: _e.matchProperties.`name`, `surname`: _e.matchProperties.`surname`}) " +
              "SET n += _e.setProperties SET n:${'$'}(_e.addLabels) REMOVE n:${'$'}(_e.removeLabels)",
          mapOf(
              "e" to
                  mapOf(
                      "matchLabels" to listOf("Person"),
                      "addLabels" to listOf("Employee"),
                      "removeLabels" to listOf("Undergrad", "Intern"),
                      "matchProperties" to mapOf("name" to "joe", "surname" to "doe"),
                      "setProperties" to mapOf("name" to "john"),
                  )
          ),
      )
    }

    override fun provideArguments(
        parameters: ParameterDeclarations?,
        context: ExtensionContext?,
    ): Stream<out Arguments?>? {
      return Stream.of(
          Arguments.of(neo4j4_4, standardCypherQuery()),
          Arguments.of(neo4j5_26, setRemoveDynamicLabelsQuery()),
          Arguments.of(neo4j2025_11, setRemoveDynamicLabelsQuery()),
          Arguments.of(neo4j2026_1, dynamicLabelsQuery()),
          Arguments.of(neo4jAura, dynamicLabelsQuery()),
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(DeleteNodeParams::class)
  fun `should build node delete statements`(neo4j: Neo4j, expectedQuery: Query) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val data =
        DeleteNodeSinkAction(
            matchLabels = setOf("Person"),
            matchProperties = mapOf("name" to "joe", "surname" to "doe"),
        )

    generator.buildStatement(data) shouldBe expectedQuery
  }

  object DeleteNodeParams : ArgumentsProvider {
    private fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (n:`Person` {`name`: _e.matchProperties.`name`, `surname`: _e.matchProperties.`surname`}) " +
              "DELETE n",
          mapOf("e" to mapOf("matchProperties" to mapOf("name" to "joe", "surname" to "doe"))),
      )
    }

    private fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (n:${'$'}(_e.matchLabels) {`name`: _e.matchProperties.`name`, `surname`: _e.matchProperties.`surname`}) " +
              "DELETE n",
          mapOf(
              "e" to
                  mapOf(
                      "matchLabels" to listOf("Person"),
                      "matchProperties" to mapOf("name" to "joe", "surname" to "doe"),
                  )
          ),
      )
    }

    override fun provideArguments(
        parameters: ParameterDeclarations?,
        context: ExtensionContext?,
    ): Stream<out Arguments?>? {
      return Stream.of(
          Arguments.of(neo4j4_4, standardCypherQuery()),
          Arguments.of(neo4j5_26, standardCypherQuery()),
          Arguments.of(neo4j2025_11, standardCypherQuery()),
          Arguments.of(neo4j2026_1, dynamicLabelsCypherQuery()),
          Arguments.of(neo4jAura, dynamicLabelsCypherQuery()),
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(CreateRelationshipWithMatchNodesWithoutKeysParams::class)
  fun `should build relationship create statements with matching nodes without keys`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val withKeys =
        CreateRelationshipSinkAction(
            SinkActionNodeReference(
                setOf("Person", "Employee"),
                mapOf("id" to 1),
                LookupMode.MATCH,
            ),
            SinkActionNodeReference(
                setOf("Company", "Corporation"),
                mapOf("id" to 7),
                LookupMode.MATCH,
            ),
            "WORKS_AT",
            mapOf("role" to "dev"),
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
  }

  object CreateRelationshipWithMatchNodesWithoutKeysParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start:`Employee`:`Person` {`id`: _e.start.matchProperties.`id`}) " +
              "MATCH (end:`Company`:`Corporation` {`id`: _e.end.matchProperties.`id`}) " +
              "CREATE (start)-[r:`WORKS_AT`]->(end) " +
              "SET r += _e.properties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to mapOf("matchProperties" to mapOf("id" to 1)),
                      "end" to mapOf("matchProperties" to mapOf("id" to 7)),
                      "properties" to mapOf("role" to "dev"),
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start:${'$'}(_e.start.matchLabels) {`id`: _e.start.matchProperties.`id`}) " +
              "MATCH (end:${'$'}(_e.end.matchLabels) {`id`: _e.end.matchProperties.`id`}) " +
              "CREATE (start)-[r:${'$'}(_e.type)]->(end) " +
              "SET r += _e.properties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to
                          mapOf(
                              "matchLabels" to listOf("Person", "Employee"),
                              "matchProperties" to mapOf("id" to 1),
                          ),
                      "end" to
                          mapOf(
                              "matchLabels" to listOf("Company", "Corporation"),
                              "matchProperties" to mapOf("id" to 7),
                          ),
                      "type" to "WORKS_AT",
                      "properties" to mapOf("role" to "dev"),
                  )
          ),
      )
    }

    override fun provideArguments(
        parameters: ParameterDeclarations?,
        context: ExtensionContext?,
    ): Stream<out Arguments?>? {
      return Stream.of(
          Arguments.of(neo4j4_4, standardCypherQuery()),
          Arguments.of(neo4j5_26, standardCypherQuery()),
          Arguments.of(neo4j2025_11, standardCypherQuery()),
          Arguments.of(neo4j2026_1, dynamicLabelsCypherQuery()),
          Arguments.of(neo4jAura, dynamicLabelsCypherQuery()),
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(CreateRelationshipWithMergeNodesWithoutKeysParams::class)
  fun `should build relationship create statements with merging nodes without keys`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val withKeys =
        CreateRelationshipSinkAction(
            SinkActionNodeReference(
                setOf("Person", "Employee"),
                mapOf("id" to 1),
                LookupMode.MERGE,
            ),
            SinkActionNodeReference(
                setOf("Company", "Corporation"),
                mapOf("id" to 7),
                LookupMode.MERGE,
            ),
            "WORKS_AT",
            mapOf("role" to "dev"),
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
  }

  object CreateRelationshipWithMergeNodesWithoutKeysParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MERGE (start:`Employee`:`Person` {`id`: _e.start.matchProperties.`id`}) " +
              "MERGE (end:`Company`:`Corporation` {`id`: _e.end.matchProperties.`id`}) " +
              "CREATE (start)-[r:`WORKS_AT`]->(end) " +
              "SET r += _e.properties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to mapOf("matchProperties" to mapOf("id" to 1)),
                      "end" to mapOf("matchProperties" to mapOf("id" to 7)),
                      "properties" to mapOf("role" to "dev"),
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MERGE (start:${'$'}(_e.start.matchLabels) {`id`: _e.start.matchProperties.`id`}) " +
              "MERGE (end:${'$'}(_e.end.matchLabels) {`id`: _e.end.matchProperties.`id`}) " +
              "CREATE (start)-[r:${'$'}(_e.type)]->(end) " +
              "SET r += _e.properties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to
                          mapOf(
                              "matchLabels" to listOf("Person", "Employee"),
                              "matchProperties" to mapOf("id" to 1),
                          ),
                      "end" to
                          mapOf(
                              "matchLabels" to listOf("Company", "Corporation"),
                              "matchProperties" to mapOf("id" to 7),
                          ),
                      "type" to "WORKS_AT",
                      "properties" to mapOf("role" to "dev"),
                  )
          ),
      )
    }

    override fun provideArguments(
        parameters: ParameterDeclarations?,
        context: ExtensionContext?,
    ): Stream<out Arguments?>? {
      return Stream.of(
          Arguments.of(neo4j4_4, standardCypherQuery()),
          Arguments.of(neo4j5_26, standardCypherQuery()),
          Arguments.of(neo4j2025_11, standardCypherQuery()),
          Arguments.of(neo4j2026_1, dynamicLabelsCypherQuery()),
          Arguments.of(neo4jAura, dynamicLabelsCypherQuery()),
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(CreateRelationshipWithMatchNodesWithKeysParams::class)
  fun `should build relationship create statements with matching nodes with keys`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val withKeys =
        CreateRelationshipSinkAction(
            SinkActionNodeReference(
                setOf("Person", "Employee"),
                mapOf("id" to 1),
                LookupMode.MATCH,
            ),
            SinkActionNodeReference(
                setOf("Company", "Corporation"),
                mapOf("id" to 7),
                LookupMode.MATCH,
            ),
            "WORKS_AT",
            mapOf("empId" to 5, "role" to "dev"),
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
  }

  object CreateRelationshipWithMatchNodesWithKeysParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start:`Employee`:`Person` {`id`: _e.start.matchProperties.`id`}) " +
              "MATCH (end:`Company`:`Corporation` {`id`: _e.end.matchProperties.`id`}) " +
              "CREATE (start)-[r:`WORKS_AT`]->(end) " +
              "SET r += _e.properties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to mapOf("matchProperties" to mapOf("id" to 1)),
                      "end" to mapOf("matchProperties" to mapOf("id" to 7)),
                      "properties" to mapOf("empId" to 5, "role" to "dev"),
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start:${'$'}(_e.start.matchLabels) {`id`: _e.start.matchProperties.`id`}) " +
              "MATCH (end:${'$'}(_e.end.matchLabels) {`id`: _e.end.matchProperties.`id`}) " +
              "CREATE (start)-[r:${'$'}(_e.type)]->(end) " +
              "SET r += _e.properties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to
                          mapOf(
                              "matchLabels" to listOf("Person", "Employee"),
                              "matchProperties" to mapOf("id" to 1),
                          ),
                      "end" to
                          mapOf(
                              "matchLabels" to listOf("Company", "Corporation"),
                              "matchProperties" to mapOf("id" to 7),
                          ),
                      "type" to "WORKS_AT",
                      "properties" to mapOf("empId" to 5, "role" to "dev"),
                  )
          ),
      )
    }

    override fun provideArguments(
        parameters: ParameterDeclarations?,
        context: ExtensionContext?,
    ): Stream<out Arguments?>? {
      return Stream.of(
          Arguments.of(neo4j4_4, standardCypherQuery()),
          Arguments.of(neo4j5_26, standardCypherQuery()),
          Arguments.of(neo4j2025_11, standardCypherQuery()),
          Arguments.of(neo4j2026_1, dynamicLabelsCypherQuery()),
          Arguments.of(neo4jAura, dynamicLabelsCypherQuery()),
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(CreateRelationshipWithMergeNodesWithKeysParams::class)
  fun `should build relationship create statements with merging nodes with keys`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val withKeys =
        CreateRelationshipSinkAction(
            SinkActionNodeReference(
                setOf("Person", "Employee"),
                mapOf("id" to 1),
                LookupMode.MERGE,
            ),
            SinkActionNodeReference(
                setOf("Company", "Corporation"),
                mapOf("id" to 7),
                LookupMode.MERGE,
            ),
            "WORKS_AT",
            mapOf("empId" to 5, "role" to "dev"),
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
  }

  object CreateRelationshipWithMergeNodesWithKeysParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MERGE (start:`Employee`:`Person` {`id`: _e.start.matchProperties.`id`}) " +
              "MERGE (end:`Company`:`Corporation` {`id`: _e.end.matchProperties.`id`}) " +
              "CREATE (start)-[r:`WORKS_AT`]->(end) " +
              "SET r += _e.properties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to mapOf("matchProperties" to mapOf("id" to 1)),
                      "end" to mapOf("matchProperties" to mapOf("id" to 7)),
                      "properties" to mapOf("empId" to 5, "role" to "dev"),
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MERGE (start:${'$'}(_e.start.matchLabels) {`id`: _e.start.matchProperties.`id`}) " +
              "MERGE (end:${'$'}(_e.end.matchLabels) {`id`: _e.end.matchProperties.`id`}) " +
              "CREATE (start)-[r:${'$'}(_e.type)]->(end) " +
              "SET r += _e.properties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to
                          mapOf(
                              "matchLabels" to listOf("Person", "Employee"),
                              "matchProperties" to mapOf("id" to 1),
                          ),
                      "end" to
                          mapOf(
                              "matchLabels" to listOf("Company", "Corporation"),
                              "matchProperties" to mapOf("id" to 7),
                          ),
                      "type" to "WORKS_AT",
                      "properties" to mapOf("empId" to 5, "role" to "dev"),
                  )
          ),
      )
    }

    override fun provideArguments(
        parameters: ParameterDeclarations?,
        context: ExtensionContext?,
    ): Stream<out Arguments?>? {
      return Stream.of(
          Arguments.of(neo4j4_4, standardCypherQuery()),
          Arguments.of(neo4j5_26, standardCypherQuery()),
          Arguments.of(neo4j2025_11, standardCypherQuery()),
          Arguments.of(neo4j2026_1, dynamicLabelsCypherQuery()),
          Arguments.of(neo4jAura, dynamicLabelsCypherQuery()),
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(UpdateRelationshipWithMatchNodesWithoutKeysParams::class)
  fun `should build relationship update statements with matching nodes without keys`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val withKeys =
        UpdateRelationshipSinkAction(
            SinkActionNodeReference(
                setOf("Person", "Employee"),
                mapOf("id" to 1),
                LookupMode.MATCH,
            ),
            SinkActionNodeReference(
                setOf("Company", "Corporation"),
                mapOf("id" to 7),
                LookupMode.MATCH,
            ),
            matchType = "WORKS_AT",
            matchProperties = mapOf("role" to "dev"),
            hasKeys = false,
            setProperties = mapOf("senior" to true),
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
  }

  object UpdateRelationshipWithMatchNodesWithoutKeysParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start:`Employee`:`Person` {`id`: _e.start.matchProperties.`id`}) " +
              "MATCH (end:`Company`:`Corporation` {`id`: _e.end.matchProperties.`id`}) " +
              "MATCH (start)-[r:`WORKS_AT` {`role`: _e.matchProperties.`role`}]->(end) " +
              "WITH _e, r LIMIT 1 " +
              "SET r += _e.setProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to mapOf("matchProperties" to mapOf("id" to 1)),
                      "end" to mapOf("matchProperties" to mapOf("id" to 7)),
                      "matchProperties" to mapOf("role" to "dev"),
                      "setProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start:${'$'}(_e.start.matchLabels) {`id`: _e.start.matchProperties.`id`}) " +
              "MATCH (end:${'$'}(_e.end.matchLabels) {`id`: _e.end.matchProperties.`id`}) " +
              "MATCH (start)-[r:${'$'}(_e.matchType) {`role`: _e.matchProperties.`role`}]->(end) " +
              "WITH _e, r LIMIT 1 " +
              "SET r += _e.setProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to
                          mapOf(
                              "matchLabels" to listOf("Person", "Employee"),
                              "matchProperties" to mapOf("id" to 1),
                          ),
                      "end" to
                          mapOf(
                              "matchLabels" to listOf("Company", "Corporation"),
                              "matchProperties" to mapOf("id" to 7),
                          ),
                      "matchType" to "WORKS_AT",
                      "matchProperties" to mapOf("role" to "dev"),
                      "setProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    override fun provideArguments(
        parameters: ParameterDeclarations?,
        context: ExtensionContext?,
    ): Stream<out Arguments?>? {
      return Stream.of(
          Arguments.of(neo4j4_4, standardCypherQuery()),
          Arguments.of(neo4j5_26, standardCypherQuery()),
          Arguments.of(neo4j2025_11, standardCypherQuery()),
          Arguments.of(neo4j2026_1, dynamicLabelsCypherQuery()),
          Arguments.of(neo4jAura, dynamicLabelsCypherQuery()),
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(UpdateRelationshipWithMergeNodesWithoutKeysParams::class)
  fun `should build relationship update statements with merging nodes without keys`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val withKeys =
        UpdateRelationshipSinkAction(
            SinkActionNodeReference(
                setOf("Person", "Employee"),
                mapOf("id" to 1),
                LookupMode.MERGE,
            ),
            SinkActionNodeReference(
                setOf("Company", "Corporation"),
                mapOf("id" to 7),
                LookupMode.MERGE,
            ),
            matchType = "WORKS_AT",
            matchProperties = mapOf("role" to "dev"),
            hasKeys = false,
            setProperties = mapOf("senior" to true),
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
  }

  object UpdateRelationshipWithMergeNodesWithoutKeysParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MERGE (start:`Employee`:`Person` {`id`: _e.start.matchProperties.`id`}) " +
              "MERGE (end:`Company`:`Corporation` {`id`: _e.end.matchProperties.`id`}) " +
              "MATCH (start)-[r:`WORKS_AT` {`role`: _e.matchProperties.`role`}]->(end) " +
              "WITH _e, r LIMIT 1 " +
              "SET r += _e.setProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to mapOf("matchProperties" to mapOf("id" to 1)),
                      "end" to mapOf("matchProperties" to mapOf("id" to 7)),
                      "matchProperties" to mapOf("role" to "dev"),
                      "setProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MERGE (start:${'$'}(_e.start.matchLabels) {`id`: _e.start.matchProperties.`id`}) " +
              "MERGE (end:${'$'}(_e.end.matchLabels) {`id`: _e.end.matchProperties.`id`}) " +
              "MATCH (start)-[r:${'$'}(_e.matchType) {`role`: _e.matchProperties.`role`}]->(end) " +
              "WITH _e, r LIMIT 1 " +
              "SET r += _e.setProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to
                          mapOf(
                              "matchLabels" to listOf("Person", "Employee"),
                              "matchProperties" to mapOf("id" to 1),
                          ),
                      "end" to
                          mapOf(
                              "matchLabels" to listOf("Company", "Corporation"),
                              "matchProperties" to mapOf("id" to 7),
                          ),
                      "matchType" to "WORKS_AT",
                      "matchProperties" to mapOf("role" to "dev"),
                      "setProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    override fun provideArguments(
        parameters: ParameterDeclarations?,
        context: ExtensionContext?,
    ): Stream<out Arguments?>? {
      return Stream.of(
          Arguments.of(neo4j4_4, standardCypherQuery()),
          Arguments.of(neo4j5_26, standardCypherQuery()),
          Arguments.of(neo4j2025_11, standardCypherQuery()),
          Arguments.of(neo4j2026_1, dynamicLabelsCypherQuery()),
          Arguments.of(neo4jAura, dynamicLabelsCypherQuery()),
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(UpdateRelationshipWithMatchNodesWithKeysParams::class)
  fun `should build relationship update statements with matching nodes with keys`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val withKeys =
        UpdateRelationshipSinkAction(
            SinkActionNodeReference(
                setOf("Person", "Employee"),
                mapOf("id" to 1),
                LookupMode.MATCH,
            ),
            SinkActionNodeReference(
                setOf("Company", "Corporation"),
                mapOf("id" to 7),
                LookupMode.MATCH,
            ),
            matchType = "WORKS_AT",
            matchProperties = mapOf("empId" to 5),
            hasKeys = true,
            setProperties = mapOf("senior" to true),
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
  }

  object UpdateRelationshipWithMatchNodesWithKeysParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start:`Employee`:`Person` {`id`: _e.start.matchProperties.`id`}) " +
              "MATCH (end:`Company`:`Corporation` {`id`: _e.end.matchProperties.`id`}) " +
              "MATCH (start)-[r:`WORKS_AT` {`empId`: _e.matchProperties.`empId`}]->(end) " +
              "SET r += _e.setProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to mapOf("matchProperties" to mapOf("id" to 1)),
                      "end" to mapOf("matchProperties" to mapOf("id" to 7)),
                      "matchProperties" to mapOf("empId" to 5),
                      "setProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start:${'$'}(_e.start.matchLabels) {`id`: _e.start.matchProperties.`id`}) " +
              "MATCH (end:${'$'}(_e.end.matchLabels) {`id`: _e.end.matchProperties.`id`}) " +
              "MATCH (start)-[r:${'$'}(_e.matchType) {`empId`: _e.matchProperties.`empId`}]->(end) " +
              "SET r += _e.setProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to
                          mapOf(
                              "matchLabels" to listOf("Person", "Employee"),
                              "matchProperties" to mapOf("id" to 1),
                          ),
                      "end" to
                          mapOf(
                              "matchLabels" to listOf("Company", "Corporation"),
                              "matchProperties" to mapOf("id" to 7),
                          ),
                      "matchType" to "WORKS_AT",
                      "matchProperties" to mapOf("empId" to 5),
                      "setProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    override fun provideArguments(
        parameters: ParameterDeclarations?,
        context: ExtensionContext?,
    ): Stream<out Arguments?>? {
      return Stream.of(
          Arguments.of(neo4j4_4, standardCypherQuery()),
          Arguments.of(neo4j5_26, standardCypherQuery()),
          Arguments.of(neo4j2025_11, standardCypherQuery()),
          Arguments.of(neo4j2026_1, dynamicLabelsCypherQuery()),
          Arguments.of(neo4jAura, dynamicLabelsCypherQuery()),
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(UpdateRelationshipWithMergeNodesWithKeysParams::class)
  fun `should build relationship update statements with merging nodes with keys`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val withKeys =
        UpdateRelationshipSinkAction(
            SinkActionNodeReference(
                setOf("Person", "Employee"),
                mapOf("id" to 1),
                LookupMode.MERGE,
            ),
            SinkActionNodeReference(
                setOf("Company", "Corporation"),
                mapOf("id" to 7),
                LookupMode.MERGE,
            ),
            matchType = "WORKS_AT",
            matchProperties = mapOf("empId" to 5),
            hasKeys = true,
            setProperties = mapOf("senior" to true),
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
  }

  object UpdateRelationshipWithMergeNodesWithKeysParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MERGE (start:`Employee`:`Person` {`id`: _e.start.matchProperties.`id`}) " +
              "MERGE (end:`Company`:`Corporation` {`id`: _e.end.matchProperties.`id`}) " +
              "MATCH (start)-[r:`WORKS_AT` {`empId`: _e.matchProperties.`empId`}]->(end) " +
              "SET r += _e.setProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to mapOf("matchProperties" to mapOf("id" to 1)),
                      "end" to mapOf("matchProperties" to mapOf("id" to 7)),
                      "matchProperties" to mapOf("empId" to 5),
                      "setProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MERGE (start:${'$'}(_e.start.matchLabels) {`id`: _e.start.matchProperties.`id`}) " +
              "MERGE (end:${'$'}(_e.end.matchLabels) {`id`: _e.end.matchProperties.`id`}) " +
              "MATCH (start)-[r:${'$'}(_e.matchType) {`empId`: _e.matchProperties.`empId`}]->(end) " +
              "SET r += _e.setProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to
                          mapOf(
                              "matchLabels" to listOf("Person", "Employee"),
                              "matchProperties" to mapOf("id" to 1),
                          ),
                      "end" to
                          mapOf(
                              "matchLabels" to listOf("Company", "Corporation"),
                              "matchProperties" to mapOf("id" to 7),
                          ),
                      "matchType" to "WORKS_AT",
                      "matchProperties" to mapOf("empId" to 5),
                      "setProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    override fun provideArguments(
        parameters: ParameterDeclarations?,
        context: ExtensionContext?,
    ): Stream<out Arguments?>? {
      return Stream.of(
          Arguments.of(neo4j4_4, standardCypherQuery()),
          Arguments.of(neo4j5_26, standardCypherQuery()),
          Arguments.of(neo4j2025_11, standardCypherQuery()),
          Arguments.of(neo4j2026_1, dynamicLabelsCypherQuery()),
          Arguments.of(neo4jAura, dynamicLabelsCypherQuery()),
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(UpdateRelationshipMatchingNodesWithKeysWithoutStartNodeKeysParams::class)
  fun `should build relationship update statements with keys and without start node keys matching nodes`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val withKeys =
        UpdateRelationshipSinkAction(
            SinkActionNodeReference(emptySet(), emptyMap(), LookupMode.MATCH),
            SinkActionNodeReference(
                setOf("Company", "Corporation"),
                mapOf("id" to 7),
                LookupMode.MATCH,
            ),
            matchType = "WORKS_AT",
            matchProperties = mapOf("empId" to 5),
            hasKeys = true,
            setProperties = mapOf("senior" to true),
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
  }

  object UpdateRelationshipMatchingNodesWithKeysWithoutStartNodeKeysParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e " +
              "MATCH (end:`Company`:`Corporation` {`id`: _e.end.matchProperties.`id`}) " +
              "MATCH (start)-[r:`WORKS_AT` {`empId`: _e.matchProperties.`empId`}]->(end) " +
              "SET r += _e.setProperties",
          mapOf(
              "e" to
                  mapOf(
                      "end" to mapOf("matchProperties" to mapOf("id" to 7)),
                      "matchProperties" to mapOf("empId" to 5),
                      "setProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e " +
              "MATCH (end:${'$'}(_e.end.matchLabels) {`id`: _e.end.matchProperties.`id`}) " +
              "MATCH (start)-[r:${'$'}(_e.matchType) {`empId`: _e.matchProperties.`empId`}]->(end) " +
              "SET r += _e.setProperties",
          mapOf(
              "e" to
                  mapOf(
                      "end" to
                          mapOf(
                              "matchLabels" to listOf("Company", "Corporation"),
                              "matchProperties" to mapOf("id" to 7),
                          ),
                      "matchType" to "WORKS_AT",
                      "matchProperties" to mapOf("empId" to 5),
                      "setProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    override fun provideArguments(
        parameters: ParameterDeclarations?,
        context: ExtensionContext?,
    ): Stream<out Arguments?>? {
      return Stream.of(
          Arguments.of(neo4j4_4, standardCypherQuery()),
          Arguments.of(neo4j5_26, standardCypherQuery()),
          Arguments.of(neo4j2025_11, standardCypherQuery()),
          Arguments.of(neo4j2026_1, dynamicLabelsCypherQuery()),
          Arguments.of(neo4jAura, dynamicLabelsCypherQuery()),
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(UpdateRelationshipMergingNodesWithKeysWithoutStartNodeKeysParams::class)
  fun `should build relationship update statements with keys and without start node keys merging nodes`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val withKeys =
        UpdateRelationshipSinkAction(
            SinkActionNodeReference(emptySet(), emptyMap(), LookupMode.MERGE),
            SinkActionNodeReference(
                setOf("Company", "Corporation"),
                mapOf("id" to 7),
                LookupMode.MERGE,
            ),
            matchType = "WORKS_AT",
            matchProperties = mapOf("empId" to 5),
            hasKeys = true,
            setProperties = mapOf("senior" to true),
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
  }

  object UpdateRelationshipMergingNodesWithKeysWithoutStartNodeKeysParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e " +
              "MERGE (end:`Company`:`Corporation` {`id`: _e.end.matchProperties.`id`}) " +
              "MATCH (start)-[r:`WORKS_AT` {`empId`: _e.matchProperties.`empId`}]->(end) " +
              "SET r += _e.setProperties",
          mapOf(
              "e" to
                  mapOf(
                      "end" to mapOf("matchProperties" to mapOf("id" to 7)),
                      "matchProperties" to mapOf("empId" to 5),
                      "setProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e " +
              "MERGE (end:${'$'}(_e.end.matchLabels) {`id`: _e.end.matchProperties.`id`}) " +
              "MATCH (start)-[r:${'$'}(_e.matchType) {`empId`: _e.matchProperties.`empId`}]->(end) " +
              "SET r += _e.setProperties",
          mapOf(
              "e" to
                  mapOf(
                      "end" to
                          mapOf(
                              "matchLabels" to listOf("Company", "Corporation"),
                              "matchProperties" to mapOf("id" to 7),
                          ),
                      "matchType" to "WORKS_AT",
                      "matchProperties" to mapOf("empId" to 5),
                      "setProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    override fun provideArguments(
        parameters: ParameterDeclarations?,
        context: ExtensionContext?,
    ): Stream<out Arguments?>? {
      return Stream.of(
          Arguments.of(neo4j4_4, standardCypherQuery()),
          Arguments.of(neo4j5_26, standardCypherQuery()),
          Arguments.of(neo4j2025_11, standardCypherQuery()),
          Arguments.of(neo4j2026_1, dynamicLabelsCypherQuery()),
          Arguments.of(neo4jAura, dynamicLabelsCypherQuery()),
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(UpdateRelationshipMatchingNodesWithKeysWithoutEndNodeKeysParams::class)
  fun `should build relationship update statements with keys and without end node keys matching nodes`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val withKeys =
        UpdateRelationshipSinkAction(
            SinkActionNodeReference(
                setOf("Employee", "Person"),
                mapOf("id" to 7),
                LookupMode.MATCH,
            ),
            SinkActionNodeReference(emptySet(), emptyMap(), LookupMode.MATCH),
            matchType = "WORKS_AT",
            matchProperties = mapOf("empId" to 5),
            hasKeys = true,
            setProperties = mapOf("senior" to true),
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
  }

  object UpdateRelationshipMatchingNodesWithKeysWithoutEndNodeKeysParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start:`Employee`:`Person` {`id`: _e.start.matchProperties.`id`}) " +
              "MATCH (start)-[r:`WORKS_AT` {`empId`: _e.matchProperties.`empId`}]->(end) " +
              "SET r += _e.setProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to mapOf("matchProperties" to mapOf("id" to 7)),
                      "matchProperties" to mapOf("empId" to 5),
                      "setProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start:${'$'}(_e.start.matchLabels) {`id`: _e.start.matchProperties.`id`}) " +
              "MATCH (start)-[r:${'$'}(_e.matchType) {`empId`: _e.matchProperties.`empId`}]->(end) " +
              "SET r += _e.setProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to
                          mapOf(
                              "matchLabels" to listOf("Employee", "Person"),
                              "matchProperties" to mapOf("id" to 7),
                          ),
                      "matchType" to "WORKS_AT",
                      "matchProperties" to mapOf("empId" to 5),
                      "setProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    override fun provideArguments(
        parameters: ParameterDeclarations?,
        context: ExtensionContext?,
    ): Stream<out Arguments?>? {
      return Stream.of(
          Arguments.of(neo4j4_4, standardCypherQuery()),
          Arguments.of(neo4j5_26, standardCypherQuery()),
          Arguments.of(neo4j2025_11, standardCypherQuery()),
          Arguments.of(neo4j2026_1, dynamicLabelsCypherQuery()),
          Arguments.of(neo4jAura, dynamicLabelsCypherQuery()),
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(UpdateRelationshipMergingNodesWithKeysWithoutEndNodeKeysParams::class)
  fun `should build relationship update statements with keys and without end node keys merging nodes`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val withKeys =
        UpdateRelationshipSinkAction(
            SinkActionNodeReference(
                setOf("Employee", "Person"),
                mapOf("id" to 7),
                LookupMode.MERGE,
            ),
            SinkActionNodeReference(emptySet(), emptyMap(), LookupMode.MERGE),
            matchType = "WORKS_AT",
            matchProperties = mapOf("empId" to 5),
            hasKeys = true,
            setProperties = mapOf("senior" to true),
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
  }

  object UpdateRelationshipMergingNodesWithKeysWithoutEndNodeKeysParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MERGE (start:`Employee`:`Person` {`id`: _e.start.matchProperties.`id`}) " +
              "MATCH (start)-[r:`WORKS_AT` {`empId`: _e.matchProperties.`empId`}]->(end) " +
              "SET r += _e.setProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to mapOf("matchProperties" to mapOf("id" to 7)),
                      "matchProperties" to mapOf("empId" to 5),
                      "setProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MERGE (start:${'$'}(_e.start.matchLabels) {`id`: _e.start.matchProperties.`id`}) " +
              "MATCH (start)-[r:${'$'}(_e.matchType) {`empId`: _e.matchProperties.`empId`}]->(end) " +
              "SET r += _e.setProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to
                          mapOf(
                              "matchLabels" to listOf("Employee", "Person"),
                              "matchProperties" to mapOf("id" to 7),
                          ),
                      "matchType" to "WORKS_AT",
                      "matchProperties" to mapOf("empId" to 5),
                      "setProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    override fun provideArguments(
        parameters: ParameterDeclarations?,
        context: ExtensionContext?,
    ): Stream<out Arguments?>? {
      return Stream.of(
          Arguments.of(neo4j4_4, standardCypherQuery()),
          Arguments.of(neo4j5_26, standardCypherQuery()),
          Arguments.of(neo4j2025_11, standardCypherQuery()),
          Arguments.of(neo4j2026_1, dynamicLabelsCypherQuery()),
          Arguments.of(neo4jAura, dynamicLabelsCypherQuery()),
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(UpdateRelationshipMatchingNodesWithKeysWithoutNodeKeysParams::class)
  fun `should build relationship update statements with keys and without node keys matching nodes`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val withKeys =
        UpdateRelationshipSinkAction(
            SinkActionNodeReference(emptySet(), emptyMap(), LookupMode.MATCH),
            SinkActionNodeReference(emptySet(), emptyMap(), LookupMode.MATCH),
            matchType = "WORKS_AT",
            matchProperties = mapOf("empId" to 5),
            hasKeys = true,
            setProperties = mapOf("senior" to true),
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
  }

  object UpdateRelationshipMatchingNodesWithKeysWithoutNodeKeysParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start)-[r:`WORKS_AT` {`empId`: _e.matchProperties.`empId`}]->(end) " +
              "SET r += _e.setProperties",
          mapOf(
              "e" to
                  mapOf(
                      "matchProperties" to mapOf("empId" to 5),
                      "setProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start)-[r:${'$'}(_e.matchType) {`empId`: _e.matchProperties.`empId`}]->(end) " +
              "SET r += _e.setProperties",
          mapOf(
              "e" to
                  mapOf(
                      "matchType" to "WORKS_AT",
                      "matchProperties" to mapOf("empId" to 5),
                      "setProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    override fun provideArguments(
        parameters: ParameterDeclarations?,
        context: ExtensionContext?,
    ): Stream<out Arguments?>? {
      return Stream.of(
          Arguments.of(neo4j4_4, standardCypherQuery()),
          Arguments.of(neo4j5_26, standardCypherQuery()),
          Arguments.of(neo4j2025_11, standardCypherQuery()),
          Arguments.of(neo4j2026_1, dynamicLabelsCypherQuery()),
          Arguments.of(neo4jAura, dynamicLabelsCypherQuery()),
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(UpdateRelationshipMergingNodesWithKeysWithoutNodeKeysParams::class)
  fun `should build relationship update statements with keys and without node keys merging nodes`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val withKeys =
        UpdateRelationshipSinkAction(
            SinkActionNodeReference(emptySet(), emptyMap(), LookupMode.MERGE),
            SinkActionNodeReference(emptySet(), emptyMap(), LookupMode.MERGE),
            matchType = "WORKS_AT",
            matchProperties = mapOf("empId" to 5),
            hasKeys = true,
            setProperties = mapOf("senior" to true),
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
  }

  object UpdateRelationshipMergingNodesWithKeysWithoutNodeKeysParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start)-[r:`WORKS_AT` {`empId`: _e.matchProperties.`empId`}]->(end) " +
              "SET r += _e.setProperties",
          mapOf(
              "e" to
                  mapOf(
                      "matchProperties" to mapOf("empId" to 5),
                      "setProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start)-[r:${'$'}(_e.matchType) {`empId`: _e.matchProperties.`empId`}]->(end) " +
              "SET r += _e.setProperties",
          mapOf(
              "e" to
                  mapOf(
                      "matchType" to "WORKS_AT",
                      "matchProperties" to mapOf("empId" to 5),
                      "setProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    override fun provideArguments(
        parameters: ParameterDeclarations?,
        context: ExtensionContext?,
    ): Stream<out Arguments?>? {
      return Stream.of(
          Arguments.of(neo4j4_4, standardCypherQuery()),
          Arguments.of(neo4j5_26, standardCypherQuery()),
          Arguments.of(neo4j2025_11, standardCypherQuery()),
          Arguments.of(neo4j2026_1, dynamicLabelsCypherQuery()),
          Arguments.of(neo4jAura, dynamicLabelsCypherQuery()),
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(MergeRelationshipWithMatchNodesWithoutKeysParams::class)
  fun `should build relationship merge statements with matching nodes without keys`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val withKeys =
        MergeRelationshipSinkAction(
            SinkActionNodeReference(
                setOf("Person", "Employee"),
                mapOf("id" to 1),
                LookupMode.MATCH,
            ),
            SinkActionNodeReference(
                setOf("Company", "Corporation"),
                mapOf("id" to 7),
                LookupMode.MATCH,
            ),
            matchType = "WORKS_AT",
            matchProperties = mapOf("role" to "dev"),
            hasKeys = false,
            setProperties = mapOf("senior" to true),
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
  }

  object MergeRelationshipWithMatchNodesWithoutKeysParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start:`Employee`:`Person` {`id`: _e.start.matchProperties.`id`}) " +
              "MATCH (end:`Company`:`Corporation` {`id`: _e.end.matchProperties.`id`}) " +
              "MERGE (start)-[r:`WORKS_AT` {`role`: _e.matchProperties.`role`}]->(end) " +
              "WITH _e, r LIMIT 1 " +
              "SET r += _e.setProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to mapOf("matchProperties" to mapOf("id" to 1)),
                      "end" to mapOf("matchProperties" to mapOf("id" to 7)),
                      "matchProperties" to mapOf("role" to "dev"),
                      "setProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start:${'$'}(_e.start.matchLabels) {`id`: _e.start.matchProperties.`id`}) " +
              "MATCH (end:${'$'}(_e.end.matchLabels) {`id`: _e.end.matchProperties.`id`}) " +
              "MERGE (start)-[r:${'$'}(_e.matchType) {`role`: _e.matchProperties.`role`}]->(end) " +
              "WITH _e, r LIMIT 1 " +
              "SET r += _e.setProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to
                          mapOf(
                              "matchLabels" to listOf("Person", "Employee"),
                              "matchProperties" to mapOf("id" to 1),
                          ),
                      "end" to
                          mapOf(
                              "matchLabels" to listOf("Company", "Corporation"),
                              "matchProperties" to mapOf("id" to 7),
                          ),
                      "matchType" to "WORKS_AT",
                      "matchProperties" to mapOf("role" to "dev"),
                      "setProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    override fun provideArguments(
        parameters: ParameterDeclarations?,
        context: ExtensionContext?,
    ): Stream<out Arguments?>? {
      return Stream.of(
          Arguments.of(neo4j4_4, standardCypherQuery()),
          Arguments.of(neo4j5_26, standardCypherQuery()),
          Arguments.of(neo4j2025_11, standardCypherQuery()),
          Arguments.of(neo4j2026_1, dynamicLabelsCypherQuery()),
          Arguments.of(neo4jAura, dynamicLabelsCypherQuery()),
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(MergeRelationshipWithMergeNodesWithoutKeysParams::class)
  fun `should build relationship merge statements with merging nodes without keys`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val withKeys =
        MergeRelationshipSinkAction(
            SinkActionNodeReference(
                setOf("Person", "Employee"),
                mapOf("id" to 1),
                LookupMode.MERGE,
            ),
            SinkActionNodeReference(
                setOf("Company", "Corporation"),
                mapOf("id" to 7),
                LookupMode.MERGE,
            ),
            matchType = "WORKS_AT",
            matchProperties = mapOf("role" to "dev"),
            hasKeys = false,
            setProperties = mapOf("senior" to true),
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
  }

  object MergeRelationshipWithMergeNodesWithoutKeysParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MERGE (start:`Employee`:`Person` {`id`: _e.start.matchProperties.`id`}) " +
              "MERGE (end:`Company`:`Corporation` {`id`: _e.end.matchProperties.`id`}) " +
              "MERGE (start)-[r:`WORKS_AT` {`role`: _e.matchProperties.`role`}]->(end) " +
              "WITH _e, r LIMIT 1 " +
              "SET r += _e.setProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to mapOf("matchProperties" to mapOf("id" to 1)),
                      "end" to mapOf("matchProperties" to mapOf("id" to 7)),
                      "matchProperties" to mapOf("role" to "dev"),
                      "setProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MERGE (start:${'$'}(_e.start.matchLabels) {`id`: _e.start.matchProperties.`id`}) " +
              "MERGE (end:${'$'}(_e.end.matchLabels) {`id`: _e.end.matchProperties.`id`}) " +
              "MERGE (start)-[r:${'$'}(_e.matchType) {`role`: _e.matchProperties.`role`}]->(end) " +
              "WITH _e, r LIMIT 1 " +
              "SET r += _e.setProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to
                          mapOf(
                              "matchLabels" to listOf("Person", "Employee"),
                              "matchProperties" to mapOf("id" to 1),
                          ),
                      "end" to
                          mapOf(
                              "matchLabels" to listOf("Company", "Corporation"),
                              "matchProperties" to mapOf("id" to 7),
                          ),
                      "matchType" to "WORKS_AT",
                      "matchProperties" to mapOf("role" to "dev"),
                      "setProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    override fun provideArguments(
        parameters: ParameterDeclarations?,
        context: ExtensionContext?,
    ): Stream<out Arguments?>? {
      return Stream.of(
          Arguments.of(neo4j4_4, standardCypherQuery()),
          Arguments.of(neo4j5_26, standardCypherQuery()),
          Arguments.of(neo4j2025_11, standardCypherQuery()),
          Arguments.of(neo4j2026_1, dynamicLabelsCypherQuery()),
          Arguments.of(neo4jAura, dynamicLabelsCypherQuery()),
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(MergeRelationshipWithMatchNodesWithKeysParams::class)
  fun `should build relationship merge statements with matching nodes with keys`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val withKeys =
        MergeRelationshipSinkAction(
            SinkActionNodeReference(
                setOf("Person", "Employee"),
                mapOf("id" to 1),
                LookupMode.MATCH,
            ),
            SinkActionNodeReference(
                setOf("Company", "Corporation"),
                mapOf("id" to 7),
                LookupMode.MATCH,
            ),
            matchType = "WORKS_AT",
            matchProperties = mapOf("empId" to 5),
            hasKeys = true,
            setProperties = mapOf("senior" to true),
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
  }

  object MergeRelationshipWithMatchNodesWithKeysParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start:`Employee`:`Person` {`id`: _e.start.matchProperties.`id`}) " +
              "MATCH (end:`Company`:`Corporation` {`id`: _e.end.matchProperties.`id`}) " +
              "MERGE (start)-[r:`WORKS_AT` {`empId`: _e.matchProperties.`empId`}]->(end) " +
              "SET r += _e.setProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to mapOf("matchProperties" to mapOf("id" to 1)),
                      "end" to mapOf("matchProperties" to mapOf("id" to 7)),
                      "matchProperties" to mapOf("empId" to 5),
                      "setProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start:${'$'}(_e.start.matchLabels) {`id`: _e.start.matchProperties.`id`}) " +
              "MATCH (end:${'$'}(_e.end.matchLabels) {`id`: _e.end.matchProperties.`id`}) " +
              "MERGE (start)-[r:${'$'}(_e.matchType) {`empId`: _e.matchProperties.`empId`}]->(end) " +
              "SET r += _e.setProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to
                          mapOf(
                              "matchLabels" to listOf("Person", "Employee"),
                              "matchProperties" to mapOf("id" to 1),
                          ),
                      "end" to
                          mapOf(
                              "matchLabels" to listOf("Company", "Corporation"),
                              "matchProperties" to mapOf("id" to 7),
                          ),
                      "matchType" to "WORKS_AT",
                      "matchProperties" to mapOf("empId" to 5),
                      "setProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    override fun provideArguments(
        parameters: ParameterDeclarations?,
        context: ExtensionContext?,
    ): Stream<out Arguments?>? {
      return Stream.of(
          Arguments.of(neo4j4_4, standardCypherQuery()),
          Arguments.of(neo4j5_26, standardCypherQuery()),
          Arguments.of(neo4j2025_11, standardCypherQuery()),
          Arguments.of(neo4j2026_1, dynamicLabelsCypherQuery()),
          Arguments.of(neo4jAura, dynamicLabelsCypherQuery()),
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(MergeRelationshipWithMergeNodesWithKeysParams::class)
  fun `should build relationship merge statements with merging nodes with keys`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val withKeys =
        MergeRelationshipSinkAction(
            SinkActionNodeReference(
                setOf("Person", "Employee"),
                mapOf("id" to 1),
                LookupMode.MERGE,
            ),
            SinkActionNodeReference(
                setOf("Company", "Corporation"),
                mapOf("id" to 7),
                LookupMode.MERGE,
            ),
            matchType = "WORKS_AT",
            matchProperties = mapOf("empId" to 5),
            hasKeys = true,
            setProperties = mapOf("senior" to true),
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
  }

  object MergeRelationshipWithMergeNodesWithKeysParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MERGE (start:`Employee`:`Person` {`id`: _e.start.matchProperties.`id`}) " +
              "MERGE (end:`Company`:`Corporation` {`id`: _e.end.matchProperties.`id`}) " +
              "MERGE (start)-[r:`WORKS_AT` {`empId`: _e.matchProperties.`empId`}]->(end) " +
              "SET r += _e.setProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to mapOf("matchProperties" to mapOf("id" to 1)),
                      "end" to mapOf("matchProperties" to mapOf("id" to 7)),
                      "matchProperties" to mapOf("empId" to 5),
                      "setProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MERGE (start:${'$'}(_e.start.matchLabels) {`id`: _e.start.matchProperties.`id`}) " +
              "MERGE (end:${'$'}(_e.end.matchLabels) {`id`: _e.end.matchProperties.`id`}) " +
              "MERGE (start)-[r:${'$'}(_e.matchType) {`empId`: _e.matchProperties.`empId`}]->(end) " +
              "SET r += _e.setProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to
                          mapOf(
                              "matchLabels" to listOf("Person", "Employee"),
                              "matchProperties" to mapOf("id" to 1),
                          ),
                      "end" to
                          mapOf(
                              "matchLabels" to listOf("Company", "Corporation"),
                              "matchProperties" to mapOf("id" to 7),
                          ),
                      "matchType" to "WORKS_AT",
                      "matchProperties" to mapOf("empId" to 5),
                      "setProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    override fun provideArguments(
        parameters: ParameterDeclarations?,
        context: ExtensionContext?,
    ): Stream<out Arguments?>? {
      return Stream.of(
          Arguments.of(neo4j4_4, standardCypherQuery()),
          Arguments.of(neo4j5_26, standardCypherQuery()),
          Arguments.of(neo4j2025_11, standardCypherQuery()),
          Arguments.of(neo4j2026_1, dynamicLabelsCypherQuery()),
          Arguments.of(neo4jAura, dynamicLabelsCypherQuery()),
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(MergeRelationshipMatchingNodesWithKeysWithoutStartNodeKeysParams::class)
  fun `should build relationship merge statements with keys and without start node keys matching nodes`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val withKeys =
        MergeRelationshipSinkAction(
            SinkActionNodeReference(emptySet(), emptyMap(), LookupMode.MATCH),
            SinkActionNodeReference(
                setOf("Company", "Corporation"),
                mapOf("id" to 7),
                LookupMode.MATCH,
            ),
            matchType = "WORKS_AT",
            matchProperties = mapOf("empId" to 5),
            hasKeys = true,
            setProperties = mapOf("senior" to true),
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
  }

  object MergeRelationshipMatchingNodesWithKeysWithoutStartNodeKeysParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e " +
              "MATCH (end:`Company`:`Corporation` {`id`: _e.end.matchProperties.`id`}) " +
              "MERGE (start)-[r:`WORKS_AT` {`empId`: _e.matchProperties.`empId`}]->(end) " +
              "SET r += _e.setProperties",
          mapOf(
              "e" to
                  mapOf(
                      "end" to mapOf("matchProperties" to mapOf("id" to 7)),
                      "matchProperties" to mapOf("empId" to 5),
                      "setProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e " +
              "MATCH (end:${'$'}(_e.end.matchLabels) {`id`: _e.end.matchProperties.`id`}) " +
              "MERGE (start)-[r:${'$'}(_e.matchType) {`empId`: _e.matchProperties.`empId`}]->(end) " +
              "SET r += _e.setProperties",
          mapOf(
              "e" to
                  mapOf(
                      "end" to
                          mapOf(
                              "matchLabels" to listOf("Company", "Corporation"),
                              "matchProperties" to mapOf("id" to 7),
                          ),
                      "matchType" to "WORKS_AT",
                      "matchProperties" to mapOf("empId" to 5),
                      "setProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    override fun provideArguments(
        parameters: ParameterDeclarations?,
        context: ExtensionContext?,
    ): Stream<out Arguments?>? {
      return Stream.of(
          Arguments.of(neo4j4_4, standardCypherQuery()),
          Arguments.of(neo4j5_26, standardCypherQuery()),
          Arguments.of(neo4j2025_11, standardCypherQuery()),
          Arguments.of(neo4j2026_1, dynamicLabelsCypherQuery()),
          Arguments.of(neo4jAura, dynamicLabelsCypherQuery()),
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(MergeRelationshipMergingNodesWithKeysWithoutStartNodeKeysParams::class)
  fun `should build relationship merge statements with keys and without start node keys merging nodes`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val withKeys =
        MergeRelationshipSinkAction(
            SinkActionNodeReference(emptySet(), emptyMap(), LookupMode.MERGE),
            SinkActionNodeReference(
                setOf("Company", "Corporation"),
                mapOf("id" to 7),
                LookupMode.MERGE,
            ),
            matchType = "WORKS_AT",
            matchProperties = mapOf("empId" to 5),
            hasKeys = true,
            setProperties = mapOf("senior" to true),
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
  }

  object MergeRelationshipMergingNodesWithKeysWithoutStartNodeKeysParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e " +
              "MERGE (end:`Company`:`Corporation` {`id`: _e.end.matchProperties.`id`}) " +
              "MERGE (start)-[r:`WORKS_AT` {`empId`: _e.matchProperties.`empId`}]->(end) " +
              "SET r += _e.setProperties",
          mapOf(
              "e" to
                  mapOf(
                      "end" to mapOf("matchProperties" to mapOf("id" to 7)),
                      "matchProperties" to mapOf("empId" to 5),
                      "setProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e " +
              "MERGE (end:${'$'}(_e.end.matchLabels) {`id`: _e.end.matchProperties.`id`}) " +
              "MERGE (start)-[r:${'$'}(_e.matchType) {`empId`: _e.matchProperties.`empId`}]->(end) " +
              "SET r += _e.setProperties",
          mapOf(
              "e" to
                  mapOf(
                      "end" to
                          mapOf(
                              "matchLabels" to listOf("Company", "Corporation"),
                              "matchProperties" to mapOf("id" to 7),
                          ),
                      "matchType" to "WORKS_AT",
                      "matchProperties" to mapOf("empId" to 5),
                      "setProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    override fun provideArguments(
        parameters: ParameterDeclarations?,
        context: ExtensionContext?,
    ): Stream<out Arguments?>? {
      return Stream.of(
          Arguments.of(neo4j4_4, standardCypherQuery()),
          Arguments.of(neo4j5_26, standardCypherQuery()),
          Arguments.of(neo4j2025_11, standardCypherQuery()),
          Arguments.of(neo4j2026_1, dynamicLabelsCypherQuery()),
          Arguments.of(neo4jAura, dynamicLabelsCypherQuery()),
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(MergeRelationshipMatchingNodesWithKeysWithoutEndNodeKeysParams::class)
  fun `should build relationship merge statements with keys and without end node keys matching nodes`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val withKeys =
        MergeRelationshipSinkAction(
            SinkActionNodeReference(
                setOf("Employee", "Person"),
                mapOf("id" to 7),
                LookupMode.MATCH,
            ),
            SinkActionNodeReference(emptySet(), emptyMap(), LookupMode.MATCH),
            matchType = "WORKS_AT",
            matchProperties = mapOf("empId" to 5),
            hasKeys = true,
            setProperties = mapOf("senior" to true),
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
  }

  object MergeRelationshipMatchingNodesWithKeysWithoutEndNodeKeysParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start:`Employee`:`Person` {`id`: _e.start.matchProperties.`id`}) " +
              "MERGE (start)-[r:`WORKS_AT` {`empId`: _e.matchProperties.`empId`}]->(end) " +
              "SET r += _e.setProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to mapOf("matchProperties" to mapOf("id" to 7)),
                      "matchProperties" to mapOf("empId" to 5),
                      "setProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start:${'$'}(_e.start.matchLabels) {`id`: _e.start.matchProperties.`id`}) " +
              "MERGE (start)-[r:${'$'}(_e.matchType) {`empId`: _e.matchProperties.`empId`}]->(end) " +
              "SET r += _e.setProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to
                          mapOf(
                              "matchLabels" to listOf("Employee", "Person"),
                              "matchProperties" to mapOf("id" to 7),
                          ),
                      "matchType" to "WORKS_AT",
                      "matchProperties" to mapOf("empId" to 5),
                      "setProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    override fun provideArguments(
        parameters: ParameterDeclarations?,
        context: ExtensionContext?,
    ): Stream<out Arguments?>? {
      return Stream.of(
          Arguments.of(neo4j4_4, standardCypherQuery()),
          Arguments.of(neo4j5_26, standardCypherQuery()),
          Arguments.of(neo4j2025_11, standardCypherQuery()),
          Arguments.of(neo4j2026_1, dynamicLabelsCypherQuery()),
          Arguments.of(neo4jAura, dynamicLabelsCypherQuery()),
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(MergeRelationshipMergingNodesWithKeysWithoutEndNodeKeysParams::class)
  fun `should build relationship merge statements with keys and without end node keys merging nodes`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val withKeys =
        MergeRelationshipSinkAction(
            SinkActionNodeReference(
                setOf("Employee", "Person"),
                mapOf("id" to 7),
                LookupMode.MERGE,
            ),
            SinkActionNodeReference(emptySet(), emptyMap(), LookupMode.MERGE),
            matchType = "WORKS_AT",
            matchProperties = mapOf("empId" to 5),
            hasKeys = true,
            setProperties = mapOf("senior" to true),
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
  }

  object MergeRelationshipMergingNodesWithKeysWithoutEndNodeKeysParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MERGE (start:`Employee`:`Person` {`id`: _e.start.matchProperties.`id`}) " +
              "MERGE (start)-[r:`WORKS_AT` {`empId`: _e.matchProperties.`empId`}]->(end) " +
              "SET r += _e.setProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to mapOf("matchProperties" to mapOf("id" to 7)),
                      "matchProperties" to mapOf("empId" to 5),
                      "setProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MERGE (start:${'$'}(_e.start.matchLabels) {`id`: _e.start.matchProperties.`id`}) " +
              "MERGE (start)-[r:${'$'}(_e.matchType) {`empId`: _e.matchProperties.`empId`}]->(end) " +
              "SET r += _e.setProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to
                          mapOf(
                              "matchLabels" to listOf("Employee", "Person"),
                              "matchProperties" to mapOf("id" to 7),
                          ),
                      "matchType" to "WORKS_AT",
                      "matchProperties" to mapOf("empId" to 5),
                      "setProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    override fun provideArguments(
        parameters: ParameterDeclarations?,
        context: ExtensionContext?,
    ): Stream<out Arguments?>? {
      return Stream.of(
          Arguments.of(neo4j4_4, standardCypherQuery()),
          Arguments.of(neo4j5_26, standardCypherQuery()),
          Arguments.of(neo4j2025_11, standardCypherQuery()),
          Arguments.of(neo4j2026_1, dynamicLabelsCypherQuery()),
          Arguments.of(neo4jAura, dynamicLabelsCypherQuery()),
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(MergeRelationshipMatchingNodesWithKeysWithoutNodeKeysParams::class)
  fun `should build relationship merge statements with keys and without node keys matching nodes`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val withKeys =
        MergeRelationshipSinkAction(
            SinkActionNodeReference(emptySet(), emptyMap(), LookupMode.MATCH),
            SinkActionNodeReference(emptySet(), emptyMap(), LookupMode.MATCH),
            matchType = "WORKS_AT",
            matchProperties = mapOf("empId" to 5),
            hasKeys = true,
            setProperties = mapOf("senior" to true),
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
  }

  object MergeRelationshipMatchingNodesWithKeysWithoutNodeKeysParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MERGE (start)-[r:`WORKS_AT` {`empId`: _e.matchProperties.`empId`}]->(end) " +
              "SET r += _e.setProperties",
          mapOf(
              "e" to
                  mapOf(
                      "matchProperties" to mapOf("empId" to 5),
                      "setProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MERGE (start)-[r:${'$'}(_e.matchType) {`empId`: _e.matchProperties.`empId`}]->(end) " +
              "SET r += _e.setProperties",
          mapOf(
              "e" to
                  mapOf(
                      "matchType" to "WORKS_AT",
                      "matchProperties" to mapOf("empId" to 5),
                      "setProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    override fun provideArguments(
        parameters: ParameterDeclarations?,
        context: ExtensionContext?,
    ): Stream<out Arguments?>? {
      return Stream.of(
          Arguments.of(neo4j4_4, standardCypherQuery()),
          Arguments.of(neo4j5_26, standardCypherQuery()),
          Arguments.of(neo4j2025_11, standardCypherQuery()),
          Arguments.of(neo4j2026_1, dynamicLabelsCypherQuery()),
          Arguments.of(neo4jAura, dynamicLabelsCypherQuery()),
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(MergeRelationshipMergingNodesWithKeysWithoutNodeKeysParams::class)
  fun `should build relationship merge statements with keys and without node keys merging nodes`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val withKeys =
        MergeRelationshipSinkAction(
            SinkActionNodeReference(emptySet(), emptyMap(), LookupMode.MERGE),
            SinkActionNodeReference(emptySet(), emptyMap(), LookupMode.MERGE),
            matchType = "WORKS_AT",
            matchProperties = mapOf("empId" to 5),
            hasKeys = true,
            setProperties = mapOf("senior" to true),
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
  }

  object MergeRelationshipMergingNodesWithKeysWithoutNodeKeysParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MERGE (start)-[r:`WORKS_AT` {`empId`: _e.matchProperties.`empId`}]->(end) " +
              "SET r += _e.setProperties",
          mapOf(
              "e" to
                  mapOf(
                      "matchProperties" to mapOf("empId" to 5),
                      "setProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MERGE (start)-[r:${'$'}(_e.matchType) {`empId`: _e.matchProperties.`empId`}]->(end) " +
              "SET r += _e.setProperties",
          mapOf(
              "e" to
                  mapOf(
                      "matchType" to "WORKS_AT",
                      "matchProperties" to mapOf("empId" to 5),
                      "setProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    override fun provideArguments(
        parameters: ParameterDeclarations?,
        context: ExtensionContext?,
    ): Stream<out Arguments?>? {
      return Stream.of(
          Arguments.of(neo4j4_4, standardCypherQuery()),
          Arguments.of(neo4j5_26, standardCypherQuery()),
          Arguments.of(neo4j2025_11, standardCypherQuery()),
          Arguments.of(neo4j2026_1, dynamicLabelsCypherQuery()),
          Arguments.of(neo4jAura, dynamicLabelsCypherQuery()),
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(DeleteRelationshipMatchingNodesWithoutKeysParams::class)
  fun `should build relationship delete statements without keys matching nodes`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val withKeys =
        DeleteRelationshipSinkAction(
            SinkActionNodeReference(
                setOf("Person", "Employee"),
                mapOf("id" to 1),
                LookupMode.MATCH,
            ),
            SinkActionNodeReference(
                setOf("Company", "Corporation"),
                mapOf("id" to 7),
                LookupMode.MATCH,
            ),
            matchType = "WORKS_AT",
            matchProperties = mapOf("role" to "dev"),
            hasKeys = false,
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
  }

  object DeleteRelationshipMatchingNodesWithoutKeysParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start:`Employee`:`Person` {`id`: _e.start.matchProperties.`id`}) " +
              "MATCH (end:`Company`:`Corporation` {`id`: _e.end.matchProperties.`id`}) " +
              "MATCH (start)-[r:`WORKS_AT` {`role`: _e.matchProperties.`role`}]->(end) " +
              "WITH _e, r LIMIT 1 " +
              "DELETE r",
          mapOf(
              "e" to
                  mapOf(
                      "start" to mapOf("matchProperties" to mapOf("id" to 1)),
                      "end" to mapOf("matchProperties" to mapOf("id" to 7)),
                      "matchProperties" to mapOf("role" to "dev"),
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start:${'$'}(_e.start.matchLabels) {`id`: _e.start.matchProperties.`id`}) " +
              "MATCH (end:${'$'}(_e.end.matchLabels) {`id`: _e.end.matchProperties.`id`}) " +
              "MATCH (start)-[r:${'$'}(_e.matchType) {`role`: _e.matchProperties.`role`}]->(end) " +
              "WITH _e, r LIMIT 1 " +
              "DELETE r",
          mapOf(
              "e" to
                  mapOf(
                      "start" to
                          mapOf(
                              "matchLabels" to listOf("Person", "Employee"),
                              "matchProperties" to mapOf("id" to 1),
                          ),
                      "end" to
                          mapOf(
                              "matchLabels" to listOf("Company", "Corporation"),
                              "matchProperties" to mapOf("id" to 7),
                          ),
                      "matchType" to "WORKS_AT",
                      "matchProperties" to mapOf("role" to "dev"),
                  )
          ),
      )
    }

    override fun provideArguments(
        parameters: ParameterDeclarations?,
        context: ExtensionContext?,
    ): Stream<out Arguments?>? {
      return Stream.of(
          Arguments.of(neo4j4_4, standardCypherQuery()),
          Arguments.of(neo4j5_26, standardCypherQuery()),
          Arguments.of(neo4j2025_11, standardCypherQuery()),
          Arguments.of(neo4j2026_1, dynamicLabelsCypherQuery()),
          Arguments.of(neo4jAura, dynamicLabelsCypherQuery()),
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(DeleteRelationshipMergingNodesWithoutKeysParams::class)
  fun `should build relationship delete statements without keys merging nodes`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val withKeys =
        DeleteRelationshipSinkAction(
            SinkActionNodeReference(
                setOf("Person", "Employee"),
                mapOf("id" to 1),
                LookupMode.MERGE,
            ),
            SinkActionNodeReference(
                setOf("Company", "Corporation"),
                mapOf("id" to 7),
                LookupMode.MERGE,
            ),
            matchType = "WORKS_AT",
            matchProperties = mapOf("role" to "dev"),
            hasKeys = false,
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
  }

  object DeleteRelationshipMergingNodesWithoutKeysParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MERGE (start:`Employee`:`Person` {`id`: _e.start.matchProperties.`id`}) " +
              "MERGE (end:`Company`:`Corporation` {`id`: _e.end.matchProperties.`id`}) " +
              "MATCH (start)-[r:`WORKS_AT` {`role`: _e.matchProperties.`role`}]->(end) " +
              "WITH _e, r LIMIT 1 " +
              "DELETE r",
          mapOf(
              "e" to
                  mapOf(
                      "start" to mapOf("matchProperties" to mapOf("id" to 1)),
                      "end" to mapOf("matchProperties" to mapOf("id" to 7)),
                      "matchProperties" to mapOf("role" to "dev"),
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MERGE (start:${'$'}(_e.start.matchLabels) {`id`: _e.start.matchProperties.`id`}) " +
              "MERGE (end:${'$'}(_e.end.matchLabels) {`id`: _e.end.matchProperties.`id`}) " +
              "MATCH (start)-[r:${'$'}(_e.matchType) {`role`: _e.matchProperties.`role`}]->(end) " +
              "WITH _e, r LIMIT 1 " +
              "DELETE r",
          mapOf(
              "e" to
                  mapOf(
                      "start" to
                          mapOf(
                              "matchLabels" to listOf("Person", "Employee"),
                              "matchProperties" to mapOf("id" to 1),
                          ),
                      "end" to
                          mapOf(
                              "matchLabels" to listOf("Company", "Corporation"),
                              "matchProperties" to mapOf("id" to 7),
                          ),
                      "matchType" to "WORKS_AT",
                      "matchProperties" to mapOf("role" to "dev"),
                  )
          ),
      )
    }

    override fun provideArguments(
        parameters: ParameterDeclarations?,
        context: ExtensionContext?,
    ): Stream<out Arguments?>? {
      return Stream.of(
          Arguments.of(neo4j4_4, standardCypherQuery()),
          Arguments.of(neo4j5_26, standardCypherQuery()),
          Arguments.of(neo4j2025_11, standardCypherQuery()),
          Arguments.of(neo4j2026_1, dynamicLabelsCypherQuery()),
          Arguments.of(neo4jAura, dynamicLabelsCypherQuery()),
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(DeleteRelationshipMatchingNodesWithKeysParams::class)
  fun `should build relationship delete statements with keys matching nodes`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val withKeys =
        DeleteRelationshipSinkAction(
            SinkActionNodeReference(
                setOf("Person", "Employee"),
                mapOf("id" to 1),
                LookupMode.MATCH,
            ),
            SinkActionNodeReference(
                setOf("Company", "Corporation"),
                mapOf("id" to 7),
                LookupMode.MATCH,
            ),
            matchType = "WORKS_AT",
            matchProperties = mapOf("empId" to 5),
            hasKeys = true,
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
    generator.buildStatement(
        withKeys.copy(startNode = SinkActionNodeReference(emptySet(), emptyMap(), LookupMode.MATCH))
    ) shouldBe expectedQuery
    generator.buildStatement(
        withKeys.copy(endNode = SinkActionNodeReference(emptySet(), emptyMap(), LookupMode.MATCH))
    ) shouldBe expectedQuery
    generator.buildStatement(
        withKeys.copy(
            startNode = SinkActionNodeReference(emptySet(), emptyMap(), LookupMode.MATCH),
            endNode = SinkActionNodeReference(emptySet(), emptyMap(), LookupMode.MATCH),
        )
    ) shouldBe expectedQuery
  }

  @ParameterizedTest
  @ArgumentsSource(DeleteRelationshipMatchingNodesWithKeysParams::class)
  fun `should build relationship delete statements with keys and without start node keys matching nodes`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val withKeys =
        DeleteRelationshipSinkAction(
            SinkActionNodeReference(emptySet(), emptyMap(), LookupMode.MATCH),
            SinkActionNodeReference(
                setOf("Company", "Corporation"),
                mapOf("id" to 7),
                LookupMode.MATCH,
            ),
            matchType = "WORKS_AT",
            matchProperties = mapOf("empId" to 5),
            hasKeys = true,
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
    generator.buildStatement(
        withKeys.copy(startNode = SinkActionNodeReference(emptySet(), emptyMap(), LookupMode.MATCH))
    ) shouldBe expectedQuery
    generator.buildStatement(
        withKeys.copy(endNode = SinkActionNodeReference(emptySet(), emptyMap(), LookupMode.MATCH))
    ) shouldBe expectedQuery
    generator.buildStatement(
        withKeys.copy(
            startNode = SinkActionNodeReference(emptySet(), emptyMap(), LookupMode.MATCH),
            endNode = SinkActionNodeReference(emptySet(), emptyMap(), LookupMode.MATCH),
        )
    ) shouldBe expectedQuery
  }

  @ParameterizedTest
  @ArgumentsSource(DeleteRelationshipMatchingNodesWithKeysParams::class)
  fun `should build relationship delete statements with keys and without end node keys matching nodes`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val withKeys =
        DeleteRelationshipSinkAction(
            SinkActionNodeReference(
                setOf("Person", "Employee"),
                mapOf("id" to 5),
                LookupMode.MATCH,
            ),
            SinkActionNodeReference(emptySet(), emptyMap(), LookupMode.MATCH),
            matchType = "WORKS_AT",
            matchProperties = mapOf("empId" to 5),
            hasKeys = true,
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
    generator.buildStatement(
        withKeys.copy(startNode = SinkActionNodeReference(emptySet(), emptyMap(), LookupMode.MATCH))
    ) shouldBe expectedQuery
    generator.buildStatement(
        withKeys.copy(endNode = SinkActionNodeReference(emptySet(), emptyMap(), LookupMode.MATCH))
    ) shouldBe expectedQuery
    generator.buildStatement(
        withKeys.copy(
            startNode = SinkActionNodeReference(emptySet(), emptyMap(), LookupMode.MATCH),
            endNode = SinkActionNodeReference(emptySet(), emptyMap(), LookupMode.MATCH),
        )
    ) shouldBe expectedQuery
  }

  @ParameterizedTest
  @ArgumentsSource(DeleteRelationshipMatchingNodesWithKeysParams::class)
  fun `should build relationship delete statements with keys and without node keys matching nodes`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val withKeys =
        DeleteRelationshipSinkAction(
            startNode =
                SinkActionNodeReference(setOf("Person", "Employee"), emptyMap(), LookupMode.MATCH),
            endNode =
                SinkActionNodeReference(
                    setOf("Company", "Corporation"),
                    emptyMap(),
                    LookupMode.MATCH,
                ),
            matchType = "WORKS_AT",
            matchProperties = mapOf("empId" to 5),
            hasKeys = true,
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
    generator.buildStatement(
        withKeys.copy(startNode = SinkActionNodeReference(emptySet(), emptyMap(), LookupMode.MATCH))
    ) shouldBe expectedQuery
    generator.buildStatement(
        withKeys.copy(endNode = SinkActionNodeReference(emptySet(), emptyMap(), LookupMode.MATCH))
    ) shouldBe expectedQuery
    generator.buildStatement(
        withKeys.copy(
            startNode = SinkActionNodeReference(emptySet(), emptyMap(), LookupMode.MATCH),
            endNode = SinkActionNodeReference(emptySet(), emptyMap(), LookupMode.MATCH),
        )
    ) shouldBe expectedQuery
  }

  object DeleteRelationshipMatchingNodesWithKeysParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH ()-[r:`WORKS_AT` {`empId`: _e.matchProperties.`empId`}]->() " +
              "DELETE r",
          mapOf("e" to mapOf("matchProperties" to mapOf("empId" to 5))),
      )
    }

    fun dynamicLabelsQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH ()-[r:${'$'}(_e.matchType) {`empId`: _e.matchProperties.`empId`}]->() " +
              "DELETE r",
          mapOf("e" to mapOf("matchType" to "WORKS_AT", "matchProperties" to mapOf("empId" to 5))),
      )
    }

    override fun provideArguments(
        parameters: ParameterDeclarations?,
        context: ExtensionContext?,
    ): Stream<out Arguments?>? {
      return Stream.of(
          Arguments.of(neo4j4_4, standardCypherQuery()),
          Arguments.of(neo4j5_26, standardCypherQuery()),
          Arguments.of(neo4j2025_11, standardCypherQuery()),
          Arguments.of(neo4j2026_1, dynamicLabelsQuery()),
          Arguments.of(neo4jAura, dynamicLabelsQuery()),
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(DeleteRelationshipMergingNodesWithKeysParams::class)
  fun `should build relationship delete statements with keys merging nodes`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val withKeys =
        DeleteRelationshipSinkAction(
            SinkActionNodeReference(
                setOf("Person", "Employee"),
                mapOf("id" to 1),
                LookupMode.MERGE,
            ),
            SinkActionNodeReference(
                setOf("Company", "Corporation"),
                mapOf("id" to 7),
                LookupMode.MERGE,
            ),
            matchType = "WORKS_AT",
            matchProperties = mapOf("empId" to 5),
            hasKeys = true,
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
    generator.buildStatement(
        withKeys.copy(startNode = SinkActionNodeReference(emptySet(), emptyMap(), LookupMode.MATCH))
    ) shouldBe expectedQuery
    generator.buildStatement(
        withKeys.copy(endNode = SinkActionNodeReference(emptySet(), emptyMap(), LookupMode.MATCH))
    ) shouldBe expectedQuery
    generator.buildStatement(
        withKeys.copy(
            startNode = SinkActionNodeReference(emptySet(), emptyMap(), LookupMode.MATCH),
            endNode = SinkActionNodeReference(emptySet(), emptyMap(), LookupMode.MATCH),
        )
    ) shouldBe expectedQuery
  }

  @ParameterizedTest
  @ArgumentsSource(DeleteRelationshipMergingNodesWithKeysParams::class)
  fun `should build relationship delete statements with keys and without start node keys merging nodes`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val withKeys =
        DeleteRelationshipSinkAction(
            SinkActionNodeReference(emptySet(), emptyMap(), LookupMode.MERGE),
            SinkActionNodeReference(
                setOf("Company", "Corporation"),
                mapOf("id" to 7),
                LookupMode.MERGE,
            ),
            matchType = "WORKS_AT",
            matchProperties = mapOf("empId" to 5),
            hasKeys = true,
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
    generator.buildStatement(
        withKeys.copy(startNode = SinkActionNodeReference(emptySet(), emptyMap(), LookupMode.MATCH))
    ) shouldBe expectedQuery
    generator.buildStatement(
        withKeys.copy(endNode = SinkActionNodeReference(emptySet(), emptyMap(), LookupMode.MATCH))
    ) shouldBe expectedQuery
    generator.buildStatement(
        withKeys.copy(
            startNode = SinkActionNodeReference(emptySet(), emptyMap(), LookupMode.MATCH),
            endNode = SinkActionNodeReference(emptySet(), emptyMap(), LookupMode.MATCH),
        )
    ) shouldBe expectedQuery
  }

  @ParameterizedTest
  @ArgumentsSource(DeleteRelationshipMergingNodesWithKeysParams::class)
  fun `should build relationship delete statements with keys and without end node keys merging nodes`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val withKeys =
        DeleteRelationshipSinkAction(
            SinkActionNodeReference(
                setOf("Person", "Employee"),
                mapOf("id" to 5),
                LookupMode.MERGE,
            ),
            SinkActionNodeReference(emptySet(), emptyMap(), LookupMode.MERGE),
            matchType = "WORKS_AT",
            matchProperties = mapOf("empId" to 5),
            hasKeys = true,
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
    generator.buildStatement(
        withKeys.copy(startNode = SinkActionNodeReference(emptySet(), emptyMap(), LookupMode.MATCH))
    ) shouldBe expectedQuery
    generator.buildStatement(
        withKeys.copy(endNode = SinkActionNodeReference(emptySet(), emptyMap(), LookupMode.MATCH))
    ) shouldBe expectedQuery
    generator.buildStatement(
        withKeys.copy(
            startNode = SinkActionNodeReference(emptySet(), emptyMap(), LookupMode.MATCH),
            endNode = SinkActionNodeReference(emptySet(), emptyMap(), LookupMode.MATCH),
        )
    ) shouldBe expectedQuery
  }

  @ParameterizedTest
  @ArgumentsSource(DeleteRelationshipMergingNodesWithKeysParams::class)
  fun `should build relationship delete statements with keys and without node keys merging nodes`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val withKeys =
        DeleteRelationshipSinkAction(
            startNode =
                SinkActionNodeReference(setOf("Person", "Employee"), emptyMap(), LookupMode.MERGE),
            endNode =
                SinkActionNodeReference(
                    setOf("Company", "Corporation"),
                    emptyMap(),
                    LookupMode.MERGE,
                ),
            matchType = "WORKS_AT",
            matchProperties = mapOf("empId" to 5),
            hasKeys = true,
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
    generator.buildStatement(
        withKeys.copy(startNode = SinkActionNodeReference(emptySet(), emptyMap(), LookupMode.MATCH))
    ) shouldBe expectedQuery
    generator.buildStatement(
        withKeys.copy(endNode = SinkActionNodeReference(emptySet(), emptyMap(), LookupMode.MATCH))
    ) shouldBe expectedQuery
    generator.buildStatement(
        withKeys.copy(
            startNode = SinkActionNodeReference(emptySet(), emptyMap(), LookupMode.MATCH),
            endNode = SinkActionNodeReference(emptySet(), emptyMap(), LookupMode.MATCH),
        )
    ) shouldBe expectedQuery
  }

  object DeleteRelationshipMergingNodesWithKeysParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH ()-[r:`WORKS_AT` {`empId`: _e.matchProperties.`empId`}]->() " +
              "DELETE r",
          mapOf("e" to mapOf("matchProperties" to mapOf("empId" to 5))),
      )
    }

    fun dynamicLabelsQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH ()-[r:${'$'}(_e.matchType) {`empId`: _e.matchProperties.`empId`}]->() " +
              "DELETE r",
          mapOf("e" to mapOf("matchType" to "WORKS_AT", "matchProperties" to mapOf("empId" to 5))),
      )
    }

    override fun provideArguments(
        parameters: ParameterDeclarations?,
        context: ExtensionContext?,
    ): Stream<out Arguments?>? {
      return Stream.of(
          Arguments.of(neo4j4_4, standardCypherQuery()),
          Arguments.of(neo4j5_26, standardCypherQuery()),
          Arguments.of(neo4j2025_11, standardCypherQuery()),
          Arguments.of(neo4j2026_1, dynamicLabelsQuery()),
          Arguments.of(neo4jAura, dynamicLabelsQuery()),
      )
    }
  }

  companion object {
    private val neo4j4_4 =
        Neo4j(Neo4jVersion(4, 4), Neo4jEdition.ENTERPRISE, Neo4jDeploymentType.SELF_MANAGED)
    private val neo4j5_26 =
        Neo4j(Neo4jVersion(5, 26), Neo4jEdition.ENTERPRISE, Neo4jDeploymentType.SELF_MANAGED)
    private val neo4j2025_11 =
        Neo4j(Neo4jVersion(2025, 11), Neo4jEdition.ENTERPRISE, Neo4jDeploymentType.SELF_MANAGED)
    private val neo4j2026_1 =
        Neo4j(Neo4jVersion(2026, 1), Neo4jEdition.ENTERPRISE, Neo4jDeploymentType.SELF_MANAGED)
    private val neo4jAura =
        Neo4j(Neo4jVersion(2026, 1), Neo4jEdition.ENTERPRISE, Neo4jDeploymentType.AURA)
  }
}
