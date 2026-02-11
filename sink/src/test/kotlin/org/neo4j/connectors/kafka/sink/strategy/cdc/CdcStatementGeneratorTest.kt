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
package org.neo4j.connectors.kafka.sink.strategy.cdc

import io.kotest.matchers.shouldBe
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
import org.neo4j.cdc.client.model.EntityOperation
import org.neo4j.driver.Query

class CdcStatementGeneratorTest {

  @ParameterizedTest
  @ArgumentsSource(CreateNodeParams::class)
  fun `should build node create statements`(neo4j: Neo4j, expectedQuery: Query) {
    val generator = DefaultCdcStatementGenerator(neo4j)
    val data =
        CdcNodeData(
            operation = EntityOperation.CREATE,
            matchLabels = setOf("Person"),
            matchProperties = mapOf("name" to "john", "surname" to "doe"),
            setProperties =
                mapOf(
                    "name" to "john",
                    "surname" to "doe",
                    "dob" to java.time.LocalDate.of(1990, 1, 1),
                ),
            addLabels = setOf("Employee"),
            removeLabels = emptySet(),
        )

    generator.buildStatement(data) shouldBe expectedQuery
  }

  object CreateNodeParams : ArgumentsProvider {
    private fun standardCypherQuery(): Query {
      return Query(
          "MERGE (n:`Person` {`name`: ${'$'}e.matchProperties.`name`, `surname`: ${'$'}e.matchProperties.`surname`}) " +
              "SET n += ${'$'}e.setProperties SET n:`Employee`",
          mapOf(
              "e" to
                  mapOf(
                      "matchProperties" to mapOf("name" to "john", "surname" to "doe"),
                      "setProperties" to
                          mapOf(
                              "name" to "john",
                              "surname" to "doe",
                              "dob" to java.time.LocalDate.of(1990, 1, 1),
                          ),
                  )
          ),
      )
    }

    private fun setRemoveDynamicLabelsQuery(): Query {
      return Query(
          "MERGE (n:`Person` {`name`: ${'$'}e.matchProperties.`name`, `surname`: ${'$'}e.matchProperties.`surname`}) " +
              "SET n += ${'$'}e.setProperties SET n:${'$'}(${'$'}e.addLabels) REMOVE n:${'$'}(${'$'}e.removeLabels)",
          mapOf(
              "e" to
                  mapOf(
                      "addLabels" to listOf("Employee"),
                      "removeLabels" to emptyList<String>(),
                      "matchProperties" to mapOf("name" to "john", "surname" to "doe"),
                      "setProperties" to
                          mapOf(
                              "name" to "john",
                              "surname" to "doe",
                              "dob" to java.time.LocalDate.of(1990, 1, 1),
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
          Arguments.of(neo4j2026_1, setRemoveDynamicLabelsQuery()),
          Arguments.of(neo4jAura, setRemoveDynamicLabelsQuery()),
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(UpdateNodeParams::class)
  fun `should build node update statements`(neo4j: Neo4j, expectedQuery: Query) {
    val generator = DefaultCdcStatementGenerator(neo4j)
    val data =
        CdcNodeData(
            operation = EntityOperation.UPDATE,
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
          "MERGE (n:`Person` {`name`: ${'$'}e.matchProperties.`name`, `surname`: ${'$'}e.matchProperties.`surname`}) " +
              "SET n += ${'$'}e.setProperties SET n:`Employee` REMOVE n:`Intern`:`Undergrad`",
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
          "MERGE (n:`Person` {`name`: ${'$'}e.matchProperties.`name`, `surname`: ${'$'}e.matchProperties.`surname`}) " +
              "SET n += ${'$'}e.setProperties SET n:${'$'}(${'$'}e.addLabels) REMOVE n:${'$'}(${'$'}e.removeLabels)",
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

    override fun provideArguments(
        parameters: ParameterDeclarations?,
        context: ExtensionContext?,
    ): Stream<out Arguments?>? {
      return Stream.of(
          Arguments.of(neo4j4_4, standardCypherQuery()),
          Arguments.of(neo4j5_26, setRemoveDynamicLabelsQuery()),
          Arguments.of(neo4j2026_1, setRemoveDynamicLabelsQuery()),
          Arguments.of(neo4jAura, setRemoveDynamicLabelsQuery()),
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(DeleteNodeParams::class)
  fun `should build node delete statements`(neo4j: Neo4j, expectedQuery: Query) {
    val generator = DefaultCdcStatementGenerator(neo4j)
    val data =
        CdcNodeData(
            operation = EntityOperation.DELETE,
            matchLabels = setOf("Person"),
            matchProperties = mapOf("name" to "joe", "surname" to "doe"),
            setProperties = emptyMap(),
            addLabels = emptySet(),
            removeLabels = emptySet(),
        )

    generator.buildStatement(data) shouldBe expectedQuery
  }

  object DeleteNodeParams : ArgumentsProvider {
    private fun standardCypherQuery(): Query {
      return Query(
          "MATCH (n:`Person` {`name`: ${'$'}e.matchProperties.`name`, `surname`: ${'$'}e.matchProperties.`surname`}) " +
              "DELETE n",
          mapOf("e" to mapOf("matchProperties" to mapOf("name" to "joe", "surname" to "doe"))),
      )
    }

    override fun provideArguments(
        parameters: ParameterDeclarations?,
        context: ExtensionContext?,
    ): Stream<out Arguments?>? {
      return Stream.of(
          Arguments.of(neo4j4_4, standardCypherQuery()),
          Arguments.of(neo4j5_26, standardCypherQuery()),
          Arguments.of(neo4j2026_1, standardCypherQuery()),
          Arguments.of(neo4jAura, standardCypherQuery()),
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(CreateRelationshipWithoutKeysParams::class)
  fun `should build relationship create statements without keys`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultCdcStatementGenerator(neo4j)
    val withKeys =
        CdcRelationshipData(
            operation = EntityOperation.CREATE,
            startMatchLabels = setOf("Person", "Employee"),
            startMatchProperties = mapOf("id" to 1),
            endMatchLabels = setOf("Company", "Corporation"),
            endMatchProperties = mapOf("id" to 7),
            matchType = "WORKS_AT",
            matchProperties = emptyMap(),
            hasKeys = false,
            setProperties = mapOf("role" to "dev"),
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
  }

  object CreateRelationshipWithoutKeysParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "MATCH (start:`Employee`:`Person` {`id`: ${'$'}e.start.matchProperties.`id`}) " +
              "MATCH (end:`Company`:`Corporation` {`id`: ${'$'}e.end.matchProperties.`id`}) " +
              "MERGE (start)-[r:`WORKS_AT`]->(end) " +
              "SET r += ${'$'}e.setProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to mapOf("matchProperties" to mapOf("id" to 1)),
                      "end" to mapOf("matchProperties" to mapOf("id" to 7)),
                      "setProperties" to mapOf("role" to "dev"),
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
          Arguments.of(neo4j2026_1, standardCypherQuery()),
          Arguments.of(neo4jAura, standardCypherQuery()),
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(CreateRelationshipWithKeysParams::class)
  fun `should build relationship create statements with keys`(neo4j: Neo4j, expectedQuery: Query) {
    val generator = DefaultCdcStatementGenerator(neo4j)
    val withKeys =
        CdcRelationshipData(
            operation = EntityOperation.CREATE,
            startMatchLabels = setOf("Person", "Employee"),
            startMatchProperties = mapOf("id" to 1),
            endMatchLabels = setOf("Company", "Corporation"),
            endMatchProperties = mapOf("id" to 7),
            matchType = "WORKS_AT",
            matchProperties = mapOf("empId" to 5),
            hasKeys = true,
            setProperties = mapOf("role" to "dev"),
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
  }

  object CreateRelationshipWithKeysParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "MATCH (start:`Employee`:`Person` {`id`: ${'$'}e.start.matchProperties.`id`}) " +
              "MATCH (end:`Company`:`Corporation` {`id`: ${'$'}e.end.matchProperties.`id`}) " +
              "MERGE (start)-[r:`WORKS_AT` {`empId`: ${'$'}e.matchProperties.`empId`}]->(end) " +
              "SET r += ${'$'}e.setProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to mapOf("matchProperties" to mapOf("id" to 1)),
                      "end" to mapOf("matchProperties" to mapOf("id" to 7)),
                      "matchProperties" to mapOf("empId" to 5),
                      "setProperties" to mapOf("role" to "dev"),
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
          Arguments.of(neo4j2026_1, standardCypherQuery()),
          Arguments.of(neo4jAura, standardCypherQuery()),
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(UpdateRelationshipWithoutKeysParams::class)
  fun `should build relationship update statements without keys`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultCdcStatementGenerator(neo4j)
    val withKeys =
        CdcRelationshipData(
            operation = EntityOperation.UPDATE,
            startMatchLabels = setOf("Person", "Employee"),
            startMatchProperties = mapOf("id" to 1),
            endMatchLabels = setOf("Company", "Corporation"),
            endMatchProperties = mapOf("id" to 7),
            matchType = "WORKS_AT",
            matchProperties = mapOf("role" to "dev"),
            hasKeys = false,
            setProperties = mapOf("senior" to true),
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
  }

  object UpdateRelationshipWithoutKeysParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "MATCH (start:`Employee`:`Person` {`id`: ${'$'}e.start.matchProperties.`id`}) " +
              "MATCH (end:`Company`:`Corporation` {`id`: ${'$'}e.end.matchProperties.`id`}) " +
              "MATCH (start)-[r:`WORKS_AT` {`role`: ${'$'}e.matchProperties.`role`}]->(end) " +
              "WITH r LIMIT 1 " +
              "SET r += ${'$'}e.setProperties",
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

    override fun provideArguments(
        parameters: ParameterDeclarations?,
        context: ExtensionContext?,
    ): Stream<out Arguments?>? {
      return Stream.of(
          Arguments.of(neo4j4_4, standardCypherQuery()),
          Arguments.of(neo4j5_26, standardCypherQuery()),
          Arguments.of(neo4j2026_1, standardCypherQuery()),
          Arguments.of(neo4jAura, standardCypherQuery()),
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(UpdateRelationshipWithKeysParams::class)
  fun `should build relationship update statements with keys`(neo4j: Neo4j, expectedQuery: Query) {
    val generator = DefaultCdcStatementGenerator(neo4j)
    val withKeys =
        CdcRelationshipData(
            operation = EntityOperation.UPDATE,
            startMatchLabels = setOf("Person", "Employee"),
            startMatchProperties = mapOf("id" to 1),
            endMatchLabels = setOf("Company", "Corporation"),
            endMatchProperties = mapOf("id" to 7),
            matchType = "WORKS_AT",
            matchProperties = mapOf("empId" to 5),
            hasKeys = true,
            setProperties = mapOf("senior" to true),
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
  }

  object UpdateRelationshipWithKeysParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "MATCH (start:`Employee`:`Person` {`id`: ${'$'}e.start.matchProperties.`id`})-" +
              "[r:`WORKS_AT` {`empId`: ${'$'}e.matchProperties.`empId`}]->" +
              "(end:`Company`:`Corporation` {`id`: ${'$'}e.end.matchProperties.`id`}) " +
              "SET r += ${'$'}e.setProperties",
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

    override fun provideArguments(
        parameters: ParameterDeclarations?,
        context: ExtensionContext?,
    ): Stream<out Arguments?>? {
      return Stream.of(
          Arguments.of(neo4j4_4, standardCypherQuery()),
          Arguments.of(neo4j5_26, standardCypherQuery()),
          Arguments.of(neo4j2026_1, standardCypherQuery()),
          Arguments.of(neo4jAura, standardCypherQuery()),
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(UpdateRelationshipWithKeysWithoutStartNodeKeysParams::class)
  fun `should build relationship update statements with keys without start node keys`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultCdcStatementGenerator(neo4j)
    val withKeys =
        CdcRelationshipData(
            operation = EntityOperation.UPDATE,
            startMatchLabels = emptySet(),
            startMatchProperties = emptyMap(),
            endMatchLabels = setOf("Company", "Corporation"),
            endMatchProperties = mapOf("id" to 7),
            matchType = "WORKS_AT",
            matchProperties = mapOf("empId" to 5),
            hasKeys = true,
            setProperties = mapOf("senior" to true),
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
  }

  object UpdateRelationshipWithKeysWithoutStartNodeKeysParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "MATCH (start)-[r:`WORKS_AT` {`empId`: ${'$'}e.matchProperties.`empId`}]->" +
              "(end:`Company`:`Corporation` {`id`: ${'$'}e.end.matchProperties.`id`}) " +
              "SET r += ${'$'}e.setProperties",
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

    override fun provideArguments(
        parameters: ParameterDeclarations?,
        context: ExtensionContext?,
    ): Stream<out Arguments?>? {
      return Stream.of(
          Arguments.of(neo4j4_4, standardCypherQuery()),
          Arguments.of(neo4j5_26, standardCypherQuery()),
          Arguments.of(neo4j2026_1, standardCypherQuery()),
          Arguments.of(neo4jAura, standardCypherQuery()),
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(UpdateRelationshipWithKeysWithoutEndNodeKeysParams::class)
  fun `should build relationship update statements with keys without end node keys`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultCdcStatementGenerator(neo4j)
    val withKeys =
        CdcRelationshipData(
            operation = EntityOperation.UPDATE,
            startMatchLabels = setOf("Employee", "Person"),
            startMatchProperties = mapOf("id" to 7),
            endMatchLabels = emptySet(),
            endMatchProperties = emptyMap(),
            matchType = "WORKS_AT",
            matchProperties = mapOf("empId" to 5),
            hasKeys = true,
            setProperties = mapOf("senior" to true),
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
  }

  object UpdateRelationshipWithKeysWithoutEndNodeKeysParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "MATCH (start:`Employee`:`Person` {`id`: ${'$'}e.start.matchProperties.`id`})-" +
              "[r:`WORKS_AT` {`empId`: ${'$'}e.matchProperties.`empId`}]->(end) " +
              "SET r += ${'$'}e.setProperties",
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

    override fun provideArguments(
        parameters: ParameterDeclarations?,
        context: ExtensionContext?,
    ): Stream<out Arguments?>? {
      return Stream.of(
          Arguments.of(neo4j4_4, standardCypherQuery()),
          Arguments.of(neo4j5_26, standardCypherQuery()),
          Arguments.of(neo4j2026_1, standardCypherQuery()),
          Arguments.of(neo4jAura, standardCypherQuery()),
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(UpdateRelationshipWithKeysWithoutNodeKeysParams::class)
  fun `should build relationship update statements with keys without node keys`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultCdcStatementGenerator(neo4j)
    val withKeys =
        CdcRelationshipData(
            operation = EntityOperation.UPDATE,
            startMatchLabels = emptySet(),
            startMatchProperties = emptyMap(),
            endMatchLabels = emptySet(),
            endMatchProperties = emptyMap(),
            matchType = "WORKS_AT",
            matchProperties = mapOf("empId" to 5),
            hasKeys = true,
            setProperties = mapOf("senior" to true),
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
  }

  object UpdateRelationshipWithKeysWithoutNodeKeysParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "MATCH (start)-[r:`WORKS_AT` {`empId`: ${'$'}e.matchProperties.`empId`}]->(end) " +
              "SET r += ${'$'}e.setProperties",
          mapOf(
              "e" to
                  mapOf(
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
          Arguments.of(neo4j2026_1, standardCypherQuery()),
          Arguments.of(neo4jAura, standardCypherQuery()),
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(DeleteRelationshipWithoutKeysParams::class)
  fun `should build relationship delete statements without keys`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultCdcStatementGenerator(neo4j)
    val withKeys =
        CdcRelationshipData(
            operation = EntityOperation.DELETE,
            startMatchLabels = setOf("Person", "Employee"),
            startMatchProperties = mapOf("id" to 1),
            endMatchLabels = setOf("Company", "Corporation"),
            endMatchProperties = mapOf("id" to 7),
            matchType = "WORKS_AT",
            matchProperties = mapOf("role" to "dev"),
            hasKeys = false,
            setProperties = emptyMap(),
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
  }

  object DeleteRelationshipWithoutKeysParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "MATCH (start:`Employee`:`Person` {`id`: ${'$'}e.start.matchProperties.`id`}) " +
              "MATCH (end:`Company`:`Corporation` {`id`: ${'$'}e.end.matchProperties.`id`}) " +
              "MATCH (start)-[r:`WORKS_AT` {`role`: ${'$'}e.matchProperties.`role`}]->(end) " +
              "WITH r LIMIT 1 " +
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

    override fun provideArguments(
        parameters: ParameterDeclarations?,
        context: ExtensionContext?,
    ): Stream<out Arguments?>? {
      return Stream.of(
          Arguments.of(neo4j4_4, standardCypherQuery()),
          Arguments.of(neo4j5_26, standardCypherQuery()),
          Arguments.of(neo4j2026_1, standardCypherQuery()),
          Arguments.of(neo4jAura, standardCypherQuery()),
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(DeleteRelationshipWithKeysParams::class)
  fun `should build relationship delete statements with keys`(neo4j: Neo4j, expectedQuery: Query) {
    val generator = DefaultCdcStatementGenerator(neo4j)
    val withKeys =
        CdcRelationshipData(
            operation = EntityOperation.DELETE,
            startMatchLabels = setOf("Person", "Employee"),
            startMatchProperties = mapOf("id" to 1),
            endMatchLabels = setOf("Company", "Corporation"),
            endMatchProperties = mapOf("id" to 7),
            matchType = "WORKS_AT",
            matchProperties = mapOf("empId" to 5),
            hasKeys = true,
            setProperties = emptyMap(),
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
    generator.buildStatement(
        withKeys.copy(startMatchLabels = emptySet(), startMatchProperties = emptyMap())
    ) shouldBe expectedQuery
    generator.buildStatement(
        withKeys.copy(endMatchLabels = emptySet(), endMatchProperties = emptyMap())
    ) shouldBe expectedQuery
    generator.buildStatement(
        withKeys.copy(
            startMatchLabels = emptySet(),
            startMatchProperties = emptyMap(),
            endMatchLabels = emptySet(),
            endMatchProperties = emptyMap(),
        )
    ) shouldBe expectedQuery
  }

  object DeleteRelationshipWithKeysParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "MATCH ()-[r:`WORKS_AT` {`empId`: ${'$'}e.matchProperties.`empId`}]->() " + "DELETE r",
          mapOf("e" to mapOf("matchProperties" to mapOf("empId" to 5))),
      )
    }

    override fun provideArguments(
        parameters: ParameterDeclarations?,
        context: ExtensionContext?,
    ): Stream<out Arguments?>? {
      return Stream.of(
          Arguments.of(neo4j4_4, standardCypherQuery()),
          Arguments.of(neo4j5_26, standardCypherQuery()),
          Arguments.of(neo4j2026_1, standardCypherQuery()),
          Arguments.of(neo4jAura, standardCypherQuery()),
      )
    }
  }

  companion object {
    private val neo4j4_4 =
        Neo4j(Neo4jVersion(4, 4), Neo4jEdition.ENTERPRISE, Neo4jDeploymentType.SELF_MANAGED)
    private val neo4j5_26 =
        Neo4j(Neo4jVersion(5, 26), Neo4jEdition.ENTERPRISE, Neo4jDeploymentType.SELF_MANAGED)
    private val neo4j2026_1 =
        Neo4j(Neo4jVersion(2026, 1), Neo4jEdition.ENTERPRISE, Neo4jDeploymentType.SELF_MANAGED)
    private val neo4jAura =
        Neo4j(Neo4jVersion(2026, 1), Neo4jEdition.ENTERPRISE, Neo4jDeploymentType.AURA)
  }
}
