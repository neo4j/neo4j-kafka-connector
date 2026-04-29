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
            matcher =
                NodeMatcher.ByLabelsAndProperties(
                    setOf("Person"),
                    mapOf("name" to "joe", "surname" to "doe"),
                ),
            mutateProperties = mapOf("name" to "john"),
            addLabels = setOf("Employee"),
            removeLabels = setOf("Undergrad", "Intern"),
        )

    generator.buildStatement(data) shouldBe expectedQuery
  }

  object UpdateNodeParams : ArgumentsProvider {
    private fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (n:`Person` {`name`: _e.matchProperties.`name`, `surname`: _e.matchProperties.`surname`}) " +
              "SET n += _e.mutateProperties SET n:`Employee` REMOVE n:`Intern`:`Undergrad`",
          mapOf(
              "e" to
                  mapOf(
                      "matchProperties" to mapOf("name" to "joe", "surname" to "doe"),
                      "mutateProperties" to mapOf("name" to "john"),
                  )
          ),
      )
    }

    private fun setRemoveDynamicLabelsQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (n:`Person` {`name`: _e.matchProperties.`name`, `surname`: _e.matchProperties.`surname`}) " +
              "SET n += _e.mutateProperties SET n:${'$'}(_e.addLabels) REMOVE n:${'$'}(_e.removeLabels)",
          mapOf(
              "e" to
                  mapOf(
                      "addLabels" to listOf("Employee"),
                      "removeLabels" to listOf("Undergrad", "Intern"),
                      "matchProperties" to mapOf("name" to "joe", "surname" to "doe"),
                      "mutateProperties" to mapOf("name" to "john"),
                  )
          ),
      )
    }

    private fun dynamicLabelsQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (n:${'$'}(_e.matchLabels) {`name`: _e.matchProperties.`name`, `surname`: _e.matchProperties.`surname`}) " +
              "SET n += _e.mutateProperties SET n:${'$'}(_e.addLabels) REMOVE n:${'$'}(_e.removeLabels)",
          mapOf(
              "e" to
                  mapOf(
                      "matchLabels" to listOf("Person"),
                      "addLabels" to listOf("Employee"),
                      "removeLabels" to listOf("Undergrad", "Intern"),
                      "matchProperties" to mapOf("name" to "joe", "surname" to "doe"),
                      "mutateProperties" to mapOf("name" to "john"),
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
  @ArgumentsSource(UpdateNodeParamsByIdMatchers::class)
  fun `should build node update statements with id matchers`(neo4j: Neo4j, expectedQuery: Query) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val data =
        UpdateNodeSinkAction(
            matcher = NodeMatcher.ById(1),
            mutateProperties = mapOf("name" to "john"),
            addLabels = setOf("Employee"),
            removeLabels = setOf("Undergrad", "Intern"),
        )

    generator.buildStatement(data) shouldBe expectedQuery
  }

  object UpdateNodeParamsByIdMatchers : ArgumentsProvider {
    private fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (n) WHERE id(n) = _e.matchId " +
              "SET n += _e.mutateProperties SET n:`Employee` REMOVE n:`Intern`:`Undergrad`",
          mapOf("e" to mapOf("matchId" to 1, "mutateProperties" to mapOf("name" to "john"))),
      )
    }

    private fun dynamicLabelsQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (n) WHERE id(n) = _e.matchId " +
              "SET n += _e.mutateProperties SET n:${'$'}(_e.addLabels) REMOVE n:${'$'}(_e.removeLabels)",
          mapOf(
              "e" to
                  mapOf(
                      "matchId" to 1,
                      "addLabels" to listOf("Employee"),
                      "removeLabels" to listOf("Undergrad", "Intern"),
                      "mutateProperties" to mapOf("name" to "john"),
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
          Arguments.of(neo4j5_26, dynamicLabelsQuery()),
          Arguments.of(neo4j2025_11, dynamicLabelsQuery()),
          Arguments.of(neo4j2026_1, dynamicLabelsQuery()),
          Arguments.of(neo4jAura, dynamicLabelsQuery()),
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(UpdateNodeParamsByIdMatchersWithSetProperties::class)
  fun `should build node update statements with id matchers and set properties`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val data =
        UpdateNodeSinkAction(
            matcher = NodeMatcher.ById(1),
            setProperties = mapOf("name" to "john"),
            mutateProperties = mapOf("id" to 1),
            addLabels = setOf("Employee"),
            removeLabels = setOf("Undergrad", "Intern"),
        )

    generator.buildStatement(data) shouldBe expectedQuery
  }

  object UpdateNodeParamsByIdMatchersWithSetProperties : ArgumentsProvider {
    private fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (n) WHERE id(n) = _e.matchId " +
              "SET n = _e.setProperties SET n += _e.mutateProperties SET n:`Employee` REMOVE n:`Intern`:`Undergrad`",
          mapOf(
              "e" to
                  mapOf(
                      "matchId" to 1,
                      "setProperties" to mapOf("name" to "john"),
                      "mutateProperties" to mapOf("id" to 1),
                  )
          ),
      )
    }

    private fun dynamicLabelsQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (n) WHERE id(n) = _e.matchId " +
              "SET n = _e.setProperties SET n += _e.mutateProperties SET n:${'$'}(_e.addLabels) REMOVE n:${'$'}(_e.removeLabels)",
          mapOf(
              "e" to
                  mapOf(
                      "matchId" to 1,
                      "addLabels" to listOf("Employee"),
                      "removeLabels" to listOf("Undergrad", "Intern"),
                      "setProperties" to mapOf("name" to "john"),
                      "mutateProperties" to mapOf("id" to 1),
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
          Arguments.of(neo4j5_26, dynamicLabelsQuery()),
          Arguments.of(neo4j2025_11, dynamicLabelsQuery()),
          Arguments.of(neo4j2026_1, dynamicLabelsQuery()),
          Arguments.of(neo4jAura, dynamicLabelsQuery()),
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(UpdateNodeParamsByElementIdMatchers::class)
  fun `should build node update statements with element id matchers`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val data =
        UpdateNodeSinkAction(
            matcher = NodeMatcher.ByElementId("4:abc:1"),
            mutateProperties = mapOf("name" to "john"),
            addLabels = setOf("Employee"),
            removeLabels = setOf("Undergrad", "Intern"),
        )

    generator.buildStatement(data) shouldBe expectedQuery
  }

  object UpdateNodeParamsByElementIdMatchers : ArgumentsProvider {
    private fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (n) WHERE elementId(n) = _e.matchElementId " +
              "SET n += _e.mutateProperties SET n:`Employee` REMOVE n:`Intern`:`Undergrad`",
          mapOf(
              "e" to
                  mapOf(
                      "matchElementId" to "4:abc:1",
                      "mutateProperties" to mapOf("name" to "john"),
                  )
          ),
      )
    }

    private fun dynamicLabelsQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (n) WHERE elementId(n) = _e.matchElementId " +
              "SET n += _e.mutateProperties SET n:${'$'}(_e.addLabels) REMOVE n:${'$'}(_e.removeLabels)",
          mapOf(
              "e" to
                  mapOf(
                      "matchElementId" to "4:abc:1",
                      "addLabels" to listOf("Employee"),
                      "removeLabels" to listOf("Undergrad", "Intern"),
                      "mutateProperties" to mapOf("name" to "john"),
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
          Arguments.of(neo4j5_26, dynamicLabelsQuery()),
          Arguments.of(neo4j2025_11, dynamicLabelsQuery()),
          Arguments.of(neo4j2026_1, dynamicLabelsQuery()),
          Arguments.of(neo4jAura, dynamicLabelsQuery()),
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(UpdateNodeParamsByElementIdMatchersWithSetProperties::class)
  fun `should build node update statements with element id matchers and set properties`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val data =
        UpdateNodeSinkAction(
            matcher = NodeMatcher.ByElementId("4:abc:1"),
            setProperties = mapOf("name" to "john"),
            mutateProperties = mapOf("id" to 1),
            addLabels = setOf("Employee"),
            removeLabels = setOf("Undergrad", "Intern"),
        )

    generator.buildStatement(data) shouldBe expectedQuery
  }

  object UpdateNodeParamsByElementIdMatchersWithSetProperties : ArgumentsProvider {
    private fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (n) WHERE elementId(n) = _e.matchElementId " +
              "SET n = _e.setProperties SET n += _e.mutateProperties SET n:`Employee` REMOVE n:`Intern`:`Undergrad`",
          mapOf(
              "e" to
                  mapOf(
                      "matchElementId" to "4:abc:1",
                      "setProperties" to mapOf("name" to "john"),
                      "mutateProperties" to mapOf("id" to 1),
                  )
          ),
      )
    }

    private fun dynamicLabelsQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (n) WHERE elementId(n) = _e.matchElementId " +
              "SET n = _e.setProperties SET n += _e.mutateProperties SET n:${'$'}(_e.addLabels) REMOVE n:${'$'}(_e.removeLabels)",
          mapOf(
              "e" to
                  mapOf(
                      "matchElementId" to "4:abc:1",
                      "addLabels" to listOf("Employee"),
                      "removeLabels" to listOf("Undergrad", "Intern"),
                      "setProperties" to mapOf("name" to "john"),
                      "mutateProperties" to mapOf("id" to 1),
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
          Arguments.of(neo4j5_26, dynamicLabelsQuery()),
          Arguments.of(neo4j2025_11, dynamicLabelsQuery()),
          Arguments.of(neo4j2026_1, dynamicLabelsQuery()),
          Arguments.of(neo4jAura, dynamicLabelsQuery()),
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(UpdateNodeWithSetPropertiesParams::class)
  fun `should build node update statements with set properties`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val data =
        UpdateNodeSinkAction(
            matcher =
                NodeMatcher.ByLabelsAndProperties(
                    setOf("Person"),
                    mapOf("name" to "joe", "surname" to "doe"),
                ),
            setProperties = mapOf("id" to 5),
            mutateProperties = mapOf("name" to "joe", "surname" to "doe"),
            addLabels = setOf("Employee"),
            removeLabels = setOf("Undergrad", "Intern"),
        )

    generator.buildStatement(data) shouldBe expectedQuery
  }

  object UpdateNodeWithSetPropertiesParams : ArgumentsProvider {
    private fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (n:`Person` {`name`: _e.matchProperties.`name`, `surname`: _e.matchProperties.`surname`}) " +
              "SET n = _e.setProperties SET n += _e.mutateProperties SET n:`Employee` REMOVE n:`Intern`:`Undergrad`",
          mapOf(
              "e" to
                  mapOf(
                      "matchProperties" to mapOf("name" to "joe", "surname" to "doe"),
                      "setProperties" to mapOf("id" to 5),
                      "mutateProperties" to mapOf("name" to "joe", "surname" to "doe"),
                  )
          ),
      )
    }

    private fun setRemoveDynamicLabelsQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (n:`Person` {`name`: _e.matchProperties.`name`, `surname`: _e.matchProperties.`surname`}) " +
              "SET n = _e.setProperties SET n += _e.mutateProperties SET n:${'$'}(_e.addLabels) REMOVE n:${'$'}(_e.removeLabels)",
          mapOf(
              "e" to
                  mapOf(
                      "addLabels" to listOf("Employee"),
                      "removeLabels" to listOf("Undergrad", "Intern"),
                      "matchProperties" to mapOf("name" to "joe", "surname" to "doe"),
                      "setProperties" to mapOf("id" to 5),
                      "mutateProperties" to mapOf("name" to "joe", "surname" to "doe"),
                  )
          ),
      )
    }

    private fun dynamicLabelsQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (n:${'$'}(_e.matchLabels) {`name`: _e.matchProperties.`name`, `surname`: _e.matchProperties.`surname`}) " +
              "SET n = _e.setProperties SET n += _e.mutateProperties SET n:${'$'}(_e.addLabels) REMOVE n:${'$'}(_e.removeLabels)",
          mapOf(
              "e" to
                  mapOf(
                      "matchLabels" to listOf("Person"),
                      "addLabels" to listOf("Employee"),
                      "removeLabels" to listOf("Undergrad", "Intern"),
                      "matchProperties" to mapOf("name" to "joe", "surname" to "doe"),
                      "setProperties" to mapOf("id" to 5),
                      "mutateProperties" to mapOf("name" to "joe", "surname" to "doe"),
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
            matcher =
                NodeMatcher.ByLabelsAndProperties(
                    setOf("Person"),
                    mapOf("name" to "joe", "surname" to "doe"),
                ),
            mutateProperties = mapOf("name" to "john"),
            addLabels = setOf("Employee"),
            removeLabels = setOf("Undergrad", "Intern"),
        )

    generator.buildStatement(data) shouldBe expectedQuery
  }

  object MergeNodeParams : ArgumentsProvider {
    private fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MERGE (n:`Person` {`name`: _e.matchProperties.`name`, `surname`: _e.matchProperties.`surname`}) " +
              "SET n += _e.mutateProperties SET n:`Employee` REMOVE n:`Intern`:`Undergrad`",
          mapOf(
              "e" to
                  mapOf(
                      "matchProperties" to mapOf("name" to "joe", "surname" to "doe"),
                      "mutateProperties" to mapOf("name" to "john"),
                  )
          ),
      )
    }

    private fun setRemoveDynamicLabelsQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MERGE (n:`Person` {`name`: _e.matchProperties.`name`, `surname`: _e.matchProperties.`surname`}) " +
              "SET n += _e.mutateProperties SET n:${'$'}(_e.addLabels) REMOVE n:${'$'}(_e.removeLabels)",
          mapOf(
              "e" to
                  mapOf(
                      "addLabels" to listOf("Employee"),
                      "removeLabels" to listOf("Undergrad", "Intern"),
                      "matchProperties" to mapOf("name" to "joe", "surname" to "doe"),
                      "mutateProperties" to mapOf("name" to "john"),
                  )
          ),
      )
    }

    private fun dynamicLabelsQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MERGE (n:${'$'}(_e.matchLabels) {`name`: _e.matchProperties.`name`, `surname`: _e.matchProperties.`surname`}) " +
              "SET n += _e.mutateProperties SET n:${'$'}(_e.addLabels) REMOVE n:${'$'}(_e.removeLabels)",
          mapOf(
              "e" to
                  mapOf(
                      "matchLabels" to listOf("Person"),
                      "addLabels" to listOf("Employee"),
                      "removeLabels" to listOf("Undergrad", "Intern"),
                      "matchProperties" to mapOf("name" to "joe", "surname" to "doe"),
                      "mutateProperties" to mapOf("name" to "john"),
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
  @ArgumentsSource(MergeNodeWithSetPropertiesParams::class)
  fun `should build node merge statements with set properties`(neo4j: Neo4j, expectedQuery: Query) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val data =
        MergeNodeSinkAction(
            matcher =
                NodeMatcher.ByLabelsAndProperties(
                    setOf("Person"),
                    mapOf("name" to "joe", "surname" to "doe"),
                ),
            setProperties = mapOf("id" to 5),
            mutateProperties = mapOf("name" to "john", "surname" to "doe"),
            addLabels = setOf("Employee"),
            removeLabels = setOf("Undergrad", "Intern"),
        )

    generator.buildStatement(data) shouldBe expectedQuery
  }

  object MergeNodeWithSetPropertiesParams : ArgumentsProvider {
    private fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MERGE (n:`Person` {`name`: _e.matchProperties.`name`, `surname`: _e.matchProperties.`surname`}) " +
              "SET n = _e.setProperties SET n += _e.mutateProperties SET n:`Employee` REMOVE n:`Intern`:`Undergrad`",
          mapOf(
              "e" to
                  mapOf(
                      "matchProperties" to mapOf("name" to "joe", "surname" to "doe"),
                      "setProperties" to mapOf("id" to 5),
                      "mutateProperties" to mapOf("name" to "john", "surname" to "doe"),
                  )
          ),
      )
    }

    private fun setRemoveDynamicLabelsQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MERGE (n:`Person` {`name`: _e.matchProperties.`name`, `surname`: _e.matchProperties.`surname`}) " +
              "SET n = _e.setProperties SET n += _e.mutateProperties SET n:${'$'}(_e.addLabels) REMOVE n:${'$'}(_e.removeLabels)",
          mapOf(
              "e" to
                  mapOf(
                      "addLabels" to listOf("Employee"),
                      "removeLabels" to listOf("Undergrad", "Intern"),
                      "matchProperties" to mapOf("name" to "joe", "surname" to "doe"),
                      "setProperties" to mapOf("id" to 5),
                      "mutateProperties" to mapOf("name" to "john", "surname" to "doe"),
                  )
          ),
      )
    }

    private fun dynamicLabelsQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MERGE (n:${'$'}(_e.matchLabels) {`name`: _e.matchProperties.`name`, `surname`: _e.matchProperties.`surname`}) " +
              "SET n = _e.setProperties SET n += _e.mutateProperties SET n:${'$'}(_e.addLabels) REMOVE n:${'$'}(_e.removeLabels)",
          mapOf(
              "e" to
                  mapOf(
                      "matchLabels" to listOf("Person"),
                      "addLabels" to listOf("Employee"),
                      "removeLabels" to listOf("Undergrad", "Intern"),
                      "matchProperties" to mapOf("name" to "joe", "surname" to "doe"),
                      "setProperties" to mapOf("id" to 5),
                      "mutateProperties" to mapOf("name" to "john", "surname" to "doe"),
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
            matcher =
                NodeMatcher.ByLabelsAndProperties(
                    setOf("Person"),
                    mapOf("name" to "joe", "surname" to "doe"),
                )
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
  @ArgumentsSource(DeleteNodeParamsByIdMatchers::class)
  fun `should build node delete statements by id matchers`(neo4j: Neo4j, expectedQuery: Query) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val data = DeleteNodeSinkAction(matcher = NodeMatcher.ById(1))

    generator.buildStatement(data) shouldBe expectedQuery
  }

  object DeleteNodeParamsByIdMatchers : ArgumentsProvider {
    private fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (n) WHERE id(n) = _e.matchId " + "DELETE n",
          mapOf("e" to mapOf("matchId" to 1)),
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
          Arguments.of(neo4j2026_1, standardCypherQuery()),
          Arguments.of(neo4jAura, standardCypherQuery()),
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(DeleteNodeParamsByElementIdMatchers::class)
  fun `should build node delete statements by element id matchers`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val data = DeleteNodeSinkAction(matcher = NodeMatcher.ByElementId("4:abc:1"))

    generator.buildStatement(data) shouldBe expectedQuery
  }

  object DeleteNodeParamsByElementIdMatchers : ArgumentsProvider {
    private fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (n) WHERE elementId(n) = _e.matchElementId " + "DELETE n",
          mapOf("e" to mapOf("matchElementId" to "4:abc:1")),
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
          Arguments.of(neo4j2026_1, standardCypherQuery()),
          Arguments.of(neo4jAura, standardCypherQuery()),
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(DetachDeleteNodeParams::class)
  fun `should build node detach delete statements`(neo4j: Neo4j, expectedQuery: Query) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val data =
        DeleteNodeSinkAction(
            matcher =
                NodeMatcher.ByLabelsAndProperties(
                    setOf("Person"),
                    mapOf("name" to "joe", "surname" to "doe"),
                ),
            detach = true,
        )

    generator.buildStatement(data) shouldBe expectedQuery
  }

  object DetachDeleteNodeParams : ArgumentsProvider {
    private fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (n:`Person` {`name`: _e.matchProperties.`name`, `surname`: _e.matchProperties.`surname`}) " +
              "DETACH DELETE n",
          mapOf("e" to mapOf("matchProperties" to mapOf("name" to "joe", "surname" to "doe"))),
      )
    }

    private fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (n:${'$'}(_e.matchLabels) {`name`: _e.matchProperties.`name`, `surname`: _e.matchProperties.`surname`}) " +
              "DETACH DELETE n",
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
  @ArgumentsSource(DetachDeleteNodeParamsByIdMatchers::class)
  fun `should build node detach delete statements by id matchers`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val data = DeleteNodeSinkAction(matcher = NodeMatcher.ById(1), detach = true)

    generator.buildStatement(data) shouldBe expectedQuery
  }

  object DetachDeleteNodeParamsByIdMatchers : ArgumentsProvider {
    private fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (n) WHERE id(n) = _e.matchId " + "DETACH DELETE n",
          mapOf("e" to mapOf("matchId" to 1)),
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
          Arguments.of(neo4j2026_1, standardCypherQuery()),
          Arguments.of(neo4jAura, standardCypherQuery()),
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(DetachDeleteNodeParamsByElementIdMatchers::class)
  fun `should build node detach delete statements by element id matchers`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val data = DeleteNodeSinkAction(matcher = NodeMatcher.ByElementId("4:abc:1"), detach = true)

    generator.buildStatement(data) shouldBe expectedQuery
  }

  object DetachDeleteNodeParamsByElementIdMatchers : ArgumentsProvider {
    private fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (n) WHERE elementId(n) = _e.matchElementId " +
              "DETACH DELETE n",
          mapOf("e" to mapOf("matchElementId" to "4:abc:1")),
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
          Arguments.of(neo4j2026_1, standardCypherQuery()),
          Arguments.of(neo4jAura, standardCypherQuery()),
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
                NodeMatcher.ByLabelsAndProperties(setOf("Person", "Employee"), mapOf("id" to 1)),
                LookupMode.MATCH,
            ),
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(
                    setOf("Company", "Corporation"),
                    mapOf("id" to 7),
                ),
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
              "WITH _e, start " +
              "MATCH (end:`Company`:`Corporation` {`id`: _e.end.matchProperties.`id`}) " +
              "WITH _e, start, end " +
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
              "WITH _e, start " +
              "MATCH (end:${'$'}(_e.end.matchLabels) {`id`: _e.end.matchProperties.`id`}) " +
              "WITH _e, start, end " +
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
                NodeMatcher.ByLabelsAndProperties(setOf("Person", "Employee"), mapOf("id" to 1)),
                LookupMode.MERGE,
            ),
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(
                    setOf("Company", "Corporation"),
                    mapOf("id" to 7),
                ),
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
              "WITH _e, start " +
              "MERGE (end:`Company`:`Corporation` {`id`: _e.end.matchProperties.`id`}) " +
              "WITH _e, start, end " +
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
              "WITH _e, start " +
              "MERGE (end:${'$'}(_e.end.matchLabels) {`id`: _e.end.matchProperties.`id`}) " +
              "WITH _e, start, end " +
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
                NodeMatcher.ByLabelsAndProperties(setOf("Person", "Employee"), mapOf("id" to 1)),
                LookupMode.MATCH,
            ),
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(
                    setOf("Company", "Corporation"),
                    mapOf("id" to 7),
                ),
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
          "WITH ${'$'}e AS _e MATCH (start:`Employee`:`Person` {`id`: _e.start.matchProperties.`id`}) WITH _e, start " +
              "MATCH (end:`Company`:`Corporation` {`id`: _e.end.matchProperties.`id`}) WITH _e, start, end " +
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
          "WITH ${'$'}e AS _e MATCH (start:${'$'}(_e.start.matchLabels) {`id`: _e.start.matchProperties.`id`}) WITH _e, start " +
              "MATCH (end:${'$'}(_e.end.matchLabels) {`id`: _e.end.matchProperties.`id`}) WITH _e, start, end " +
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
                NodeMatcher.ByLabelsAndProperties(setOf("Person", "Employee"), mapOf("id" to 1)),
                LookupMode.MERGE,
            ),
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(
                    setOf("Company", "Corporation"),
                    mapOf("id" to 7),
                ),
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
          "WITH ${'$'}e AS _e MERGE (start:`Employee`:`Person` {`id`: _e.start.matchProperties.`id`}) WITH _e, start " +
              "MERGE (end:`Company`:`Corporation` {`id`: _e.end.matchProperties.`id`}) WITH _e, start, end " +
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
          "WITH ${'$'}e AS _e MERGE (start:${'$'}(_e.start.matchLabels) {`id`: _e.start.matchProperties.`id`}) WITH _e, start " +
              "MERGE (end:${'$'}(_e.end.matchLabels) {`id`: _e.end.matchProperties.`id`}) WITH _e, start, end " +
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
                NodeMatcher.ByLabelsAndProperties(setOf("Person", "Employee"), mapOf("id" to 1)),
                LookupMode.MATCH,
            ),
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(
                    setOf("Company", "Corporation"),
                    mapOf("id" to 7),
                ),
                LookupMode.MATCH,
            ),
            RelationshipMatcher.ByTypeAndProperties("WORKS_AT", mapOf("role" to "dev"), false),
            mutateProperties = mapOf("senior" to true),
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
  }

  object UpdateRelationshipWithMatchNodesWithoutKeysParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start:`Employee`:`Person` {`id`: _e.start.matchProperties.`id`}) " +
              "WITH _e, start " +
              "MATCH (end:`Company`:`Corporation` {`id`: _e.end.matchProperties.`id`}) " +
              "WITH _e, start, end " +
              "MATCH (start)-[r:`WORKS_AT` {`role`: _e.matchProperties.`role`}]->(end) " +
              "WITH _e, r LIMIT 1 " +
              "SET r += _e.mutateProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to mapOf("matchProperties" to mapOf("id" to 1)),
                      "end" to mapOf("matchProperties" to mapOf("id" to 7)),
                      "matchProperties" to mapOf("role" to "dev"),
                      "mutateProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start:${'$'}(_e.start.matchLabels) {`id`: _e.start.matchProperties.`id`}) " +
              "WITH _e, start " +
              "MATCH (end:${'$'}(_e.end.matchLabels) {`id`: _e.end.matchProperties.`id`}) " +
              "WITH _e, start, end " +
              "MATCH (start)-[r:${'$'}(_e.matchType) {`role`: _e.matchProperties.`role`}]->(end) " +
              "WITH _e, r LIMIT 1 " +
              "SET r += _e.mutateProperties",
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
                      "mutateProperties" to mapOf("senior" to true),
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
                NodeMatcher.ByLabelsAndProperties(setOf("Person", "Employee"), mapOf("id" to 1)),
                LookupMode.MATCH,
            ),
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(
                    setOf("Company", "Corporation"),
                    mapOf("id" to 7),
                ),
                LookupMode.MATCH,
            ),
            RelationshipMatcher.ByTypeAndProperties("WORKS_AT", mapOf("empId" to 5), true),
            mutateProperties = mapOf("senior" to true),
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
  }

  object UpdateRelationshipWithMatchNodesWithKeysParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start:`Employee`:`Person` {`id`: _e.start.matchProperties.`id`}) " +
              "WITH _e, start " +
              "MATCH (end:`Company`:`Corporation` {`id`: _e.end.matchProperties.`id`}) " +
              "WITH _e, start, end " +
              "MATCH (start)-[r:`WORKS_AT` {`empId`: _e.matchProperties.`empId`}]->(end) " +
              "SET r += _e.mutateProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to mapOf("matchProperties" to mapOf("id" to 1)),
                      "end" to mapOf("matchProperties" to mapOf("id" to 7)),
                      "matchProperties" to mapOf("empId" to 5),
                      "mutateProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start:${'$'}(_e.start.matchLabels) {`id`: _e.start.matchProperties.`id`}) " +
              "WITH _e, start " +
              "MATCH (end:${'$'}(_e.end.matchLabels) {`id`: _e.end.matchProperties.`id`}) " +
              "WITH _e, start, end " +
              "MATCH (start)-[r:${'$'}(_e.matchType) {`empId`: _e.matchProperties.`empId`}]->(end) " +
              "SET r += _e.mutateProperties",
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
                      "mutateProperties" to mapOf("senior" to true),
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
  @ArgumentsSource(UpdateRelationshipWithMatchAnyNodesWithKeysParams::class)
  fun `should build relationship update statements with matching any node with keys`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val withKeys =
        UpdateRelationshipSinkAction(
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(emptySet(), emptyMap()),
                LookupMode.MATCH,
            ),
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(emptySet(), emptyMap()),
                LookupMode.MATCH,
            ),
            RelationshipMatcher.ByTypeAndProperties("WORKS_AT", mapOf("empId" to 5), true),
            mutateProperties = mapOf("senior" to true),
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
  }

  object UpdateRelationshipWithMatchAnyNodesWithKeysParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start) " +
              "WITH _e, start " +
              "MATCH (end) " +
              "WITH _e, start, end " +
              "MATCH (start)-[r:`WORKS_AT` {`empId`: _e.matchProperties.`empId`}]->(end) " +
              "SET r += _e.mutateProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to mapOf("matchProperties" to emptyMap<String, Any>()),
                      "end" to mapOf("matchProperties" to emptyMap<String, Any>()),
                      "matchProperties" to mapOf("empId" to 5),
                      "mutateProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start:${'$'}(_e.start.matchLabels)) " +
              "WITH _e, start " +
              "MATCH (end:${'$'}(_e.end.matchLabels)) " +
              "WITH _e, start, end " +
              "MATCH (start)-[r:${'$'}(_e.matchType) {`empId`: _e.matchProperties.`empId`}]->(end) " +
              "SET r += _e.mutateProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to
                          mapOf(
                              "matchLabels" to emptySet<String>(),
                              "matchProperties" to emptyMap<String, Any>(),
                          ),
                      "end" to
                          mapOf(
                              "matchLabels" to emptySet<String>(),
                              "matchProperties" to emptyMap<String, Any>(),
                          ),
                      "matchType" to "WORKS_AT",
                      "matchProperties" to mapOf("empId" to 5),
                      "mutateProperties" to mapOf("senior" to true),
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
  @ArgumentsSource(UpdateRelationshipWithSetPropertiesParams::class)
  fun `should build relationship update statements with set properties`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val withKeys =
        UpdateRelationshipSinkAction(
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(setOf("Person", "Employee"), mapOf("id" to 1)),
                LookupMode.MATCH,
            ),
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(
                    setOf("Company", "Corporation"),
                    mapOf("id" to 7),
                ),
                LookupMode.MATCH,
            ),
            RelationshipMatcher.ByTypeAndProperties("WORKS_AT", mapOf("empId" to 5), true),
            setProperties = mapOf("senior" to true),
            mutateProperties = mapOf("empId" to 5),
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
  }

  object UpdateRelationshipWithSetPropertiesParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start:`Employee`:`Person` {`id`: _e.start.matchProperties.`id`}) " +
              "WITH _e, start " +
              "MATCH (end:`Company`:`Corporation` {`id`: _e.end.matchProperties.`id`}) " +
              "WITH _e, start, end " +
              "MATCH (start)-[r:`WORKS_AT` {`empId`: _e.matchProperties.`empId`}]->(end) " +
              "SET r = _e.setProperties SET r += _e.mutateProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to mapOf("matchProperties" to mapOf("id" to 1)),
                      "end" to mapOf("matchProperties" to mapOf("id" to 7)),
                      "matchProperties" to mapOf("empId" to 5),
                      "setProperties" to mapOf("senior" to true),
                      "mutateProperties" to mapOf("empId" to 5),
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start:${'$'}(_e.start.matchLabels) {`id`: _e.start.matchProperties.`id`}) " +
              "WITH _e, start " +
              "MATCH (end:${'$'}(_e.end.matchLabels) {`id`: _e.end.matchProperties.`id`}) " +
              "WITH _e, start, end " +
              "MATCH (start)-[r:${'$'}(_e.matchType) {`empId`: _e.matchProperties.`empId`}]->(end) " +
              "SET r = _e.setProperties SET r += _e.mutateProperties",
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
                      "mutateProperties" to mapOf("empId" to 5),
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
  @ArgumentsSource(UpdateRelationshipWithStartNodeSetPropertiesParams::class)
  fun `should build relationship update statements with start node set properties`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val withKeys =
        UpdateRelationshipSinkAction(
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(setOf("Person", "Employee"), mapOf("id" to 1)),
                LookupMode.MATCH,
                setProperties = mapOf("name" to "Alice"),
                mutateProperties = mapOf("id" to 1),
            ),
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(
                    setOf("Company", "Corporation"),
                    mapOf("id" to 7),
                ),
                LookupMode.MATCH,
            ),
            RelationshipMatcher.ByTypeAndProperties("WORKS_AT", mapOf("empId" to 5), true),
            mutateProperties = mapOf("senior" to true),
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
  }

  object UpdateRelationshipWithStartNodeSetPropertiesParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start:`Employee`:`Person` {`id`: _e.start.matchProperties.`id`}) " +
              "SET start = _e.start.setProperties SET start += _e.start.mutateProperties " +
              "WITH _e, start " +
              "MATCH (end:`Company`:`Corporation` {`id`: _e.end.matchProperties.`id`}) " +
              "WITH _e, start, end " +
              "MATCH (start)-[r:`WORKS_AT` {`empId`: _e.matchProperties.`empId`}]->(end) " +
              "SET r += _e.mutateProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to
                          mapOf(
                              "matchProperties" to mapOf("id" to 1),
                              "setProperties" to mapOf("name" to "Alice"),
                              "mutateProperties" to mapOf("id" to 1),
                          ),
                      "end" to mapOf("matchProperties" to mapOf("id" to 7)),
                      "matchProperties" to mapOf("empId" to 5),
                      "mutateProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start:${'$'}(_e.start.matchLabels) {`id`: _e.start.matchProperties.`id`}) " +
              "SET start = _e.start.setProperties SET start += _e.start.mutateProperties " +
              "WITH _e, start " +
              "MATCH (end:${'$'}(_e.end.matchLabels) {`id`: _e.end.matchProperties.`id`}) " +
              "WITH _e, start, end " +
              "MATCH (start)-[r:${'$'}(_e.matchType) {`empId`: _e.matchProperties.`empId`}]->(end) " +
              "SET r += _e.mutateProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to
                          mapOf(
                              "matchLabels" to listOf("Person", "Employee"),
                              "matchProperties" to mapOf("id" to 1),
                              "setProperties" to mapOf("name" to "Alice"),
                              "mutateProperties" to mapOf("id" to 1),
                          ),
                      "end" to
                          mapOf(
                              "matchLabels" to listOf("Company", "Corporation"),
                              "matchProperties" to mapOf("id" to 7),
                          ),
                      "matchType" to "WORKS_AT",
                      "matchProperties" to mapOf("empId" to 5),
                      "mutateProperties" to mapOf("senior" to true),
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
  @ArgumentsSource(UpdateRelationshipWithNodeAndRelationshipSetPropertiesParams::class)
  fun `should build relationship update statements with node and relationship set properties`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val withKeys =
        UpdateRelationshipSinkAction(
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(setOf("Person", "Employee"), mapOf("id" to 1)),
                LookupMode.MATCH,
                setProperties = mapOf("name" to "Alice"),
                mutateProperties = mapOf("id" to 1),
            ),
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(
                    setOf("Company", "Corporation"),
                    mapOf("id" to 7),
                ),
                LookupMode.MATCH,
                mutateProperties = mapOf("name" to "Acme Corp"),
            ),
            RelationshipMatcher.ByTypeAndProperties("WORKS_AT", mapOf("empId" to 5), true),
            setProperties = mapOf("senior" to true),
            mutateProperties = mapOf("empId" to 5),
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
  }

  object UpdateRelationshipWithNodeAndRelationshipSetPropertiesParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start:`Employee`:`Person` {`id`: _e.start.matchProperties.`id`}) " +
              "SET start = _e.start.setProperties SET start += _e.start.mutateProperties " +
              "WITH _e, start " +
              "MATCH (end:`Company`:`Corporation` {`id`: _e.end.matchProperties.`id`}) " +
              "SET end += _e.end.mutateProperties " +
              "WITH _e, start, end " +
              "MATCH (start)-[r:`WORKS_AT` {`empId`: _e.matchProperties.`empId`}]->(end) " +
              "SET r = _e.setProperties SET r += _e.mutateProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to
                          mapOf(
                              "matchProperties" to mapOf("id" to 1),
                              "setProperties" to mapOf("name" to "Alice"),
                              "mutateProperties" to mapOf("id" to 1),
                          ),
                      "end" to
                          mapOf(
                              "matchProperties" to mapOf("id" to 7),
                              "mutateProperties" to mapOf("name" to "Acme Corp"),
                          ),
                      "matchProperties" to mapOf("empId" to 5),
                      "setProperties" to mapOf("senior" to true),
                      "mutateProperties" to mapOf("empId" to 5),
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start:${'$'}(_e.start.matchLabels) {`id`: _e.start.matchProperties.`id`}) " +
              "SET start = _e.start.setProperties SET start += _e.start.mutateProperties " +
              "WITH _e, start " +
              "MATCH (end:${'$'}(_e.end.matchLabels) {`id`: _e.end.matchProperties.`id`}) " +
              "SET end += _e.end.mutateProperties " +
              "WITH _e, start, end " +
              "MATCH (start)-[r:${'$'}(_e.matchType) {`empId`: _e.matchProperties.`empId`}]->(end) " +
              "SET r = _e.setProperties SET r += _e.mutateProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to
                          mapOf(
                              "matchLabels" to listOf("Person", "Employee"),
                              "matchProperties" to mapOf("id" to 1),
                              "setProperties" to mapOf("name" to "Alice"),
                              "mutateProperties" to mapOf("id" to 1),
                          ),
                      "end" to
                          mapOf(
                              "matchLabels" to listOf("Company", "Corporation"),
                              "matchProperties" to mapOf("id" to 7),
                              "mutateProperties" to mapOf("name" to "Acme Corp"),
                          ),
                      "matchType" to "WORKS_AT",
                      "matchProperties" to mapOf("empId" to 5),
                      "setProperties" to mapOf("senior" to true),
                      "mutateProperties" to mapOf("empId" to 5),
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
                NodeMatcher.ByLabelsAndProperties(setOf("Person", "Employee"), mapOf("id" to 1)),
                LookupMode.MATCH,
            ),
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(
                    setOf("Company", "Corporation"),
                    mapOf("id" to 7),
                ),
                LookupMode.MATCH,
            ),
            RelationshipMatcher.ByTypeAndProperties("WORKS_AT", mapOf("role" to "dev"), false),
            mutateProperties = mapOf("senior" to true),
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
  }

  object MergeRelationshipWithMatchNodesWithoutKeysParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start:`Employee`:`Person` {`id`: _e.start.matchProperties.`id`}) " +
              "WITH _e, start " +
              "MATCH (end:`Company`:`Corporation` {`id`: _e.end.matchProperties.`id`}) " +
              "WITH _e, start, end " +
              "MERGE (start)-[r:`WORKS_AT` {`role`: _e.matchProperties.`role`}]->(end) " +
              "WITH _e, r LIMIT 1 " +
              "SET r += _e.mutateProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to mapOf("matchProperties" to mapOf("id" to 1)),
                      "end" to mapOf("matchProperties" to mapOf("id" to 7)),
                      "matchProperties" to mapOf("role" to "dev"),
                      "mutateProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start:${'$'}(_e.start.matchLabels) {`id`: _e.start.matchProperties.`id`}) " +
              "WITH _e, start " +
              "MATCH (end:${'$'}(_e.end.matchLabels) {`id`: _e.end.matchProperties.`id`}) " +
              "WITH _e, start, end " +
              "MERGE (start)-[r:${'$'}(_e.matchType) {`role`: _e.matchProperties.`role`}]->(end) " +
              "WITH _e, r LIMIT 1 " +
              "SET r += _e.mutateProperties",
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
                      "mutateProperties" to mapOf("senior" to true),
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
                NodeMatcher.ByLabelsAndProperties(setOf("Person", "Employee"), mapOf("id" to 1)),
                LookupMode.MERGE,
            ),
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(
                    setOf("Company", "Corporation"),
                    mapOf("id" to 7),
                ),
                LookupMode.MERGE,
            ),
            RelationshipMatcher.ByTypeAndProperties("WORKS_AT", mapOf("role" to "dev"), false),
            mutateProperties = mapOf("senior" to true),
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
  }

  object MergeRelationshipWithMergeNodesWithoutKeysParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MERGE (start:`Employee`:`Person` {`id`: _e.start.matchProperties.`id`}) " +
              "WITH _e, start " +
              "MERGE (end:`Company`:`Corporation` {`id`: _e.end.matchProperties.`id`}) " +
              "WITH _e, start, end " +
              "MERGE (start)-[r:`WORKS_AT` {`role`: _e.matchProperties.`role`}]->(end) " +
              "WITH _e, r LIMIT 1 " +
              "SET r += _e.mutateProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to mapOf("matchProperties" to mapOf("id" to 1)),
                      "end" to mapOf("matchProperties" to mapOf("id" to 7)),
                      "matchProperties" to mapOf("role" to "dev"),
                      "mutateProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MERGE (start:${'$'}(_e.start.matchLabels) {`id`: _e.start.matchProperties.`id`}) " +
              "WITH _e, start " +
              "MERGE (end:${'$'}(_e.end.matchLabels) {`id`: _e.end.matchProperties.`id`}) " +
              "WITH _e, start, end " +
              "MERGE (start)-[r:${'$'}(_e.matchType) {`role`: _e.matchProperties.`role`}]->(end) " +
              "WITH _e, r LIMIT 1 " +
              "SET r += _e.mutateProperties",
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
                      "mutateProperties" to mapOf("senior" to true),
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
                NodeMatcher.ByLabelsAndProperties(setOf("Person", "Employee"), mapOf("id" to 1)),
                LookupMode.MATCH,
            ),
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(
                    setOf("Company", "Corporation"),
                    mapOf("id" to 7),
                ),
                LookupMode.MATCH,
            ),
            RelationshipMatcher.ByTypeAndProperties("WORKS_AT", mapOf("empId" to 5), true),
            mutateProperties = mapOf("senior" to true),
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
  }

  object MergeRelationshipWithMatchNodesWithKeysParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start:`Employee`:`Person` {`id`: _e.start.matchProperties.`id`}) " +
              "WITH _e, start " +
              "MATCH (end:`Company`:`Corporation` {`id`: _e.end.matchProperties.`id`}) " +
              "WITH _e, start, end " +
              "MERGE (start)-[r:`WORKS_AT` {`empId`: _e.matchProperties.`empId`}]->(end) " +
              "SET r += _e.mutateProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to mapOf("matchProperties" to mapOf("id" to 1)),
                      "end" to mapOf("matchProperties" to mapOf("id" to 7)),
                      "matchProperties" to mapOf("empId" to 5),
                      "mutateProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start:${'$'}(_e.start.matchLabels) {`id`: _e.start.matchProperties.`id`}) " +
              "WITH _e, start " +
              "MATCH (end:${'$'}(_e.end.matchLabels) {`id`: _e.end.matchProperties.`id`}) " +
              "WITH _e, start, end " +
              "MERGE (start)-[r:${'$'}(_e.matchType) {`empId`: _e.matchProperties.`empId`}]->(end) " +
              "SET r += _e.mutateProperties",
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
                      "mutateProperties" to mapOf("senior" to true),
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
                NodeMatcher.ByLabelsAndProperties(setOf("Person", "Employee"), mapOf("id" to 1)),
                LookupMode.MERGE,
            ),
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(
                    setOf("Company", "Corporation"),
                    mapOf("id" to 7),
                ),
                LookupMode.MERGE,
            ),
            RelationshipMatcher.ByTypeAndProperties("WORKS_AT", mapOf("empId" to 5), true),
            mutateProperties = mapOf("senior" to true),
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
  }

  object MergeRelationshipWithMergeNodesWithKeysParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MERGE (start:`Employee`:`Person` {`id`: _e.start.matchProperties.`id`}) " +
              "WITH _e, start " +
              "MERGE (end:`Company`:`Corporation` {`id`: _e.end.matchProperties.`id`}) " +
              "WITH _e, start, end " +
              "MERGE (start)-[r:`WORKS_AT` {`empId`: _e.matchProperties.`empId`}]->(end) " +
              "SET r += _e.mutateProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to mapOf("matchProperties" to mapOf("id" to 1)),
                      "end" to mapOf("matchProperties" to mapOf("id" to 7)),
                      "matchProperties" to mapOf("empId" to 5),
                      "mutateProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MERGE (start:${'$'}(_e.start.matchLabels) {`id`: _e.start.matchProperties.`id`}) " +
              "WITH _e, start " +
              "MERGE (end:${'$'}(_e.end.matchLabels) {`id`: _e.end.matchProperties.`id`}) " +
              "WITH _e, start, end " +
              "MERGE (start)-[r:${'$'}(_e.matchType) {`empId`: _e.matchProperties.`empId`}]->(end) " +
              "SET r += _e.mutateProperties",
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
                      "mutateProperties" to mapOf("senior" to true),
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
  @ArgumentsSource(MergeRelationshipWithStartNodeSetPropertiesParams::class)
  fun `should build relationship merge statements with start node set properties`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val withKeys =
        MergeRelationshipSinkAction(
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(setOf("Person", "Employee"), mapOf("id" to 1)),
                LookupMode.MERGE,
                setProperties = mapOf("name" to "Alice"),
                mutateProperties = mapOf("id" to 1),
            ),
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(
                    setOf("Company", "Corporation"),
                    mapOf("id" to 7),
                ),
                LookupMode.MERGE,
            ),
            RelationshipMatcher.ByTypeAndProperties("WORKS_AT", mapOf("empId" to 5), true),
            mutateProperties = mapOf("senior" to true),
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
  }

  object MergeRelationshipWithStartNodeSetPropertiesParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MERGE (start:`Employee`:`Person` {`id`: _e.start.matchProperties.`id`}) " +
              "SET start = _e.start.setProperties SET start += _e.start.mutateProperties " +
              "WITH _e, start " +
              "MERGE (end:`Company`:`Corporation` {`id`: _e.end.matchProperties.`id`}) " +
              "WITH _e, start, end " +
              "MERGE (start)-[r:`WORKS_AT` {`empId`: _e.matchProperties.`empId`}]->(end) " +
              "SET r += _e.mutateProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to
                          mapOf(
                              "matchProperties" to mapOf("id" to 1),
                              "setProperties" to mapOf("name" to "Alice"),
                              "mutateProperties" to mapOf("id" to 1),
                          ),
                      "end" to mapOf("matchProperties" to mapOf("id" to 7)),
                      "matchProperties" to mapOf("empId" to 5),
                      "mutateProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MERGE (start:${'$'}(_e.start.matchLabels) {`id`: _e.start.matchProperties.`id`}) " +
              "SET start = _e.start.setProperties SET start += _e.start.mutateProperties " +
              "WITH _e, start " +
              "MERGE (end:${'$'}(_e.end.matchLabels) {`id`: _e.end.matchProperties.`id`}) " +
              "WITH _e, start, end " +
              "MERGE (start)-[r:${'$'}(_e.matchType) {`empId`: _e.matchProperties.`empId`}]->(end) " +
              "SET r += _e.mutateProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to
                          mapOf(
                              "matchLabels" to listOf("Person", "Employee"),
                              "matchProperties" to mapOf("id" to 1),
                              "setProperties" to mapOf("name" to "Alice"),
                              "mutateProperties" to mapOf("id" to 1),
                          ),
                      "end" to
                          mapOf(
                              "matchLabels" to listOf("Company", "Corporation"),
                              "matchProperties" to mapOf("id" to 7),
                          ),
                      "matchType" to "WORKS_AT",
                      "matchProperties" to mapOf("empId" to 5),
                      "mutateProperties" to mapOf("senior" to true),
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
  @ArgumentsSource(MergeRelationshipWithSetPropertiesParams::class)
  fun `should build relationship merge statements with set properties`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val withKeys =
        MergeRelationshipSinkAction(
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(setOf("Person", "Employee"), mapOf("id" to 1)),
                LookupMode.MERGE,
            ),
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(
                    setOf("Company", "Corporation"),
                    mapOf("id" to 7),
                ),
                LookupMode.MERGE,
            ),
            RelationshipMatcher.ByTypeAndProperties("WORKS_AT", mapOf("empId" to 5), true),
            setProperties = mapOf("senior" to true),
            mutateProperties = mapOf("empId" to 5),
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
  }

  object MergeRelationshipWithSetPropertiesParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MERGE (start:`Employee`:`Person` {`id`: _e.start.matchProperties.`id`}) " +
              "WITH _e, start " +
              "MERGE (end:`Company`:`Corporation` {`id`: _e.end.matchProperties.`id`}) " +
              "WITH _e, start, end " +
              "MERGE (start)-[r:`WORKS_AT` {`empId`: _e.matchProperties.`empId`}]->(end) " +
              "SET r = _e.setProperties SET r += _e.mutateProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to mapOf("matchProperties" to mapOf("id" to 1)),
                      "end" to mapOf("matchProperties" to mapOf("id" to 7)),
                      "matchProperties" to mapOf("empId" to 5),
                      "setProperties" to mapOf("senior" to true),
                      "mutateProperties" to mapOf("empId" to 5),
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MERGE (start:${'$'}(_e.start.matchLabels) {`id`: _e.start.matchProperties.`id`}) " +
              "WITH _e, start " +
              "MERGE (end:${'$'}(_e.end.matchLabels) {`id`: _e.end.matchProperties.`id`}) " +
              "WITH _e, start, end " +
              "MERGE (start)-[r:${'$'}(_e.matchType) {`empId`: _e.matchProperties.`empId`}]->(end) " +
              "SET r = _e.setProperties SET r += _e.mutateProperties",
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
                      "mutateProperties" to mapOf("empId" to 5),
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
  @ArgumentsSource(MergeRelationshipWithNodeAndRelationshipSetPropertiesParams::class)
  fun `should build relationship merge statements with node and relationship set properties`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val withKeys =
        MergeRelationshipSinkAction(
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(setOf("Person", "Employee"), mapOf("id" to 1)),
                LookupMode.MERGE,
                setProperties = mapOf("name" to "Alice"),
                mutateProperties = mapOf("id" to 1),
            ),
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(
                    setOf("Company", "Corporation"),
                    mapOf("id" to 7),
                ),
                LookupMode.MERGE,
                mutateProperties = mapOf("name" to "Acme Corp"),
            ),
            RelationshipMatcher.ByTypeAndProperties("WORKS_AT", mapOf("empId" to 5), true),
            setProperties = mapOf("senior" to true),
            mutateProperties = mapOf("empId" to 5),
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
  }

  object MergeRelationshipWithNodeAndRelationshipSetPropertiesParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MERGE (start:`Employee`:`Person` {`id`: _e.start.matchProperties.`id`}) " +
              "SET start = _e.start.setProperties SET start += _e.start.mutateProperties " +
              "WITH _e, start " +
              "MERGE (end:`Company`:`Corporation` {`id`: _e.end.matchProperties.`id`}) " +
              "SET end += _e.end.mutateProperties " +
              "WITH _e, start, end " +
              "MERGE (start)-[r:`WORKS_AT` {`empId`: _e.matchProperties.`empId`}]->(end) " +
              "SET r = _e.setProperties SET r += _e.mutateProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to
                          mapOf(
                              "matchProperties" to mapOf("id" to 1),
                              "setProperties" to mapOf("name" to "Alice"),
                              "mutateProperties" to mapOf("id" to 1),
                          ),
                      "end" to
                          mapOf(
                              "matchProperties" to mapOf("id" to 7),
                              "mutateProperties" to mapOf("name" to "Acme Corp"),
                          ),
                      "matchProperties" to mapOf("empId" to 5),
                      "setProperties" to mapOf("senior" to true),
                      "mutateProperties" to mapOf("empId" to 5),
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MERGE (start:${'$'}(_e.start.matchLabels) {`id`: _e.start.matchProperties.`id`}) " +
              "SET start = _e.start.setProperties SET start += _e.start.mutateProperties " +
              "WITH _e, start " +
              "MERGE (end:${'$'}(_e.end.matchLabels) {`id`: _e.end.matchProperties.`id`}) " +
              "SET end += _e.end.mutateProperties " +
              "WITH _e, start, end " +
              "MERGE (start)-[r:${'$'}(_e.matchType) {`empId`: _e.matchProperties.`empId`}]->(end) " +
              "SET r = _e.setProperties SET r += _e.mutateProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to
                          mapOf(
                              "matchLabels" to listOf("Person", "Employee"),
                              "matchProperties" to mapOf("id" to 1),
                              "setProperties" to mapOf("name" to "Alice"),
                              "mutateProperties" to mapOf("id" to 1),
                          ),
                      "end" to
                          mapOf(
                              "matchLabels" to listOf("Company", "Corporation"),
                              "matchProperties" to mapOf("id" to 7),
                              "mutateProperties" to mapOf("name" to "Acme Corp"),
                          ),
                      "matchType" to "WORKS_AT",
                      "matchProperties" to mapOf("empId" to 5),
                      "setProperties" to mapOf("senior" to true),
                      "mutateProperties" to mapOf("empId" to 5),
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
                NodeMatcher.ByLabelsAndProperties(setOf("Person", "Employee"), mapOf("id" to 1)),
                LookupMode.MATCH,
            ),
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(
                    setOf("Company", "Corporation"),
                    mapOf("id" to 7),
                ),
                LookupMode.MATCH,
            ),
            RelationshipMatcher.ByTypeAndProperties("WORKS_AT", mapOf("role" to "dev"), false),
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
  }

  object DeleteRelationshipMatchingNodesWithoutKeysParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start:`Employee`:`Person` {`id`: _e.start.matchProperties.`id`}) " +
              "WITH _e, start " +
              "MATCH (end:`Company`:`Corporation` {`id`: _e.end.matchProperties.`id`}) " +
              "WITH _e, start, end " +
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
              "WITH _e, start " +
              "MATCH (end:${'$'}(_e.end.matchLabels) {`id`: _e.end.matchProperties.`id`}) " +
              "WITH _e, start, end " +
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
                NodeMatcher.ByLabelsAndProperties(setOf("Person", "Employee"), mapOf("id" to 1)),
                LookupMode.MATCH,
            ),
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(
                    setOf("Company", "Corporation"),
                    mapOf("id" to 7),
                ),
                LookupMode.MATCH,
            ),
            RelationshipMatcher.ByTypeAndProperties("WORKS_AT", mapOf("empId" to 5), true),
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
  }

  object DeleteRelationshipMatchingNodesWithKeysParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e " +
              "MATCH (start:`Employee`:`Person` {`id`: _e.start.matchProperties.`id`}) " +
              "WITH _e, start " +
              "MATCH (end:`Company`:`Corporation` {`id`: _e.end.matchProperties.`id`}) " +
              "WITH _e, start, end " +
              "MATCH (start)-[r:`WORKS_AT` {`empId`: _e.matchProperties.`empId`}]->(end) " +
              "DELETE r",
          mapOf(
              "e" to
                  mapOf(
                      "start" to mapOf("matchProperties" to mapOf("id" to 1)),
                      "end" to mapOf("matchProperties" to mapOf("id" to 7)),
                      "matchProperties" to mapOf("empId" to 5),
                  )
          ),
      )
    }

    fun dynamicLabelsQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e " +
              "MATCH (start:\$(_e.start.matchLabels) {`id`: _e.start.matchProperties.`id`}) " +
              "WITH _e, start " +
              "MATCH (end:\$(_e.end.matchLabels) {`id`: _e.end.matchProperties.`id`}) " +
              "WITH _e, start, end " +
              "MATCH (start)-[r:${'$'}(_e.matchType) {`empId`: _e.matchProperties.`empId`}]->(end) " +
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
                      "matchProperties" to mapOf("empId" to 5),
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
          Arguments.of(neo4j2026_1, dynamicLabelsQuery()),
          Arguments.of(neo4jAura, dynamicLabelsQuery()),
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(DeleteRelationshipMatchingAnyNodeWithKeysParams::class)
  fun `should build relationship delete statements with keys matching any node`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val withKeys =
        DeleteRelationshipSinkAction(
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(emptySet(), emptyMap()),
                LookupMode.MATCH,
            ),
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(emptySet(), emptyMap()),
                LookupMode.MATCH,
            ),
            RelationshipMatcher.ByTypeAndProperties("WORKS_AT", mapOf("empId" to 5), true),
        )

    generator.buildStatement(withKeys) shouldBe expectedQuery
  }

  object DeleteRelationshipMatchingAnyNodeWithKeysParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e " +
              "MATCH (start) " +
              "WITH _e, start " +
              "MATCH (end) " +
              "WITH _e, start, end " +
              "MATCH (start)-[r:`WORKS_AT` {`empId`: _e.matchProperties.`empId`}]->(end) " +
              "DELETE r",
          mapOf(
              "e" to
                  mapOf(
                      "start" to mapOf("matchProperties" to emptyMap<String, Any>()),
                      "end" to mapOf("matchProperties" to emptyMap<String, Any>()),
                      "matchProperties" to mapOf("empId" to 5),
                  )
          ),
      )
    }

    fun dynamicLabelsQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e " +
              "MATCH (start:\$(_e.start.matchLabels)) " +
              "WITH _e, start " +
              "MATCH (end:\$(_e.end.matchLabels)) " +
              "WITH _e, start, end " +
              "MATCH (start)-[r:${'$'}(_e.matchType) {`empId`: _e.matchProperties.`empId`}]->(end) " +
              "DELETE r",
          mapOf(
              "e" to
                  mapOf(
                      "start" to
                          mapOf(
                              "matchLabels" to emptySet<String>(),
                              "matchProperties" to emptyMap<String, Any>(),
                          ),
                      "end" to
                          mapOf(
                              "matchLabels" to emptySet<String>(),
                              "matchProperties" to emptyMap<String, Any>(),
                          ),
                      "matchType" to "WORKS_AT",
                      "matchProperties" to mapOf("empId" to 5),
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
          Arguments.of(neo4j2026_1, dynamicLabelsQuery()),
          Arguments.of(neo4jAura, dynamicLabelsQuery()),
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(UpdateRelationshipByIdMatcherParams::class)
  fun `should build relationship update statements with id matcher`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val action =
        UpdateRelationshipSinkAction(
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(setOf("Person"), mapOf("id" to 1)),
                LookupMode.MATCH,
            ),
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(setOf("Company"), mapOf("id" to 7)),
                LookupMode.MATCH,
            ),
            RelationshipMatcher.ById(42),
            mutateProperties = mapOf("senior" to true),
        )

    generator.buildStatement(action) shouldBe expectedQuery
  }

  object UpdateRelationshipByIdMatcherParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start:`Person` {`id`: _e.start.matchProperties.`id`}) " +
              "WITH _e, start " +
              "MATCH (end:`Company` {`id`: _e.end.matchProperties.`id`}) " +
              "WITH _e, start, end " +
              "MATCH (start)-[r]->(end) WHERE id(r) = _e.matchId " +
              "SET r += _e.mutateProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to mapOf("matchProperties" to mapOf("id" to 1)),
                      "end" to mapOf("matchProperties" to mapOf("id" to 7)),
                      "matchId" to 42,
                      "mutateProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start:${'$'}(_e.start.matchLabels) {`id`: _e.start.matchProperties.`id`}) " +
              "WITH _e, start " +
              "MATCH (end:${'$'}(_e.end.matchLabels) {`id`: _e.end.matchProperties.`id`}) " +
              "WITH _e, start, end " +
              "MATCH (start)-[r]->(end) WHERE id(r) = _e.matchId " +
              "SET r += _e.mutateProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to
                          mapOf(
                              "matchLabels" to listOf("Person"),
                              "matchProperties" to mapOf("id" to 1),
                          ),
                      "end" to
                          mapOf(
                              "matchLabels" to listOf("Company"),
                              "matchProperties" to mapOf("id" to 7),
                          ),
                      "matchId" to 42,
                      "mutateProperties" to mapOf("senior" to true),
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
  @ArgumentsSource(UpdateRelationshipByElementIdMatcherParams::class)
  fun `should build relationship update statements with element id matcher`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val action =
        UpdateRelationshipSinkAction(
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(setOf("Person"), mapOf("id" to 1)),
                LookupMode.MATCH,
            ),
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(setOf("Company"), mapOf("id" to 7)),
                LookupMode.MATCH,
            ),
            RelationshipMatcher.ByElementId("5:rel:42"),
            mutateProperties = mapOf("senior" to true),
        )

    generator.buildStatement(action) shouldBe expectedQuery
  }

  object UpdateRelationshipByElementIdMatcherParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start:`Person` {`id`: _e.start.matchProperties.`id`}) " +
              "WITH _e, start " +
              "MATCH (end:`Company` {`id`: _e.end.matchProperties.`id`}) " +
              "WITH _e, start, end " +
              "MATCH (start)-[r]->(end) WHERE elementId(r) = _e.matchElementId " +
              "SET r += _e.mutateProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to mapOf("matchProperties" to mapOf("id" to 1)),
                      "end" to mapOf("matchProperties" to mapOf("id" to 7)),
                      "matchElementId" to "5:rel:42",
                      "mutateProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start:${'$'}(_e.start.matchLabels) {`id`: _e.start.matchProperties.`id`}) " +
              "WITH _e, start " +
              "MATCH (end:${'$'}(_e.end.matchLabels) {`id`: _e.end.matchProperties.`id`}) " +
              "WITH _e, start, end " +
              "MATCH (start)-[r]->(end) WHERE elementId(r) = _e.matchElementId " +
              "SET r += _e.mutateProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to
                          mapOf(
                              "matchLabels" to listOf("Person"),
                              "matchProperties" to mapOf("id" to 1),
                          ),
                      "end" to
                          mapOf(
                              "matchLabels" to listOf("Company"),
                              "matchProperties" to mapOf("id" to 7),
                          ),
                      "matchElementId" to "5:rel:42",
                      "mutateProperties" to mapOf("senior" to true),
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
  @ArgumentsSource(MergeRelationshipByIdMatcherParams::class)
  fun `should build relationship merge statements with id matcher`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val action =
        MergeRelationshipSinkAction(
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(setOf("Person"), mapOf("id" to 1)),
                LookupMode.MATCH,
            ),
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(setOf("Company"), mapOf("id" to 7)),
                LookupMode.MATCH,
            ),
            RelationshipMatcher.ById(42),
            mutateProperties = mapOf("senior" to true),
        )

    generator.buildStatement(action) shouldBe expectedQuery
  }

  object MergeRelationshipByIdMatcherParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start:`Person` {`id`: _e.start.matchProperties.`id`}) " +
              "WITH _e, start " +
              "MATCH (end:`Company` {`id`: _e.end.matchProperties.`id`}) " +
              "WITH _e, start, end " +
              "MERGE (start)-[r]->(end) WHERE id(r) = _e.matchId " +
              "SET r += _e.mutateProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to mapOf("matchProperties" to mapOf("id" to 1)),
                      "end" to mapOf("matchProperties" to mapOf("id" to 7)),
                      "matchId" to 42,
                      "mutateProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start:${'$'}(_e.start.matchLabels) {`id`: _e.start.matchProperties.`id`}) " +
              "WITH _e, start " +
              "MATCH (end:${'$'}(_e.end.matchLabels) {`id`: _e.end.matchProperties.`id`}) " +
              "WITH _e, start, end " +
              "MERGE (start)-[r]->(end) WHERE id(r) = _e.matchId " +
              "SET r += _e.mutateProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to
                          mapOf(
                              "matchLabels" to listOf("Person"),
                              "matchProperties" to mapOf("id" to 1),
                          ),
                      "end" to
                          mapOf(
                              "matchLabels" to listOf("Company"),
                              "matchProperties" to mapOf("id" to 7),
                          ),
                      "matchId" to 42,
                      "mutateProperties" to mapOf("senior" to true),
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
  @ArgumentsSource(MergeRelationshipByElementIdMatcherParams::class)
  fun `should build relationship merge statements with element id matcher`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val action =
        MergeRelationshipSinkAction(
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(setOf("Person"), mapOf("id" to 1)),
                LookupMode.MATCH,
            ),
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(setOf("Company"), mapOf("id" to 7)),
                LookupMode.MATCH,
            ),
            RelationshipMatcher.ByElementId("5:rel:42"),
            mutateProperties = mapOf("senior" to true),
        )

    generator.buildStatement(action) shouldBe expectedQuery
  }

  object MergeRelationshipByElementIdMatcherParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start:`Person` {`id`: _e.start.matchProperties.`id`}) " +
              "WITH _e, start " +
              "MATCH (end:`Company` {`id`: _e.end.matchProperties.`id`}) " +
              "WITH _e, start, end " +
              "MERGE (start)-[r]->(end) WHERE elementId(r) = _e.matchElementId " +
              "SET r += _e.mutateProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to mapOf("matchProperties" to mapOf("id" to 1)),
                      "end" to mapOf("matchProperties" to mapOf("id" to 7)),
                      "matchElementId" to "5:rel:42",
                      "mutateProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start:${'$'}(_e.start.matchLabels) {`id`: _e.start.matchProperties.`id`}) " +
              "WITH _e, start " +
              "MATCH (end:${'$'}(_e.end.matchLabels) {`id`: _e.end.matchProperties.`id`}) " +
              "WITH _e, start, end " +
              "MERGE (start)-[r]->(end) WHERE elementId(r) = _e.matchElementId " +
              "SET r += _e.mutateProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to
                          mapOf(
                              "matchLabels" to listOf("Person"),
                              "matchProperties" to mapOf("id" to 1),
                          ),
                      "end" to
                          mapOf(
                              "matchLabels" to listOf("Company"),
                              "matchProperties" to mapOf("id" to 7),
                          ),
                      "matchElementId" to "5:rel:42",
                      "mutateProperties" to mapOf("senior" to true),
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
  @ArgumentsSource(DeleteRelationshipByIdMatcherParams::class)
  fun `should build relationship delete statements with id matcher`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val action =
        DeleteRelationshipSinkAction(
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(setOf("Person"), mapOf("id" to 1)),
                LookupMode.MATCH,
            ),
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(setOf("Company"), mapOf("id" to 7)),
                LookupMode.MATCH,
            ),
            RelationshipMatcher.ById(42),
        )

    generator.buildStatement(action) shouldBe expectedQuery
  }

  object DeleteRelationshipByIdMatcherParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start:`Person` {`id`: _e.start.matchProperties.`id`}) " +
              "WITH _e, start " +
              "MATCH (end:`Company` {`id`: _e.end.matchProperties.`id`}) " +
              "WITH _e, start, end " +
              "MATCH (start)-[r]->(end) WHERE id(r) = _e.matchId " +
              "DELETE r",
          mapOf(
              "e" to
                  mapOf(
                      "start" to mapOf("matchProperties" to mapOf("id" to 1)),
                      "end" to mapOf("matchProperties" to mapOf("id" to 7)),
                      "matchId" to 42,
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start:${'$'}(_e.start.matchLabels) {`id`: _e.start.matchProperties.`id`}) " +
              "WITH _e, start " +
              "MATCH (end:${'$'}(_e.end.matchLabels) {`id`: _e.end.matchProperties.`id`}) " +
              "WITH _e, start, end " +
              "MATCH (start)-[r]->(end) WHERE id(r) = _e.matchId " +
              "DELETE r",
          mapOf(
              "e" to
                  mapOf(
                      "start" to
                          mapOf(
                              "matchLabels" to listOf("Person"),
                              "matchProperties" to mapOf("id" to 1),
                          ),
                      "end" to
                          mapOf(
                              "matchLabels" to listOf("Company"),
                              "matchProperties" to mapOf("id" to 7),
                          ),
                      "matchId" to 42,
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
  @ArgumentsSource(DeleteRelationshipByElementIdMatcherParams::class)
  fun `should build relationship delete statements with element id matcher`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val action =
        DeleteRelationshipSinkAction(
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(setOf("Person"), mapOf("id" to 1)),
                LookupMode.MATCH,
            ),
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(setOf("Company"), mapOf("id" to 7)),
                LookupMode.MATCH,
            ),
            RelationshipMatcher.ByElementId("5:rel:42"),
        )

    generator.buildStatement(action) shouldBe expectedQuery
  }

  object DeleteRelationshipByElementIdMatcherParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start:`Person` {`id`: _e.start.matchProperties.`id`}) " +
              "WITH _e, start " +
              "MATCH (end:`Company` {`id`: _e.end.matchProperties.`id`}) " +
              "WITH _e, start, end " +
              "MATCH (start)-[r]->(end) WHERE elementId(r) = _e.matchElementId " +
              "DELETE r",
          mapOf(
              "e" to
                  mapOf(
                      "start" to mapOf("matchProperties" to mapOf("id" to 1)),
                      "end" to mapOf("matchProperties" to mapOf("id" to 7)),
                      "matchElementId" to "5:rel:42",
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start:${'$'}(_e.start.matchLabels) {`id`: _e.start.matchProperties.`id`}) " +
              "WITH _e, start " +
              "MATCH (end:${'$'}(_e.end.matchLabels) {`id`: _e.end.matchProperties.`id`}) " +
              "WITH _e, start, end " +
              "MATCH (start)-[r]->(end) WHERE elementId(r) = _e.matchElementId " +
              "DELETE r",
          mapOf(
              "e" to
                  mapOf(
                      "start" to
                          mapOf(
                              "matchLabels" to listOf("Person"),
                              "matchProperties" to mapOf("id" to 1),
                          ),
                      "end" to
                          mapOf(
                              "matchLabels" to listOf("Company"),
                              "matchProperties" to mapOf("id" to 7),
                          ),
                      "matchElementId" to "5:rel:42",
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
  @ArgumentsSource(CreateRelationshipWithNodeIdMatchersParams::class)
  fun `should build relationship create statements with node id matchers`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val action =
        CreateRelationshipSinkAction(
            SinkActionNodeReference(NodeMatcher.ById(1), LookupMode.MATCH),
            SinkActionNodeReference(NodeMatcher.ById(7), LookupMode.MATCH),
            "WORKS_AT",
            mapOf("role" to "dev"),
        )

    generator.buildStatement(action) shouldBe expectedQuery
  }

  object CreateRelationshipWithNodeIdMatchersParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start) WHERE id(start) = _e.start.matchId " +
              "WITH _e, start " +
              "MATCH (end) WHERE id(end) = _e.end.matchId " +
              "WITH _e, start, end " +
              "CREATE (start)-[r:`WORKS_AT`]->(end) " +
              "SET r += _e.properties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to mapOf("matchId" to 1),
                      "end" to mapOf("matchId" to 7),
                      "properties" to mapOf("role" to "dev"),
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start) WHERE id(start) = _e.start.matchId " +
              "WITH _e, start " +
              "MATCH (end) WHERE id(end) = _e.end.matchId " +
              "WITH _e, start, end " +
              "CREATE (start)-[r:${'$'}(_e.type)]->(end) " +
              "SET r += _e.properties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to mapOf("matchId" to 1),
                      "end" to mapOf("matchId" to 7),
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
  @ArgumentsSource(CreateRelationshipWithNodeIdMatchersAndSetPropertiesParams::class)
  fun `should build relationship create statements with node id matchers and set properties`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val action =
        CreateRelationshipSinkAction(
            SinkActionNodeReference(
                NodeMatcher.ById(1),
                LookupMode.MATCH,
                setProperties = mapOf("name" to "Alice"),
                mutateProperties = mapOf("id" to 1),
            ),
            SinkActionNodeReference(
                NodeMatcher.ById(7),
                LookupMode.MATCH,
                mutateProperties = mapOf("name" to "Acme Corp"),
            ),
            "WORKS_AT",
            mapOf("role" to "dev"),
        )

    generator.buildStatement(action) shouldBe expectedQuery
  }

  object CreateRelationshipWithNodeIdMatchersAndSetPropertiesParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start) WHERE id(start) = _e.start.matchId " +
              "SET start = _e.start.setProperties SET start += _e.start.mutateProperties " +
              "WITH _e, start " +
              "MATCH (end) WHERE id(end) = _e.end.matchId " +
              "SET end += _e.end.mutateProperties " +
              "WITH _e, start, end " +
              "CREATE (start)-[r:`WORKS_AT`]->(end) " +
              "SET r += _e.properties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to
                          mapOf(
                              "matchId" to 1,
                              "setProperties" to mapOf("name" to "Alice"),
                              "mutateProperties" to mapOf("id" to 1),
                          ),
                      "end" to
                          mapOf("matchId" to 7, "mutateProperties" to mapOf("name" to "Acme Corp")),
                      "properties" to mapOf("role" to "dev"),
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start) WHERE id(start) = _e.start.matchId " +
              "SET start = _e.start.setProperties SET start += _e.start.mutateProperties " +
              "WITH _e, start " +
              "MATCH (end) WHERE id(end) = _e.end.matchId " +
              "SET end += _e.end.mutateProperties " +
              "WITH _e, start, end " +
              "CREATE (start)-[r:${'$'}(_e.type)]->(end) " +
              "SET r += _e.properties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to
                          mapOf(
                              "matchId" to 1,
                              "setProperties" to mapOf("name" to "Alice"),
                              "mutateProperties" to mapOf("id" to 1),
                          ),
                      "end" to
                          mapOf("matchId" to 7, "mutateProperties" to mapOf("name" to "Acme Corp")),
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
  @ArgumentsSource(CreateRelationshipWithNodeElementIdMatchersParams::class)
  fun `should build relationship create statements with node element id matchers`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val action =
        CreateRelationshipSinkAction(
            SinkActionNodeReference(NodeMatcher.ByElementId("4:abc:1"), LookupMode.MATCH),
            SinkActionNodeReference(NodeMatcher.ByElementId("4:abc:7"), LookupMode.MATCH),
            "WORKS_AT",
            mapOf("role" to "dev"),
        )

    generator.buildStatement(action) shouldBe expectedQuery
  }

  object CreateRelationshipWithNodeElementIdMatchersParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start) WHERE elementId(start) = _e.start.matchElementId " +
              "WITH _e, start " +
              "MATCH (end) WHERE elementId(end) = _e.end.matchElementId " +
              "WITH _e, start, end " +
              "CREATE (start)-[r:`WORKS_AT`]->(end) " +
              "SET r += _e.properties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to mapOf("matchElementId" to "4:abc:1"),
                      "end" to mapOf("matchElementId" to "4:abc:7"),
                      "properties" to mapOf("role" to "dev"),
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start) WHERE elementId(start) = _e.start.matchElementId " +
              "WITH _e, start " +
              "MATCH (end) WHERE elementId(end) = _e.end.matchElementId " +
              "WITH _e, start, end " +
              "CREATE (start)-[r:${'$'}(_e.type)]->(end) " +
              "SET r += _e.properties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to mapOf("matchElementId" to "4:abc:1"),
                      "end" to mapOf("matchElementId" to "4:abc:7"),
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
  @ArgumentsSource(CreateRelationshipWithNodeElementIdMatchersAndSetPropertiesParams::class)
  fun `should build relationship create statements with node element id matchers and set properties`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val action =
        CreateRelationshipSinkAction(
            SinkActionNodeReference(
                NodeMatcher.ByElementId("4:abc:1"),
                LookupMode.MATCH,
                setProperties = mapOf("name" to "Alice"),
                mutateProperties = mapOf("id" to 1),
            ),
            SinkActionNodeReference(
                NodeMatcher.ByElementId("4:abc:7"),
                LookupMode.MATCH,
                mutateProperties = mapOf("name" to "Acme Corp"),
            ),
            "WORKS_AT",
            mapOf("role" to "dev"),
        )

    generator.buildStatement(action) shouldBe expectedQuery
  }

  object CreateRelationshipWithNodeElementIdMatchersAndSetPropertiesParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start) WHERE elementId(start) = _e.start.matchElementId " +
              "SET start = _e.start.setProperties SET start += _e.start.mutateProperties " +
              "WITH _e, start " +
              "MATCH (end) WHERE elementId(end) = _e.end.matchElementId " +
              "SET end += _e.end.mutateProperties " +
              "WITH _e, start, end " +
              "CREATE (start)-[r:`WORKS_AT`]->(end) " +
              "SET r += _e.properties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to
                          mapOf(
                              "matchElementId" to "4:abc:1",
                              "setProperties" to mapOf("name" to "Alice"),
                              "mutateProperties" to mapOf("id" to 1),
                          ),
                      "end" to
                          mapOf(
                              "matchElementId" to "4:abc:7",
                              "mutateProperties" to mapOf("name" to "Acme Corp"),
                          ),
                      "properties" to mapOf("role" to "dev"),
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start) WHERE elementId(start) = _e.start.matchElementId " +
              "SET start = _e.start.setProperties SET start += _e.start.mutateProperties " +
              "WITH _e, start " +
              "MATCH (end) WHERE elementId(end) = _e.end.matchElementId " +
              "SET end += _e.end.mutateProperties " +
              "WITH _e, start, end " +
              "CREATE (start)-[r:${'$'}(_e.type)]->(end) " +
              "SET r += _e.properties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to
                          mapOf(
                              "matchElementId" to "4:abc:1",
                              "setProperties" to mapOf("name" to "Alice"),
                              "mutateProperties" to mapOf("id" to 1),
                          ),
                      "end" to
                          mapOf(
                              "matchElementId" to "4:abc:7",
                              "mutateProperties" to mapOf("name" to "Acme Corp"),
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
  @ArgumentsSource(UpdateRelationshipWithNodeIdMatchersParams::class)
  fun `should build relationship update statements with node id matchers`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val action =
        UpdateRelationshipSinkAction(
            SinkActionNodeReference(NodeMatcher.ById(1), LookupMode.MATCH),
            SinkActionNodeReference(NodeMatcher.ById(7), LookupMode.MATCH),
            RelationshipMatcher.ByTypeAndProperties("WORKS_AT", mapOf("role" to "dev"), true),
            mutateProperties = mapOf("senior" to true),
        )

    generator.buildStatement(action) shouldBe expectedQuery
  }

  object UpdateRelationshipWithNodeIdMatchersParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start) WHERE id(start) = _e.start.matchId " +
              "WITH _e, start " +
              "MATCH (end) WHERE id(end) = _e.end.matchId " +
              "WITH _e, start, end " +
              "MATCH (start)-[r:`WORKS_AT` {`role`: _e.matchProperties.`role`}]->(end) " +
              "SET r += _e.mutateProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to mapOf("matchId" to 1),
                      "end" to mapOf("matchId" to 7),
                      "matchProperties" to mapOf("role" to "dev"),
                      "mutateProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start) WHERE id(start) = _e.start.matchId " +
              "WITH _e, start " +
              "MATCH (end) WHERE id(end) = _e.end.matchId " +
              "WITH _e, start, end " +
              "MATCH (start)-[r:${'$'}(_e.matchType) {`role`: _e.matchProperties.`role`}]->(end) " +
              "SET r += _e.mutateProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to mapOf("matchId" to 1),
                      "end" to mapOf("matchId" to 7),
                      "matchType" to "WORKS_AT",
                      "matchProperties" to mapOf("role" to "dev"),
                      "mutateProperties" to mapOf("senior" to true),
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
  @ArgumentsSource(UpdateRelationshipWithNodeIdMatchersAndSetPropertiesParams::class)
  fun `should build relationship update statements with node id matchers and set properties`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val action =
        UpdateRelationshipSinkAction(
            SinkActionNodeReference(
                NodeMatcher.ById(1),
                LookupMode.MATCH,
                setProperties = mapOf("name" to "Alice"),
                mutateProperties = mapOf("id" to 1),
            ),
            SinkActionNodeReference(
                NodeMatcher.ById(7),
                LookupMode.MATCH,
                mutateProperties = mapOf("name" to "Acme Corp"),
            ),
            RelationshipMatcher.ByTypeAndProperties("WORKS_AT", mapOf("role" to "dev"), true),
            mutateProperties = mapOf("senior" to true),
        )

    generator.buildStatement(action) shouldBe expectedQuery
  }

  object UpdateRelationshipWithNodeIdMatchersAndSetPropertiesParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start) WHERE id(start) = _e.start.matchId " +
              "SET start = _e.start.setProperties SET start += _e.start.mutateProperties " +
              "WITH _e, start " +
              "MATCH (end) WHERE id(end) = _e.end.matchId " +
              "SET end += _e.end.mutateProperties " +
              "WITH _e, start, end " +
              "MATCH (start)-[r:`WORKS_AT` {`role`: _e.matchProperties.`role`}]->(end) " +
              "SET r += _e.mutateProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to
                          mapOf(
                              "matchId" to 1,
                              "setProperties" to mapOf("name" to "Alice"),
                              "mutateProperties" to mapOf("id" to 1),
                          ),
                      "end" to
                          mapOf("matchId" to 7, "mutateProperties" to mapOf("name" to "Acme Corp")),
                      "matchProperties" to mapOf("role" to "dev"),
                      "mutateProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start) WHERE id(start) = _e.start.matchId " +
              "SET start = _e.start.setProperties SET start += _e.start.mutateProperties " +
              "WITH _e, start " +
              "MATCH (end) WHERE id(end) = _e.end.matchId " +
              "SET end += _e.end.mutateProperties " +
              "WITH _e, start, end " +
              "MATCH (start)-[r:${'$'}(_e.matchType) {`role`: _e.matchProperties.`role`}]->(end) " +
              "SET r += _e.mutateProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to
                          mapOf(
                              "matchId" to 1,
                              "setProperties" to mapOf("name" to "Alice"),
                              "mutateProperties" to mapOf("id" to 1),
                          ),
                      "end" to
                          mapOf("matchId" to 7, "mutateProperties" to mapOf("name" to "Acme Corp")),
                      "matchType" to "WORKS_AT",
                      "matchProperties" to mapOf("role" to "dev"),
                      "mutateProperties" to mapOf("senior" to true),
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
  @ArgumentsSource(UpdateRelationshipWithNodeElementIdMatchersParams::class)
  fun `should build relationship update statements with node element id matchers`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val action =
        UpdateRelationshipSinkAction(
            SinkActionNodeReference(NodeMatcher.ByElementId("4:abc:1"), LookupMode.MATCH),
            SinkActionNodeReference(NodeMatcher.ByElementId("4:abc:7"), LookupMode.MATCH),
            RelationshipMatcher.ByTypeAndProperties("WORKS_AT", mapOf("role" to "dev"), true),
            mutateProperties = mapOf("senior" to true),
        )

    generator.buildStatement(action) shouldBe expectedQuery
  }

  object UpdateRelationshipWithNodeElementIdMatchersParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start) WHERE elementId(start) = _e.start.matchElementId " +
              "WITH _e, start " +
              "MATCH (end) WHERE elementId(end) = _e.end.matchElementId " +
              "WITH _e, start, end " +
              "MATCH (start)-[r:`WORKS_AT` {`role`: _e.matchProperties.`role`}]->(end) " +
              "SET r += _e.mutateProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to mapOf("matchElementId" to "4:abc:1"),
                      "end" to mapOf("matchElementId" to "4:abc:7"),
                      "matchProperties" to mapOf("role" to "dev"),
                      "mutateProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start) WHERE elementId(start) = _e.start.matchElementId " +
              "WITH _e, start " +
              "MATCH (end) WHERE elementId(end) = _e.end.matchElementId " +
              "WITH _e, start, end " +
              "MATCH (start)-[r:${'$'}(_e.matchType) {`role`: _e.matchProperties.`role`}]->(end) " +
              "SET r += _e.mutateProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to mapOf("matchElementId" to "4:abc:1"),
                      "end" to mapOf("matchElementId" to "4:abc:7"),
                      "matchType" to "WORKS_AT",
                      "matchProperties" to mapOf("role" to "dev"),
                      "mutateProperties" to mapOf("senior" to true),
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
  @ArgumentsSource(UpdateRelationshipWithNodeElementIdMatchersAndSetPropertiesParams::class)
  fun `should build relationship update statements with node element id matchers and set properties`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val action =
        UpdateRelationshipSinkAction(
            SinkActionNodeReference(
                NodeMatcher.ByElementId("4:abc:1"),
                LookupMode.MATCH,
                setProperties = mapOf("name" to "Alice"),
                mutateProperties = mapOf("id" to 1),
            ),
            SinkActionNodeReference(
                NodeMatcher.ByElementId("4:abc:7"),
                LookupMode.MATCH,
                mutateProperties = mapOf("name" to "Acme Corp"),
            ),
            RelationshipMatcher.ByTypeAndProperties("WORKS_AT", mapOf("role" to "dev"), true),
            mutateProperties = mapOf("senior" to true),
        )

    generator.buildStatement(action) shouldBe expectedQuery
  }

  object UpdateRelationshipWithNodeElementIdMatchersAndSetPropertiesParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start) WHERE elementId(start) = _e.start.matchElementId " +
              "SET start = _e.start.setProperties SET start += _e.start.mutateProperties " +
              "WITH _e, start " +
              "MATCH (end) WHERE elementId(end) = _e.end.matchElementId " +
              "SET end += _e.end.mutateProperties " +
              "WITH _e, start, end " +
              "MATCH (start)-[r:`WORKS_AT` {`role`: _e.matchProperties.`role`}]->(end) " +
              "SET r += _e.mutateProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to
                          mapOf(
                              "matchElementId" to "4:abc:1",
                              "setProperties" to mapOf("name" to "Alice"),
                              "mutateProperties" to mapOf("id" to 1),
                          ),
                      "end" to
                          mapOf(
                              "matchElementId" to "4:abc:7",
                              "mutateProperties" to mapOf("name" to "Acme Corp"),
                          ),
                      "matchProperties" to mapOf("role" to "dev"),
                      "mutateProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start) WHERE elementId(start) = _e.start.matchElementId " +
              "SET start = _e.start.setProperties SET start += _e.start.mutateProperties " +
              "WITH _e, start " +
              "MATCH (end) WHERE elementId(end) = _e.end.matchElementId " +
              "SET end += _e.end.mutateProperties " +
              "WITH _e, start, end " +
              "MATCH (start)-[r:${'$'}(_e.matchType) {`role`: _e.matchProperties.`role`}]->(end) " +
              "SET r += _e.mutateProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to
                          mapOf(
                              "matchElementId" to "4:abc:1",
                              "setProperties" to mapOf("name" to "Alice"),
                              "mutateProperties" to mapOf("id" to 1),
                          ),
                      "end" to
                          mapOf(
                              "matchElementId" to "4:abc:7",
                              "mutateProperties" to mapOf("name" to "Acme Corp"),
                          ),
                      "matchType" to "WORKS_AT",
                      "matchProperties" to mapOf("role" to "dev"),
                      "mutateProperties" to mapOf("senior" to true),
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
  @ArgumentsSource(MergeRelationshipWithNodeIdMatchersParams::class)
  fun `should build relationship merge statements with node id matchers`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val action =
        MergeRelationshipSinkAction(
            SinkActionNodeReference(NodeMatcher.ById(1), LookupMode.MATCH),
            SinkActionNodeReference(NodeMatcher.ById(7), LookupMode.MATCH),
            RelationshipMatcher.ByTypeAndProperties("WORKS_AT", mapOf("role" to "dev"), true),
            mutateProperties = mapOf("senior" to true),
        )

    generator.buildStatement(action) shouldBe expectedQuery
  }

  object MergeRelationshipWithNodeIdMatchersParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start) WHERE id(start) = _e.start.matchId " +
              "WITH _e, start " +
              "MATCH (end) WHERE id(end) = _e.end.matchId " +
              "WITH _e, start, end " +
              "MERGE (start)-[r:`WORKS_AT` {`role`: _e.matchProperties.`role`}]->(end) " +
              "SET r += _e.mutateProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to mapOf("matchId" to 1),
                      "end" to mapOf("matchId" to 7),
                      "matchProperties" to mapOf("role" to "dev"),
                      "mutateProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start) WHERE id(start) = _e.start.matchId " +
              "WITH _e, start " +
              "MATCH (end) WHERE id(end) = _e.end.matchId " +
              "WITH _e, start, end " +
              "MERGE (start)-[r:${'$'}(_e.matchType) {`role`: _e.matchProperties.`role`}]->(end) " +
              "SET r += _e.mutateProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to mapOf("matchId" to 1),
                      "end" to mapOf("matchId" to 7),
                      "matchType" to "WORKS_AT",
                      "matchProperties" to mapOf("role" to "dev"),
                      "mutateProperties" to mapOf("senior" to true),
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
  @ArgumentsSource(MergeRelationshipWithNodeIdMatchersAndSetPropertiesParams::class)
  fun `should build relationship merge statements with node id matchers and set properties`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val action =
        MergeRelationshipSinkAction(
            SinkActionNodeReference(
                NodeMatcher.ById(1),
                LookupMode.MATCH,
                setProperties = mapOf("name" to "Alice"),
                mutateProperties = mapOf("id" to 1),
            ),
            SinkActionNodeReference(
                NodeMatcher.ById(7),
                LookupMode.MATCH,
                mutateProperties = mapOf("name" to "Acme Corp"),
            ),
            RelationshipMatcher.ByTypeAndProperties("WORKS_AT", mapOf("role" to "dev"), true),
            mutateProperties = mapOf("senior" to true),
        )

    generator.buildStatement(action) shouldBe expectedQuery
  }

  object MergeRelationshipWithNodeIdMatchersAndSetPropertiesParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start) WHERE id(start) = _e.start.matchId " +
              "SET start = _e.start.setProperties SET start += _e.start.mutateProperties " +
              "WITH _e, start " +
              "MATCH (end) WHERE id(end) = _e.end.matchId " +
              "SET end += _e.end.mutateProperties " +
              "WITH _e, start, end " +
              "MERGE (start)-[r:`WORKS_AT` {`role`: _e.matchProperties.`role`}]->(end) " +
              "SET r += _e.mutateProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to
                          mapOf(
                              "matchId" to 1,
                              "setProperties" to mapOf("name" to "Alice"),
                              "mutateProperties" to mapOf("id" to 1),
                          ),
                      "end" to
                          mapOf("matchId" to 7, "mutateProperties" to mapOf("name" to "Acme Corp")),
                      "matchProperties" to mapOf("role" to "dev"),
                      "mutateProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start) WHERE id(start) = _e.start.matchId " +
              "SET start = _e.start.setProperties SET start += _e.start.mutateProperties " +
              "WITH _e, start " +
              "MATCH (end) WHERE id(end) = _e.end.matchId " +
              "SET end += _e.end.mutateProperties " +
              "WITH _e, start, end " +
              "MERGE (start)-[r:${'$'}(_e.matchType) {`role`: _e.matchProperties.`role`}]->(end) " +
              "SET r += _e.mutateProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to
                          mapOf(
                              "matchId" to 1,
                              "setProperties" to mapOf("name" to "Alice"),
                              "mutateProperties" to mapOf("id" to 1),
                          ),
                      "end" to
                          mapOf("matchId" to 7, "mutateProperties" to mapOf("name" to "Acme Corp")),
                      "matchType" to "WORKS_AT",
                      "matchProperties" to mapOf("role" to "dev"),
                      "mutateProperties" to mapOf("senior" to true),
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
  @ArgumentsSource(MergeRelationshipWithNodeElementIdMatchersParams::class)
  fun `should build relationship merge statements with node element id matchers`(
      neo4j: Neo4j,
      expectedQuery: Query,
  ) {
    val generator = DefaultSinkActionStatementGenerator(neo4j)
    val action =
        MergeRelationshipSinkAction(
            SinkActionNodeReference(NodeMatcher.ByElementId("4:abc:1"), LookupMode.MATCH),
            SinkActionNodeReference(NodeMatcher.ByElementId("4:abc:7"), LookupMode.MATCH),
            RelationshipMatcher.ByTypeAndProperties("WORKS_AT", mapOf("role" to "dev"), true),
            mutateProperties = mapOf("senior" to true),
        )

    generator.buildStatement(action) shouldBe expectedQuery
  }

  object MergeRelationshipWithNodeElementIdMatchersParams : ArgumentsProvider {
    fun standardCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start) WHERE elementId(start) = _e.start.matchElementId " +
              "WITH _e, start " +
              "MATCH (end) WHERE elementId(end) = _e.end.matchElementId " +
              "WITH _e, start, end " +
              "MERGE (start)-[r:`WORKS_AT` {`role`: _e.matchProperties.`role`}]->(end) " +
              "SET r += _e.mutateProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to mapOf("matchElementId" to "4:abc:1"),
                      "end" to mapOf("matchElementId" to "4:abc:7"),
                      "matchProperties" to mapOf("role" to "dev"),
                      "mutateProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    fun dynamicLabelsCypherQuery(): Query {
      return Query(
          "WITH ${'$'}e AS _e MATCH (start) WHERE elementId(start) = _e.start.matchElementId " +
              "WITH _e, start " +
              "MATCH (end) WHERE elementId(end) = _e.end.matchElementId " +
              "WITH _e, start, end " +
              "MERGE (start)-[r:${'$'}(_e.matchType) {`role`: _e.matchProperties.`role`}]->(end) " +
              "SET r += _e.mutateProperties",
          mapOf(
              "e" to
                  mapOf(
                      "start" to mapOf("matchElementId" to "4:abc:1"),
                      "end" to mapOf("matchElementId" to "4:abc:7"),
                      "matchType" to "WORKS_AT",
                      "matchProperties" to mapOf("role" to "dev"),
                      "mutateProperties" to mapOf("senior" to true),
                  )
          ),
      )
    }

    @ParameterizedTest
    @ArgumentsSource(MergeRelationshipWithNodeElementIdMatchersAndSetPropertiesParams::class)
    fun `should build relationship merge statements with node element id matchers and set properties`(
        neo4j: Neo4j,
        expectedQuery: Query,
    ) {
      val generator = DefaultSinkActionStatementGenerator(neo4j)
      val action =
          MergeRelationshipSinkAction(
              SinkActionNodeReference(
                  NodeMatcher.ByElementId("4:abc:1"),
                  LookupMode.MATCH,
                  setProperties = mapOf("name" to "Alice"),
                  mutateProperties = mapOf("id" to 1),
              ),
              SinkActionNodeReference(
                  NodeMatcher.ByElementId("4:abc:7"),
                  LookupMode.MATCH,
                  mutateProperties = mapOf("name" to "Acme Corp"),
              ),
              RelationshipMatcher.ByTypeAndProperties("WORKS_AT", mapOf("role" to "dev"), true),
              mutateProperties = mapOf("senior" to true),
          )

      generator.buildStatement(action) shouldBe expectedQuery
    }

    object MergeRelationshipWithNodeElementIdMatchersAndSetPropertiesParams : ArgumentsProvider {
      fun standardCypherQuery(): Query {
        return Query(
            "WITH ${'$'}e AS _e MATCH (start) WHERE elementId(start) = _e.start.matchElementId " +
                "SET start = _e.start.setProperties SET start += _e.start.mutateProperties " +
                "MATCH (end) WHERE elementId(end) = _e.end.matchElementId " +
                "SET end += _e.end.mutateProperties " +
                "MERGE (start)-[r:`WORKS_AT` {`role`: _e.matchProperties.`role`}]->(end) " +
                "SET r += _e.mutateProperties",
            mapOf(
                "e" to
                    mapOf(
                        "start" to
                            mapOf(
                                "matchElementId" to "4:abc:1",
                                "setProperties" to mapOf("name" to "Alice"),
                                "mutateProperties" to mapOf("id" to 1),
                            ),
                        "end" to
                            mapOf(
                                "matchElementId" to "4:abc:7",
                                "mutateProperties" to mapOf("name" to "Acme Corp"),
                            ),
                        "matchProperties" to mapOf("role" to "dev"),
                        "mutateProperties" to mapOf("senior" to true),
                    )
            ),
        )
      }

      fun dynamicLabelsCypherQuery(): Query {
        return Query(
            "WITH ${'$'}e AS _e MATCH (start) WHERE elementId(start) = _e.start.matchElementId " +
                "SET start = _e.start.setProperties SET start += _e.start.mutateProperties " +
                "MATCH (end) WHERE elementId(end) = _e.end.matchElementId " +
                "SET end += _e.end.mutateProperties " +
                "MERGE (start)-[r:${'$'}(_e.matchType) {`role`: _e.matchProperties.`role`}]->(end) " +
                "SET r += _e.mutateProperties",
            mapOf(
                "e" to
                    mapOf(
                        "start" to
                            mapOf(
                                "matchElementId" to "4:abc:1",
                                "setProperties" to mapOf("name" to "Alice"),
                                "mutateProperties" to mapOf("id" to 1),
                            ),
                        "end" to
                            mapOf(
                                "matchElementId" to "4:abc:7",
                                "mutateProperties" to mapOf("name" to "Acme Corp"),
                            ),
                        "matchType" to "WORKS_AT",
                        "matchProperties" to mapOf("role" to "dev"),
                        "mutateProperties" to mapOf("senior" to true),
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
