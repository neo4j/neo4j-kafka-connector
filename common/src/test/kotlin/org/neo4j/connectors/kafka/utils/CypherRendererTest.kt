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
package org.neo4j.connectors.kafka.utils

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
import org.neo4j.cypherdsl.core.Cypher

class CypherRendererTest {

  // CALL (e) {...} vs the legacy CALL {WITH e ...} is the clearest textual signal that a given
  // Neo4j version picked Dialect.NEO4J_5_DEFAULT_CYPHER over Dialect.NEO4J_5 - it is exactly the
  // dialect difference org.neo4j.caniuse.Cypher.callSubqueryWithVariableScopeClause() (the 5.23
  // threshold) governs.
  private fun render(neo4j: Neo4j): String {
    val event = Cypher.name("e")
    val subquery = Cypher.match(Cypher.anyNode()).returning(Cypher.literalTrue()).build()
    val statement =
        Cypher.unwind(Cypher.parameter("events")).`as`(event).call(subquery, event).finish().build()
    return CypherRenderer(neo4j).render(statement)
  }

  @ParameterizedTest
  @ArgumentsSource(BelowThresholdParams::class)
  fun `renders the legacy CALL import form below Neo4j 5_23`(neo4j: Neo4j) {
    render(neo4j) shouldBe "UNWIND \$events AS e CALL {WITH e MATCH () RETURN true} FINISH"
  }

  object BelowThresholdParams : ArgumentsProvider {
    override fun provideArguments(
        parameters: ParameterDeclarations,
        context: ExtensionContext,
    ): Stream<out Arguments> {
      return Stream.of(
          Arguments.of(
              Neo4j(Neo4jVersion(4, 4), Neo4jEdition.ENTERPRISE, Neo4jDeploymentType.SELF_MANAGED)
          ),
          Arguments.of(
              Neo4j(Neo4jVersion(5, 22), Neo4jEdition.ENTERPRISE, Neo4jDeploymentType.SELF_MANAGED)
          ),
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(AtOrAboveThresholdParams::class)
  fun `renders the variable-scope CALL form at and above Neo4j 5_23`(neo4j: Neo4j) {
    render(neo4j) shouldBe "UNWIND \$events AS e CALL (e) {MATCH () RETURN true} FINISH"
  }

  object AtOrAboveThresholdParams : ArgumentsProvider {
    override fun provideArguments(
        parameters: ParameterDeclarations,
        context: ExtensionContext,
    ): Stream<out Arguments> {
      return Stream.of(
          Arguments.of(
              Neo4j(Neo4jVersion(5, 23), Neo4jEdition.ENTERPRISE, Neo4jDeploymentType.SELF_MANAGED)
          ),
          Arguments.of(
              Neo4j(Neo4jVersion(5, 26), Neo4jEdition.ENTERPRISE, Neo4jDeploymentType.SELF_MANAGED)
          ),
          Arguments.of(
              Neo4j(
                  Neo4jVersion(2026, 1),
                  Neo4jEdition.ENTERPRISE,
                  Neo4jDeploymentType.SELF_MANAGED,
              )
          ),
          Arguments.of(
              Neo4j(Neo4jVersion(2026, 1), Neo4jEdition.ENTERPRISE, Neo4jDeploymentType.AURA)
          ),
      )
    }
  }
}
