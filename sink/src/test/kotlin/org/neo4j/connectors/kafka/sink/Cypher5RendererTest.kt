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
package org.neo4j.connectors.kafka.sink

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

class Cypher5RendererTest {

  @ParameterizedTest
  @ArgumentsSource(Cypher5PrefixSupportedVersions::class)
  fun `should prefix statements with cypher 5 on compatible neo4j versions`(neo4j: Neo4j) {
    val person = Cypher.node("Person").named("p")
    val stmt = Cypher.match(person).returning(person).build()

    Cypher5Renderer(neo4j).render(stmt) shouldBe "CYPHER 5 MATCH (p:`Person`) RETURN p"
  }

  @ParameterizedTest
  @ArgumentsSource(UnsupportedCypher5PrefixVersions::class)
  fun `should not prefix statements on earlier neo4j versions`(neo4j: Neo4j) {
    val person = Cypher.node("Person").named("p")
    val stmt = Cypher.match(person).returning(person).build()

    Cypher5Renderer(neo4j).render(stmt) shouldBe "MATCH (p:`Person`) RETURN p"
  }

  class Cypher5PrefixSupportedVersions : ArgumentsProvider {
    override fun provideArguments(
        parameters: ParameterDeclarations?,
        context: ExtensionContext?,
    ): Stream<out Arguments?>? {
      return Stream.of(
          Arguments.of(
              Neo4j(
                  Neo4jVersion(5, 21, 0),
                  Neo4jEdition.ENTERPRISE,
                  Neo4jDeploymentType.SELF_MANAGED,
              )
          ),
          Arguments.of(
              Neo4j(Neo4jVersion(5, 21, 0), Neo4jEdition.ENTERPRISE, Neo4jDeploymentType.AURA)
          ),
          Arguments.of(
              Neo4j(
                  Neo4jVersion(5, 21, 0),
                  Neo4jEdition.COMMUNITY,
                  Neo4jDeploymentType.SELF_MANAGED,
              )
          ),
          Arguments.of(
              Neo4j(
                  Neo4jVersion(5, 26, 0),
                  Neo4jEdition.ENTERPRISE,
                  Neo4jDeploymentType.SELF_MANAGED,
              )
          ),
          Arguments.of(
              Neo4j(Neo4jVersion(5, 26, 0), Neo4jEdition.ENTERPRISE, Neo4jDeploymentType.AURA)
          ),
          Arguments.of(
              Neo4j(
                  Neo4jVersion(5, 26, 0),
                  Neo4jEdition.COMMUNITY,
                  Neo4jDeploymentType.SELF_MANAGED,
              )
          ),
          Arguments.of(
              Neo4j(
                  Neo4jVersion(5, 26, 2),
                  Neo4jEdition.ENTERPRISE,
                  Neo4jDeploymentType.SELF_MANAGED,
              )
          ),
          Arguments.of(
              Neo4j(Neo4jVersion(5, 26, 2), Neo4jEdition.ENTERPRISE, Neo4jDeploymentType.AURA)
          ),
          Arguments.of(
              Neo4j(
                  Neo4jVersion(5, 26, 2),
                  Neo4jEdition.COMMUNITY,
                  Neo4jDeploymentType.SELF_MANAGED,
              )
          ),
          Arguments.of(
              Neo4j(
                  Neo4jVersion(5, 27, 0),
                  Neo4jEdition.ENTERPRISE,
                  Neo4jDeploymentType.SELF_MANAGED,
              )
          ),
          Arguments.of(
              Neo4j(Neo4jVersion(5, 27, 0), Neo4jEdition.ENTERPRISE, Neo4jDeploymentType.AURA)
          ),
          Arguments.of(
              Neo4j(
                  Neo4jVersion(5, 27, 0),
                  Neo4jEdition.COMMUNITY,
                  Neo4jDeploymentType.SELF_MANAGED,
              )
          ),
          Arguments.of(
              Neo4j(
                  Neo4jVersion(2025, 1, 0),
                  Neo4jEdition.ENTERPRISE,
                  Neo4jDeploymentType.SELF_MANAGED,
              )
          ),
          Arguments.of(
              Neo4j(Neo4jVersion(2025, 1, 0), Neo4jEdition.ENTERPRISE, Neo4jDeploymentType.AURA)
          ),
          Arguments.of(
              Neo4j(
                  Neo4jVersion(2025, 1, 0),
                  Neo4jEdition.COMMUNITY,
                  Neo4jDeploymentType.SELF_MANAGED,
              )
          ),
          Arguments.of(
              Neo4j(Neo4jVersion.LATEST, Neo4jEdition.ENTERPRISE, Neo4jDeploymentType.SELF_MANAGED)
          ),
          Arguments.of(
              Neo4j(Neo4jVersion.LATEST, Neo4jEdition.ENTERPRISE, Neo4jDeploymentType.AURA)
          ),
      )
    }
  }

  class UnsupportedCypher5PrefixVersions : ArgumentsProvider {
    override fun provideArguments(
        parameters: ParameterDeclarations?,
        context: ExtensionContext?,
    ): Stream<out Arguments?>? {
      return Stream.of(
          Arguments.of(
              Neo4j(
                  Neo4jVersion(4, 4, 41),
                  Neo4jEdition.ENTERPRISE,
                  Neo4jDeploymentType.SELF_MANAGED,
              )
          ),
          Arguments.of(
              Neo4j(Neo4jVersion(4, 4, 0), Neo4jEdition.ENTERPRISE, Neo4jDeploymentType.AURA)
          ),
          Arguments.of(
              Neo4j(
                  Neo4jVersion(4, 4, 41),
                  Neo4jEdition.COMMUNITY,
                  Neo4jDeploymentType.SELF_MANAGED,
              )
          ),
          Arguments.of(
              Neo4j(
                  Neo4jVersion(5, 8, 0),
                  Neo4jEdition.ENTERPRISE,
                  Neo4jDeploymentType.SELF_MANAGED,
              )
          ),
          Arguments.of(
              Neo4j(Neo4jVersion(5, 8, 0), Neo4jEdition.ENTERPRISE, Neo4jDeploymentType.AURA)
          ),
          Arguments.of(
              Neo4j(Neo4jVersion(5, 8, 0), Neo4jEdition.COMMUNITY, Neo4jDeploymentType.SELF_MANAGED)
          ),
          Arguments.of(
              Neo4j(
                  Neo4jVersion(5, 20, 0),
                  Neo4jEdition.ENTERPRISE,
                  Neo4jDeploymentType.SELF_MANAGED,
              )
          ),
          Arguments.of(
              Neo4j(Neo4jVersion(5, 20, 0), Neo4jEdition.ENTERPRISE, Neo4jDeploymentType.AURA)
          ),
          Arguments.of(
              Neo4j(
                  Neo4jVersion(5, 20, 0),
                  Neo4jEdition.COMMUNITY,
                  Neo4jDeploymentType.SELF_MANAGED,
              )
          ),
      )
    }
  }
}
