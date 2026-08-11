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

import org.neo4j.caniuse.CanIUse.canIUse
import org.neo4j.caniuse.Cypher
import org.neo4j.caniuse.Neo4j
import org.neo4j.cypherdsl.core.Statement
import org.neo4j.cypherdsl.core.renderer.Configuration
import org.neo4j.cypherdsl.core.renderer.Dialect
import org.neo4j.cypherdsl.core.renderer.Renderer

/**
 * Renders Cypher-DSL [Statement]s to Cypher text, picking a [Dialect] from the target [Neo4j]'s
 * capabilities. [dialectOverride] is an escape hatch for a future configuration-driven dialect
 * selection; when absent, the dialect is derived from [neo4j] via [CanIUse].
 */
class CypherRenderer(neo4j: Neo4j, dialectOverride: Dialect? = null) {

  private val renderer =
      Renderer.getRenderer(
          Configuration.newConfig().withDialect(dialectOverride ?: dialectFor(neo4j)).build()
      )

  fun render(statement: Statement): String = renderer.render(statement)

  companion object {
    private fun dialectFor(neo4j: Neo4j): Dialect =
        if (canIUse(Cypher.callSubqueryWithVariableScopeClause()).withNeo4j(neo4j)) {
          Dialect.NEO4J_5_DEFAULT_CYPHER
        } else {
          Dialect.NEO4J_5
        }
  }
}
