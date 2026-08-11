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

import java.util.concurrent.ConcurrentHashMap
import org.neo4j.caniuse.CanIUse.canIUse
import org.neo4j.caniuse.Cypher
import org.neo4j.caniuse.Neo4j
import org.neo4j.cypherdsl.core.Statement
import org.neo4j.cypherdsl.core.renderer.Configuration
import org.neo4j.cypherdsl.core.renderer.Dialect
import org.neo4j.cypherdsl.core.renderer.Renderer

/**
 * Renders Cypher-DSL [Statement]s to Cypher text, picking a [Dialect] from the target [Neo4j]'s
 * capabilities via [CanIUse]. The underlying [Renderer] is cached per [Neo4j] value, since a given
 * [Neo4j] always maps onto the same [Dialect] and building a [Renderer] from a [Configuration] is
 * not free.
 *
 * Lives in `common` (rather than `sink`, where it originated) so that `source` can depend on it
 * too.
 *
 * [neo4j] is currently the only input to dialect selection. A future configuration-file-driven
 * override would most naturally take the form of an externally-supplied [Configuration] (of which
 * [Dialect] is only one part) rather than a bare [Dialect] override, but that dependency is not
 * introduced yet.
 */
class CypherRenderer(private val neo4j: Neo4j) {

  fun render(statement: Statement): String = rendererFor(neo4j).render(statement)

  companion object {
    private val renderers = ConcurrentHashMap<Neo4j, Renderer>()

    private fun rendererFor(neo4j: Neo4j): Renderer =
        renderers.computeIfAbsent(neo4j) {
          Renderer.getRenderer(Configuration.newConfig().withDialect(dialectFor(neo4j)).build())
        }

    private fun dialectFor(neo4j: Neo4j): Dialect =
        if (canIUse(Cypher.callSubqueryWithVariableScopeClause()).withNeo4j(neo4j)) {
          Dialect.NEO4J_5_DEFAULT_CYPHER
        } else {
          Dialect.NEO4J_5
        }
  }
}
