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

import org.neo4j.caniuse.CanIUse.canIUse
import org.neo4j.caniuse.Cypher
import org.neo4j.caniuse.Neo4j
import org.neo4j.caniuse.Neo4jVersion
import org.neo4j.cypherdsl.core.Statement
import org.neo4j.cypherdsl.core.renderer.Configuration
import org.neo4j.cypherdsl.core.renderer.Dialect
import org.neo4j.cypherdsl.core.renderer.Renderer

internal class Cypher5Renderer(neo4j: Neo4j) : Renderer {
  companion object {
    private val Neo4jVersion5 = Neo4jVersion(major = 5, minor = 0, patch = 0)
  }

  private val isCypher5PrefixSupported = canIUse(Cypher.explicitCypher5Selection()).withNeo4j(neo4j)
  private val delegateRenderer =
      Renderer.getRenderer(
          Configuration.newConfig()
              .withDialect(
                  if (neo4j.version < Neo4jVersion5) {
                    Dialect.DEFAULT
                  } else {
                    Dialect.NEO4J_5
                  })
              .build())

  override fun render(statement: Statement?): String? {
    val rendered = delegateRenderer.render(statement)

    if (isCypher5PrefixSupported) {
      return "CYPHER 5 $rendered"
    }

    return rendered
  }
}
