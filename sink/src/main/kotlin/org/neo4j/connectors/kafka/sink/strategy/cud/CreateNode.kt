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
package org.neo4j.connectors.kafka.sink.strategy.cud

import com.fasterxml.jackson.annotation.JsonProperty
import org.neo4j.cypherdsl.core.Cypher
import org.neo4j.cypherdsl.core.renderer.Renderer
import org.neo4j.driver.Query

data class CreateNode(
    @JsonProperty(Keys.LABELS) val labels: Set<String>,
    @JsonProperty(Keys.PROPERTIES) val properties: Map<String, Any?>
) : Operation {
  override fun toQuery(renderer: Renderer): Query {
    val node = buildNode(labels, emptyMap(), Cypher.parameter("keys")).named("n")
    val stmt =
        renderer.render(Cypher.create(node).set(node, Cypher.parameter("properties")).build())

    return Query(stmt, mapOf("properties" to properties))
  }
}