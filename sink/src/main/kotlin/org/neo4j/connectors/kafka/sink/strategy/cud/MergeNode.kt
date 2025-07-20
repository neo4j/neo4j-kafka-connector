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

import org.neo4j.connectors.kafka.exceptions.InvalidDataException
import org.neo4j.connectors.kafka.utils.MapUtils.getIterable
import org.neo4j.connectors.kafka.utils.MapUtils.getMap
import org.neo4j.cypherdsl.core.Cypher
import org.neo4j.cypherdsl.core.renderer.Renderer
import org.neo4j.driver.Query

data class MergeNode(
    val labels: Set<String>,
    val ids: Map<String, Any?>,
    val properties: Map<String, Any?>,
) : Operation {
  override fun toQuery(renderer: Renderer): Query {
    if (ids.isEmpty()) {
      throw InvalidDataException("Node must contain at least one ID property.")
    }

    val keysParam = Cypher.parameter("keys")
    val propertiesParam = Cypher.parameter("properties")
    val node = buildNode(labels, ids, keysParam).named("n")
    val stmt =
        renderer.render(
            if (ids.containsKey(Keys.PHYSICAL_ID) || ids.containsKey(Keys.ELEMENT_ID)) {
              Cypher.match(node)
                  .applyFilter(node, ids, keysParam)
                  .mutate(node, propertiesParam)
                  .build()
            } else {
              Cypher.merge(node).mutate(node, propertiesParam).build()
            }
        )

    return Query(stmt, mapOf("keys" to ids, "properties" to properties))
  }

  companion object {
    fun from(values: Map<String, Any?>): MergeNode {
      return MergeNode(
          values.getIterable<String>(Keys.LABELS)?.toSet() ?: emptySet(),
          values.getMap<String, Any?>(Keys.IDS)
              ?: throw InvalidDataException("No ${Keys.IDS} found"),
          values.getMap<String, Any?>(Keys.PROPERTIES)
              ?: throw InvalidDataException("No ${Keys.PROPERTIES} found"),
      )
    }
  }
}
