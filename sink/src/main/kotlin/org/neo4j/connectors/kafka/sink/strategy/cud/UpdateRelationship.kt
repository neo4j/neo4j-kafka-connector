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
import org.neo4j.connectors.kafka.utils.MapUtils.getMap
import org.neo4j.connectors.kafka.utils.MapUtils.getTyped
import org.neo4j.cypherdsl.core.Cypher
import org.neo4j.cypherdsl.core.renderer.Renderer
import org.neo4j.driver.Query

data class UpdateRelationship(
    val type: String,
    val start: NodeReference,
    val end: NodeReference,
    val ids: Map<String, Any?> = emptyMap(),
    val properties: Map<String, Any?>
) : Operation {
  override fun toQuery(renderer: Renderer): Query {
    if (type.isEmpty()) {
      throw InvalidDataException("'${Keys.RELATION_TYPE}' must be specified.")
    }

    if (start.ids.isEmpty() || end.ids.isEmpty()) {
      throw InvalidDataException(
          "'${Keys.FROM}' and '${Keys.TO}' must contain at least one ID property.")
    }

    val startParam = Cypher.parameter("start")
    val endParam = Cypher.parameter("end")
    val keysParam = Cypher.parameter("keys")
    val propertiesParam = Cypher.parameter("properties")
    val (startNode, endNode, lookup) = lookupNodes(start, end, startParam, endParam)
    val relationship =
        startNode
            .relationshipTo(endNode, type)
            .withProperties(ids.entries.associate { it.key to keysParam.property(it.key) })
            .named("r")

    val stmt =
        renderer.render(lookup.match(relationship).mutate(relationship, propertiesParam).build())

    return Query(
        stmt,
        mapOf(
            "start" to mapOf("keys" to start.ids),
            "end" to mapOf("keys" to end.ids),
            "keys" to ids,
            "properties" to properties))
  }

  companion object {
    fun from(values: Map<String, Any?>): UpdateRelationship {
      return UpdateRelationship(
          values.getTyped<String>(Keys.RELATION_TYPE)
              ?: throw InvalidDataException("No ${Keys.RELATION_TYPE} found"),
          NodeReference.from(
              values.getMap<String, Any?>(Keys.FROM)
                  ?: throw InvalidDataException("No ${Keys.FROM} found")),
          NodeReference.from(
              values.getMap<String, Any?>(Keys.TO)
                  ?: throw InvalidDataException("No ${Keys.TO} found")),
          values.getMap<String, Any?>(Keys.IDS) ?: emptyMap(),
          values.getMap<String, Any?>(Keys.PROPERTIES)
              ?: throw InvalidDataException("No ${Keys.PROPERTIES} found"))
    }
  }
}
