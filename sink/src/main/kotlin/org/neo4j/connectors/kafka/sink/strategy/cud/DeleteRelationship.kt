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
import org.neo4j.connectors.kafka.sink.strategy.InvalidDataException
import org.neo4j.cypherdsl.core.Cypher
import org.neo4j.cypherdsl.core.renderer.Renderer
import org.neo4j.driver.Query

data class DeleteRelationship(
    @JsonProperty(Keys.RELATION_TYPE) val type: String,
    @JsonProperty(Keys.FROM) val start: NodeReference,
    @JsonProperty(Keys.TO) val end: NodeReference,
    @JsonProperty(Keys.IDS, defaultValue = "{}", required = false)
    val ids: Map<String, Any?> = emptyMap()
) : Operation {
  override fun toQuery(renderer: Renderer): Query {
    if (type.isEmpty()) {
      throw InvalidDataException("'${Keys.RELATION_TYPE}' must be specified.")
    }

    if (start.op != LookupMode.MATCH || end.op != LookupMode.MATCH) {
      throw InvalidDataException(
          "'${Keys.FROM}' and '${Keys.TO}' must have '${Keys.OPERATION}' as 'MATCH' for relationship deletion operations.")
    }

    if (start.ids.isEmpty() || end.ids.isEmpty()) {
      throw InvalidDataException(
          "'${Keys.FROM}' and '${Keys.TO}' must contain at least one ID property.")
    }

    val startParam = Cypher.parameter("start")
    val endParam = Cypher.parameter("end")
    val keysParam = Cypher.parameter("keys")
    val (startNode, endNode, lookup) = lookupNodes(start, end, startParam, endParam)
    val relationship =
        startNode
            .relationshipTo(endNode, type)
            .withProperties(ids.entries.associate { it.key to keysParam.property(it.key) })
            .named("r")

    val stmt = renderer.render(lookup.match(relationship).delete(relationship).build())

    return Query(
        stmt,
        mapOf(
            "start" to mapOf("keys" to start.ids),
            "end" to mapOf("keys" to end.ids),
            "keys" to ids))
  }
}