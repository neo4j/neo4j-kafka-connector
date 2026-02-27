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
import org.neo4j.connectors.kafka.sink.strategy.DeleteRelationshipSinkAction
import org.neo4j.connectors.kafka.sink.strategy.LookupMode
import org.neo4j.connectors.kafka.sink.strategy.SinkAction
import org.neo4j.connectors.kafka.sink.strategy.SinkActionNodeReference
import org.neo4j.connectors.kafka.utils.MapUtils.getMap
import org.neo4j.connectors.kafka.utils.MapUtils.getTyped

data class DeleteRelationship(
    val type: String,
    val start: NodeReference,
    val end: NodeReference,
    val ids: Map<String, Any?> = emptyMap(),
) : Operation {
  override fun toAction(): SinkAction {
    if (type.isEmpty()) {
      throw InvalidDataException("'${Keys.RELATION_TYPE}' must be specified.")
    }

    if (start.op != LookupMode.MATCH || end.op != LookupMode.MATCH) {
      throw InvalidDataException(
          "'${Keys.FROM}' and '${Keys.TO}' must have '${Keys.OPERATION}' as 'MATCH' for relationship deletion operations."
      )
    }

    if (start.ids.isEmpty() || end.ids.isEmpty()) {
      throw InvalidDataException(
          "'${Keys.FROM}' and '${Keys.TO}' must contain at least one ID property."
      )
    }

    return DeleteRelationshipSinkAction(
        startNode = SinkActionNodeReference(buildNodeMatcher(start.labels, start.ids), start.op),
        endNode = SinkActionNodeReference(buildNodeMatcher(end.labels, end.ids), end.op),
        matcher = buildRelationshipMatcher(type, ids),
    )
  }

  companion object {
    fun from(values: Map<String, Any?>): DeleteRelationship {
      return DeleteRelationship(
          values.getTyped<String>(Keys.RELATION_TYPE)
              ?: throw InvalidDataException("No ${Keys.RELATION_TYPE} found"),
          NodeReference.from(
              values.getMap<String, Any?>(Keys.FROM)
                  ?: throw InvalidDataException("No ${Keys.FROM} found")
          ),
          NodeReference.from(
              values.getMap<String, Any?>(Keys.TO)
                  ?: throw InvalidDataException("No ${Keys.TO} found")
          ),
          values.getMap<String, Any?>(Keys.IDS) ?: emptyMap(),
      )
    }
  }
}
