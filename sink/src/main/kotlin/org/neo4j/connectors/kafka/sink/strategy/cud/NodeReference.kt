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
import org.neo4j.connectors.kafka.utils.MapUtils.getTyped

data class NodeReference(
    val labels: Set<String>,
    val ids: Map<String, Any?>,
    val op: LookupMode = LookupMode.MATCH,
) {
  companion object {
    fun from(values: Map<String, Any?>): NodeReference {
      return NodeReference(
          values.getIterable<String>(Keys.LABELS)?.toSet() ?: emptySet(),
          values.getMap<String, Any?>(Keys.IDS)?.toMap()
              ?: throw InvalidDataException("No ${Keys.IDS} found"),
          LookupMode.fromString(values.getTyped<String>(Keys.OPERATION)) ?: LookupMode.MATCH,
      )
    }
  }
}
