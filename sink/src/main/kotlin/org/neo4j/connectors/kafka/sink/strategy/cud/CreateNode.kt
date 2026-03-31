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
import org.neo4j.connectors.kafka.sink.strategy.CreateNodeSinkAction
import org.neo4j.connectors.kafka.sink.strategy.SinkAction
import org.neo4j.connectors.kafka.utils.MapUtils.getIterable
import org.neo4j.connectors.kafka.utils.MapUtils.getMap

data class CreateNode(val labels: Set<String>, val properties: Map<String, Any?>) : Operation {

  override fun toAction(): SinkAction {
    return CreateNodeSinkAction(labels, properties)
  }

  companion object {
    fun from(values: Map<String, Any?>): CreateNode {
      return CreateNode(
          values.getIterable<String>(Keys.LABELS)?.toSet() ?: emptySet(),
          values.getMap<String, Any?>(Keys.PROPERTIES)
              ?: throw InvalidDataException("No ${Keys.PROPERTIES} found"),
      )
    }
  }
}
