/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.neo4j.cdc.client.model.EntityEvent
import org.neo4j.cdc.client.model.NodeEvent
import org.neo4j.cdc.client.model.State

fun <T : State> EntityEvent<T>.mutatedProperties(): Map<String, Any?> {
  val result = this.after.properties.toMutableMap()

  this.before.properties.forEach { (key, value) ->
    if (!result.containsKey(key)) {
      result[key] = null
    } else if (result[key]?.equals(value) == true) {
      result.remove(key)
    }
  }

  return result.toMap()
}

fun NodeEvent.addedLabels(): Collection<String> {
  return after.labels.minus(before.labels.toSet())
}

fun NodeEvent.removedLabels(): Collection<String> {
  return before.labels.minus(after.labels.toSet())
}
