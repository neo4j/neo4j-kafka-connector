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
package streams.utils

import streams.events.Constraint
import streams.events.RelKeyStrategy
import streams.events.StreamsConstraintType
import streams.events.StreamsTransactionEvent
import streams.service.StreamsSinkEntity

object SchemaUtils {
  fun getNodeKeys(
    labels: List<String>,
    propertyKeys: Set<String>,
    constraints: List<Constraint>,
    keyStrategy: RelKeyStrategy = RelKeyStrategy.DEFAULT
  ): Set<String> =
    constraints
      .filter { constraint ->
        constraint.type == StreamsConstraintType.UNIQUE &&
          propertyKeys.containsAll(constraint.properties) &&
          labels.contains(constraint.label)
      }
      .let {
        when (keyStrategy) {
          RelKeyStrategy.DEFAULT -> {
            // we order first by properties.size, then by label name and finally by properties name
            // alphabetically
            // with properties.sorted() we ensure that ("foo", "bar") and ("bar", "foo") are no
            // different
            // with toString() we force it.properties to have the natural sort order, that is
            // alphabetically
            it
              .minWithOrNull(
                (compareBy(
                  { it.properties.size }, { it.label }, { it.properties.sorted().toString() })))
              ?.properties
              .orEmpty()
          }
          // with 'ALL' strategy we get a set with all properties
          RelKeyStrategy.ALL -> it.flatMap { it.properties }.toSet()
        }
      }

  fun toStreamsTransactionEvent(
    streamsSinkEntity: StreamsSinkEntity,
    evaluation: (StreamsTransactionEvent) -> Boolean
  ): StreamsTransactionEvent? =
    if (streamsSinkEntity.value != null) {
      val data = JSONUtils.asStreamsTransactionEvent(streamsSinkEntity.value)
      if (evaluation(data)) data else null
    } else {
      null
    }
}
