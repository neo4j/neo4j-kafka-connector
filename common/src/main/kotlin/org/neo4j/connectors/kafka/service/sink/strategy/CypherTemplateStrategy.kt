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
package org.neo4j.connectors.kafka.service.sink.strategy

import org.neo4j.connectors.kafka.service.StreamsSinkEntity
import org.neo4j.connectors.kafka.utils.StreamsUtils

class CypherTemplateStrategy(query: String) : IngestionStrategy {
  private val fullQuery = "${StreamsUtils.UNWIND} $query"

  @Suppress("UNCHECKED_CAST")
  override fun mergeNodeEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents> {
    return listOf(QueryEvents(fullQuery, events.mapNotNull { it.value as? Map<String, Any?> }))
  }

  override fun deleteNodeEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents> =
      emptyList()

  override fun mergeRelationshipEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents> =
      emptyList()

  override fun deleteRelationshipEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents> =
      emptyList()
}
