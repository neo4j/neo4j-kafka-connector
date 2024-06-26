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
package org.neo4j.connectors.kafka.sink.legacy.strategy

import org.neo4j.connectors.kafka.events.Constraint
import org.neo4j.connectors.kafka.events.RelationshipPayload

data class QueryEvents(val query: String, val events: List<Map<String, Any?>>)

interface IngestionStrategy {
  fun mergeNodeEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents>

  fun deleteNodeEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents>

  fun mergeRelationshipEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents>

  fun deleteRelationshipEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents>
}

data class RelationshipSchemaMetadata(
    val label: String,
    val startLabels: List<String>,
    val endLabels: List<String>,
    val startKeys: Set<String>,
    val endKeys: Set<String>
) {
  constructor(
      payload: RelationshipPayload
  ) : this(
      label = payload.label,
      startLabels = payload.start.labels.orEmpty(),
      endLabels = payload.end.labels.orEmpty(),
      startKeys = payload.start.ids.keys,
      endKeys = payload.end.ids.keys)
}

data class NodeSchemaMetadata(
    val constraints: List<Constraint>,
    val labelsToAdd: List<String>,
    val labelsToDelete: List<String>,
    val keys: Set<String>
)

data class NodeMergeMetadata(val labelsToAdd: Set<String>, val labelsToDelete: Set<String>)
