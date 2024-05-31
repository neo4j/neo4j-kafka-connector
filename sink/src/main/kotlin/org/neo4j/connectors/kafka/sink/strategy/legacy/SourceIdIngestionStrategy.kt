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
package org.neo4j.connectors.kafka.sink.strategy.legacy

import org.neo4j.connectors.kafka.events.EntityType
import org.neo4j.connectors.kafka.events.NodeChange
import org.neo4j.connectors.kafka.events.OperationType
import org.neo4j.connectors.kafka.events.RelationshipChange
import org.neo4j.connectors.kafka.events.RelationshipPayload
import org.neo4j.connectors.kafka.sink.strategy.legacy.IngestionUtils.getLabelsAsString
import org.neo4j.connectors.kafka.utils.StreamsUtils

data class SourceIdIngestionStrategyConfig(
    val labelName: String = "SourceEvent",
    val idName: String = "sourceId"
) {
  companion object {
    val DEFAULT = SourceIdIngestionStrategyConfig()
  }
}

class SourceIdIngestionStrategy(
    config: SourceIdIngestionStrategyConfig = SourceIdIngestionStrategyConfig()
) : IngestionStrategy {

  private val quotedLabelName = config.labelName.quote()
  private val quotedIdName = config.idName.quote()

  override fun mergeRelationshipEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents> {
    return events
        .mapNotNull {
          SchemaUtils.toStreamsTransactionEvent(it) {
            it.payload.type == EntityType.relationship && it.meta.operation != OperationType.deleted
          }
        }
        .map { data ->
          val payload = data.payload as RelationshipPayload
          val changeEvt =
              when (data.meta.operation) {
                OperationType.deleted -> {
                  data.payload.before as RelationshipChange
                }
                else -> data.payload.after as RelationshipChange
              }
          payload.label to
              mapOf(
                  "id" to payload.id,
                  "start" to payload.start.id,
                  "end" to payload.end.id,
                  "properties" to changeEvt.properties)
        }
        .groupBy({ it.first }, { it.second })
        .map {
          val query =
              """
                        |${StreamsUtils.UNWIND}
                        |MERGE (start:$quotedLabelName{$quotedIdName: event.start})
                        |MERGE (end:$quotedLabelName{$quotedIdName: event.end})
                        |MERGE (start)-[r:${it.key.quote()}{$quotedIdName: event.id}]->(end)
                        |SET r = event.properties
                        |SET r.$quotedIdName = event.id
                    """
                  .trimMargin()
          QueryEvents(query, it.value)
        }
  }

  override fun deleteRelationshipEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents> {
    return events
        .mapNotNull {
          SchemaUtils.toStreamsTransactionEvent(it) {
            it.payload.type == EntityType.relationship && it.meta.operation == OperationType.deleted
          }
        }
        .map { data ->
          val payload = data.payload as RelationshipPayload
          payload.label to mapOf("id" to data.payload.id)
        }
        .groupBy({ it.first }, { it.second })
        .map {
          val query =
              "${StreamsUtils.UNWIND} MATCH ()-[r:${it.key.quote()}{$quotedIdName: event.id}]-() DELETE r"
          QueryEvents(query, it.value)
        }
  }

  override fun deleteNodeEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents> {
    val data =
        events
            .mapNotNull {
              SchemaUtils.toStreamsTransactionEvent(it) {
                it.payload.type == EntityType.node && it.meta.operation == OperationType.deleted
              }
            }
            .map { mapOf("id" to it.payload.id) }
    if (data.isNullOrEmpty()) {
      return emptyList()
    }
    val query =
        "${StreamsUtils.UNWIND} MATCH (n:$quotedLabelName{$quotedIdName: event.id}) DETACH DELETE n"
    return listOf(QueryEvents(query, data))
  }

  override fun mergeNodeEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents> {
    return events
        .mapNotNull {
          SchemaUtils.toStreamsTransactionEvent(it) {
            it.payload.type == EntityType.node && it.meta.operation != OperationType.deleted
          }
        }
        .map { data ->
          val changeEvtAfter = data.payload.after as NodeChange
          val labelsAfter = changeEvtAfter.labels ?: emptyList()
          val labelsBefore =
              if (data.payload.before != null) {
                val changeEvtBefore = data.payload.before as NodeChange
                changeEvtBefore.labels ?: emptyList()
              } else {
                emptyList()
              }
          val labelsToAdd = (labelsAfter - labelsBefore).toSet()
          val labelsToDelete = (labelsBefore - labelsAfter).toSet()
          NodeMergeMetadata(labelsToAdd = labelsToAdd, labelsToDelete = labelsToDelete) to
              mapOf("id" to data.payload.id, "properties" to changeEvtAfter.properties)
        }
        .groupBy({ it.first }, { it.second })
        .map {
          var query =
              """
                        |${StreamsUtils.UNWIND}
                        |MERGE (n:$quotedLabelName{$quotedIdName: event.id})
                        |SET n = event.properties
                        |SET n.$quotedIdName = event.id
                    """
                  .trimMargin()
          if (it.key.labelsToDelete.isNotEmpty()) {
            query += "\nREMOVE n${getLabelsAsString(it.key.labelsToDelete)}"
          }
          if (it.key.labelsToAdd.isNotEmpty()) {
            query += "\nSET n${getLabelsAsString(it.key.labelsToAdd)}"
          }
          QueryEvents(query, it.value)
        }
  }
}
