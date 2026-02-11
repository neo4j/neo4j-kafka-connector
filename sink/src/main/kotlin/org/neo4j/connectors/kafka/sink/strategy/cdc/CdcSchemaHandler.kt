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
package org.neo4j.connectors.kafka.sink.strategy.cdc

import org.neo4j.caniuse.Neo4j
import org.neo4j.cdc.client.model.EntityOperation
import org.neo4j.cdc.client.model.NodeEvent
import org.neo4j.cdc.client.model.RelationshipEvent
import org.neo4j.connectors.kafka.exceptions.InvalidDataException
import org.neo4j.connectors.kafka.sink.SinkStrategy
import org.neo4j.connectors.kafka.sink.strategy.addedLabels
import org.neo4j.connectors.kafka.sink.strategy.mutatedProperties
import org.neo4j.connectors.kafka.sink.strategy.removedLabels
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class CdcSchemaHandler(val topic: String, neo4j: Neo4j) : CdcHandler(neo4j) {
  private val logger: Logger = LoggerFactory.getLogger(javaClass)

  init {
    logger.info("using default CDC SCHEMA strategy for topic '{}'", topic)
  }

  override fun strategy() = SinkStrategy.CDC_SCHEMA

  override fun transformCreate(event: NodeEvent): CdcNodeData {
    if (event.before != null) {
      throw InvalidDataException(
          "create operation requires 'before' field to be unset in the event object"
      )
    }

    if (event.after == null) {
      throw InvalidDataException("create operation requires 'after' field in the event object")
    }

    val (matchLabels, matchProperties) = buildMatchLabelsAndProperties(event.keys)

    return CdcNodeData(
        EntityOperation.CREATE,
        matchLabels,
        matchProperties,
        event.after.properties,
        event.after.labels.minus(matchLabels).toSet(),
        emptySet(),
    )
  }

  override fun transformUpdate(event: NodeEvent): CdcNodeData {
    if (event.before == null) {
      throw InvalidDataException("update operation requires 'before' field in the event object")
    }
    if (event.after == null) {
      throw InvalidDataException("update operation requires 'after' field in the event object")
    }

    val (matchLabels, matchProperties) = buildMatchLabelsAndProperties(event.keys)

    return CdcNodeData(
        EntityOperation.UPDATE,
        matchLabels,
        matchProperties,
        event.mutatedProperties(),
        event.addedLabels().toSet(),
        event.removedLabels().toSet(),
    )
  }

  override fun transformDelete(event: NodeEvent): CdcNodeData {
    if (event.after != null) {
      throw InvalidDataException(
          "delete operation requires 'after' field to be unset in the event object"
      )
    }

    val (matchLabels, matchProperties) = buildMatchLabelsAndProperties(event.keys)

    return CdcNodeData(
        EntityOperation.DELETE,
        matchLabels,
        matchProperties,
        emptyMap(),
        emptySet(),
        emptySet(),
    )
  }

  override fun transformCreate(event: RelationshipEvent): CdcRelationshipData {
    if (event.before != null) {
      throw InvalidDataException(
          "create operation requires 'before' field to be unset in the event object"
      )
    }

    if (event.after == null) {
      throw InvalidDataException("create operation requires 'after' field in the event object")
    }

    val (startMatchLabels, startMatchProperties) = buildMatchLabelsAndProperties(event.start.keys)
    val (endMatchLabels, endMatchProperties) = buildMatchLabelsAndProperties(event.end.keys)
    val (relMatchType, relMatchProperties) =
        buildMatchLabelsAndProperties(event.type, event.keys, event.after.properties)

    return CdcRelationshipData(
        EntityOperation.CREATE,
        startMatchLabels,
        startMatchProperties,
        endMatchLabels,
        endMatchProperties,
        relMatchType,
        relMatchProperties,
        event.keys.isNotEmpty(),
        event.after.properties,
    )
  }

  override fun transformUpdate(event: RelationshipEvent): CdcRelationshipData {
    if (event.before == null) {
      throw InvalidDataException("update operation requires 'before' field in the event object")
    }
    if (event.after == null) {
      throw InvalidDataException("update operation requires 'after' field in the event object")
    }

    val relationshipKeys = event.keys
    val (startMatchLabels, startMatchProperties) =
        buildMatchLabelsAndProperties(event.start.keys, relationshipKeys.isEmpty())
    val (endMatchLabels, endMatchProperties) =
        buildMatchLabelsAndProperties(event.end.keys, relationshipKeys.isEmpty())
    val (relMatchType, relMatchProperties) =
        buildMatchLabelsAndProperties(event.type, relationshipKeys, event.before.properties)

    return CdcRelationshipData(
        EntityOperation.UPDATE,
        startMatchLabels,
        startMatchProperties,
        endMatchLabels,
        endMatchProperties,
        relMatchType,
        relMatchProperties,
        relationshipKeys.isNotEmpty(),
        event.mutatedProperties(),
    )
  }

  override fun transformDelete(event: RelationshipEvent): CdcRelationshipData {
    if (event.after != null) {
      throw InvalidDataException(
          "delete operation requires 'after' field to be unset in the event object"
      )
    }

    val relationshipKeys = event.keys
    val (startMatchLabels, startMatchProperties) =
        buildMatchLabelsAndProperties(event.start.keys, relationshipKeys.isEmpty())
    val (endMatchLabels, endMatchProperties) =
        buildMatchLabelsAndProperties(event.end.keys, relationshipKeys.isEmpty())
    val (relMatchType, relMatchProperties) =
        buildMatchLabelsAndProperties(event.type, relationshipKeys, event.before.properties)

    return CdcRelationshipData(
        EntityOperation.DELETE,
        startMatchLabels,
        startMatchProperties,
        endMatchLabels,
        endMatchProperties,
        relMatchType,
        relMatchProperties,
        relationshipKeys.isNotEmpty(),
        emptyMap(),
    )
  }

  private fun buildMatchLabelsAndProperties(
      keys: Map<String, List<Map<String, Any>>>,
      forceKeys: Boolean = true,
  ): Pair<Set<String>, Map<String, Any>> {
    val validKeys =
        keys
            .mapValues { kvp -> kvp.value.filter { it.isNotEmpty() } }
            .filterValues { it.isNotEmpty() }

    if (forceKeys && validKeys.isEmpty()) {
      throw InvalidDataException(
          "schema strategy requires at least one node key with valid properties on nodes."
      )
    }

    return Pair(
        validKeys.keys.toSet(),
        validKeys
            .flatMap { it.value }
            .asSequence()
            .flatMap { it.asSequence() }
            .associate { it.key to it.value },
    )
  }

  private fun buildMatchLabelsAndProperties(
      type: String,
      keys: List<Map<String, Any>>,
      properties: Map<String, Any>?,
  ): Pair<String, Map<String, Any>> {
    return Pair(
        type,
        if (keys.isEmpty()) {
          properties ?: emptyMap()
        } else {
          keys.asSequence().flatMap { it.asSequence() }.associate { it.key to it.value }
        },
    )
  }
}
