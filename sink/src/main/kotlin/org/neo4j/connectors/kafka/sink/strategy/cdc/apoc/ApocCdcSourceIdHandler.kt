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
package org.neo4j.connectors.kafka.sink.strategy.cdc.apoc

import org.neo4j.caniuse.Neo4j
import org.neo4j.cdc.client.model.EntityOperation
import org.neo4j.cdc.client.model.NodeEvent
import org.neo4j.cdc.client.model.RelationshipEvent
import org.neo4j.connectors.kafka.exceptions.InvalidDataException
import org.neo4j.connectors.kafka.sink.SinkConfiguration
import org.neo4j.connectors.kafka.sink.SinkStrategy
import org.neo4j.connectors.kafka.sink.strategy.addedLabels
import org.neo4j.connectors.kafka.sink.strategy.mutatedProperties
import org.neo4j.connectors.kafka.sink.strategy.removedLabels
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class ApocCdcSourceIdHandler(
    val topic: String,
    neo4j: Neo4j,
    batchSize: Int,
    val labelName: String = SinkConfiguration.DEFAULT_SOURCE_ID_LABEL_NAME,
    val propertyName: String = SinkConfiguration.DEFAULT_SOURCE_ID_PROPERTY_NAME,
) : ApocCdcHandler(neo4j, batchSize) {
  private val logger: Logger = LoggerFactory.getLogger(javaClass)

  init {
    logger.info("using CYPHER 25 compatible CDC SOURCE_ID strategy for topic '{}'", topic)
  }

  override fun strategy() = SinkStrategy.CDC_SOURCE_ID

  override fun transformCreate(event: NodeEvent): CdcNodeData {
    if (event.after == null) {
      throw InvalidDataException("create operation requires 'after' field in the event object")
    }

    return CdcNodeData(
        EntityOperation.CREATE,
        setOf(labelName),
        mapOf(propertyName to event.elementId),
        event.after.properties,
        event.after.labels.minus(labelName).toSet(),
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

    return CdcNodeData(
        EntityOperation.UPDATE,
        setOf(labelName),
        mapOf(propertyName to event.elementId),
        event.mutatedProperties(),
        event.addedLabels().toSet(),
        event.removedLabels().toSet(),
    )
  }

  override fun transformDelete(event: NodeEvent): CdcNodeData {
    return CdcNodeData(
        EntityOperation.DELETE,
        setOf(labelName),
        mapOf(propertyName to event.elementId),
        emptyMap(),
        emptySet(),
        emptySet(),
    )
  }

  override fun transformCreate(event: RelationshipEvent): CdcRelationshipData {
    if (event.after == null) {
      throw InvalidDataException("create operation requires 'after' field in the event object")
    }

    return CdcRelationshipData(
        EntityOperation.CREATE,
        setOf(labelName),
        mapOf(propertyName to event.start.elementId),
        setOf(labelName),
        mapOf(propertyName to event.end.elementId),
        event.type,
        mapOf(propertyName to event.elementId),
        true,
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

    return CdcRelationshipData(
        EntityOperation.UPDATE,
        setOf(labelName),
        mapOf(propertyName to event.start.elementId),
        setOf(labelName),
        mapOf(propertyName to event.end.elementId),
        event.type,
        mapOf(propertyName to event.elementId),
        true,
        event.mutatedProperties(),
    )
  }

  override fun transformDelete(event: RelationshipEvent): CdcRelationshipData {
    return CdcRelationshipData(
        EntityOperation.DELETE,
        emptySet(),
        emptyMap(),
        emptySet(),
        emptyMap(),
        event.type,
        mapOf(propertyName to event.elementId),
        true,
        emptyMap(),
    )
  }
}
