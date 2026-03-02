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

import org.neo4j.cdc.client.model.NodeEvent
import org.neo4j.cdc.client.model.RelationshipEvent
import org.neo4j.connectors.kafka.exceptions.InvalidDataException
import org.neo4j.connectors.kafka.sink.SinkConfiguration
import org.neo4j.connectors.kafka.sink.strategy.DeleteNodeSinkAction
import org.neo4j.connectors.kafka.sink.strategy.DeleteRelationshipSinkAction
import org.neo4j.connectors.kafka.sink.strategy.LookupMode
import org.neo4j.connectors.kafka.sink.strategy.MergeNodeSinkAction
import org.neo4j.connectors.kafka.sink.strategy.MergeRelationshipSinkAction
import org.neo4j.connectors.kafka.sink.strategy.SinkAction
import org.neo4j.connectors.kafka.sink.strategy.SinkActionNodeReference
import org.neo4j.connectors.kafka.sink.strategy.addedLabels
import org.neo4j.connectors.kafka.sink.strategy.mutatedProperties
import org.neo4j.connectors.kafka.sink.strategy.removedLabels

class CdcSourceIdEventTransformer(
    val topic: String,
    val labelName: String = SinkConfiguration.DEFAULT_SOURCE_ID_LABEL_NAME,
    val propertyName: String = SinkConfiguration.DEFAULT_SOURCE_ID_PROPERTY_NAME,
) : CdcEventTransformer {

  override fun transformCreate(event: NodeEvent): SinkAction {
    if (event.before != null) {
      throw InvalidDataException(
          "create operation requires 'before' field to be unset in the event object."
      )
    }

    if (event.after == null) {
      throw InvalidDataException("create operation requires 'after' field in the event object.")
    }

    return MergeNodeSinkAction(
        setOf(labelName),
        mapOf(propertyName to event.elementId),
        event.after.properties,
        event.after.labels.minus(labelName).toSet(),
        emptySet(),
    )
  }

  override fun transformUpdate(event: NodeEvent): SinkAction {
    if (event.before == null) {
      throw InvalidDataException("update operation requires 'before' field in the event object.")
    }
    if (event.after == null) {
      throw InvalidDataException("update operation requires 'after' field in the event object.")
    }

    return MergeNodeSinkAction(
        setOf(labelName),
        mapOf(propertyName to event.elementId),
        event.mutatedProperties(),
        event.addedLabels().toSet(),
        event.removedLabels().toSet(),
    )
  }

  override fun transformDelete(event: NodeEvent): SinkAction {
    if (event.before == null) {
      throw InvalidDataException("delete operation requires 'before' field in the event object.")
    }

    if (event.after != null) {
      throw InvalidDataException(
          "delete operation requires 'after' field to be unset in the event object."
      )
    }

    return DeleteNodeSinkAction(setOf(labelName), mapOf(propertyName to event.elementId))
  }

  override fun transformCreate(event: RelationshipEvent): SinkAction {
    if (event.before != null) {
      throw InvalidDataException(
          "create operation requires 'before' field to be unset in the event object."
      )
    }

    if (event.after == null) {
      throw InvalidDataException("create operation requires 'after' field in the event object.")
    }

    return MergeRelationshipSinkAction(
        SinkActionNodeReference(
            setOf(labelName),
            mapOf(propertyName to event.start.elementId),
            LookupMode.MERGE,
        ),
        SinkActionNodeReference(
            setOf(labelName),
            mapOf(propertyName to event.end.elementId),
            LookupMode.MERGE,
        ),
        event.type,
        mapOf(propertyName to event.elementId),
        event.after.properties,
        true,
    )
  }

  override fun transformUpdate(event: RelationshipEvent): SinkAction {
    if (event.before == null) {
      throw InvalidDataException("update operation requires 'before' field in the event object.")
    }
    if (event.after == null) {
      throw InvalidDataException("update operation requires 'after' field in the event object.")
    }

    return MergeRelationshipSinkAction(
        SinkActionNodeReference(
            setOf(labelName),
            mapOf(propertyName to event.start.elementId),
            LookupMode.MERGE,
        ),
        SinkActionNodeReference(
            setOf(labelName),
            mapOf(propertyName to event.end.elementId),
            LookupMode.MERGE,
        ),
        event.type,
        mapOf(propertyName to event.elementId),
        event.mutatedProperties(),
        true,
    )
  }

  override fun transformDelete(event: RelationshipEvent): SinkAction {
    if (event.before == null) {
      throw InvalidDataException("delete operation requires 'before' field in the event object.")
    }

    if (event.after != null) {
      throw InvalidDataException(
          "delete operation requires 'after' field to be unset in the event object."
      )
    }

    return DeleteRelationshipSinkAction(
        SinkActionNodeReference(
            setOf(labelName),
            mapOf(propertyName to event.start.elementId),
            LookupMode.MATCH,
        ),
        SinkActionNodeReference(
            setOf(labelName),
            mapOf(propertyName to event.end.elementId),
            LookupMode.MATCH,
        ),
        event.type,
        mapOf(propertyName to event.elementId),
        true,
    )
  }
}
