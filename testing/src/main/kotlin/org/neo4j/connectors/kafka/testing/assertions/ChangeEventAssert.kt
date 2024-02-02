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

package org.neo4j.connectors.kafka.testing.assertions

import org.assertj.core.api.AbstractAssert
import org.neo4j.cdc.client.model.ChangeEvent
import org.neo4j.cdc.client.model.EntityEvent
import org.neo4j.cdc.client.model.EntityOperation
import org.neo4j.cdc.client.model.EventType
import org.neo4j.cdc.client.model.NodeEvent
import org.neo4j.cdc.client.model.RelationshipEvent
import org.neo4j.connectors.kafka.testing.MapSupport.excludingKeys

class ChangeEventAssert(actual: ChangeEvent) :
    AbstractAssert<ChangeEventAssert, ChangeEvent>(actual, ChangeEventAssert::class.java) {

  fun hasEventType(eventType: EventType): ChangeEventAssert {
    isNotNull
    if (event().eventType != eventType) {
      failWithMessage(
          "Expected event type to be <${eventType.name}> but was <${event().eventType}>")
    }
    return this
  }

  fun hasOperation(operation: EntityOperation): ChangeEventAssert {
    isNotNull
    if (event().operation != operation) {
      failWithMessage("Expected operation to be <${operation.name}> but was <${event().operation}>")
    }
    return this
  }

  fun labelledAs(label: String): ChangeEventAssert = hasLabels(setOf(label))

  fun hasLabels(labels: Set<String>): ChangeEventAssert {
    isNotNull
    if (nodeEvent().labels.toSet() != labels) {
      failWithMessage("Expected labels to be <${labels}> but was <${nodeEvent().labels}>")
    }
    return this
  }

  fun hasNoBeforeState(): ChangeEventAssert {
    isNotNull
    if (event().before != null) {
      failWithMessage("Expected before state to be <null> but was <${event().before}>")
    }
    return this
  }

  fun hasNoAfterState(): ChangeEventAssert {
    isNotNull
    if (event().after != null) {
      failWithMessage("Expected after state to be <null> but was <${event().after}>")
    }
    return this
  }

  fun hasBeforeStateProperties(
      properties: Map<String, Any>,
      vararg excludingKeys: String
  ): ChangeEventAssert {
    isNotNull
    val actualProperties = event().before?.properties?.excludingKeys(*excludingKeys) ?: emptyMap()
    if (actualProperties != properties) {
      failWithMessage(
          "Expected before state's properties to be <$properties> but was <$actualProperties>")
    }
    return this
  }

  fun hasAfterStateProperties(
      properties: Map<String, Any>,
      vararg excludingKeys: String
  ): ChangeEventAssert {

    isNotNull
    val actualProperties = event().after?.properties?.excludingKeys(*excludingKeys) ?: emptyMap()
    if (actualProperties != properties) {
      failWithMessage(
          "Expected after state's properties to be <$properties> but was <$actualProperties>")
    }
    return this
  }

  fun hasTxMetadata(txMetadata: Map<String, Any>): ChangeEventAssert {
    isNotNull
    if (actual.metadata.txMetadata != txMetadata) {
      failWithMessage(
          "Expect txMetadata to be <$txMetadata> but was <${actual.metadata.txMetadata}>",
      )
    }
    return this
  }

  fun hasType(type: String): ChangeEventAssert {
    isNotNull
    if (relationshipEvent().type != type) {
      failWithMessage("Expected type to be <$type> but was <$${relationshipEvent().type}>")
    }
    return this
  }

  fun startLabelledAs(label: String): ChangeEventAssert {
    return hasStartLabels(setOf(label))
  }

  fun hasStartLabels(labels: Set<String>): ChangeEventAssert {
    isNotNull
    if (relationshipEvent().start.labels.toSet() != labels) {
      failWithMessage(
          "Expected start labels to be <${labels}> but was <${relationshipEvent().start.labels}>")
    }
    return this
  }

  fun endLabelledAs(label: String): ChangeEventAssert {
    return hasEndLabels(setOf(label))
  }

  fun hasEndLabels(labels: Set<String>): ChangeEventAssert {
    isNotNull
    if (relationshipEvent().end.labels.toSet() != labels) {
      failWithMessage(
          "Expected end labels to be <${labels}> but was <${relationshipEvent().end.labels}>")
    }
    return this
  }

  fun hasNodeKeys(keys: Map<String, List<Map<String, Any>>>): ChangeEventAssert {
    isNotNull
    if (nodeEvent().keys != keys) {
      failWithMessage("Expected nodes keys to be <$keys> but was <${nodeEvent().keys}>")
    }
    return this
  }

  fun hasRelationshipKeys(keys: List<Map<String, Any>>): ChangeEventAssert {
    isNotNull
    if (relationshipEvent().keys != keys) {
      failWithMessage(
          "Expected relationship keys to be <$keys> but was <${relationshipEvent().keys}>")
    }
    return this
  }

  private fun nodeEvent(): NodeEvent {
    val event = actual.event ?: throw this.objects.failures.failure("Field 'event' is missing")
    if (event is NodeEvent) {
      return event
    }
    throw this.objects.failures.failure("Field 'event' has unexpected type ${event::class.java}")
  }

  private fun relationshipEvent(): RelationshipEvent {
    val event = actual.event ?: throw this.objects.failures.failure("Field 'event' is missing")
    if (event is RelationshipEvent) {
      return event
    }
    throw this.objects.failures.failure("Field 'event' has unexpected type ${event::class.java}")
  }

  private fun event(): EntityEvent<*> {
    val event = actual.event ?: throw this.objects.failures.failure("Field 'event' is missing")
    return event as EntityEvent<*>
  }

  companion object {
    fun assertThat(actual: ChangeEvent): ChangeEventAssert = ChangeEventAssert(actual)
  }
}
