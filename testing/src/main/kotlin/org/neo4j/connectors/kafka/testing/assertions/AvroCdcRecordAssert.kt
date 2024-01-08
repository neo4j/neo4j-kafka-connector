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

import org.apache.avro.generic.GenericArray
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8
import org.assertj.core.api.AbstractAssert
import org.neo4j.connectors.kafka.testing.GenericRecordSupport.asMap
import org.neo4j.connectors.kafka.testing.GenericRecordSupport.getArray
import org.neo4j.connectors.kafka.testing.GenericRecordSupport.getMap
import org.neo4j.connectors.kafka.testing.GenericRecordSupport.getRecord
import org.neo4j.connectors.kafka.testing.GenericRecordSupport.getString
import org.neo4j.connectors.kafka.testing.MapSupport.excludingKeys

class AvroCdcRecordAssert(actual: GenericRecord) :
    AbstractAssert<AvroCdcRecordAssert, GenericRecord>(actual, AvroCdcRecordAssert::class.java) {

  fun hasEventType(eventType: EventType): AvroCdcRecordAssert {
    isNotNull
    val actualEventType = actualEvent().getString("eventType")
    if (actualEventType != eventType.name) {
      failWithMessage("Expected event type to be <${eventType.name}> but was <$actualEventType>")
    }
    return this
  }

  fun hasOperation(operation: Operation): AvroCdcRecordAssert {
    isNotNull
    val actualOperation = actualEvent().getString("operation")
    if (actualOperation != operation.name) {
      failWithMessage("Expected operation to be <${operation.name}> but was <$actualOperation>")
    }
    return this
  }

  fun labelledAs(label: String): AvroCdcRecordAssert = hasLabels(setOf(label))

  fun hasLabels(labels: Set<String>): AvroCdcRecordAssert {
    isNotNull
    val actualLabels = actualEvent().getArray<Utf8>("labels")?.map { it.toString() }?.toSet()
    if (actualLabels != labels) {
      failWithMessage("Expected labels to be <${labels}> but was <$actualLabels>")
    }
    return this
  }

  fun hasNoBeforeState(): AvroCdcRecordAssert = hasNoState("before")

  fun hasNoAfterState(): AvroCdcRecordAssert = hasNoState("after")

  private fun hasNoState(state: String): AvroCdcRecordAssert {
    isNotNull
    val actualState = actualState().getRecord(state)
    if (actualState != null) {
      failWithMessage("Expected $state state to be <null> but was <$actualState>")
    }
    return this
  }

  fun hasBeforeStateProperties(
      properties: Map<String, Any>,
      vararg excludingKeys: String
  ): AvroCdcRecordAssert = hasStateProperties("before", properties, *excludingKeys)

  fun hasAfterStateProperties(
      properties: Map<String, Any>,
      vararg excludingKeys: String
  ): AvroCdcRecordAssert = hasStateProperties("after", properties, *excludingKeys)

  private fun hasStateProperties(
      state: String,
      props: Map<String, Any>,
      vararg excludingKeys: String
  ): AvroCdcRecordAssert {
    isNotNull
    val actualStateProperties =
        actualState()
            .getRecord(state)
            ?.getRecord("properties")
            ?.asMap()
            ?.excludingKeys(*excludingKeys) ?: emptyMap()
    if (actualStateProperties != props) {
      failWithMessage(
          "Expected $state state's properties to be <$props> but was <$actualStateProperties>")
    }
    return this
  }

  fun hasTxMetadata(txMetadata: Map<String, Any>): AvroCdcRecordAssert {
    isNotNull
    val actualTxMetadata = actualMetadata().getMap("txMetadata") ?: emptyMap()
    if (txMetadata != actualTxMetadata) {
      failWithMessage("Expect txMetadata to be <$txMetadata> but was <$actualTxMetadata>")
    }
    return this
  }

  fun hasType(type: String): AvroCdcRecordAssert {
    isNotNull
    val actualType = actualEvent().getString("type")
    if (actualType != type) {
      failWithMessage("Expected type to be <$type> but was <$$actualType>")
    }
    return this
  }

  fun startLabelledAs(label: String): AvroCdcRecordAssert {
    return hasEdgeLabels("start", setOf(label))
  }

  fun hasStartLabels(labels: Set<String>): AvroCdcRecordAssert {
    return hasEdgeLabels("start", labels)
  }

  fun endLabelledAs(label: String): AvroCdcRecordAssert {
    return hasEdgeLabels("end", setOf(label))
  }

  fun hasEndLabels(labels: Set<String>): AvroCdcRecordAssert {
    return hasEdgeLabels("end", labels)
  }

  private fun hasEdgeLabels(side: String, labels: Set<String>): AvroCdcRecordAssert {
    isNotNull
    val actualLabels =
        actualEvent().getRecord(side)?.getArray<Utf8>("labels")?.map { it.toString() }?.toSet()
    if (actualLabels != labels) {
      failWithMessage("Expected <$side> labels to be <${labels}> but was <$actualLabels>")
    }
    return this
  }

  fun hasNodeKeys(keys: Map<String, List<Map<String, Any>>>): AvroCdcRecordAssert {
    isNotNull
    val actualKeys =
        actualEvent().getRecord("keys")?.asMap()?.mapValues { array ->
          (array.value as GenericArray<*>).toList().map { (it as GenericRecord).asMap() }
        }
    if (actualKeys != keys) {
      failWithMessage("Expected nodes keys to be <$keys> but was <$actualKeys>")
    }
    return this
  }

  fun hasRelationshipKeys(keys: List<Map<String, Any>>): AvroCdcRecordAssert {
    isNotNull
    val actualKeys = actualEvent().getArray<GenericRecord>("keys")?.map { it.asMap() }
    if (actualKeys != keys) {
      failWithMessage("Expected relationship keys to be <$keys> but was <$actualKeys>")
    }
    return this
  }

  private fun actualEvent(): GenericRecord =
      actual.getRecord("event") ?: throw this.objects.failures.failure("Field 'event' is missing")

  private fun actualMetadata(): GenericRecord =
      actual.getRecord("metadata")
          ?: throw this.objects.failures.failure("Field 'metadata' is missing")

  private fun actualState(): GenericRecord =
      actualEvent().getRecord("state")
          ?: throw this.objects.failures.failure("Field 'event.state' is missing")

  companion object {
    fun assertThat(actual: GenericRecord): AvroCdcRecordAssert = AvroCdcRecordAssert(actual)
  }
}

enum class EventType {
  NODE,
  RELATIONSHIP
}

enum class Operation {
  CREATE,
  UPDATE,
  DELETE
}
