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

package org.neo4j.connectors.kafka.testing.format

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import org.neo4j.cdc.client.model.CaptureMode
import org.neo4j.cdc.client.model.ChangeEvent
import org.neo4j.cdc.client.model.ChangeIdentifier
import org.neo4j.cdc.client.model.EntityOperation
import org.neo4j.cdc.client.model.Event
import org.neo4j.cdc.client.model.EventType
import org.neo4j.cdc.client.model.Node
import org.neo4j.cdc.client.model.NodeEvent
import org.neo4j.cdc.client.model.NodeState
import org.neo4j.cdc.client.model.RelationshipEvent
import org.neo4j.cdc.client.model.RelationshipState
import org.slf4j.Logger
import org.slf4j.LoggerFactory

object ChangeEventSupport {

  private val log: Logger = LoggerFactory.getLogger(this::class.java)

  fun mapToChangeEvent(map: Map<String, Any?>): ChangeEvent {
    return ChangeEvent(
        ChangeIdentifier(map.getString("id") ?: ""),
        map.getLong("txId"),
        map.getLong("seq")?.toInt(),
        mapToMetadata(map.getMap("metadata")),
        mapToEvent(map.getMap("event")))
  }

  private fun mapToEvent(eventMap: Map<String, Any?>?): Event? {
    if (eventMap == null) {
      return null
    }
    val eventType = EventType.valueOf(eventMap.getRequiredString("eventType"))
    val elementId = eventMap.getString("elementId")
    val operation = EntityOperation.valueOf(eventMap.getRequiredString("operation"))
    val state = eventMap.getMap("state")
    return when (eventType) {
      EventType.NODE ->
          NodeEvent(
              elementId,
              operation,
              eventMap.getList("labels"),
              eventMap.getNodeKeys(),
              mapToNodeState(state?.getMap("before")),
              mapToNodeState(state?.getMap("after")))
      EventType.RELATIONSHIP ->
          RelationshipEvent(
              elementId,
              eventMap.getString("type"),
              mapToNode(eventMap.getMap("start")),
              mapToNode(eventMap.getMap("emd")),
              eventMap.getRelationshipKeys(),
              operation,
              mapToRelationshipState(state?.getMap("before")),
              mapToRelationshipState(state?.getMap("after")))
    }
  }

  private fun mapToNodeState(stateMap: Map<String, Any?>?): NodeState? {
    if (stateMap == null) {
      return null
    }
    return NodeState(stateMap.getList("labels"), stateMap.getMap("properties"))
  }

  private fun mapToNode(nodeMap: Map<String, Any?>?): Node? {
    if (nodeMap == null) {
      return null
    }
    return Node(nodeMap.getString("elementId"), nodeMap.getList("labels"), nodeMap.getNodeKeys())
  }

  private fun mapToRelationshipState(stateMap: Map<String, Any?>?): RelationshipState? {
    if (stateMap == null) {
      return null
    }
    return RelationshipState(stateMap.getMap("properties"))
  }

  private fun mapToMetadata(metadataMap: Map<String, Any?>?): org.neo4j.cdc.client.model.Metadata? {
    if (metadataMap == null) {
      return null
    }
    val additionalMetadata =
        metadataMap.filterKeys {
          !setOf(
                  "authenticatedUser",
                  "executingUser",
                  "serverId",
                  "captureMode",
                  "connectionType",
                  "connectionClient",
                  "connectionServer",
                  "txStartTime",
                  "txCommitTime",
                  "txMetadata")
              .contains(it)
        }
    return org.neo4j.cdc.client.model.Metadata(
        metadataMap.getString("authenticatedUser"),
        metadataMap.getString("executingUser"),
        metadataMap.getString("serverId"),
        CaptureMode.valueOf(metadataMap.getString("captureMode") ?: "FULL"),
        metadataMap.getString("connectionType"),
        metadataMap.getString("connectionClient"),
        metadataMap.getString("connectionServer"),
        metadataMap.getZonedDateTime("txStartTime"),
        metadataMap.getZonedDateTime("txCommitTime"),
        metadataMap.getMap("txMetadata"),
        additionalMetadata,
    )
  }

  @Suppress("UNCHECKED_CAST")
  private fun <V> Map<String, Any?>.getValueAs(key: String, valueClass: Class<V>): V? {
    val v = this[key] ?: return null
    if (!valueClass.isAssignableFrom(v::class.java)) {
      log.debug("Cannot assign {} from {} ({})", valueClass, v, v::class.java)
      return null
    }
    return v as V?
  }

  private fun Map<String, Any?>.getString(key: String): String? =
      this.getValueAs(key, String::class.java)

  private fun Map<String, Any?>.getRequiredString(key: String): String =
      this.getString(key)
          ?: throw IllegalArgumentException(
              "Value for key $key is not found or doesn't have String type")

  private fun Map<String, Any?>.getLong(key: String): Long? =
      this.getValueAs(key, java.lang.Long::class.java)?.toLong()

  @Suppress("UNCHECKED_CAST")
  private fun Map<String, Any?>.getMap(key: String): Map<String, Any?>? {
    val map = this.getValueAs(key, Map::class.java) ?: return null
    return map as Map<String, Any?>
  }

  private fun Map<String, Any?>.getZonedDateTime(key: String): ZonedDateTime? =
      this.getString(key)?.let { ZonedDateTime.parse(it, DateTimeFormatter.ISO_DATE_TIME) }

  @Suppress("UNCHECKED_CAST")
  private fun Map<String, Any?>.getList(key: String): List<String> =
      this.getValueAs(key, List::class.java) as List<String>

  @Suppress("UNCHECKED_CAST")
  private fun Map<String, Any?>.getNodeKeys(): Map<String, List<Map<String, Any>>>? {
    val keys = this.getMap("keys") ?: return null
    return keys as Map<String, List<Map<String, Any>>>
  }

  @Suppress("UNCHECKED_CAST")
  private fun Map<String, Any?>.getRelationshipKeys(): List<Map<String, Any>>? {
    val keys = this.getMap("keys") ?: return null
    return keys as List<Map<String, Any>>
  }
}
