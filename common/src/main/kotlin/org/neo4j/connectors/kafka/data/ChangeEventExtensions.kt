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
package org.neo4j.connectors.kafka.data

import java.time.format.DateTimeFormatter
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaAndValue
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.neo4j.cdc.client.model.ChangeEvent
import org.neo4j.cdc.client.model.Event
import org.neo4j.cdc.client.model.Metadata
import org.neo4j.cdc.client.model.Node
import org.neo4j.cdc.client.model.NodeEvent
import org.neo4j.cdc.client.model.NodeState
import org.neo4j.cdc.client.model.RelationshipEvent
import org.neo4j.cdc.client.model.RelationshipState

object ChangeEventExtensions {

  fun ChangeEvent.toConnectValue(): SchemaAndValue {
    val schema = this.toConnectSchema()
    return SchemaAndValue(schema, this.toConnectValue(schema))
  }

  private fun ChangeEvent.toConnectSchema(): Schema =
      SchemaBuilder.struct()
          .namespaced("cdc.ChangeEvent")
          .field("id", SimpleTypes.STRING.schema())
          .field("txId", SimpleTypes.LONG.schema())
          .field("seq", SimpleTypes.LONG.schema())
          .field("metadata", this.metadata.toConnectSchema())
          .field("event", this.event.toConnectSchema())
          .build()

  private fun ChangeEvent.toConnectValue(schema: Schema): Struct =
      Struct(schema).also {
        it.put("id", this.id.id)
        it.put("txId", this.txId)
        it.put("seq", this.seq.toLong())
        it.put("metadata", this.metadata.toConnectValue(schema.field("metadata").schema()))
        it.put("event", this.event.toConnectValue(schema.field("event").schema()))
      }

  private fun Metadata.toConnectSchema(): Schema =
      SchemaBuilder.struct()
          .namespaced("cdc.Metadata")
          .field("authenticatedUser", SimpleTypes.STRING.schema())
          .field("executingUser", SimpleTypes.STRING.schema())
          .field("connectionType", SimpleTypes.STRING.schema(true))
          .field("connectionClient", SimpleTypes.STRING.schema(true))
          .field("connectionServer", SimpleTypes.STRING.schema(true))
          .field("serverId", SimpleTypes.STRING.schema())
          .field("captureMode", SimpleTypes.STRING.schema())
          .field("txStartTime", SimpleTypes.ZONEDDATETIME.schema())
          .field("txCommitTime", SimpleTypes.ZONEDDATETIME.schema())
          .field("txMetadata", DynamicTypes.schemaFor(this.txMetadata, true).schema())
          .also {
            this.additionalEntries.forEach { entry ->
              it.field(entry.key, DynamicTypes.schemaFor(entry.value, true))
            }
          }
          .build()

  private fun Metadata.toConnectValue(schema: Schema): Struct =
      Struct(schema).also {
        it.put("authenticatedUser", this.authenticatedUser)
        it.put("executingUser", this.executingUser)
        it.put("connectionType", this.connectionType)
        it.put("connectionClient", this.connectionClient)
        it.put("connectionServer", this.connectionServer)
        it.put("serverId", this.serverId)
        it.put("captureMode", this.captureMode.name)
        it.put("txStartTime", DateTimeFormatter.ISO_DATE_TIME.format(this.txStartTime))
        it.put("txCommitTime", DateTimeFormatter.ISO_DATE_TIME.format(this.txCommitTime))
        it.put(
            "txMetadata",
            DynamicTypes.valueFor(schema.field("txMetadata").schema(), this.txMetadata))

        this.additionalEntries.forEach { entry ->
          it.put(entry.key, DynamicTypes.valueFor(schema.field(entry.key).schema(), entry.value))
        }
      }

  private fun Event.toConnectSchema(): Schema =
      when (val event = this) {
        is NodeEvent -> event.toConnectSchema()
        is RelationshipEvent -> event.toConnectSchema()
        else ->
            throw IllegalArgumentException(
                "unsupported event type in change data: ${event.javaClass.name}")
      }

  private fun Event.toConnectValue(schema: Schema): Struct =
      when (val event = this) {
        is NodeEvent -> event.toConnectValue(schema)
        is RelationshipEvent -> event.toConnectValue(schema)
        else -> throw IllegalArgumentException("unsupported event type ${event.javaClass.name}")
      }

  private fun NodeEvent.toConnectSchema(): Schema =
      SchemaBuilder.struct()
          .namespaced("cdc.NodeEvent")
          .field("elementId", SimpleTypes.STRING.schema())
          .field("eventType", SimpleTypes.STRING.schema())
          .field("operation", SimpleTypes.STRING.schema())
          .field("labels", SchemaBuilder.array(SimpleTypes.STRING.schema()).build())
          .field("keys", schemaForKeysByLabel(this.keys))
          .field("state", nodeStateSchema(before, after))
          .build()

  private fun NodeEvent.toConnectValue(schema: Schema): Struct =
      Struct(schema).also {
        val keys =
            if (this.keys.isEmpty()) {
              null
            } else {
              DynamicTypes.valueFor(schema.field("keys").schema(), this.keys)
            }
        it.put("elementId", this.elementId)
        it.put("eventType", this.eventType.name)
        it.put("operation", this.operation.name)
        it.put("labels", this.labels)
        it.put("keys", keys)
        it.put("state", nodeStateValue(schema.field("state").schema(), this.before, this.after))
      }

  private fun nodeStateSchema(before: NodeState?, after: NodeState?): Schema {
    val stateSchema =
        SchemaBuilder.struct()
            .namespaced("cdc.NodeState")
            .apply {
              this.field("labels", SchemaBuilder.array(SimpleTypes.STRING.schema()).build())
              this.field(
                  "properties",
                  SchemaBuilder.struct()
                      .also {
                        val combinedProperties =
                            (before?.properties?.toMutableMap() ?: mutableMapOf()).also {
                              it.putAll(after?.properties ?: emptyMap())
                            }
                        combinedProperties.toSortedMap().forEach { entry ->
                          if (it.field(entry.key) == null) {
                            it.field(entry.key, DynamicTypes.schemaFor(entry.value, true))
                          }
                        }
                      }
                      .build())
            }
            .optional()
            .build()

    return SchemaBuilder.struct()
        .namespaced("cdc.NodeStates")
        .field("before", stateSchema)
        .field("after", stateSchema)
        .build()
  }

  private fun nodeStateValue(schema: Schema, before: NodeState?, after: NodeState?): Struct =
      Struct(schema).apply {
        if (before != null) {
          this.put(
              "before",
              Struct(this.schema().field("before").schema()).also {
                it.put("labels", before.labels)
                it.put(
                    "properties",
                    DynamicTypes.valueFor(
                        it.schema().field("properties").schema(), before.properties))
              })
        }

        if (after != null) {
          this.put(
              "after",
              Struct(this.schema().field("after").schema()).also {
                it.put("labels", after.labels)
                it.put(
                    "properties",
                    DynamicTypes.valueFor(
                        it.schema().field("properties").schema(), after.properties))
              })
        }
      }

  private fun RelationshipEvent.toConnectSchema(): Schema =
      SchemaBuilder.struct()
          .namespaced("cdc.RelationshipEvent")
          .field("elementId", SimpleTypes.STRING.schema())
          .field("eventType", SimpleTypes.STRING.schema())
          .field("operation", SimpleTypes.STRING.schema())
          .field("type", SimpleTypes.STRING.schema())
          .field("start", this.start.toConnectSchema())
          .field("end", this.end.toConnectSchema())
          .field("keys", schemaForKeys(this.keys))
          .field("state", relationshipStateSchema(this.before, this.after))
          .build()

  private fun RelationshipEvent.toConnectValue(schema: Schema): Struct =
      Struct(schema).also {
        val keys =
            if (this.keys.isEmpty()) {
              null
            } else {
              DynamicTypes.valueFor(schema.field("keys").schema(), this.keys)
            }
        it.put("elementId", this.elementId)
        it.put("eventType", this.eventType.name)
        it.put("operation", this.operation.name)
        it.put("type", this.type)
        it.put("start", this.start.toConnectValue(schema.field("start").schema()))
        it.put("end", this.end.toConnectValue(schema.field("end").schema()))
        it.put("keys", keys)
        it.put(
            "state",
            relationshipStateValue(schema.field("state").schema(), this.before, this.after))
      }

  private fun Node.toConnectSchema(): Schema {
    return SchemaBuilder.struct()
        .namespaced("cdc.Node")
        .field("elementId", SimpleTypes.STRING.schema())
        .field("labels", SchemaBuilder.array(SimpleTypes.STRING.schema()).build())
        .field("keys", schemaForKeysByLabel(this.keys))
        .build()
  }

  private fun Node.toConnectValue(schema: Schema): Struct =
      Struct(schema).also {
        it.put("elementId", this.elementId)
        it.put("labels", this.labels)
        it.put("keys", DynamicTypes.valueFor(schema.field("keys").schema(), this.keys))
      }

  private fun relationshipStateSchema(
      before: RelationshipState?,
      after: RelationshipState?
  ): Schema {
    val stateSchema =
        SchemaBuilder.struct()
            .namespaced("cdc.RelationshipState")
            .apply {
              this.field(
                  "properties",
                  SchemaBuilder.struct()
                      .also {
                        val combinedProperties =
                            (before?.properties?.toMutableMap() ?: mutableMapOf()).also {
                              it.putAll(after?.properties ?: emptyMap())
                            }
                        combinedProperties.toSortedMap().forEach { entry ->
                          if (it.field(entry.key) == null) {
                            it.field(entry.key, DynamicTypes.schemaFor(entry.value, true))
                          }
                        }
                      }
                      .build())
            }
            .optional()
            .build()

    return SchemaBuilder.struct()
        .namespaced("cdc.RelationshipStates")
        .field("before", stateSchema)
        .field("after", stateSchema)
        .build()
  }

  private fun relationshipStateValue(
      schema: Schema,
      before: RelationshipState?,
      after: RelationshipState?
  ): Struct =
      Struct(schema).apply {
        if (before != null) {
          this.put(
              "before",
              Struct(this.schema().field("before").schema()).also {
                it.put(
                    "properties",
                    DynamicTypes.valueFor(
                        it.schema().field("properties").schema(), before.properties))
              })
        }

        if (after != null) {
          this.put(
              "after",
              Struct(this.schema().field("after").schema()).also {
                it.put(
                    "properties",
                    DynamicTypes.valueFor(
                        it.schema().field("properties").schema(), after.properties))
              })
        }
      }

  private fun schemaForKeysByLabel(keys: Map<String, List<Map<String, Any>>>): Schema {
    return SchemaBuilder.struct()
        .apply { keys.forEach { field(it.key, schemaForKeys(it.value)) } }
        .optional()
        .build()
  }

  private fun schemaForKeys(keys: List<Map<String, Any>>): Schema {
    return SchemaBuilder.array(
            // We need to define a uniform structure of key array elements. Because all elements
            // must have identical structure, we list all available keys as optional fields.
            SchemaBuilder.struct()
                .apply {
                  keys.forEach { key ->
                    key.forEach { field(it.key, DynamicTypes.schemaFor(it.value, true)) }
                  }
                }
                .optional()
                .build())
        .optional()
        .build()
  }
}
