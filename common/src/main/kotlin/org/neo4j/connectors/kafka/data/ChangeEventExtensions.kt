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
          .field("keys", schemaForKeys(this.keys))
          .field(
              "state",
              SchemaBuilder.struct()
                  .namespaced("cdc.NodeStates")
                  .field("before", this.before.toConnectSchema())
                  .field("after", this.after.toConnectSchema())
                  .build())
          .build()

  private fun NodeEvent.toConnectValue(schema: Schema): Struct =
      Struct(schema).also {
        it.put("elementId", this.elementId)
        it.put("eventType", this.eventType.name)
        it.put("operation", this.operation.name)
        it.put("labels", this.labels)
        it.put("keys", DynamicTypes.valueFor(schema.field("keys").schema(), this.keys))
        it.put(
            "state",
            schema.field("state").schema().let { stateSchema ->
              Struct(stateSchema)
                  .put("before", this.before.toConnectValue(stateSchema.field("before").schema()))
                  .put("after", this.after.toConnectValue(stateSchema.field("after").schema()))
            })
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
          .field("key", schemaForKey(this.key))
          .field(
              "state",
              SchemaBuilder.struct()
                  .namespaced("cdc.RelationshipStates")
                  .field("before", this.before.toConnectSchema())
                  .field("after", this.after.toConnectSchema())
                  .build())
          .build()

  private fun RelationshipEvent.toConnectValue(schema: Schema): Struct =
      Struct(schema).also {
        it.put("elementId", this.elementId)
        it.put("eventType", this.eventType.name)
        it.put("operation", this.operation.name)
        it.put("type", this.type)
        it.put("start", this.start.toConnectValue(schema.field("start").schema()))
        it.put("end", this.end.toConnectValue(schema.field("end").schema()))
        it.put("key", DynamicTypes.valueFor(schema.field("key").schema(), this.key))
        it.put(
            "state",
            schema.field("state").schema().let { stateSchema ->
              Struct(stateSchema)
                  .put("before", this.before.toConnectValue(stateSchema.field("before").schema()))
                  .put("after", this.after.toConnectValue(stateSchema.field("after").schema()))
            })
      }

  private fun NodeState?.toConnectSchema(): Schema {
    if (this == null) {
      return SchemaBuilder.struct()
          .namespaced("cdc.EmptyNodeState")
          .field("labels", SchemaBuilder.array(SimpleTypes.STRING.schema()).build())
          .field("properties", SchemaBuilder.struct().build())
          .optional()
          .build()
    }

    return SchemaBuilder.struct()
        .namespaced("cdc.NodeState")
        .field("labels", SchemaBuilder.array(SimpleTypes.STRING.schema()).build())
        .field(
            "properties",
            SchemaBuilder.struct()
                .also {
                  this.properties.forEach { entry ->
                    it.field(entry.key, DynamicTypes.schemaFor(entry.value, true))
                  }
                }
                .build())
        .optional()
        .build()
  }

  private fun NodeState?.toConnectValue(schema: Schema): Struct? =
      if (this == null) {
        null
      } else {
        Struct(schema).also {
          it.put("labels", this.labels)
          it.put(
              "properties",
              DynamicTypes.valueFor(schema.field("properties").schema(), this.properties))
        }
      }

  private fun Node.toConnectSchema(): Schema {
    return SchemaBuilder.struct()
        .namespaced("cdc.Node")
        .field("elementId", SimpleTypes.STRING.schema())
        .field("labels", SchemaBuilder.array(SimpleTypes.STRING.schema()).build())
        .field("keys", DynamicTypes.schemaFor(this.keys, true))
        .build()
  }

  private fun Node.toConnectValue(schema: Schema): Struct =
      Struct(schema).also {
        it.put("elementId", this.elementId)
        it.put("labels", this.labels)
        it.put("keys", DynamicTypes.valueFor(schema.field("keys").schema(), this.keys))
      }

  private fun RelationshipState?.toConnectSchema(): Schema {
    if (this == null) {
      return SchemaBuilder.struct()
          .namespaced("cdc.EmptyRelationshipState")
          .field("properties", SchemaBuilder.struct().build())
          .optional()
          .build()
    }

    return SchemaBuilder.struct()
        .namespaced("cdc.RelationshipState")
        .field(
            "properties",
            SchemaBuilder.struct()
                .also {
                  this.properties.forEach { entry ->
                    it.field(entry.key, DynamicTypes.schemaFor(entry.value, true))
                  }
                }
                .build())
        .optional()
        .build()
  }

  private fun RelationshipState?.toConnectValue(schema: Schema): Struct? =
      if (this == null) {
        null
      } else {
        Struct(schema).also {
          it.put(
              "properties",
              DynamicTypes.valueFor(schema.field("properties").schema(), this.properties))
        }
      }

  private fun schemaForKeys(keys: Map<String, Map<String, Any>>): Schema {
    return SchemaBuilder.struct()
        .apply { keys.forEach { field(it.key, schemaForKey(it.value)) } }
        .optional()
        .build()
  }

  private fun schemaForKey(key: Map<String, Any>): Schema {
    return SchemaBuilder.struct()
        .apply { key.forEach { field(it.key, DynamicTypes.schemaFor(it.value, true)) } }
        .optional()
        .build()
  }
}
