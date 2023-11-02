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
    val schema = schemaFor(this)
    return SchemaAndValue(schema, valueFor(schema, this))
  }

  private fun valueFor(schema: Schema, value: ChangeEvent): Struct =
      Struct(schema).apply {
        put("id", value.id.id)
        put("txId", value.txId)
        put("seq", value.seq.toLong())
        put("metadata", valueFor(schema.field("metadata").schema(), value.metadata))
        put("event", valueFor(schema.field("event").schema(), value.event))
      }

  private fun valueFor(schema: Schema, value: Metadata): Struct =
      Struct(schema).apply {
        put("authenticatedUser", value.authenticatedUser)
        put("executingUser", value.executingUser)
        put("connectionType", value.connectionType)
        put("connectionClient", value.connectionClient)
        put("connectionServer", value.connectionServer)
        put("serverId", value.serverId)
        put("captureMode", value.captureMode.name)
        put("txStartTime", DateTimeFormatter.ISO_DATE_TIME.format(value.txStartTime))
        put("txCommitTime", DateTimeFormatter.ISO_DATE_TIME.format(value.txCommitTime))

        value.additionalEntries.forEach {
          put(it.key, DynamicTypes.valueFor(schema.field(it.key).schema(), it.value))
        }
      }

  private fun valueFor(schema: Schema, value: Event): Struct =
      when (value) {
        is NodeEvent ->
            Struct(schema).apply {
              put("elementId", value.elementId)
              put("eventType", value.eventType.name)
              put("operation", value.operation.name)
              put("labels", value.labels)
              put("keys", DynamicTypes.valueFor(schema.field("keys").schema(), value.keys))
              put(
                  "state",
                  schema.field("state").schema().let { stateSchema ->
                    Struct(stateSchema)
                        .put("before", valueFor(stateSchema.field("before").schema(), value.before))
                        .put("after", valueFor(stateSchema.field("after").schema(), value.after))
                  })
            }
        is RelationshipEvent ->
            Struct(schema).apply {
              put("elementId", value.elementId)
              put("eventType", value.eventType.name)
              put("operation", value.operation.name)
              put("type", value.type)
              put("start", valueFor(schema.field("start").schema(), value.start))
              put("end", valueFor(schema.field("end").schema(), value.end))
              put("key", DynamicTypes.valueFor(schema.field("key").schema(), value.key))
              put(
                  "state",
                  schema.field("state").schema().let { stateSchema ->
                    Struct(stateSchema)
                        .put("before", valueFor(stateSchema.field("before").schema(), value.before))
                        .put("after", valueFor(stateSchema.field("after").schema(), value.after))
                  })
            }
        else -> throw IllegalArgumentException("unsupported event type ${value.javaClass.name}")
      }

  private fun valueFor(schema: Schema, value: NodeState?): Struct? =
      if (value == null) {
        null
      } else {
        Struct(schema).apply {
          put("labels", value.labels)
          put(
              "properties",
              DynamicTypes.valueFor(schema.field("properties").schema(), value.properties))
        }
      }

  private fun valueFor(schema: Schema, value: Node): Struct =
      Struct(schema).apply {
        put("elementId", value.elementId)
        put("labels", value.labels)
        put("keys", DynamicTypes.valueFor(schema.field("keys").schema(), value.keys))
      }

  private fun valueFor(schema: Schema, value: RelationshipState?): Struct? =
      if (value == null) {
        null
      } else {
        Struct(schema).apply {
          put(
              "properties",
              DynamicTypes.valueFor(schema.field("properties").schema(), value.properties))
        }
      }

  private fun schemaFor(change: ChangeEvent): Schema =
      SchemaBuilder.struct()
          .namespaced("cdc.ChangeEvent")
          .field("id", SimpleTypes.STRING.schema)
          .field("txId", SimpleTypes.LONG.schema)
          .field("seq", SimpleTypes.LONG.schema)
          .field(
              "metadata",
              SchemaBuilder.struct()
                  .namespaced("cdc.Metadata")
                  .field("authenticatedUser", SimpleTypes.STRING.schema)
                  .field("executingUser", SimpleTypes.STRING.schema)
                  .field("connectionType", SimpleTypes.STRING_NULLABLE.schema)
                  .field("connectionClient", SimpleTypes.STRING_NULLABLE.schema)
                  .field("connectionServer", SimpleTypes.STRING_NULLABLE.schema)
                  .field("serverId", SimpleTypes.STRING.schema)
                  .field("captureMode", SimpleTypes.STRING.schema)
                  .field("txStartTime", SimpleTypes.ZONEDDATETIME.schema)
                  .field("txCommitTime", SimpleTypes.ZONEDDATETIME.schema)
                  .apply {
                    change.metadata.additionalEntries.forEach {
                      field(it.key, DynamicTypes.schemaFor(it.value, true))
                    }
                  }
                  .build())
          .field(
              "event",
              when (val event = change.event) {
                is NodeEvent ->
                    SchemaBuilder.struct()
                        .namespaced("cdc.NodeEvent")
                        .field("elementId", SimpleTypes.STRING.schema)
                        .field("eventType", SimpleTypes.STRING.schema)
                        .field("operation", SimpleTypes.STRING.schema)
                        .field("labels", SchemaBuilder.array(SimpleTypes.STRING.schema).build())
                        .field("keys", schemaForKeys(event.keys))
                        .field(
                            "state",
                            SchemaBuilder.struct()
                                .namespaced("cdc.NodeStates")
                                .field("before", schemaFor(event.before))
                                .field("after", schemaFor(event.after))
                                .build())
                        .build()
                is RelationshipEvent ->
                    SchemaBuilder.struct()
                        .namespaced("cdc.RelationshipEvent")
                        .field("elementId", SimpleTypes.STRING.schema)
                        .field("eventType", SimpleTypes.STRING.schema)
                        .field("operation", SimpleTypes.STRING.schema)
                        .field("type", SimpleTypes.STRING.schema)
                        .field("start", schemaFor(event.start))
                        .field("end", schemaFor(event.end))
                        .field("key", schemaForKey(event.key))
                        .field(
                            "state",
                            SchemaBuilder.struct()
                                .namespaced("cdc.RelationshipStates")
                                .field("before", schemaFor(event.before))
                                .field("after", schemaFor(event.after))
                                .build())
                        .build()
                else ->
                    throw IllegalArgumentException(
                        "unsupported event type in change data: ${event.javaClass.name}")
              })
          .build()

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

  private fun schemaFor(state: NodeState?): Schema {
    if (state == null) {
      return SchemaBuilder.struct()
          .namespaced("cdc.EmptyNodeState")
          .field("labels", SchemaBuilder.array(SimpleTypes.STRING.schema).build())
          .field("properties", SchemaBuilder.struct().build())
          .optional()
          .build()
    }

    return SchemaBuilder.struct()
        .namespaced("cdc.NodeState")
        .field("labels", SchemaBuilder.array(SimpleTypes.STRING.schema).build())
        .field(
            "properties",
            SchemaBuilder.struct()
                .apply {
                  state.properties.forEach { field(it.key, DynamicTypes.schemaFor(it.value, true)) }
                }
                .build())
        .optional()
        .build()
  }

  private fun schemaFor(state: RelationshipState?): Schema {
    if (state == null) {
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
                .apply {
                  state.properties.forEach { field(it.key, DynamicTypes.schemaFor(it.value, true)) }
                }
                .build())
        .optional()
        .build()
  }

  private fun schemaFor(node: Node): Schema {
    return SchemaBuilder.struct()
        .namespaced("cdc.Node")
        .field("elementId", SimpleTypes.STRING.schema)
        .field("labels", SchemaBuilder.array(SimpleTypes.STRING.schema).build())
        .field("keys", DynamicTypes.schemaFor(node.keys, true))
        .build()
  }
}
