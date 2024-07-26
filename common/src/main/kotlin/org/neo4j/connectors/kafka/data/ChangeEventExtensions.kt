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
package org.neo4j.connectors.kafka.data

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaAndValue
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.neo4j.cdc.client.model.ChangeEvent
import org.neo4j.cdc.client.model.ChangeIdentifier
import org.neo4j.cdc.client.model.EntityOperation
import org.neo4j.cdc.client.model.Event
import org.neo4j.cdc.client.model.EventType
import org.neo4j.cdc.client.model.Metadata
import org.neo4j.cdc.client.model.Node
import org.neo4j.cdc.client.model.NodeEvent
import org.neo4j.cdc.client.model.NodeState
import org.neo4j.cdc.client.model.RelationshipEvent
import org.neo4j.cdc.client.model.RelationshipState
import org.neo4j.connectors.kafka.data.DynamicTypes.toConnectSchema

class ChangeEventConverter(
    val temporalDataSchemaType: TemporalDataSchemaType = TemporalDataSchemaType.STRUCT,
) {

  fun toConnectValue(changeEvent: ChangeEvent): SchemaAndValue {
    val schema = toConnectSchema(changeEvent)
    return SchemaAndValue(schema, toConnectValue(changeEvent, schema))
  }

  private fun toConnectSchema(changeEvent: ChangeEvent): Schema =
      SchemaBuilder.struct()
          .field("id", Schema.STRING_SCHEMA)
          .field("txId", Schema.INT64_SCHEMA)
          .field("seq", Schema.INT64_SCHEMA)
          .field("metadata", metadataToConnectSchema(changeEvent.metadata))
          .field("event", eventToConnectSchema(changeEvent.event))
          .build()

  private fun toConnectValue(changeEvent: ChangeEvent, schema: Schema): Struct =
      Struct(schema).also {
        it.put("id", changeEvent.id.id)
        it.put("txId", changeEvent.txId)
        it.put("seq", changeEvent.seq.toLong())
        it.put(
            "metadata",
            metadataToConnectValue(changeEvent.metadata, schema.field("metadata").schema()))
        it.put("event", eventToConnectValue(changeEvent.event, schema.field("event").schema()))
      }

  internal fun metadataToConnectSchema(metadata: Metadata): Schema =
      SchemaBuilder.struct()
          .field("authenticatedUser", Schema.STRING_SCHEMA)
          .field("executingUser", Schema.STRING_SCHEMA)
          .field("connectionType", Schema.OPTIONAL_STRING_SCHEMA)
          .field("connectionClient", Schema.OPTIONAL_STRING_SCHEMA)
          .field("connectionServer", Schema.OPTIONAL_STRING_SCHEMA)
          .field("serverId", Schema.STRING_SCHEMA)
          .field("captureMode", Schema.STRING_SCHEMA)
          .field("txStartTime", PropertyType.schema)
          .field("txCommitTime", PropertyType.schema)
          .field(
              "txMetadata",
              toConnectSchema(
                      metadata.txMetadata,
                      optional = true,
                      forceMapsAsStruct = true,
                      temporalDataSchemaType = temporalDataSchemaType)
                  .schema())
          .also {
            metadata.additionalEntries.forEach { entry ->
              it.field(
                  entry.key,
                  toConnectSchema(
                      entry.value,
                      optional = true,
                      temporalDataSchemaType = temporalDataSchemaType))
            }
          }
          .build()

  internal fun metadataToConnectValue(metadata: Metadata, schema: Schema): Struct =
      Struct(schema).also {
        it.put("authenticatedUser", metadata.authenticatedUser)
        it.put("executingUser", metadata.executingUser)
        it.put("connectionType", metadata.connectionType)
        it.put("connectionClient", metadata.connectionClient)
        it.put("connectionServer", metadata.connectionServer)
        it.put("serverId", metadata.serverId)
        it.put("captureMode", metadata.captureMode.name)
        it.put(
            "txStartTime", DynamicTypes.toConnectValue(PropertyType.schema, metadata.txStartTime))
        it.put(
            "txCommitTime", DynamicTypes.toConnectValue(PropertyType.schema, metadata.txCommitTime))
        it.put(
            "txMetadata",
            DynamicTypes.toConnectValue(schema.field("txMetadata").schema(), metadata.txMetadata))

        metadata.additionalEntries.forEach { entry ->
          it.put(
              entry.key, DynamicTypes.toConnectValue(schema.field(entry.key).schema(), entry.value))
        }
      }

  private fun eventToConnectSchema(event: Event): Schema =
      when (event) {
        is NodeEvent -> nodeEventToConnectSchema(event)
        is RelationshipEvent -> relationshipEventToConnectSchema(event)
        else ->
            throw IllegalArgumentException(
                "unsupported event type in change data: ${event.javaClass.name}")
      }

  private fun eventToConnectValue(event: Event, schema: Schema): Struct =
      when (event) {
        is NodeEvent -> nodeEventToConnectValue(event, schema)
        is RelationshipEvent -> relationshipEventToConnectValue(event, schema)
        else -> throw IllegalArgumentException("unsupported event type ${event.javaClass.name}")
      }

  internal fun nodeEventToConnectSchema(nodeEvent: NodeEvent): Schema =
      SchemaBuilder.struct()
          .field("elementId", Schema.STRING_SCHEMA)
          .field("eventType", Schema.STRING_SCHEMA)
          .field("operation", Schema.STRING_SCHEMA)
          .field("labels", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
          .field("keys", schemaForKeysByLabel(nodeEvent.keys))
          .field("state", nodeStateSchema(nodeEvent.before, nodeEvent.after))
          .build()

  internal fun nodeEventToConnectValue(nodeEvent: NodeEvent, schema: Schema): Struct =
      Struct(schema).also {
        val keys = DynamicTypes.toConnectValue(schema.field("keys").schema(), nodeEvent.keys)
        it.put("elementId", nodeEvent.elementId)
        it.put("eventType", nodeEvent.eventType.name)
        it.put("operation", nodeEvent.operation.name)
        it.put("labels", nodeEvent.labels)
        it.put("keys", keys)
        it.put(
            "state",
            nodeStateValue(schema.field("state").schema(), nodeEvent.before, nodeEvent.after))
      }

  internal fun relationshipEventToConnectSchema(relationshipEvent: RelationshipEvent): Schema =
      SchemaBuilder.struct()
          .field("elementId", Schema.STRING_SCHEMA)
          .field("eventType", Schema.STRING_SCHEMA)
          .field("operation", Schema.STRING_SCHEMA)
          .field("type", Schema.STRING_SCHEMA)
          .field("start", nodeToConnectSchema(relationshipEvent.start))
          .field("end", nodeToConnectSchema(relationshipEvent.end))
          .field("keys", schemaForKeys())
          .field(
              "state", relationshipStateSchema(relationshipEvent.before, relationshipEvent.after))
          .build()

  internal fun relationshipEventToConnectValue(
      relationshipEvent: RelationshipEvent,
      schema: Schema
  ): Struct =
      Struct(schema).also {
        val keys =
            DynamicTypes.toConnectValue(schema.field("keys").schema(), relationshipEvent.keys)
        it.put("elementId", relationshipEvent.elementId)
        it.put("eventType", relationshipEvent.eventType.name)
        it.put("operation", relationshipEvent.operation.name)
        it.put("type", relationshipEvent.type)
        it.put("start", nodeToConnectValue(relationshipEvent.start, schema.field("start").schema()))
        it.put("end", nodeToConnectValue(relationshipEvent.end, schema.field("end").schema()))
        it.put("keys", keys)
        it.put(
            "state",
            relationshipStateValue(
                schema.field("state").schema(), relationshipEvent.before, relationshipEvent.after))
      }

  internal fun nodeToConnectSchema(node: Node): Schema {
    return SchemaBuilder.struct()
        .field("elementId", Schema.STRING_SCHEMA)
        .field("labels", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
        .field("keys", schemaForKeysByLabel(node.keys))
        .build()
  }

  internal fun nodeToConnectValue(node: Node, schema: Schema): Struct =
      Struct(schema).also {
        it.put("elementId", node.elementId)
        it.put("labels", node.labels)
        it.put("keys", DynamicTypes.toConnectValue(schema.field("keys").schema(), node.keys))
      }

  private fun schemaForKeysByLabel(keys: Map<String, List<Map<String, Any>>>?): Schema {
    return SchemaBuilder.struct()
        .apply { keys?.forEach { field(it.key, schemaForKeys()) } }
        .optional()
        .build()
  }

  private fun schemaForKeys(): Schema {
    return SchemaBuilder.array(SchemaBuilder.map(Schema.STRING_SCHEMA, PropertyType.schema).build())
        .optional()
        .build()
  }

  private fun nodeStateSchema(before: NodeState?, after: NodeState?): Schema {
    val stateSchema =
        SchemaBuilder.struct()
            .apply {
              this.field("labels", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
              this.field(
                  "properties",
                  SchemaBuilder.map(Schema.STRING_SCHEMA, PropertyType.schema).build())
            }
            .optional()
            .build()

    return SchemaBuilder.struct().field("before", stateSchema).field("after", stateSchema).build()
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
                    before.properties.mapValues { e ->
                      DynamicTypes.toConnectValue(PropertyType.schema, e.value)
                    })
              })
        }

        if (after != null) {
          this.put(
              "after",
              Struct(this.schema().field("after").schema()).also {
                it.put("labels", after.labels)
                it.put(
                    "properties",
                    after.properties.mapValues { e ->
                      DynamicTypes.toConnectValue(PropertyType.schema, e.value)
                    })
              })
        }
      }

  private fun relationshipStateSchema(
      before: RelationshipState?,
      after: RelationshipState?
  ): Schema {
    val stateSchema =
        SchemaBuilder.struct()
            .apply {
              this.field(
                  "properties",
                  SchemaBuilder.map(Schema.STRING_SCHEMA, PropertyType.schema).build())
            }
            .optional()
            .build()

    return SchemaBuilder.struct().field("before", stateSchema).field("after", stateSchema).build()
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
                    before.properties.mapValues { e ->
                      DynamicTypes.toConnectValue(PropertyType.schema, e.value)
                    })
              })
        }

        if (after != null) {
          this.put(
              "after",
              Struct(this.schema().field("after").schema()).also {
                it.put(
                    "properties",
                    after.properties.mapValues { e ->
                      DynamicTypes.toConnectValue(PropertyType.schema, e.value)
                    })
              })
        }
      }
}

fun Struct.toChangeEvent(): ChangeEvent =
    ChangeEvent(
        ChangeIdentifier(getString("id")),
        getInt64("txId"),
        getInt64("seq").toInt(),
        getStruct("metadata").toMetadata(),
        getStruct("event").toEvent(),
    )

internal fun Struct.toMetadata(): Metadata =
    Metadata.fromMap(DynamicTypes.fromConnectValue(schema(), this) as Map<*, *>)

private fun Struct.toEvent(): Event =
    when (val eventType = getString("eventType")) {
      EventType.NODE.name,
      EventType.NODE.shorthand -> {
        toNodeEvent()
      }
      EventType.RELATIONSHIP.name,
      EventType.RELATIONSHIP.shorthand -> {
        toRelationshipEvent()
      }
      else -> throw IllegalArgumentException("unsupported event type $eventType")
    }

@Suppress("UNCHECKED_CAST")
internal fun Struct.toNodeEvent(): NodeEvent =
    getStruct("state").toNodeState().let { (before, after) ->
      NodeEvent(
          getString("elementId"),
          EntityOperation.valueOf(getString("operation")),
          getArray("labels"),
          DynamicTypes.fromConnectValue(
              schema().field("keys").schema(),
              get("keys"),
              skipNullValuesInMaps = true,
          ) as Map<String, List<MutableMap<String, Any>>>?,
          before,
          after,
      )
    }

@Suppress("UNCHECKED_CAST")
internal fun Struct.toRelationshipEvent(): RelationshipEvent =
    getStruct("state").toRelationshipState().let { (before, after) ->
      RelationshipEvent(
          getString("elementId"),
          getString("type"),
          getStruct("start").toNode(),
          getStruct("end").toNode(),
          DynamicTypes.fromConnectValue(
              schema().field("keys").schema(), get("keys"), skipNullValuesInMaps = true)
              as List<Map<String, Any>>?,
          EntityOperation.valueOf(getString("operation")),
          before,
          after)
    }

@Suppress("UNCHECKED_CAST")
internal fun Struct.toNodeState(): Pair<NodeState?, NodeState?> =
    Pair(
        getStruct("before")?.let {
          val labels = it.getArray<String>("labels")
          val properties = it.getMap<String, Any?>("properties")
          NodeState(
              labels,
              DynamicTypes.fromConnectValue(
                  it.schema().field("properties").schema(), properties, true) as Map<String, Any?>,
          )
        },
        getStruct("after")?.let {
          val labels = it.getArray<String>("labels")
          val properties = it.getMap<String, Any?>("properties")
          NodeState(
              labels,
              DynamicTypes.fromConnectValue(
                  it.schema().field("properties").schema(), properties, true) as Map<String, Any?>,
          )
        },
    )

@Suppress("UNCHECKED_CAST")
internal fun Struct.toRelationshipState(): Pair<RelationshipState?, RelationshipState?> =
    Pair(
        getStruct("before")?.let {
          val properties = it.getMap<String, Any?>("properties")
          RelationshipState(
              DynamicTypes.fromConnectValue(
                  it.schema().field("properties").schema(), properties, true) as Map<String, Any?>,
          )
        },
        getStruct("after")?.let {
          val properties = it.getMap<String, Any?>("properties")
          RelationshipState(
              DynamicTypes.fromConnectValue(
                  it.schema().field("properties").schema(), properties, true) as Map<String, Any?>,
          )
        },
    )

@Suppress("UNCHECKED_CAST")
internal fun Struct.toNode(): Node =
    Node(
        this.getString("elementId"),
        this.getArray("labels"),
        DynamicTypes.fromConnectValue(
            schema().field("keys").schema(),
            this.get("keys"),
            skipNullValuesInMaps = true,
        ) as Map<String, List<Map<String, Any>>>,
    )
