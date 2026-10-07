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
import org.neo4j.cdc.client.model.EntityEvent
import org.neo4j.cdc.client.model.EntityOperation
import org.neo4j.cdc.client.model.Event
import org.neo4j.cdc.client.model.EventType
import org.neo4j.cdc.client.model.Metadata
import org.neo4j.cdc.client.model.Node
import org.neo4j.cdc.client.model.NodeEvent
import org.neo4j.cdc.client.model.NodeState
import org.neo4j.cdc.client.model.RelationshipEvent
import org.neo4j.cdc.client.model.RelationshipState
import org.neo4j.connectors.kafka.configuration.MapEncoding
import org.neo4j.connectors.kafka.configuration.PayloadMode

class ChangeEventConverter(private val payloadMode: PayloadMode = PayloadMode.EXTENDED) {

  // Change data capture always describes a Neo4j map as a STRUCT, whatever map encoding is
  // configured: the schemas it builds for the keys of an event need every list element to have the
  // same shape.
  private val converter = payloadMode.converter(MapEncoding.STRUCT)

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
            metadataToConnectValue(changeEvent.metadata, schema.field("metadata").schema()),
        )
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
          .field(
              "txStartTime",
              if (payloadMode == PayloadMode.EXTENDED) PropertyType.schema
              else SimpleTypes.ZONEDDATETIME.schema,
          )
          .field(
              "txCommitTime",
              if (payloadMode == PayloadMode.EXTENDED) PropertyType.schema
              else SimpleTypes.ZONEDDATETIME.schema,
          )
          .field("txMetadata", converter.schema(metadata.txMetadata, optional = true).schema())
          .also {
            metadata.additionalEntries.forEach { entry ->
              it.field(entry.key, converter.schema(entry.value, optional = true))
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
            "txStartTime",
            converter.value(schema.field("txStartTime").schema(), metadata.txStartTime),
        )
        it.put(
            "txCommitTime",
            converter.value(schema.field("txCommitTime").schema(), metadata.txCommitTime),
        )
        it.put(
            "txMetadata",
            converter.value(schema.field("txMetadata").schema(), metadata.txMetadata),
        )

        metadata.additionalEntries.forEach { entry ->
          it.put(entry.key, converter.value(schema.field(entry.key).schema(), entry.value))
        }
      }

  // org.neo4j.connectors.kafka.cdc.Event: shared by node and relationship events, so that both
  // can live under a single schema registry subject.
  internal fun eventToConnectSchema(event: Event): Schema {
    val beforeProperties: Map<String, Any>?
    val afterProperties: Map<String, Any>?
    when (event) {
      is EntityEvent<*> -> {
        beforeProperties = event.before?.properties
        afterProperties = event.after?.properties
      }
      else ->
          throw IllegalArgumentException(
              "unsupported event type in change data: ${event.javaClass.name}"
          )
    }

    return SchemaBuilder.struct()
        .name("org.neo4j.connectors.kafka.cdc.Event")
        .field("elementId", Schema.STRING_SCHEMA)
        .field("eventType", Schema.STRING_SCHEMA)
        .field("operation", Schema.STRING_SCHEMA)
        .field("labels", SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())
        .field("type", Schema.OPTIONAL_STRING_SCHEMA)
        .field("start", unifiedNodeToConnectSchema())
        .field("end", unifiedNodeToConnectSchema())
        .field("keys", entityKeysSchema())
        .field("state", entityStateSchema(beforeProperties, afterProperties))
        .build()
  }

  internal fun eventToConnectValue(event: Event, schema: Schema): Struct =
      Struct(schema).also {
        it.put("eventType", event.eventType.name)

        when (event) {
          is NodeEvent -> {
            it.put("elementId", event.elementId)
            it.put("operation", event.operation.name)
            it.put("labels", event.labels)
            it.put("keys", entityKeysValue(schema.field("keys").schema(), event.keys))
            it.put(
                "state",
                entityStateValue(
                    schema.field("state").schema(),
                    event.before?.labels,
                    event.before?.properties,
                    event.after?.labels,
                    event.after?.properties,
                ),
            )
          }
          is RelationshipEvent -> {
            it.put("elementId", event.elementId)
            it.put("operation", event.operation.name)
            it.put("type", event.type)
            it.put("start", unifiedNodeToConnectValue(event.start, schema.field("start").schema()))
            it.put("end", unifiedNodeToConnectValue(event.end, schema.field("end").schema()))
            it.put(
                "keys",
                entityKeysValue(
                    schema.field("keys").schema(),
                    // relationship keys are listed under the relationship type, as node keys are
                    // by label
                    when {
                      event.keys == null -> null
                      event.keys.isEmpty() -> emptyMap()
                      else -> mapOf(event.type to event.keys)
                    },
                ),
            )
            it.put(
                "state",
                entityStateValue(
                    schema.field("state").schema(),
                    null,
                    event.before?.properties,
                    null,
                    event.after?.properties,
                ),
            )
          }
          else -> throw IllegalArgumentException("unsupported event type ${event.javaClass.name}")
        }
      }

  // start and end nodes of a relationship; optional, as they are null on node events
  private fun unifiedNodeToConnectSchema(): Schema =
      SchemaBuilder.struct()
          .field("elementId", Schema.STRING_SCHEMA)
          .field("labels", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
          .field("keys", entityKeysSchema())
          .optional()
          .build()

  private fun unifiedNodeToConnectValue(node: Node, schema: Schema): Struct =
      Struct(schema).also {
        it.put("elementId", node.elementId)
        it.put("labels", node.labels)
        it.put("keys", entityKeysValue(schema.field("keys").schema(), node.keys))
      }

  // EntityKeys is an array of {name, rows}, where name is a label (or a relationship type) and each
  // row holds the properties of one key.
  private fun entityKeysSchema(): Schema =
      SchemaBuilder.array(
              SchemaBuilder.struct()
                  .field("name", Schema.STRING_SCHEMA)
                  .field(
                      "rows",
                      SchemaBuilder.array(
                              SchemaBuilder.struct()
                                  .field(
                                      "properties",
                                      SchemaBuilder.map(Schema.STRING_SCHEMA, PropertyType.schema)
                                          .build(),
                                  )
                                  .build()
                          )
                          .build(),
                  )
                  .build()
          )
          .optional()
          .build()

  private fun entityKeysValue(
      schema: Schema,
      keysByName: Map<String, List<Map<String, Any>>>?,
  ): List<Struct>? {
    val entrySchema = schema.valueSchema()
    val rowSchema = entrySchema.field("rows").schema().valueSchema()

    return keysByName?.map { (name, rows) ->
      Struct(entrySchema)
          .put("name", name)
          .put(
              "rows",
              rows.map { row ->
                Struct(rowSchema)
                    .put("properties", row.mapValues { (_, v) -> PropertyType.toConnectValue(v) })
              },
          )
    }
  }

  private fun entityStateSchema(
      beforeProperties: Map<String, Any>?,
      afterProperties: Map<String, Any>?,
  ): Schema {
    val entitySchema =
        SchemaBuilder.struct()
            .field("labels", SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())
            .field("properties", entityPropertiesSchema(beforeProperties, afterProperties))
            .optional()
            .build()

    return SchemaBuilder.struct().field("before", entitySchema).field("after", entitySchema).build()
  }

  private fun entityPropertiesSchema(
      beforeProperties: Map<String, Any>?,
      afterProperties: Map<String, Any>?,
  ): Schema =
      if (payloadMode == PayloadMode.EXTENDED)
          SchemaBuilder.map(Schema.STRING_SCHEMA, PropertyType.schema).build()
      else
          SchemaBuilder.struct()
              .also {
                val combinedProperties =
                    (beforeProperties ?: mapOf()) + (afterProperties ?: mapOf())
                combinedProperties.toSortedMap().forEach { entry ->
                  if (it.field(entry.key) == null) {
                    it.field(entry.key, converter.schema(entry.value, optional = true))
                  }
                }
              }
              .build()

  private fun entityStateValue(
      schema: Schema,
      beforeLabels: List<String>?,
      beforeProperties: Map<String, Any>?,
      afterLabels: List<String>?,
      afterProperties: Map<String, Any>?,
  ): Struct =
      Struct(schema).apply {
        if (beforeProperties != null) {
          put(
              "before",
              entitySingleStateValue(
                  schema.field("before").schema(),
                  beforeLabels,
                  beforeProperties,
              ),
          )
        }

        if (afterProperties != null) {
          put(
              "after",
              entitySingleStateValue(schema.field("after").schema(), afterLabels, afterProperties),
          )
        }
      }

  private fun entitySingleStateValue(
      schema: Schema,
      labels: List<String>?,
      properties: Map<String, Any>,
  ): Struct =
      Struct(schema).also {
        it.put("labels", labels)
        it.put(
            "properties",
            if (payloadMode == PayloadMode.EXTENDED)
                properties.mapValues { e -> converter.value(PropertyType.schema, e.value) }
            else converter.value(schema.field("properties").schema(), properties),
        )
      }
}

fun SchemaAndValue.extractEventSchema(): Schema {
  return this.schema().field("event").schema()
}

fun SchemaAndValue.extractEventValue(): Struct {
  val value = this.value()
  if (value !is Struct) {
    throw IllegalArgumentException("expected value to be a struct, but got: ${value?.javaClass}")
  }
  val eventData = value.get("event")
  if (eventData !is Struct) {
    throw IllegalArgumentException(
        "expected event attribute to be a struct, but got: ${value.javaClass}"
    )
  }
  return eventData
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
          decodeEntityKeys() as Map<String, List<MutableMap<String, Any>>>?,
          before,
          after,
      )
    }

@Suppress("UNCHECKED_CAST")
internal fun Struct.toRelationshipEvent(): RelationshipEvent =
    getStruct("state").toRelationshipState().let { (before, after) ->
      val keysByName = decodeEntityKeys()

      RelationshipEvent(
          getString("elementId"),
          getString("type"),
          getStruct("start").toNode(),
          getStruct("end").toNode(),
          // relationship keys are stored under the relationship type, as node keys are by label
          keysByName?.get(getString("type")) ?: emptyList(),
          EntityOperation.valueOf(getString("operation")),
          before,
          after,
      )
    }

@Suppress("UNCHECKED_CAST", "IMPLICIT_CAST_TO_ANY")
internal fun Struct.toNodeState(): Pair<NodeState?, NodeState?> =
    Pair(
        getStruct("before")?.let {
          val labels = it.getArray<String>("labels")
          val propertiesField = it.schema().field("properties")
          val properties =
              when (propertiesField.schema().type()) {
                Schema.Type.MAP -> it.getMap<String, Any?>("properties")
                Schema.Type.STRUCT -> it.getStruct("properties")
                else -> throw IllegalArgumentException("Unsupported schema type for properties")
              }
          NodeState(
              labels,
              DynamicTypes.fromConnectValue(propertiesField.schema(), properties, true)
                  as Map<String, Any?>,
          )
        },
        getStruct("after")?.let {
          val labels = it.getArray<String>("labels")
          val propertiesField = it.schema().field("properties")
          val properties =
              when (propertiesField.schema().type()) {
                Schema.Type.MAP -> it.getMap<String, Any?>("properties")
                Schema.Type.STRUCT -> it.getStruct("properties")
                else -> throw IllegalArgumentException("Unsupported schema type for properties")
              }
          NodeState(
              labels,
              DynamicTypes.fromConnectValue(propertiesField.schema(), properties, true)
                  as Map<String, Any?>,
          )
        },
    )

@Suppress("UNCHECKED_CAST", "IMPLICIT_CAST_TO_ANY")
internal fun Struct.toRelationshipState(): Pair<RelationshipState?, RelationshipState?> =
    Pair(
        getStruct("before")?.let {
          val propertiesField = it.schema().field("properties")
          val properties =
              when (propertiesField.schema().type()) {
                Schema.Type.MAP -> it.getMap<String, Any?>("properties")
                Schema.Type.STRUCT -> it.getStruct("properties")
                else -> throw IllegalArgumentException("Unsupported schema type for properties")
              }
          RelationshipState(
              DynamicTypes.fromConnectValue(propertiesField.schema(), properties, true)
                  as Map<String, Any?>
          )
        },
        getStruct("after")?.let {
          val propertiesField = it.schema().field("properties")
          val properties =
              when (propertiesField.schema().type()) {
                Schema.Type.MAP -> it.getMap<String, Any?>("properties")
                Schema.Type.STRUCT -> it.getStruct("properties")
                else -> throw IllegalArgumentException("Unsupported schema type for properties")
              }
          RelationshipState(
              DynamicTypes.fromConnectValue(propertiesField.schema(), properties, true)
                  as Map<String, Any?>
          )
        },
    )

@Suppress("UNCHECKED_CAST")
internal fun Struct.toNode(): Node =
    Node(this.getString("elementId"), this.getArray("labels"), decodeEntityKeys() ?: emptyMap())

// Keys are a list of {name, rows}, where name is a label (or a relationship type). They decode to
// a map of name to rows.
@Suppress("UNCHECKED_CAST")
private fun Struct.decodeEntityKeys(): Map<String, List<Map<String, Any>>>? =
    getArray<Struct>("keys")?.associate { entry ->
      entry.getString("name") to
          entry.getArray<Struct>("rows").map { row ->
            DynamicTypes.fromConnectValue(
                row.schema().field("properties").schema(),
                row.get("properties"),
                skipNullValuesInMaps = true,
            ) as Map<String, Any>
          }
    }
