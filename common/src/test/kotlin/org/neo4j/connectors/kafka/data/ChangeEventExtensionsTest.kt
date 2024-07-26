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

import io.kotest.matchers.shouldBe
import java.time.LocalDate
import java.time.ZonedDateTime
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.junit.jupiter.api.Test
import org.neo4j.cdc.client.model.CaptureMode
import org.neo4j.cdc.client.model.ChangeEvent
import org.neo4j.cdc.client.model.ChangeIdentifier
import org.neo4j.cdc.client.model.EntityOperation
import org.neo4j.cdc.client.model.Event
import org.neo4j.cdc.client.model.Metadata
import org.neo4j.cdc.client.model.Node
import org.neo4j.cdc.client.model.NodeEvent
import org.neo4j.cdc.client.model.NodeState
import org.neo4j.cdc.client.model.RelationshipEvent
import org.neo4j.cdc.client.model.RelationshipState

class ChangeEventExtensionsTest {

  @Test
  fun `schema and value should be generated correctly for common fields`() {
    val (_, change, schema, value) =
        newChangeEvent(
            NodeEvent(
                "element-0",
                EntityOperation.CREATE,
                listOf("Label1", "Label2"),
                mapOf(
                    "Label1" to listOf(mapOf("name" to "john", "surname" to "doe")),
                    "Label2" to listOf(mapOf("id" to 5L))),
                null,
                NodeState(
                    listOf("Label1", "Label2"),
                    mapOf("id" to 5L, "name" to "john", "surname" to "doe"))))

    schema.nestedSchema("id") shouldBe Schema.STRING_SCHEMA
    schema.nestedSchema("txId") shouldBe Schema.INT64_SCHEMA
    schema.nestedSchema("seq") shouldBe Schema.INT64_SCHEMA
    schema.nestedSchema("metadata") shouldBe
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
                SchemaBuilder.struct()
                    .field("user", PropertyType.schema)
                    .field("app", PropertyType.schema)
                    .optional()
                    .build())
            .build()

    value.get("id") shouldBe change.id.id
    value.get("txId") shouldBe change.txId
    value.get("seq") shouldBe change.seq
    value.get("metadata") shouldBe
        Struct(schema.nestedSchema("metadata"))
            .put("authenticatedUser", change.metadata.authenticatedUser)
            .put("executingUser", change.metadata.executingUser)
            .put("connectionType", change.metadata.connectionType)
            .put("connectionClient", change.metadata.connectionClient)
            .put("connectionServer", change.metadata.connectionServer)
            .put("serverId", change.metadata.serverId)
            .put("captureMode", change.metadata.captureMode.name)
            .put("txStartTime", change.metadata.txStartTime.let { PropertyType.toConnectValue(it) })
            .put(
                "txCommitTime",
                change.metadata.txCommitTime.let { PropertyType.toConnectValue(it) })
            .put(
                "txMetadata",
                Struct(schema.nestedSchema("metadata.txMetadata"))
                    .put("user", PropertyType.toConnectValue("app_user"))
                    .put("app", PropertyType.toConnectValue("hr")))
  }

  @Test
  fun `schema and value should be generated and converted back correctly for node create events`() {
    val (_, change, schema, value) =
        newChangeEvent(
            NodeEvent(
                "element-0",
                EntityOperation.CREATE,
                listOf("Label1", "Label2"),
                mapOf(
                    "Label1" to listOf(mapOf("name" to "john", "surname" to "doe")),
                    "Label2" to listOf(mapOf("id" to 5L))),
                null,
                NodeState(
                    listOf("Label1", "Label2"),
                    mapOf("id" to 5L, "name" to "john", "surname" to "doe"))))

    schema.nestedSchema("event") shouldBe
        SchemaBuilder.struct()
            .field("elementId", Schema.STRING_SCHEMA)
            .field("eventType", Schema.STRING_SCHEMA)
            .field("operation", Schema.STRING_SCHEMA)
            .field("labels", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
            .field(
                "keys",
                SchemaBuilder.struct()
                    .field(
                        "Label1",
                        SchemaBuilder.array(
                                SchemaBuilder.map(Schema.STRING_SCHEMA, PropertyType.schema)
                                    .build())
                            .optional()
                            .build())
                    .field(
                        "Label2",
                        SchemaBuilder.array(
                                SchemaBuilder.map(Schema.STRING_SCHEMA, PropertyType.schema)
                                    .build())
                            .optional()
                            .build())
                    .optional()
                    .build())
            .field(
                "state",
                SchemaBuilder.struct()
                    .field(
                        "before",
                        SchemaBuilder.struct()
                            .field("labels", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                            .field(
                                "properties",
                                SchemaBuilder.map(Schema.STRING_SCHEMA, PropertyType.schema)
                                    .build())
                            .optional()
                            .build())
                    .field(
                        "after",
                        SchemaBuilder.struct()
                            .field("labels", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                            .field(
                                "properties",
                                SchemaBuilder.map(Schema.STRING_SCHEMA, PropertyType.schema)
                                    .build())
                            .optional()
                            .build())
                    .build())
            .build()

    value.get("event") shouldBe
        Struct(schema.nestedSchema("event"))
            .put("elementId", "element-0")
            .put("eventType", "NODE")
            .put("operation", "CREATE")
            .put("labels", listOf("Label1", "Label2"))
            .put(
                "keys",
                Struct(schema.nestedSchema("event.keys"))
                    .put(
                        "Label1",
                        listOf(
                            mapOf(
                                "name" to PropertyType.toConnectValue("john"),
                                "surname" to PropertyType.toConnectValue("doe"))))
                    .put("Label2", listOf(mapOf("id" to PropertyType.toConnectValue(5L)))))
            .put(
                "state",
                Struct(schema.nestedSchema("event.state"))
                    .put(
                        "after",
                        Struct(schema.nestedSchema("event.state.after"))
                            .put("labels", listOf("Label1", "Label2"))
                            .put(
                                "properties",
                                mapOf(
                                    "id" to PropertyType.toConnectValue(5L),
                                    "name" to PropertyType.toConnectValue("john"),
                                    "surname" to PropertyType.toConnectValue("doe")))))

    val reverted = value.toChangeEvent()
    reverted shouldBe change
  }

  @Test
  fun `schema and value should be generated and converted back correctly for node update events`() {
    val (_, change, schema, value) =
        newChangeEvent(
            NodeEvent(
                "element-0",
                EntityOperation.UPDATE,
                listOf("Label1", "Label2"),
                mapOf(
                    "Label1" to listOf(mapOf("name" to "john", "surname" to "doe")),
                    "Label2" to listOf(mapOf("id" to 5L))),
                NodeState(
                    listOf("Label1", "Label2"),
                    mapOf("id" to 5L, "name" to "john", "surname" to "doe")),
                NodeState(
                    listOf("Label1", "Label2", "Label3"),
                    mapOf("id" to 5L, "name" to "john", "surname" to "doe", "age" to 25L))))

    schema.nestedSchema("event") shouldBe
        SchemaBuilder.struct()
            .field("elementId", Schema.STRING_SCHEMA)
            .field("eventType", Schema.STRING_SCHEMA)
            .field("operation", Schema.STRING_SCHEMA)
            .field("labels", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
            .field(
                "keys",
                SchemaBuilder.struct()
                    .field(
                        "Label1",
                        SchemaBuilder.array(
                                SchemaBuilder.map(Schema.STRING_SCHEMA, PropertyType.schema)
                                    .build())
                            .optional()
                            .build())
                    .field(
                        "Label2",
                        SchemaBuilder.array(
                                SchemaBuilder.map(Schema.STRING_SCHEMA, PropertyType.schema)
                                    .build())
                            .optional()
                            .build())
                    .optional()
                    .build())
            .field(
                "state",
                SchemaBuilder.struct()
                    .field(
                        "before",
                        SchemaBuilder.struct()
                            .field("labels", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                            .field(
                                "properties",
                                SchemaBuilder.map(Schema.STRING_SCHEMA, PropertyType.schema)
                                    .build())
                            .optional()
                            .build())
                    .field(
                        "after",
                        SchemaBuilder.struct()
                            .field("labels", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                            .field(
                                "properties",
                                SchemaBuilder.map(Schema.STRING_SCHEMA, PropertyType.schema)
                                    .build())
                            .optional()
                            .build())
                    .build())
            .build()

    value.get("event") shouldBe
        Struct(schema.nestedSchema("event"))
            .put("elementId", "element-0")
            .put("eventType", "NODE")
            .put("operation", "UPDATE")
            .put("labels", listOf("Label1", "Label2"))
            .put(
                "keys",
                Struct(schema.nestedSchema("event.keys"))
                    .put(
                        "Label1",
                        listOf(
                            mapOf(
                                "name" to PropertyType.toConnectValue("john"),
                                "surname" to PropertyType.toConnectValue("doe"))))
                    .put("Label2", listOf(mapOf("id" to PropertyType.toConnectValue(5L)))))
            .put(
                "state",
                Struct(schema.nestedSchema("event.state"))
                    .put(
                        "before",
                        Struct(schema.nestedSchema("event.state.before"))
                            .put("labels", listOf("Label1", "Label2"))
                            .put(
                                "properties",
                                mapOf(
                                    "id" to PropertyType.toConnectValue(5L),
                                    "name" to PropertyType.toConnectValue("john"),
                                    "surname" to PropertyType.toConnectValue("doe"))))
                    .put(
                        "after",
                        Struct(schema.nestedSchema("event.state.after"))
                            .put("labels", listOf("Label1", "Label2", "Label3"))
                            .put(
                                "properties",
                                mapOf(
                                    "id" to PropertyType.toConnectValue(5L),
                                    "name" to PropertyType.toConnectValue("john"),
                                    "surname" to PropertyType.toConnectValue("doe"),
                                    "age" to PropertyType.toConnectValue(25L)))))

    val reverted = value.toChangeEvent()
    reverted shouldBe change
  }

  @Test
  fun `schema and value should be generated and converted back correctly for node delete events`() {
    val (_, change, schema, value) =
        newChangeEvent(
            NodeEvent(
                "element-0",
                EntityOperation.DELETE,
                listOf("Label1", "Label2", "Label3"),
                mapOf(
                    "Label1" to listOf(mapOf("name" to "john", "surname" to "doe")),
                    "Label2" to listOf(mapOf("id" to 5L))),
                NodeState(
                    listOf("Label1", "Label2", "Label3"),
                    mapOf("id" to 5L, "name" to "john", "surname" to "doe", "age" to 25L)),
                null))

    schema.nestedSchema("event") shouldBe
        SchemaBuilder.struct()
            .field("elementId", Schema.STRING_SCHEMA)
            .field("eventType", Schema.STRING_SCHEMA)
            .field("operation", Schema.STRING_SCHEMA)
            .field("labels", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
            .field(
                "keys",
                SchemaBuilder.struct()
                    .field(
                        "Label1",
                        SchemaBuilder.array(
                                SchemaBuilder.map(Schema.STRING_SCHEMA, PropertyType.schema)
                                    .build())
                            .optional()
                            .build())
                    .field(
                        "Label2",
                        SchemaBuilder.array(
                                SchemaBuilder.map(Schema.STRING_SCHEMA, PropertyType.schema)
                                    .build())
                            .optional()
                            .build())
                    .optional()
                    .build())
            .field(
                "state",
                SchemaBuilder.struct()
                    .field(
                        "before",
                        SchemaBuilder.struct()
                            .field("labels", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                            .field(
                                "properties",
                                SchemaBuilder.map(Schema.STRING_SCHEMA, PropertyType.schema)
                                    .build())
                            .optional()
                            .build())
                    .field(
                        "after",
                        SchemaBuilder.struct()
                            .field("labels", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                            .field(
                                "properties",
                                SchemaBuilder.map(Schema.STRING_SCHEMA, PropertyType.schema)
                                    .build())
                            .optional()
                            .build())
                    .build())
            .build()

    value.get("event") shouldBe
        Struct(schema.nestedSchema("event"))
            .put("elementId", "element-0")
            .put("eventType", "NODE")
            .put("operation", "DELETE")
            .put("labels", listOf("Label1", "Label2", "Label3"))
            .put(
                "keys",
                Struct(schema.nestedSchema("event.keys"))
                    .put(
                        "Label1",
                        listOf(
                            mapOf(
                                "name" to PropertyType.toConnectValue("john"),
                                "surname" to PropertyType.toConnectValue("doe"))))
                    .put("Label2", listOf(mapOf("id" to PropertyType.toConnectValue(5L)))))
            .put(
                "state",
                Struct(schema.nestedSchema("event.state"))
                    .put(
                        "before",
                        Struct(schema.nestedSchema("event.state.before"))
                            .put("labels", listOf("Label1", "Label2", "Label3"))
                            .put(
                                "properties",
                                mapOf(
                                    "id" to PropertyType.toConnectValue(5L),
                                    "name" to PropertyType.toConnectValue("john"),
                                    "surname" to PropertyType.toConnectValue("doe"),
                                    "age" to PropertyType.toConnectValue(25L)))))

    val reverted = value.toChangeEvent()
    reverted shouldBe change
  }

  @Test
  fun `schema and value should be generated and converted back correctly for relationship create events`() {
    val (_, change, schema, value) =
        newChangeEvent(
            RelationshipEvent(
                "rel-0",
                "WORKS_FOR",
                Node(
                    "node-0", listOf("Person"), mapOf("Person" to listOf(mapOf("name" to "john")))),
                Node(
                    "node-1",
                    listOf("Company"),
                    mapOf("Company" to listOf(mapOf("name" to "acme corp")))),
                listOf(mapOf("id" to 5L)),
                EntityOperation.CREATE,
                null,
                RelationshipState(mapOf("id" to 5L, "since" to LocalDate.of(1999, 12, 31)))))

    schema.nestedSchema("event") shouldBe
        SchemaBuilder.struct()
            .field("elementId", Schema.STRING_SCHEMA)
            .field("eventType", Schema.STRING_SCHEMA)
            .field("operation", Schema.STRING_SCHEMA)
            .field("type", Schema.STRING_SCHEMA)
            .field(
                "start",
                SchemaBuilder.struct()
                    .field("elementId", Schema.STRING_SCHEMA)
                    .field("labels", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                    .field(
                        "keys",
                        SchemaBuilder.struct()
                            .field(
                                "Person",
                                SchemaBuilder.array(
                                        SchemaBuilder.map(Schema.STRING_SCHEMA, PropertyType.schema)
                                            .build())
                                    .optional()
                                    .schema())
                            .optional()
                            .build())
                    .build())
            .field(
                "end",
                SchemaBuilder.struct()
                    .field("elementId", Schema.STRING_SCHEMA)
                    .field("labels", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                    .field(
                        "keys",
                        SchemaBuilder.struct()
                            .field(
                                "Company",
                                SchemaBuilder.array(
                                        SchemaBuilder.map(Schema.STRING_SCHEMA, PropertyType.schema)
                                            .build())
                                    .optional()
                                    .schema())
                            .optional()
                            .build())
                    .build())
            .field(
                "keys",
                SchemaBuilder.array(
                        SchemaBuilder.map(Schema.STRING_SCHEMA, PropertyType.schema).build())
                    .optional()
                    .build())
            .field(
                "state",
                SchemaBuilder.struct()
                    .field(
                        "before",
                        SchemaBuilder.struct()
                            .field(
                                "properties",
                                SchemaBuilder.map(Schema.STRING_SCHEMA, PropertyType.schema)
                                    .build())
                            .optional()
                            .build())
                    .field(
                        "after",
                        SchemaBuilder.struct()
                            .field(
                                "properties",
                                SchemaBuilder.map(Schema.STRING_SCHEMA, PropertyType.schema)
                                    .build())
                            .optional()
                            .build())
                    .build())
            .build()

    value.get("event") shouldBe
        Struct(schema.nestedSchema("event"))
            .put("elementId", "rel-0")
            .put("eventType", "RELATIONSHIP")
            .put("operation", "CREATE")
            .put("type", "WORKS_FOR")
            .put(
                "start",
                Struct(schema.nestedSchema("event.start"))
                    .put("elementId", "node-0")
                    .put("labels", listOf("Person"))
                    .put(
                        "keys",
                        Struct(schema.nestedSchema("event.start.keys"))
                            .put(
                                "Person",
                                listOf(mapOf("name" to PropertyType.toConnectValue("john"))))))
            .put(
                "end",
                Struct(schema.nestedSchema("event.end"))
                    .put("elementId", "node-1")
                    .put("labels", listOf("Company"))
                    .put(
                        "keys",
                        Struct(schema.nestedSchema("event.end.keys"))
                            .put(
                                "Company",
                                listOf(mapOf("name" to PropertyType.toConnectValue("acme corp"))))))
            .put("keys", listOf(mapOf("id" to PropertyType.toConnectValue(5L))))
            .put(
                "state",
                Struct(schema.nestedSchema("event.state"))
                    .put(
                        "after",
                        Struct(schema.nestedSchema("event.state.after"))
                            .put(
                                "properties",
                                mapOf(
                                    "id" to PropertyType.toConnectValue(5L),
                                    "since" to
                                        PropertyType.toConnectValue(LocalDate.of(1999, 12, 31))))))

    val reverted = value.toChangeEvent()
    reverted shouldBe change
  }

  @Test
  fun `schema and value should be generated and converted back correctly for relationship update events`() {
    val (_, change, schema, value) =
        newChangeEvent(
            RelationshipEvent(
                "rel-0",
                "WORKS_FOR",
                Node(
                    "node-0", listOf("Person"), mapOf("Person" to listOf(mapOf("name" to "john")))),
                Node(
                    "node-1",
                    listOf("Company"),
                    mapOf("Company" to listOf(mapOf("name" to "acme corp")))),
                listOf(mapOf("id" to 5L)),
                EntityOperation.UPDATE,
                RelationshipState(mapOf("id" to 5L, "since" to LocalDate.of(1999, 12, 31))),
                RelationshipState(mapOf("id" to 5L, "since" to LocalDate.of(2000, 1, 1)))))

    schema.nestedSchema("event") shouldBe
        SchemaBuilder.struct()
            .field("elementId", Schema.STRING_SCHEMA)
            .field("eventType", Schema.STRING_SCHEMA)
            .field("operation", Schema.STRING_SCHEMA)
            .field("type", Schema.STRING_SCHEMA)
            .field(
                "start",
                SchemaBuilder.struct()
                    .field("elementId", Schema.STRING_SCHEMA)
                    .field("labels", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                    .field(
                        "keys",
                        SchemaBuilder.struct()
                            .field(
                                "Person",
                                SchemaBuilder.array(
                                        SchemaBuilder.map(Schema.STRING_SCHEMA, PropertyType.schema)
                                            .build())
                                    .optional()
                                    .schema())
                            .optional()
                            .build())
                    .build())
            .field(
                "end",
                SchemaBuilder.struct()
                    .field("elementId", Schema.STRING_SCHEMA)
                    .field("labels", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                    .field(
                        "keys",
                        SchemaBuilder.struct()
                            .field(
                                "Company",
                                SchemaBuilder.array(
                                        SchemaBuilder.map(Schema.STRING_SCHEMA, PropertyType.schema)
                                            .build())
                                    .optional()
                                    .schema())
                            .optional()
                            .build())
                    .build())
            .field(
                "keys",
                SchemaBuilder.array(
                        SchemaBuilder.map(Schema.STRING_SCHEMA, PropertyType.schema).build())
                    .optional()
                    .build())
            .field(
                "state",
                SchemaBuilder.struct()
                    .field(
                        "before",
                        SchemaBuilder.struct()
                            .field(
                                "properties",
                                SchemaBuilder.map(Schema.STRING_SCHEMA, PropertyType.schema)
                                    .build())
                            .optional()
                            .build())
                    .field(
                        "after",
                        SchemaBuilder.struct()
                            .field(
                                "properties",
                                SchemaBuilder.map(Schema.STRING_SCHEMA, PropertyType.schema)
                                    .build())
                            .optional()
                            .build())
                    .build())
            .build()

    value.get("event") shouldBe
        Struct(schema.nestedSchema("event"))
            .put("elementId", "rel-0")
            .put("eventType", "RELATIONSHIP")
            .put("operation", "UPDATE")
            .put("type", "WORKS_FOR")
            .put(
                "start",
                Struct(schema.nestedSchema("event.start"))
                    .put("elementId", "node-0")
                    .put("labels", listOf("Person"))
                    .put(
                        "keys",
                        Struct(schema.nestedSchema("event.start.keys"))
                            .put(
                                "Person",
                                listOf(mapOf("name" to PropertyType.toConnectValue("john"))))))
            .put(
                "end",
                Struct(schema.nestedSchema("event.end"))
                    .put("elementId", "node-1")
                    .put("labels", listOf("Company"))
                    .put(
                        "keys",
                        Struct(schema.nestedSchema("event.end.keys"))
                            .put(
                                "Company",
                                listOf(mapOf("name" to PropertyType.toConnectValue("acme corp"))))))
            .put("keys", listOf(mapOf("id" to PropertyType.toConnectValue(5L))))
            .put(
                "state",
                Struct(schema.nestedSchema("event.state"))
                    .put(
                        "before",
                        Struct(schema.nestedSchema("event.state.before"))
                            .put(
                                "properties",
                                mapOf(
                                    "id" to PropertyType.toConnectValue(5L),
                                    "since" to
                                        PropertyType.toConnectValue(LocalDate.of(1999, 12, 31)))))
                    .put(
                        "after",
                        Struct(schema.nestedSchema("event.state.after"))
                            .put(
                                "properties",
                                mapOf(
                                    "id" to PropertyType.toConnectValue(5L),
                                    "since" to
                                        PropertyType.toConnectValue(LocalDate.of(2000, 1, 1))))))

    val reverted = value.toChangeEvent()
    reverted shouldBe change
  }

  @Test
  fun `schema and value should be generated and converted back correctly for relationship delete events`() {
    val (_, change, schema, value) =
        newChangeEvent(
            RelationshipEvent(
                "rel-0",
                "WORKS_FOR",
                Node(
                    "node-0", listOf("Person"), mapOf("Person" to listOf(mapOf("name" to "john")))),
                Node(
                    "node-1",
                    listOf("Company"),
                    mapOf("Company" to listOf(mapOf("name" to "acme corp")))),
                listOf(mapOf("id" to 5L)),
                EntityOperation.DELETE,
                RelationshipState(mapOf("id" to 5L, "since" to LocalDate.of(2000, 1, 1))),
                null))

    schema.nestedSchema("event") shouldBe
        SchemaBuilder.struct()
            .field("elementId", Schema.STRING_SCHEMA)
            .field("eventType", Schema.STRING_SCHEMA)
            .field("operation", Schema.STRING_SCHEMA)
            .field("type", Schema.STRING_SCHEMA)
            .field(
                "start",
                SchemaBuilder.struct()
                    .field("elementId", Schema.STRING_SCHEMA)
                    .field("labels", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                    .field(
                        "keys",
                        SchemaBuilder.struct()
                            .field(
                                "Person",
                                SchemaBuilder.array(
                                        SchemaBuilder.map(Schema.STRING_SCHEMA, PropertyType.schema)
                                            .build())
                                    .optional()
                                    .build())
                            .optional()
                            .build())
                    .build())
            .field(
                "end",
                SchemaBuilder.struct()
                    .field("elementId", Schema.STRING_SCHEMA)
                    .field("labels", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                    .field(
                        "keys",
                        SchemaBuilder.struct()
                            .field(
                                "Company",
                                SchemaBuilder.array(
                                        SchemaBuilder.map(Schema.STRING_SCHEMA, PropertyType.schema)
                                            .build())
                                    .optional()
                                    .build())
                            .optional()
                            .build())
                    .build())
            .field(
                "keys",
                SchemaBuilder.array(
                        SchemaBuilder.map(Schema.STRING_SCHEMA, PropertyType.schema).build())
                    .optional()
                    .schema())
            .field(
                "state",
                SchemaBuilder.struct()
                    .field(
                        "before",
                        SchemaBuilder.struct()
                            .field(
                                "properties",
                                SchemaBuilder.map(Schema.STRING_SCHEMA, PropertyType.schema)
                                    .build())
                            .optional()
                            .build())
                    .field(
                        "after",
                        SchemaBuilder.struct()
                            .field(
                                "properties",
                                SchemaBuilder.map(Schema.STRING_SCHEMA, PropertyType.schema)
                                    .build())
                            .optional()
                            .build())
                    .build())
            .build()

    value.get("event") shouldBe
        Struct(schema.nestedSchema("event"))
            .put("elementId", "rel-0")
            .put("eventType", "RELATIONSHIP")
            .put("operation", "DELETE")
            .put("type", "WORKS_FOR")
            .put(
                "start",
                Struct(schema.nestedSchema("event.start"))
                    .put("elementId", "node-0")
                    .put("labels", listOf("Person"))
                    .put(
                        "keys",
                        Struct(schema.nestedSchema("event.start.keys"))
                            .put(
                                "Person",
                                listOf(mapOf("name" to PropertyType.toConnectValue("john"))))))
            .put(
                "end",
                Struct(schema.nestedSchema("event.end"))
                    .put("elementId", "node-1")
                    .put("labels", listOf("Company"))
                    .put(
                        "keys",
                        Struct(schema.nestedSchema("event.end.keys"))
                            .put(
                                "Company",
                                listOf(mapOf("name" to PropertyType.toConnectValue("acme corp"))))))
            .put("keys", listOf(mapOf("id" to PropertyType.toConnectValue(5L))))
            .put(
                "state",
                Struct(schema.nestedSchema("event.state"))
                    .put(
                        "before",
                        Struct(schema.nestedSchema("event.state.before"))
                            .put(
                                "properties",
                                mapOf(
                                    "id" to PropertyType.toConnectValue(5L),
                                    "since" to
                                        PropertyType.toConnectValue(LocalDate.of(2000, 1, 1))))))

    val reverted = value.toChangeEvent()
    reverted shouldBe change
  }

  @Test
  fun `node event keys should be nullified when node keys are not defined`() {
    val (_, _, schema, value) =
        newChangeEvent(
            NodeEvent(
                "element-0",
                EntityOperation.CREATE,
                listOf("Label1", "Label2"),
                mapOf(),
                null,
                NodeState(listOf("Label1"), mapOf("id" to 5L))))

    val expectedKeySchema = SchemaBuilder.struct().optional().build()
    schema.nestedSchema("event.keys") shouldBe expectedKeySchema
    value.nestedValue("event.keys") shouldBe Struct(expectedKeySchema)
  }

  @Test
  fun `relationship event keys should be nullified when rel keys are not defined`() {
    val (_, _, schema, value) =
        newChangeEvent(
            RelationshipEvent(
                "rel-0",
                "WORKS_FOR",
                Node("node-0", listOf("Person"), mapOf()),
                Node("node-1", listOf("Company"), mapOf()),
                listOf(),
                EntityOperation.DELETE,
                RelationshipState(mapOf("id" to 5L)),
                null))

    val expectedKeySchema =
        SchemaBuilder.array(SchemaBuilder.map(Schema.STRING_SCHEMA, PropertyType.schema).build())
            .optional()
            .build()
    schema.nestedSchema("event.keys") shouldBe expectedKeySchema
    value.nestedValue("event.keys") shouldBe emptyList<Any>()
  }

  @Test
  fun `metadata should be converted to struct and back`() {
    val startTime = ZonedDateTime.now().minusSeconds(1)
    val commitTime = startTime.plusSeconds(1)
    val metadata =
        Metadata(
            "service",
            "neo4j",
            "server-1",
            CaptureMode.DIFF,
            "bolt",
            "127.0.0.1:32000",
            "127.0.0.1:7687",
            startTime,
            commitTime,
            mapOf("user" to "app_user", "app" to "hr", "xyz" to mapOf("a" to 1L, "b" to 2L)),
            mapOf("new_field" to "abc", "another_field" to 1L))

    val changeEventConverter = ChangeEventConverter()
    val schema = changeEventConverter.metadataToConnectSchema(metadata)
    val converted = changeEventConverter.metadataToConnectValue(metadata, schema)

    converted shouldBe
        Struct(schema)
            .put("authenticatedUser", "service")
            .put("executingUser", "neo4j")
            .put("serverId", "server-1")
            .put("captureMode", CaptureMode.DIFF.name)
            .put("connectionType", "bolt")
            .put("connectionClient", "127.0.0.1:32000")
            .put("connectionServer", "127.0.0.1:7687")
            .put("txStartTime", PropertyType.toConnectValue(startTime))
            .put("txCommitTime", PropertyType.toConnectValue(commitTime))
            .put(
                "txMetadata",
                Struct(schema.nestedSchema("txMetadata").schema())
                    .put("user", PropertyType.toConnectValue("app_user"))
                    .put("app", PropertyType.toConnectValue("hr"))
                    .put(
                        "xyz",
                        Struct(schema.nestedSchema("txMetadata.xyz"))
                            .put("a", PropertyType.toConnectValue(1L))
                            .put("b", PropertyType.toConnectValue(2L))))
            .put("new_field", PropertyType.toConnectValue("abc"))
            .put("another_field", PropertyType.toConnectValue(1L))

    val reverted = converted.toMetadata()
    reverted shouldBe metadata
  }

  @Test
  fun `node events should be converted to struct and back`() {
    val changeEventConverter = ChangeEventConverter()

    listOf(
            NodeEvent(
                "rel-1",
                EntityOperation.CREATE,
                listOf("Person", "Employee"),
                null,
                null,
                NodeState(
                    listOf("Person", "Employee"),
                    mapOf(
                        "id" to 1L,
                        "name" to "john",
                        "surname" to "doe",
                        "dob" to LocalDate.of(1990, 1, 1)))),
            NodeEvent(
                "rel-1",
                EntityOperation.CREATE,
                listOf("Person", "Employee"),
                mapOf(),
                null,
                NodeState(
                    listOf("Person", "Employee"),
                    mapOf(
                        "id" to 1L,
                        "name" to "john",
                        "surname" to "doe",
                        "dob" to LocalDate.of(1990, 1, 1)))),
            NodeEvent(
                "rel-1",
                EntityOperation.CREATE,
                listOf("Person", "Employee"),
                mapOf(
                    "Person" to
                        listOf(mapOf("id" to 1L), mapOf("name" to "john", "surname" to "doe")),
                    "Employee" to listOf(mapOf("id" to 1L))),
                null,
                NodeState(
                    listOf("Person", "Employee"),
                    mapOf(
                        "id" to 1L,
                        "name" to "john",
                        "surname" to "doe",
                        "dob" to LocalDate.of(1990, 1, 1)))),
            NodeEvent(
                "rel-1",
                EntityOperation.UPDATE,
                listOf("Person", "Employee"),
                mapOf(
                    "Person" to
                        listOf(mapOf("id" to 1L), mapOf("name" to "john", "surname" to "doe")),
                    "Employee" to listOf(mapOf("id" to 1L))),
                NodeState(
                    listOf("Person"),
                    mapOf(
                        "id" to 1L,
                        "name" to "john",
                        "surname" to "doe",
                        "dob" to LocalDate.of(1990, 1, 1),
                        "pob" to "London")),
                NodeState(
                    listOf("Person", "Employee"),
                    mapOf(
                        "id" to 1L,
                        "name" to "john",
                        "surname" to "doe",
                        "dob" to LocalDate.of(1990, 1, 1)))),
            NodeEvent(
                "rel-1",
                EntityOperation.DELETE,
                listOf("Person", "Employee"),
                mapOf(
                    "Person" to
                        listOf(mapOf("id" to 1L), mapOf("name" to "john", "surname" to "doe")),
                    "Employee" to listOf(mapOf("id" to 1L))),
                NodeState(
                    listOf("Person", "Employee"),
                    mapOf(
                        "id" to 1L,
                        "name" to "john",
                        "surname" to "doe",
                        "dob" to LocalDate.of(1990, 1, 1))),
                null))
        .forEach { event ->
          val schema = changeEventConverter.nodeEventToConnectSchema(event)
          val converted = changeEventConverter.nodeEventToConnectValue(event, schema)
          val reverted = converted.toNodeEvent()

          reverted shouldBe event
        }
  }

  @Test
  fun `node should be converted to struct and back`() {
    val changeEventConverter = ChangeEventConverter()

    val node =
        Node(
            "element-id-1",
            listOf("Person", "Employee"),
            mapOf(
                "Person" to listOf(mapOf("id" to 1L), mapOf("name" to "john", "surname" to "doe")),
                "Employee" to listOf(mapOf("id" to 5L, "company_id" to 7L))))
    val schema = changeEventConverter.nodeToConnectSchema(node)
    val converted = changeEventConverter.nodeToConnectValue(node, schema)

    converted shouldBe
        Struct(schema)
            .put("elementId", "element-id-1")
            .put("labels", listOf("Person", "Employee"))
            .put(
                "keys",
                Struct(schema.nestedSchema("keys"))
                    .put(
                        "Person",
                        listOf(
                            mapOf("id" to PropertyType.toConnectValue(1L)),
                            mapOf(
                                "name" to PropertyType.toConnectValue("john"),
                                "surname" to PropertyType.toConnectValue("doe"))))
                    .put(
                        "Employee",
                        listOf(
                            mapOf(
                                "id" to PropertyType.toConnectValue(5L),
                                "company_id" to PropertyType.toConnectValue(7L)))))

    val reverted = converted.toNode()
    reverted shouldBe node
  }

  @Test
  fun `relationship events should be converted to struct and back`() {
    val changeEventConverter = ChangeEventConverter()

    listOf(
            RelationshipEvent(
                "rel-1",
                "KNOWS",
                Node(
                    "node-1",
                    listOf("Person", "Employee"),
                    mapOf(
                        "Person" to
                            listOf(
                                mapOf("id" to 5L), mapOf("name" to "john", "surname" to "doe")))),
                Node(
                    "node-2",
                    listOf("Person"),
                    mapOf(
                        "Person" to
                            listOf(
                                mapOf("id" to 4L), mapOf("name" to "mary", "surname" to "doe")))),
                null,
                EntityOperation.CREATE,
                null,
                RelationshipState(
                    mapOf("since" to LocalDate.of(2012, 10, 1), "met_at" to "London"))),
            RelationshipEvent(
                "rel-1",
                "KNOWS",
                Node(
                    "node-1",
                    listOf("Person", "Employee"),
                    mapOf(
                        "Person" to
                            listOf(
                                mapOf("id" to 5L), mapOf("name" to "john", "surname" to "doe")))),
                Node(
                    "node-2",
                    listOf("Person"),
                    mapOf(
                        "Person" to
                            listOf(
                                mapOf("id" to 4L), mapOf("name" to "mary", "surname" to "doe")))),
                listOf(),
                EntityOperation.CREATE,
                null,
                RelationshipState(
                    mapOf("since" to LocalDate.of(2012, 10, 1), "met_at" to "London"))),
            RelationshipEvent(
                "rel-1",
                "KNOWS",
                Node(
                    "node-1",
                    listOf("Person", "Employee"),
                    mapOf(
                        "Person" to
                            listOf(
                                mapOf("id" to 5L), mapOf("name" to "john", "surname" to "doe")))),
                Node(
                    "node-2",
                    listOf("Person"),
                    mapOf(
                        "Person" to
                            listOf(
                                mapOf("id" to 4L), mapOf("name" to "mary", "surname" to "doe")))),
                listOf(mapOf("a" to 1L), mapOf("b" to "another")),
                EntityOperation.CREATE,
                null,
                RelationshipState(
                    mapOf(
                        "since" to LocalDate.of(2012, 10, 1),
                        "met_at" to "London",
                        "a" to 1L,
                        "b" to "another"))),
            RelationshipEvent(
                "rel-1",
                "KNOWS",
                Node(
                    "node-1",
                    listOf("Person", "Employee"),
                    mapOf(
                        "Person" to
                            listOf(
                                mapOf("id" to 5L), mapOf("name" to "john", "surname" to "doe")))),
                Node(
                    "node-2",
                    listOf("Person"),
                    mapOf(
                        "Person" to
                            listOf(
                                mapOf("id" to 4L), mapOf("name" to "mary", "surname" to "doe")))),
                listOf(mapOf("a" to 1L), mapOf("b" to "another")),
                EntityOperation.UPDATE,
                RelationshipState(mapOf("a" to 1L, "b" to "another", "c" to 5L)),
                RelationshipState(
                    mapOf(
                        "since" to LocalDate.of(2012, 10, 1),
                        "met_at" to "London",
                        "a" to 1L,
                        "b" to "another"))),
            RelationshipEvent(
                "rel-1",
                "KNOWS",
                Node(
                    "node-1",
                    listOf("Person", "Employee"),
                    mapOf(
                        "Person" to
                            listOf(
                                mapOf("id" to 5L), mapOf("name" to "john", "surname" to "doe")))),
                Node(
                    "node-2",
                    listOf("Person"),
                    mapOf(
                        "Person" to
                            listOf(
                                mapOf("id" to 4L), mapOf("name" to "mary", "surname" to "doe")))),
                listOf(mapOf("a" to 1L), mapOf("b" to "another")),
                EntityOperation.DELETE,
                RelationshipState(
                    mapOf(
                        "since" to LocalDate.of(2012, 10, 1),
                        "met_at" to "London",
                        "a" to 1L,
                        "b" to "another")),
                null))
        .forEach { event ->
          val schema = changeEventConverter.relationshipEventToConnectSchema(event)
          val converted = changeEventConverter.relationshipEventToConnectValue(event, schema)
          val reverted = converted.toRelationshipEvent()

          reverted shouldBe event
        }
  }

  data class ChangeEventResult<T : Event>(
      val event: T,
      val change: ChangeEvent,
      val schema: Schema,
      val converted: Struct
  )

  private fun <T : Event> newChangeEvent(event: T): ChangeEventResult<T> {
    val changeEventConverter = ChangeEventConverter()
    val change =
        ChangeEvent(
            ChangeIdentifier("change-id"),
            1,
            0,
            Metadata(
                "service",
                "neo4j",
                "server-1",
                CaptureMode.DIFF,
                "bolt",
                "127.0.0.1:32000",
                "127.0.0.1:7687",
                ZonedDateTime.now().minusSeconds(1),
                ZonedDateTime.now(),
                mapOf("user" to "app_user", "app" to "hr"),
                emptyMap()),
            event)
    val schemaAndValue = changeEventConverter.toConnectValue(change)

    return ChangeEventResult(
        event, change, schemaAndValue.schema(), schemaAndValue.value() as Struct)
  }

  private fun Schema.nestedSchema(path: String): Schema {
    require(path.isNotBlank())

    return path.split('.').fold(this) { schema, field -> schema.field(field).schema() }
  }

  private fun Schema.nestedValueSchema(path: String): Schema {
    return nestedSchema(path).valueSchema()
  }

  private fun Struct.nestedValue(path: String): Any? {
    require(path.isNotBlank())

    val fields = path.split('.')
    return fields
        .dropLast(1)
        .fold(this) { struct, field -> struct[field] as Struct }
        .get(fields.last())
  }
}
