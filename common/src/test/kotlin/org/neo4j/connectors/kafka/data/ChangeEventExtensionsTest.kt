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

import io.kotest.matchers.shouldBe
import java.time.LocalDate
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
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
import org.neo4j.connectors.kafka.data.ChangeEventExtensions.toConnectValue

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
            .name("org.neo4j.connectors.kafka.cdc.Metadata")
            .field("authenticatedUser", Schema.STRING_SCHEMA)
            .field("executingUser", Schema.STRING_SCHEMA)
            .field("connectionType", Schema.OPTIONAL_STRING_SCHEMA)
            .field("connectionClient", Schema.OPTIONAL_STRING_SCHEMA)
            .field("connectionServer", Schema.OPTIONAL_STRING_SCHEMA)
            .field("serverId", Schema.STRING_SCHEMA)
            .field("captureMode", Schema.STRING_SCHEMA)
            .field("txStartTime", SimpleTypes.ZONEDDATETIME.schema())
            .field("txCommitTime", SimpleTypes.ZONEDDATETIME.schema())
            .field(
                "txMetadata",
                SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA)
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
            .put("txStartTime", change.metadata.txStartTime.format(DateTimeFormatter.ISO_DATE_TIME))
            .put(
                "txCommitTime",
                change.metadata.txCommitTime.format(DateTimeFormatter.ISO_DATE_TIME))
            .put("txMetadata", mapOf("user" to "app_user", "app" to "hr"))
  }

  @Test
  fun `schema and value should be generated correctly for node create events`() {
    val (_, _, schema, value) =
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
            .name("org.neo4j.connectors.kafka.cdc.NodeEvent")
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
                                SchemaBuilder.struct()
                                    .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                                    .field("surname", Schema.OPTIONAL_STRING_SCHEMA)
                                    .optional()
                                    .build())
                            .optional()
                            .build())
                    .field(
                        "Label2",
                        SchemaBuilder.array(
                                SchemaBuilder.struct()
                                    .field("id", Schema.OPTIONAL_INT64_SCHEMA)
                                    .optional()
                                    .build())
                            .optional()
                            .build())
                    .optional()
                    .build())
            .field(
                "state",
                SchemaBuilder.struct()
                    .name("org.neo4j.connectors.kafka.cdc.NodeStates")
                    .field(
                        "before",
                        SchemaBuilder.struct()
                            .name("org.neo4j.connectors.kafka.cdc.NodeState")
                            .field("labels", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                            .field(
                                "properties",
                                SchemaBuilder.struct()
                                    .field("id", Schema.OPTIONAL_INT64_SCHEMA)
                                    .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                                    .field("surname", Schema.OPTIONAL_STRING_SCHEMA)
                                    .build())
                            .optional()
                            .build())
                    .field(
                        "after",
                        SchemaBuilder.struct()
                            .name("org.neo4j.connectors.kafka.cdc.NodeState")
                            .field("labels", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                            .field(
                                "properties",
                                SchemaBuilder.struct()
                                    .field("id", Schema.OPTIONAL_INT64_SCHEMA)
                                    .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                                    .field("surname", Schema.OPTIONAL_STRING_SCHEMA)
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
                            Struct(schema.nestedValueSchema("event.keys.Label1"))
                                .put("name", "john")
                                .put("surname", "doe")))
                    .put(
                        "Label2",
                        listOf(
                            Struct(schema.nestedValueSchema("event.keys.Label2")).put("id", 5L))))
            .put(
                "state",
                Struct(schema.nestedSchema("event.state"))
                    .put(
                        "after",
                        Struct(schema.nestedSchema("event.state.after"))
                            .put("labels", listOf("Label1", "Label2"))
                            .put(
                                "properties",
                                Struct(schema.nestedSchema("event.state.after.properties"))
                                    .put("id", 5L)
                                    .put("name", "john")
                                    .put("surname", "doe"))))
  }

  @Test
  fun `schema and value should be generated correctly for node update events`() {
    val (_, _, schema, value) =
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
                    mapOf("id" to 5L, "name" to "john", "surname" to "doe", "age" to 25))))

    schema.nestedSchema("event") shouldBe
        SchemaBuilder.struct()
            .name("org.neo4j.connectors.kafka.cdc.NodeEvent")
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
                                SchemaBuilder.struct()
                                    .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                                    .field("surname", Schema.OPTIONAL_STRING_SCHEMA)
                                    .optional()
                                    .build())
                            .optional()
                            .build())
                    .field(
                        "Label2",
                        SchemaBuilder.array(
                                SchemaBuilder.struct()
                                    .field("id", Schema.OPTIONAL_INT64_SCHEMA)
                                    .optional()
                                    .build())
                            .optional()
                            .build())
                    .optional()
                    .build())
            .field(
                "state",
                SchemaBuilder.struct()
                    .name("org.neo4j.connectors.kafka.cdc.NodeStates")
                    .field(
                        "before",
                        SchemaBuilder.struct()
                            .name("org.neo4j.connectors.kafka.cdc.NodeState")
                            .field("labels", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                            .field(
                                "properties",
                                SchemaBuilder.struct()
                                    .field("id", Schema.OPTIONAL_INT64_SCHEMA)
                                    .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                                    .field("surname", Schema.OPTIONAL_STRING_SCHEMA)
                                    .field("age", Schema.OPTIONAL_INT64_SCHEMA)
                                    .build())
                            .optional()
                            .build())
                    .field(
                        "after",
                        SchemaBuilder.struct()
                            .name("org.neo4j.connectors.kafka.cdc.NodeState")
                            .field("labels", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                            .field(
                                "properties",
                                SchemaBuilder.struct()
                                    .field("id", Schema.OPTIONAL_INT64_SCHEMA)
                                    .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                                    .field("surname", Schema.OPTIONAL_STRING_SCHEMA)
                                    .field("age", Schema.OPTIONAL_INT64_SCHEMA)
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
                            Struct(schema.nestedValueSchema("event.keys.Label1"))
                                .put("name", "john")
                                .put("surname", "doe")))
                    .put(
                        "Label2",
                        listOf(
                            Struct(schema.nestedValueSchema("event.keys.Label2")).put("id", 5L))))
            .put(
                "state",
                Struct(schema.nestedSchema("event.state"))
                    .put(
                        "before",
                        Struct(schema.nestedSchema("event.state.before"))
                            .put("labels", listOf("Label1", "Label2"))
                            .put(
                                "properties",
                                Struct(schema.nestedSchema("event.state.before.properties"))
                                    .put("id", 5L)
                                    .put("name", "john")
                                    .put("surname", "doe")))
                    .put(
                        "after",
                        Struct(schema.nestedSchema("event.state.after"))
                            .put("labels", listOf("Label1", "Label2", "Label3"))
                            .put(
                                "properties",
                                Struct(schema.nestedSchema("event.state.after.properties"))
                                    .put("id", 5L)
                                    .put("name", "john")
                                    .put("surname", "doe")
                                    .put("age", 25L))))
  }

  @Test
  fun `schema and value should be generated correctly for node delete events`() {
    val (_, _, schema, value) =
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
                    mapOf("id" to 5L, "name" to "john", "surname" to "doe", "age" to 25)),
                null))

    schema.nestedSchema("event") shouldBe
        SchemaBuilder.struct()
            .name("org.neo4j.connectors.kafka.cdc.NodeEvent")
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
                                SchemaBuilder.struct()
                                    .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                                    .field("surname", Schema.OPTIONAL_STRING_SCHEMA)
                                    .optional()
                                    .build())
                            .optional()
                            .build())
                    .field(
                        "Label2",
                        SchemaBuilder.array(
                                SchemaBuilder.struct()
                                    .field("id", Schema.OPTIONAL_INT64_SCHEMA)
                                    .optional()
                                    .build())
                            .optional()
                            .build())
                    .optional()
                    .build())
            .field(
                "state",
                SchemaBuilder.struct()
                    .name("org.neo4j.connectors.kafka.cdc.NodeStates")
                    .field(
                        "before",
                        SchemaBuilder.struct()
                            .name("org.neo4j.connectors.kafka.cdc.NodeState")
                            .field("labels", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                            .field(
                                "properties",
                                SchemaBuilder.struct()
                                    .field("id", Schema.OPTIONAL_INT64_SCHEMA)
                                    .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                                    .field("surname", Schema.OPTIONAL_STRING_SCHEMA)
                                    .field("age", Schema.OPTIONAL_INT64_SCHEMA)
                                    .build())
                            .optional()
                            .build())
                    .field(
                        "after",
                        SchemaBuilder.struct()
                            .name("org.neo4j.connectors.kafka.cdc.NodeState")
                            .field("labels", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                            .field(
                                "properties",
                                SchemaBuilder.struct()
                                    .field("id", Schema.OPTIONAL_INT64_SCHEMA)
                                    .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                                    .field("surname", Schema.OPTIONAL_STRING_SCHEMA)
                                    .field("age", Schema.OPTIONAL_INT64_SCHEMA)
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
                            Struct(schema.nestedValueSchema("event.keys.Label1"))
                                .put("name", "john")
                                .put("surname", "doe")))
                    .put(
                        "Label2",
                        listOf(
                            Struct(schema.nestedValueSchema("event.keys.Label2")).put("id", 5L))))
            .put(
                "state",
                Struct(schema.nestedSchema("event.state"))
                    .put(
                        "before",
                        Struct(schema.nestedSchema("event.state.before"))
                            .put("labels", listOf("Label1", "Label2", "Label3"))
                            .put(
                                "properties",
                                Struct(schema.nestedSchema("event.state.before.properties"))
                                    .put("id", 5L)
                                    .put("name", "john")
                                    .put("surname", "doe")
                                    .put("age", 25L))))
  }

  @Test
  fun `schema and value should be generated correctly for relationship create events`() {
    val (_, _, schema, value) =
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
            .name("org.neo4j.connectors.kafka.cdc.RelationshipEvent")
            .field("elementId", Schema.STRING_SCHEMA)
            .field("eventType", Schema.STRING_SCHEMA)
            .field("operation", Schema.STRING_SCHEMA)
            .field("type", Schema.STRING_SCHEMA)
            .field(
                "start",
                SchemaBuilder.struct()
                    .name("org.neo4j.connectors.kafka.cdc.Node")
                    .field("elementId", Schema.STRING_SCHEMA)
                    .field("labels", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                    .field(
                        "keys",
                        SchemaBuilder.struct()
                            .field(
                                "Person",
                                SchemaBuilder.array(
                                        SchemaBuilder.struct()
                                            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                                            .optional()
                                            .schema())
                                    .optional()
                                    .schema())
                            .optional()
                            .build())
                    .build())
            .field(
                "end",
                SchemaBuilder.struct()
                    .name("org.neo4j.connectors.kafka.cdc.Node")
                    .field("elementId", Schema.STRING_SCHEMA)
                    .field("labels", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                    .field(
                        "keys",
                        SchemaBuilder.struct()
                            .field(
                                "Company",
                                SchemaBuilder.array(
                                        SchemaBuilder.struct()
                                            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                                            .optional()
                                            .schema())
                                    .optional()
                                    .schema())
                            .optional()
                            .build())
                    .build())
            .field(
                "keys",
                SchemaBuilder.array(
                        SchemaBuilder.struct()
                            .field("id", Schema.OPTIONAL_INT64_SCHEMA)
                            .optional()
                            .build())
                    .optional()
                    .build())
            .field(
                "state",
                SchemaBuilder.struct()
                    .name("org.neo4j.connectors.kafka.cdc.RelationshipStates")
                    .field(
                        "before",
                        SchemaBuilder.struct()
                            .name("org.neo4j.connectors.kafka.cdc.RelationshipState")
                            .field(
                                "properties",
                                SchemaBuilder.struct()
                                    .field("id", Schema.OPTIONAL_INT64_SCHEMA)
                                    .field("since", SimpleTypes.LOCALDATE.schema(true))
                                    .build())
                            .optional()
                            .build())
                    .field(
                        "after",
                        SchemaBuilder.struct()
                            .name("org.neo4j.connectors.kafka.cdc.RelationshipState")
                            .field(
                                "properties",
                                SchemaBuilder.struct()
                                    .field("id", Schema.OPTIONAL_INT64_SCHEMA)
                                    .field("since", SimpleTypes.LOCALDATE.schema(true))
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
                                listOf(
                                    Struct(schema.nestedValueSchema("event.start.keys.Person"))
                                        .put("name", "john")))))
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
                                listOf(
                                    Struct(schema.nestedValueSchema("event.end.keys.Company"))
                                        .put("name", "acme corp")))))
            .put("keys", listOf(Struct(schema.nestedValueSchema("event.keys")).put("id", 5L)))
            .put(
                "state",
                Struct(schema.nestedSchema("event.state"))
                    .put(
                        "after",
                        Struct(schema.nestedSchema("event.state.after"))
                            .put(
                                "properties",
                                Struct(schema.nestedSchema("event.state.after.properties"))
                                    .put("id", 5L)
                                    .put("since", "1999-12-31"))))
  }

  @Test
  fun `schema and value should be generated correctly for relationship update events`() {
    val (_, _, schema, value) =
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
            .name("org.neo4j.connectors.kafka.cdc.RelationshipEvent")
            .field("elementId", Schema.STRING_SCHEMA)
            .field("eventType", Schema.STRING_SCHEMA)
            .field("operation", Schema.STRING_SCHEMA)
            .field("type", Schema.STRING_SCHEMA)
            .field(
                "start",
                SchemaBuilder.struct()
                    .name("org.neo4j.connectors.kafka.cdc.Node")
                    .field("elementId", Schema.STRING_SCHEMA)
                    .field("labels", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                    .field(
                        "keys",
                        SchemaBuilder.struct()
                            .field(
                                "Person",
                                SchemaBuilder.array(
                                        SchemaBuilder.struct()
                                            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                                            .optional()
                                            .schema())
                                    .optional()
                                    .schema())
                            .optional()
                            .build())
                    .build())
            .field(
                "end",
                SchemaBuilder.struct()
                    .name("org.neo4j.connectors.kafka.cdc.Node")
                    .field("elementId", Schema.STRING_SCHEMA)
                    .field("labels", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                    .field(
                        "keys",
                        SchemaBuilder.struct()
                            .field(
                                "Company",
                                SchemaBuilder.array(
                                        SchemaBuilder.struct()
                                            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                                            .optional()
                                            .schema())
                                    .optional()
                                    .schema())
                            .optional()
                            .build())
                    .build())
            .field(
                "keys",
                SchemaBuilder.array(
                        SchemaBuilder.struct()
                            .field("id", Schema.OPTIONAL_INT64_SCHEMA)
                            .optional()
                            .build())
                    .optional()
                    .build())
            .field(
                "state",
                SchemaBuilder.struct()
                    .name("org.neo4j.connectors.kafka.cdc.RelationshipStates")
                    .field(
                        "before",
                        SchemaBuilder.struct()
                            .name("org.neo4j.connectors.kafka.cdc.RelationshipState")
                            .field(
                                "properties",
                                SchemaBuilder.struct()
                                    .field("id", Schema.OPTIONAL_INT64_SCHEMA)
                                    .field("since", SimpleTypes.LOCALDATE.schema(true))
                                    .build())
                            .optional()
                            .build())
                    .field(
                        "after",
                        SchemaBuilder.struct()
                            .name("org.neo4j.connectors.kafka.cdc.RelationshipState")
                            .field(
                                "properties",
                                SchemaBuilder.struct()
                                    .field("id", Schema.OPTIONAL_INT64_SCHEMA)
                                    .field("since", SimpleTypes.LOCALDATE.schema(true))
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
                                listOf(
                                    Struct(schema.nestedValueSchema("event.start.keys.Person"))
                                        .put("name", "john")))))
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
                                listOf(
                                    Struct(schema.nestedValueSchema("event.end.keys.Company"))
                                        .put("name", "acme corp")))))
            .put("keys", listOf(Struct(schema.nestedValueSchema("event.keys")).put("id", 5L)))
            .put(
                "state",
                Struct(schema.nestedSchema("event.state"))
                    .put(
                        "before",
                        Struct(schema.nestedSchema("event.state.before"))
                            .put(
                                "properties",
                                Struct(schema.nestedSchema("event.state.before.properties"))
                                    .put("id", 5L)
                                    .put("since", "1999-12-31")))
                    .put(
                        "after",
                        Struct(schema.nestedSchema("event.state.after"))
                            .put(
                                "properties",
                                Struct(schema.nestedSchema("event.state.after.properties"))
                                    .put("id", 5L)
                                    .put("since", "2000-01-01"))))
  }

  @Test
  fun `schema and value should be generated correctly for relationship delete events`() {
    val (_, _, schema, value) =
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
            .name("org.neo4j.connectors.kafka.cdc.RelationshipEvent")
            .field("elementId", Schema.STRING_SCHEMA)
            .field("eventType", Schema.STRING_SCHEMA)
            .field("operation", Schema.STRING_SCHEMA)
            .field("type", Schema.STRING_SCHEMA)
            .field(
                "start",
                SchemaBuilder.struct()
                    .name("org.neo4j.connectors.kafka.cdc.Node")
                    .field("elementId", Schema.STRING_SCHEMA)
                    .field("labels", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                    .field(
                        "keys",
                        SchemaBuilder.struct()
                            .field(
                                "Person",
                                SchemaBuilder.array(
                                        SchemaBuilder.struct()
                                            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                                            .optional()
                                            .build())
                                    .optional()
                                    .build())
                            .optional()
                            .build())
                    .build())
            .field(
                "end",
                SchemaBuilder.struct()
                    .name("org.neo4j.connectors.kafka.cdc.Node")
                    .field("elementId", Schema.STRING_SCHEMA)
                    .field("labels", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                    .field(
                        "keys",
                        SchemaBuilder.struct()
                            .field(
                                "Company",
                                SchemaBuilder.array(
                                        SchemaBuilder.struct()
                                            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                                            .optional()
                                            .build())
                                    .optional()
                                    .build())
                            .optional()
                            .build())
                    .build())
            .field(
                "keys",
                SchemaBuilder.array(
                        SchemaBuilder.struct()
                            .field("id", Schema.OPTIONAL_INT64_SCHEMA)
                            .optional()
                            .build())
                    .optional()
                    .schema())
            .field(
                "state",
                SchemaBuilder.struct()
                    .name("org.neo4j.connectors.kafka.cdc.RelationshipStates")
                    .field(
                        "before",
                        SchemaBuilder.struct()
                            .name("org.neo4j.connectors.kafka.cdc.RelationshipState")
                            .field(
                                "properties",
                                SchemaBuilder.struct()
                                    .field("id", Schema.OPTIONAL_INT64_SCHEMA)
                                    .field("since", SimpleTypes.LOCALDATE.schema(true))
                                    .build())
                            .optional()
                            .build())
                    .field(
                        "after",
                        SchemaBuilder.struct()
                            .name("org.neo4j.connectors.kafka.cdc.RelationshipState")
                            .field(
                                "properties",
                                SchemaBuilder.struct()
                                    .field("id", Schema.OPTIONAL_INT64_SCHEMA)
                                    .field("since", SimpleTypes.LOCALDATE.schema(true))
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
                                listOf(
                                    Struct(schema.nestedValueSchema("event.start.keys.Person"))
                                        .put("name", "john")))))
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
                                listOf(
                                    Struct(schema.nestedValueSchema("event.end.keys.Company"))
                                        .put("name", "acme corp")))))
            .put("keys", listOf(Struct(schema.nestedValueSchema("event.keys")).put("id", 5L)))
            .put(
                "state",
                Struct(schema.nestedSchema("event.state"))
                    .put(
                        "before",
                        Struct(schema.nestedSchema("event.state.before"))
                            .put(
                                "properties",
                                Struct(schema.nestedSchema("event.state.before.properties"))
                                    .put("id", 5L)
                                    .put("since", "2000-01-01"))))
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
    value.nestedValue("event.keys") shouldBe null
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
        SchemaBuilder.array(SchemaBuilder.struct().optional().build()).optional().build()
    schema.nestedSchema("event.keys") shouldBe expectedKeySchema
    value.nestedValue("event.keys") shouldBe null
  }

  data class ChangeEventResult<T : Event>(
      val event: T,
      val change: ChangeEvent,
      val schema: Schema,
      val converted: Struct
  )

  private fun <T : Event> newChangeEvent(event: T): ChangeEventResult<T> {
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
    val schemaAndValue = change.toConnectValue()

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
