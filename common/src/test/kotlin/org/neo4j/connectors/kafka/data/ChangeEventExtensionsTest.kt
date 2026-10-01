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
import java.time.format.DateTimeFormatter
import java.util.stream.Stream
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.ArgumentsProvider
import org.junit.jupiter.params.provider.ArgumentsSource
import org.junit.jupiter.params.provider.EnumSource
import org.junit.jupiter.params.support.ParameterDeclarations
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
import org.neo4j.connectors.kafka.configuration.PayloadMode

class ChangeEventExtensionsTest {

  @Test
  fun `schema and value should be generated correctly for common fields with extended payload`() {
    val (_, change, schema, value) =
        newChangeEvent(
            PayloadMode.EXTENDED,
            NodeEvent(
                "element-0",
                EntityOperation.CREATE,
                listOf("Label1", "Label2"),
                mapOf(
                    "Label1" to listOf(mapOf("name" to "john", "surname" to "doe")),
                    "Label2" to listOf(mapOf("id" to 5L)),
                ),
                null,
                NodeState(
                    listOf("Label1", "Label2"),
                    mapOf("id" to 5L, "name" to "john", "surname" to "doe"),
                ),
            ),
        )

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
                    .field("app", PropertyType.schema)
                    .field("user", PropertyType.schema)
                    .optional()
                    .build(),
            )
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
                change.metadata.txCommitTime.let { PropertyType.toConnectValue(it) },
            )
            .put(
                "txMetadata",
                Struct(schema.nestedSchema("metadata.txMetadata"))
                    .put("user", PropertyType.toConnectValue("app_user"))
                    .put("app", PropertyType.toConnectValue("hr")),
            )
  }

  @Test
  fun `schema and value should be generated correctly for common fields with compact payload`() {
    val (_, change, schema, value) =
        newChangeEvent(
            PayloadMode.COMPACT,
            NodeEvent(
                "element-0",
                EntityOperation.CREATE,
                listOf("Label1", "Label2"),
                mapOf(
                    "Label1" to listOf(mapOf("name" to "john", "surname" to "doe")),
                    "Label2" to listOf(mapOf("id" to 5L)),
                ),
                null,
                NodeState(
                    listOf("Label1", "Label2"),
                    mapOf("id" to 5L, "name" to "john", "surname" to "doe"),
                ),
            ),
        )

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
            .field("txStartTime", SimpleTypes.ZONEDDATETIME.schema)
            .field("txCommitTime", SimpleTypes.ZONEDDATETIME.schema)
            .field(
                "txMetadata",
                SchemaBuilder.struct()
                    .field("app", Schema.OPTIONAL_STRING_SCHEMA)
                    .field("user", Schema.OPTIONAL_STRING_SCHEMA)
                    .optional()
                    .build(),
            )
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
            .put(
                "txStartTime",
                change.metadata.txStartTime.let { DateTimeFormatter.ISO_DATE_TIME.format(it) },
            )
            .put(
                "txCommitTime",
                change.metadata.txCommitTime.let { DateTimeFormatter.ISO_DATE_TIME.format(it) },
            )
            .put(
                "txMetadata",
                Struct(schema.nestedSchema("metadata.txMetadata"))
                    .put("user", "app_user")
                    .put("app", "hr"),
            )
  }

  @ParameterizedTest(name = "{0}")
  @ArgumentsSource(PayloadModeValues::class)
  fun `schema and value should be generated and converted back correctly for node create events`(
      name: String,
      payloadMode: PayloadMode,
  ) {
    val props = mapOf("id" to 5L, "name" to "john", "surname" to "doe")
    val (_, change, schema, value) =
        newChangeEvent(
            payloadMode,
            NodeEvent(
                "element-0",
                EntityOperation.CREATE,
                listOf("Label1", "Label2"),
                mapOf(
                    "Label1" to listOf(mapOf("name" to "john", "surname" to "doe")),
                    "Label2" to listOf(mapOf("id" to 5L)),
                ),
                null,
                NodeState(listOf("Label1", "Label2"), props),
            ),
        )

    val propertiesSchema = propertiesSchema(payloadMode, props)
    schema.nestedSchema("event") shouldBe unifiedEventSchema(propertiesSchema)
    value.get("event") shouldBe
        Struct(schema.nestedSchema("event"))
            .put("elementId", "element-0")
            .put("eventType", "NODE")
            .put("operation", "CREATE")
            .put("labels", listOf("Label1", "Label2"))
            .put(
                "keys",
                keysValue(
                    mapOf(
                        "Label1" to listOf(mapOf("name" to "john", "surname" to "doe")),
                        "Label2" to listOf(mapOf("id" to 5L)),
                    )
                ),
            )
            .put(
                "state",
                Struct(schema.nestedSchema("event.state"))
                    .put(
                        "after",
                        entityState(
                            schema.nestedSchema("event.state.after"),
                            listOf("Label1", "Label2"),
                            payloadMode,
                            props,
                        ),
                    ),
            )

    value.toChangeEvent() shouldBe change
  }

  @ParameterizedTest(name = "{0}")
  @ArgumentsSource(PayloadModeValues::class)
  fun `schema and value should be generated and converted back correctly for node update events`(
      name: String,
      payloadMode: PayloadMode,
  ) {
    val before = mapOf("id" to 5L, "name" to "john")
    val after = mapOf("id" to 5L, "name" to "johnny", "age" to 30L)
    val (_, change, schema, value) =
        newChangeEvent(
            payloadMode,
            NodeEvent(
                "element-0",
                EntityOperation.UPDATE,
                listOf("Label1", "Label2"),
                mapOf("Label1" to listOf(mapOf("id" to 5L))),
                NodeState(listOf("Label1"), before),
                NodeState(listOf("Label1", "Label2"), after),
            ),
        )

    schema.nestedSchema("event") shouldBe
        unifiedEventSchema(propertiesSchema(payloadMode, before + after))
    value.get("event") shouldBe
        Struct(schema.nestedSchema("event"))
            .put("elementId", "element-0")
            .put("eventType", "NODE")
            .put("operation", "UPDATE")
            .put("labels", listOf("Label1", "Label2"))
            .put("keys", keysValue(mapOf("Label1" to listOf(mapOf("id" to 5L)))))
            .put(
                "state",
                Struct(schema.nestedSchema("event.state"))
                    .put(
                        "before",
                        entityState(
                            schema.nestedSchema("event.state.before"),
                            listOf("Label1"),
                            payloadMode,
                            before,
                        ),
                    )
                    .put(
                        "after",
                        entityState(
                            schema.nestedSchema("event.state.after"),
                            listOf("Label1", "Label2"),
                            payloadMode,
                            after,
                        ),
                    ),
            )

    value.toChangeEvent() shouldBe change
  }

  @ParameterizedTest(name = "{0}")
  @ArgumentsSource(PayloadModeValues::class)
  fun `schema and value should be generated and converted back correctly for node delete events`(
      name: String,
      payloadMode: PayloadMode,
  ) {
    val before = mapOf("id" to 5L, "dob" to LocalDate.of(2000, 1, 1))
    val (_, change, schema, value) =
        newChangeEvent(
            payloadMode,
            NodeEvent(
                "element-0",
                EntityOperation.DELETE,
                listOf("Label1"),
                mapOf("Label1" to listOf(mapOf("id" to 5L))),
                NodeState(listOf("Label1"), before),
                null,
            ),
        )

    schema.nestedSchema("event") shouldBe unifiedEventSchema(propertiesSchema(payloadMode, before))
    value.get("event") shouldBe
        Struct(schema.nestedSchema("event"))
            .put("elementId", "element-0")
            .put("eventType", "NODE")
            .put("operation", "DELETE")
            .put("labels", listOf("Label1"))
            .put("keys", keysValue(mapOf("Label1" to listOf(mapOf("id" to 5L)))))
            .put(
                "state",
                Struct(schema.nestedSchema("event.state"))
                    .put(
                        "before",
                        entityState(
                            schema.nestedSchema("event.state.before"),
                            listOf("Label1"),
                            payloadMode,
                            before,
                        ),
                    ),
            )

    value.toChangeEvent() shouldBe change
  }

  private val personNode =
      Node("node-0", listOf("Person"), mapOf("Person" to listOf(mapOf("name" to "john"))))
  private val companyNode =
      Node("node-1", listOf("Company"), mapOf("Company" to listOf(mapOf("name" to "acme corp"))))

  private fun relationshipEventStruct(
      schema: Schema,
      operation: String,
      keys: List<Map<String, Any>>,
      state: Struct,
  ): Struct =
      Struct(schema.nestedSchema("event"))
          .put("elementId", "rel-0")
          .put("eventType", "RELATIONSHIP")
          .put("operation", operation)
          .put("type", "WORKS_FOR")
          .put("start", nodeRefValue(schema.nestedSchema("event.start"), personNode))
          .put("end", nodeRefValue(schema.nestedSchema("event.end"), companyNode))
          .put("keys", keysValue(mapOf("WORKS_FOR" to keys)))
          .put("state", state)

  @ParameterizedTest(name = "{0}")
  @ArgumentsSource(PayloadModeValues::class)
  fun `schema and value should be generated and converted back correctly for relationship create events`(
      name: String,
      payloadMode: PayloadMode,
  ) {
    val after = mapOf("id" to 5L, "since" to LocalDate.of(2000, 1, 1))
    val (_, change, schema, value) =
        newChangeEvent(
            payloadMode,
            RelationshipEvent(
                "rel-0",
                "WORKS_FOR",
                personNode,
                companyNode,
                listOf(mapOf("id" to 5L)),
                EntityOperation.CREATE,
                null,
                RelationshipState(after),
            ),
        )

    schema.nestedSchema("event") shouldBe unifiedEventSchema(propertiesSchema(payloadMode, after))
    value.get("event") shouldBe
        relationshipEventStruct(
            schema,
            "CREATE",
            listOf(mapOf("id" to 5L)),
            Struct(schema.nestedSchema("event.state"))
                .put(
                    "after",
                    entityState(schema.nestedSchema("event.state.after"), null, payloadMode, after),
                ),
        )

    value.toChangeEvent() shouldBe change
  }

  @ParameterizedTest(name = "{0}")
  @ArgumentsSource(PayloadModeValues::class)
  fun `schema and value should be generated and converted back correctly for relationship update events`(
      name: String,
      payloadMode: PayloadMode,
  ) {
    val before = mapOf("id" to 5L, "since" to LocalDate.of(1999, 12, 31))
    val after = mapOf("id" to 5L, "since" to LocalDate.of(2000, 1, 1), "role" to "dev")
    val (_, change, schema, value) =
        newChangeEvent(
            payloadMode,
            RelationshipEvent(
                "rel-0",
                "WORKS_FOR",
                personNode,
                companyNode,
                listOf(mapOf("id" to 5L)),
                EntityOperation.UPDATE,
                RelationshipState(before),
                RelationshipState(after),
            ),
        )

    schema.nestedSchema("event") shouldBe
        unifiedEventSchema(propertiesSchema(payloadMode, before + after))
    value.get("event") shouldBe
        relationshipEventStruct(
            schema,
            "UPDATE",
            listOf(mapOf("id" to 5L)),
            Struct(schema.nestedSchema("event.state"))
                .put(
                    "before",
                    entityState(
                        schema.nestedSchema("event.state.before"),
                        null,
                        payloadMode,
                        before,
                    ),
                )
                .put(
                    "after",
                    entityState(schema.nestedSchema("event.state.after"), null, payloadMode, after),
                ),
        )

    value.toChangeEvent() shouldBe change
  }

  @ParameterizedTest(name = "{0}")
  @ArgumentsSource(PayloadModeValues::class)
  fun `schema and value should be generated and converted back correctly for relationship delete events`(
      name: String,
      payloadMode: PayloadMode,
  ) {
    val before = mapOf("id" to 5L, "since" to LocalDate.of(1999, 12, 31))
    val (_, change, schema, value) =
        newChangeEvent(
            payloadMode,
            RelationshipEvent(
                "rel-0",
                "WORKS_FOR",
                personNode,
                companyNode,
                listOf(mapOf("id" to 5L)),
                EntityOperation.DELETE,
                RelationshipState(before),
                null,
            ),
        )

    schema.nestedSchema("event") shouldBe unifiedEventSchema(propertiesSchema(payloadMode, before))
    value.get("event") shouldBe
        relationshipEventStruct(
            schema,
            "DELETE",
            listOf(mapOf("id" to 5L)),
            Struct(schema.nestedSchema("event.state"))
                .put(
                    "before",
                    entityState(
                        schema.nestedSchema("event.state.before"),
                        null,
                        payloadMode,
                        before,
                    ),
                ),
        )

    value.toChangeEvent() shouldBe change
  }

  @ParameterizedTest(name = "{0}")
  @ArgumentsSource(PayloadModeValues::class)
  fun `node event keys should be empty when node keys are not defined`(
      name: String,
      payloadMode: PayloadMode,
  ) {
    val (_, _, schema, value) =
        newChangeEvent(
            payloadMode,
            NodeEvent(
                "element-0",
                EntityOperation.CREATE,
                listOf("Label1", "Label2"),
                mapOf(),
                null,
                NodeState(listOf("Label1"), mapOf("id" to 5L)),
            ),
        )

    schema.nestedSchema("event.keys") shouldBe keysSchema()
    value.nestedValue("event.keys") shouldBe emptyMap<String, Any>()
  }

  @ParameterizedTest(name = "{0}")
  @ArgumentsSource(PayloadModeValues::class)
  fun `relationship event keys should be empty when rel keys are not defined`(
      name: String,
      payloadMode: PayloadMode,
  ) {
    val (_, _, schema, value) =
        newChangeEvent(
            payloadMode,
            RelationshipEvent(
                "rel-0",
                "WORKS_FOR",
                Node("node-0", listOf("Person"), mapOf()),
                Node("node-1", listOf("Company"), mapOf()),
                listOf(),
                EntityOperation.DELETE,
                RelationshipState(mapOf("id" to 5L)),
                null,
            ),
        )

    schema.nestedSchema("event.keys") shouldBe keysSchema()
    value.nestedValue("event.keys") shouldBe mapOf("WORKS_FOR" to emptyList<Any>())
  }

  @Test
  fun `event schema should be equal for node and relationship create events`() {
    val (_, _, nodeSchema, _) = newChangeEvent(PayloadMode.EXTENDED, unifiedNodeEvent)
    val (_, _, relSchema, _) = newChangeEvent(PayloadMode.EXTENDED, unifiedRelationshipEvent)

    nodeSchema.nestedSchema("event") shouldBe relSchema.nestedSchema("event")
  }

  @Test
  fun `event schema should be equal for node events with different labels and properties`() {
    val (_, _, schema1, _) = newChangeEvent(PayloadMode.EXTENDED, unifiedNodeEvent)
    val (_, _, schema2, _) =
        newChangeEvent(
            PayloadMode.EXTENDED,
            NodeEvent(
                "element-9",
                EntityOperation.UPDATE,
                listOf("Company", "Org"),
                mapOf("Org" to listOf(mapOf("code" to "NEO", "region" to "EU"))),
                NodeState(listOf("Company"), mapOf("code" to "NEO", "founded" to 2000L)),
                NodeState(listOf("Company", "Org"), mapOf("code" to "NEO", "active" to true)),
            ),
        )

    schema1.nestedSchema("event") shouldBe schema2.nestedSchema("event")
  }

  // expected-shape helpers for the unified event schema

  private fun keysSchema(): Schema =
      SchemaBuilder.map(
              Schema.STRING_SCHEMA,
              SchemaBuilder.array(
                      SchemaBuilder.map(Schema.STRING_SCHEMA, PropertyType.schema).build()
                  )
                  .build(),
          )
          .optional()
          .build()

  private fun nodeRefSchema(): Schema =
      SchemaBuilder.struct()
          .field("elementId", Schema.STRING_SCHEMA)
          .field("labels", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
          .field("keys", keysSchema())
          .optional()
          .build()

  private fun compactSchemaOf(value: Any): Schema =
      when (value) {
        is Long -> Schema.OPTIONAL_INT64_SCHEMA
        is Boolean -> Schema.OPTIONAL_BOOLEAN_SCHEMA
        is LocalDate -> SimpleTypes.LOCALDATE.schema(optional = true)
        else -> Schema.OPTIONAL_STRING_SCHEMA
      }

  private fun compactValueOf(value: Any): Any =
      if (value is LocalDate) DateTimeFormatter.ISO_DATE.format(value) else value

  private fun propertiesSchema(payloadMode: PayloadMode, props: Map<String, Any>): Schema =
      if (payloadMode == PayloadMode.EXTENDED)
          SchemaBuilder.map(Schema.STRING_SCHEMA, PropertyType.schema).build()
      else
          SchemaBuilder.struct()
              .also { b ->
                props.toSortedMap().forEach { (k, v) -> b.field(k, compactSchemaOf(v)) }
              }
              .build()

  private fun unifiedEventSchema(propertiesSchema: Schema): Schema {
    val entity =
        SchemaBuilder.struct()
            .field("labels", SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())
            .field("properties", propertiesSchema)
            .optional()
            .build()
    return SchemaBuilder.struct()
        .name("org.neo4j.connectors.kafka.cdc.Event")
        .field("elementId", Schema.STRING_SCHEMA)
        .field("eventType", Schema.STRING_SCHEMA)
        .field("operation", Schema.STRING_SCHEMA)
        .field("labels", SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())
        .field("type", Schema.OPTIONAL_STRING_SCHEMA)
        .field("start", nodeRefSchema())
        .field("end", nodeRefSchema())
        .field("keys", keysSchema())
        .field(
            "state",
            SchemaBuilder.struct().field("before", entity).field("after", entity).build(),
        )
        .build()
  }

  private fun keysValue(
      keys: Map<String, List<Map<String, Any>>>
  ): Map<String, List<Map<String, Any?>>> =
      keys.mapValues { (_, rows) ->
        rows.map { row -> row.mapValues { (_, v) -> PropertyType.toConnectValue(v) } }
      }

  private fun nodeRefValue(schema: Schema, node: Node): Struct =
      Struct(schema)
          .put("elementId", node.elementId)
          .put("labels", node.labels)
          .put("keys", keysValue(node.keys))

  private fun entityState(
      schema: Schema,
      labels: List<String>?,
      payloadMode: PayloadMode,
      props: Map<String, Any>,
  ): Struct =
      Struct(schema)
          .put("labels", labels)
          .put(
              "properties",
              if (payloadMode == PayloadMode.EXTENDED)
                  props.mapValues { (_, v) -> PropertyType.toConnectValue(v) }
              else
                  Struct(schema.field("properties").schema()).also { s ->
                    props.forEach { (k, v) -> s.put(k, compactValueOf(v)) }
                  },
          )

  @Test
  fun `metadata should be converted to struct and back with extended payload`() {
    val startTime = ZonedDateTime.now().minusSeconds(1)
    val commitTime = startTime.plusSeconds(1)
    val metadata =
        Metadata(
            "service",
            "neo4j",
            "server-1",
            "neo4j",
            CaptureMode.DIFF,
            "bolt",
            "127.0.0.1:32000",
            "127.0.0.1:7687",
            startTime,
            commitTime,
            mapOf("user" to "app_user", "app" to "hr", "xyz" to mapOf("a" to 1L, "b" to 2L)),
            mapOf("new_field" to "abc", "another_field" to 1L),
        )

    val changeEventConverter = ChangeEventConverter(PayloadMode.EXTENDED)
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
                            .put("b", PropertyType.toConnectValue(2L)),
                    ),
            )
            .put("new_field", PropertyType.toConnectValue("abc"))
            .put("another_field", PropertyType.toConnectValue(1L))

    val reverted = converted.toMetadata()
    reverted shouldBe metadata
  }

  @Test
  fun `metadata should be converted to struct and back with compact payload`() {
    val startTime = ZonedDateTime.now().minusSeconds(1)
    val commitTime = startTime.plusSeconds(1)
    val metadata =
        Metadata(
            "service",
            "neo4j",
            "server-1",
            "neo4j",
            CaptureMode.DIFF,
            "bolt",
            "127.0.0.1:32000",
            "127.0.0.1:7687",
            startTime,
            commitTime,
            mapOf("user" to "app_user", "app" to "hr", "xyz" to mapOf("a" to 1L, "b" to 2L)),
            mapOf("new_field" to "abc", "another_field" to 1L),
        )

    val changeEventConverter = ChangeEventConverter(PayloadMode.COMPACT)
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
            .put("txStartTime", DateTimeFormatter.ISO_DATE_TIME.format(startTime))
            .put("txCommitTime", DateTimeFormatter.ISO_DATE_TIME.format(commitTime))
            .put(
                "txMetadata",
                Struct(schema.nestedSchema("txMetadata").schema())
                    .put("user", "app_user")
                    .put("app", "hr")
                    .put(
                        "xyz",
                        Struct(schema.nestedSchema("txMetadata.xyz")).put("a", 1L).put("b", 2L),
                    ),
            )
            .put("new_field", "abc")
            .put("another_field", 1L)

    val reverted = converted.toMetadata()
    reverted shouldBe metadata
  }

  @ParameterizedTest(name = "{0}")
  @ArgumentsSource(PayloadModeValues::class)
  fun `node events should be converted to struct and back with extended payload`(
      name: String,
      payloadMode: PayloadMode,
  ) {
    val changeEventConverter = ChangeEventConverter(payloadMode)

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
                        "dob" to LocalDate.of(1990, 1, 1),
                    ),
                ),
            ),
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
                        "dob" to LocalDate.of(1990, 1, 1),
                    ),
                ),
            ),
            NodeEvent(
                "rel-1",
                EntityOperation.CREATE,
                listOf("Person", "Employee"),
                mapOf(
                    "Person" to
                        listOf(mapOf("id" to 1L), mapOf("name" to "john", "surname" to "doe")),
                    "Employee" to listOf(mapOf("id" to 1L)),
                ),
                null,
                NodeState(
                    listOf("Person", "Employee"),
                    mapOf(
                        "id" to 1L,
                        "name" to "john",
                        "surname" to "doe",
                        "dob" to LocalDate.of(1990, 1, 1),
                    ),
                ),
            ),
            NodeEvent(
                "rel-1",
                EntityOperation.UPDATE,
                listOf("Person", "Employee"),
                mapOf(
                    "Person" to
                        listOf(mapOf("id" to 1L), mapOf("name" to "john", "surname" to "doe")),
                    "Employee" to listOf(mapOf("id" to 1L)),
                ),
                NodeState(
                    listOf("Person"),
                    mapOf(
                        "id" to 1L,
                        "name" to "john",
                        "surname" to "doe",
                        "dob" to LocalDate.of(1990, 1, 1),
                        "pob" to "London",
                    ),
                ),
                NodeState(
                    listOf("Person", "Employee"),
                    mapOf(
                        "id" to 1L,
                        "name" to "john",
                        "surname" to "doe",
                        "dob" to LocalDate.of(1990, 1, 1),
                    ),
                ),
            ),
            NodeEvent(
                "rel-1",
                EntityOperation.DELETE,
                listOf("Person", "Employee"),
                mapOf(
                    "Person" to
                        listOf(mapOf("id" to 1L), mapOf("name" to "john", "surname" to "doe")),
                    "Employee" to listOf(mapOf("id" to 1L)),
                ),
                NodeState(
                    listOf("Person", "Employee"),
                    mapOf(
                        "id" to 1L,
                        "name" to "john",
                        "surname" to "doe",
                        "dob" to LocalDate.of(1990, 1, 1),
                    ),
                ),
                null,
            ),
        )
        .forEach { event ->
          val schema = changeEventConverter.unifiedEventToConnectSchema(event)
          val converted = changeEventConverter.unifiedEventToConnectValue(event, schema)
          val reverted = converted.toNodeEvent()

          reverted shouldBe event
        }
  }

  @ParameterizedTest(name = "{0}")
  @ArgumentsSource(PayloadModeValues::class)
  fun `relationship events should be converted to struct and back`(
      name: String,
      payloadMode: PayloadMode,
  ) {
    val changeEventConverter = ChangeEventConverter(payloadMode)

    listOf(
            RelationshipEvent(
                "rel-1",
                "KNOWS",
                Node(
                    "node-1",
                    listOf("Person", "Employee"),
                    mapOf(
                        "Person" to
                            listOf(mapOf("id" to 5L), mapOf("name" to "john", "surname" to "doe"))
                    ),
                ),
                Node(
                    "node-2",
                    listOf("Person"),
                    mapOf(
                        "Person" to
                            listOf(mapOf("id" to 4L), mapOf("name" to "mary", "surname" to "doe"))
                    ),
                ),
                null,
                EntityOperation.CREATE,
                null,
                RelationshipState(mapOf("since" to LocalDate.of(2012, 10, 1), "met_at" to "London")),
            ),
            RelationshipEvent(
                "rel-1",
                "KNOWS",
                Node(
                    "node-1",
                    listOf("Person", "Employee"),
                    mapOf(
                        "Person" to
                            listOf(mapOf("id" to 5L), mapOf("name" to "john", "surname" to "doe"))
                    ),
                ),
                Node(
                    "node-2",
                    listOf("Person"),
                    mapOf(
                        "Person" to
                            listOf(mapOf("id" to 4L), mapOf("name" to "mary", "surname" to "doe"))
                    ),
                ),
                listOf(),
                EntityOperation.CREATE,
                null,
                RelationshipState(mapOf("since" to LocalDate.of(2012, 10, 1), "met_at" to "London")),
            ),
            RelationshipEvent(
                "rel-1",
                "KNOWS",
                Node(
                    "node-1",
                    listOf("Person", "Employee"),
                    mapOf(
                        "Person" to
                            listOf(mapOf("id" to 5L), mapOf("name" to "john", "surname" to "doe"))
                    ),
                ),
                Node(
                    "node-2",
                    listOf("Person"),
                    mapOf(
                        "Person" to
                            listOf(mapOf("id" to 4L), mapOf("name" to "mary", "surname" to "doe"))
                    ),
                ),
                listOf(mapOf("a" to 1L), mapOf("b" to "another")),
                EntityOperation.CREATE,
                null,
                RelationshipState(
                    mapOf(
                        "since" to LocalDate.of(2012, 10, 1),
                        "met_at" to "London",
                        "a" to 1L,
                        "b" to "another",
                    )
                ),
            ),
            RelationshipEvent(
                "rel-1",
                "KNOWS",
                Node(
                    "node-1",
                    listOf("Person", "Employee"),
                    mapOf(
                        "Person" to
                            listOf(mapOf("id" to 5L), mapOf("name" to "john", "surname" to "doe"))
                    ),
                ),
                Node(
                    "node-2",
                    listOf("Person"),
                    mapOf(
                        "Person" to
                            listOf(mapOf("id" to 4L), mapOf("name" to "mary", "surname" to "doe"))
                    ),
                ),
                listOf(mapOf("a" to 1L), mapOf("b" to "another")),
                EntityOperation.UPDATE,
                RelationshipState(mapOf("a" to 1L, "b" to "another", "c" to 5L)),
                RelationshipState(
                    mapOf(
                        "since" to LocalDate.of(2012, 10, 1),
                        "met_at" to "London",
                        "a" to 1L,
                        "b" to "another",
                    )
                ),
            ),
            RelationshipEvent(
                "rel-1",
                "KNOWS",
                Node(
                    "node-1",
                    listOf("Person", "Employee"),
                    mapOf(
                        "Person" to
                            listOf(mapOf("id" to 5L), mapOf("name" to "john", "surname" to "doe"))
                    ),
                ),
                Node(
                    "node-2",
                    listOf("Person"),
                    mapOf(
                        "Person" to
                            listOf(mapOf("id" to 4L), mapOf("name" to "mary", "surname" to "doe"))
                    ),
                ),
                listOf(mapOf("a" to 1L), mapOf("b" to "another")),
                EntityOperation.DELETE,
                RelationshipState(
                    mapOf(
                        "since" to LocalDate.of(2012, 10, 1),
                        "met_at" to "London",
                        "a" to 1L,
                        "b" to "another",
                    )
                ),
                null,
            ),
        )
        .forEach { event ->
          val schema = changeEventConverter.unifiedEventToConnectSchema(event)
          val converted = changeEventConverter.unifiedEventToConnectValue(event, schema)
          val reverted = converted.toUnifiedRelationshipEvent()

          // unified events always carry the key rows as a list, so absent keys come back empty
          reverted shouldBe
              RelationshipEvent(
                  event.elementId,
                  event.type,
                  event.start,
                  event.end,
                  event.keys ?: emptyList(),
                  event.operation,
                  event.before,
                  event.after,
              )
        }
  }

  object PayloadModeValues : ArgumentsProvider {
    override fun provideArguments(
        parameters: ParameterDeclarations,
        context: ExtensionContext,
    ): Stream<out Arguments> {
      return Stream.of(
          Arguments.of("extended", PayloadMode.EXTENDED),
          Arguments.of("compact", PayloadMode.COMPACT),
      )
    }
  }

  data class ChangeEventResult<T : Event>(
      val event: T,
      val change: ChangeEvent,
      val schema: Schema,
      val converted: Struct,
  )

  private val unifiedNodeEvent =
      NodeEvent(
          "element-0",
          EntityOperation.CREATE,
          listOf("Person"),
          mapOf("Person" to listOf(mapOf("id" to 1L))),
          null,
          NodeState(listOf("Person"), mapOf("id" to 1L, "name" to "john")),
      )

  private val unifiedRelationshipEvent =
      RelationshipEvent(
          "element-1",
          "KNOWS",
          Node("node-0", listOf("Person"), mapOf("Person" to listOf(mapOf("id" to 1L)))),
          Node("node-1", listOf("Company"), mapOf("Company" to listOf(mapOf("code" to "NEO")))),
          listOf(mapOf("since" to 2020L)),
          EntityOperation.CREATE,
          null,
          RelationshipState(mapOf("since" to 2020L, "role" to "friend")),
      )

  @ParameterizedTest
  @EnumSource(value = PayloadMode::class, names = ["EXTENDED", "COMPACT"])
  fun `unified node and relationship events are converted back correctly`(
      payloadMode: PayloadMode
  ) {
    listOf(unifiedNodeEvent, unifiedRelationshipEvent).forEach { event ->
      val (_, change, _, value) = newChangeEvent(payloadMode, event)

      value.toChangeEvent() shouldBe change
    }
  }

  @Test
  fun `legacy events are still converted back correctly when unified events are supported`() {
    listOf(unifiedNodeEvent, unifiedRelationshipEvent).forEach { event ->
      val (_, change, _, value) = newChangeEvent(PayloadMode.EXTENDED, event)

      value.toChangeEvent() shouldBe change
    }
  }

  private fun <T : Event> newChangeEvent(payloadMode: PayloadMode, event: T): ChangeEventResult<T> {
    val changeEventConverter = ChangeEventConverter(payloadMode)
    val change =
        ChangeEvent(
            ChangeIdentifier("change-id"),
            1,
            0,
            Metadata(
                "service",
                "neo4j",
                "server-1",
                "neo4j",
                CaptureMode.DIFF,
                "bolt",
                "127.0.0.1:32000",
                "127.0.0.1:7687",
                ZonedDateTime.now().minusSeconds(1),
                ZonedDateTime.now(),
                mapOf("user" to "app_user", "app" to "hr"),
                emptyMap(),
            ),
            event,
        )
    val schemaAndValue = changeEventConverter.toConnectValue(change)

    return ChangeEventResult(
        event,
        change,
        schemaAndValue.schema(),
        schemaAndValue.value() as Struct,
    )
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
