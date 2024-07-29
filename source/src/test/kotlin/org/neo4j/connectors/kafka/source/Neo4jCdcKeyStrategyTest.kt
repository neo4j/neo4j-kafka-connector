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
package org.neo4j.connectors.kafka.source

import java.time.ZonedDateTime
import java.util.stream.Stream
import kotlin.test.assertEquals
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaAndValue
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.ArgumentsProvider
import org.junit.jupiter.params.provider.ArgumentsSource
import org.neo4j.cdc.client.model.CaptureMode
import org.neo4j.cdc.client.model.ChangeEvent
import org.neo4j.cdc.client.model.ChangeIdentifier
import org.neo4j.cdc.client.model.EntityOperation
import org.neo4j.cdc.client.model.Metadata
import org.neo4j.cdc.client.model.Node
import org.neo4j.cdc.client.model.NodeEvent
import org.neo4j.cdc.client.model.NodeState
import org.neo4j.cdc.client.model.RelationshipEvent
import org.neo4j.cdc.client.model.RelationshipState
import org.neo4j.connectors.kafka.data.ChangeEventConverter
import org.neo4j.connectors.kafka.data.PropertyType
import org.neo4j.connectors.kafka.source.Neo4jCdcKeyStrategy.ELEMENT_ID
import org.neo4j.connectors.kafka.source.Neo4jCdcKeyStrategy.ENTITY_KEYS
import org.neo4j.connectors.kafka.source.Neo4jCdcKeyStrategy.SKIP
import org.neo4j.connectors.kafka.source.Neo4jCdcKeyStrategy.WHOLE_VALUE

class Neo4jCdcKeyStrategyTest {

  @ParameterizedTest
  @ArgumentsSource(KeySchemaSerializationArgument::class)
  fun `serializes key schema`(
      message: SchemaAndValue,
      strategy: Neo4jCdcKeyStrategy,
      expectedKeySchema: Schema?
  ) {
    val actualKeySchema = strategy.schema(message)

    assertEquals(expectedKeySchema, actualKeySchema)
  }

  @ParameterizedTest
  @ArgumentsSource(KeyValueSerializationArgument::class)
  fun `serializes key value`(
      message: SchemaAndValue,
      strategy: Neo4jCdcKeyStrategy,
      expectedKeyValue: Any?
  ) {
    val actualKeyValue = strategy.value(message)

    assertEquals(expectedKeyValue, actualKeyValue)
  }
}

class KeySchemaSerializationArgument : ArgumentsProvider {
  override fun provideArguments(ignored: ExtensionContext?): Stream<out Arguments> {
    return Stream.of(
        Arguments.of(TestData.nodeChange, SKIP, null),
        Arguments.of(TestData.nodeChange, WHOLE_VALUE, TestData.nodeChange.schema()),
        Arguments.of(TestData.nodeChange, ELEMENT_ID, TestData.elementIdSchema),
        Arguments.of(TestData.nodeChange, ENTITY_KEYS, TestData.nodeKeysSchema),
        Arguments.of(TestData.relChange, SKIP, null),
        Arguments.of(TestData.relChange, WHOLE_VALUE, TestData.relChange.schema()),
        Arguments.of(TestData.relChange, ELEMENT_ID, TestData.elementIdSchema),
        Arguments.of(TestData.relChange, ENTITY_KEYS, TestData.relKeysSchema),
    )
  }
}

class KeyValueSerializationArgument : ArgumentsProvider {
  override fun provideArguments(ignored: ExtensionContext?): Stream<out Arguments> {
    return Stream.of(
        Arguments.of(TestData.nodeChange, SKIP, null),
        Arguments.of(TestData.nodeChange, WHOLE_VALUE, TestData.nodeChange.value()),
        Arguments.of(TestData.nodeChange, ELEMENT_ID, TestData.NODE_ELEMENT_ID),
        Arguments.of(TestData.nodeChange, ENTITY_KEYS, TestData.nodeKeys),
        Arguments.of(TestData.relChange, SKIP, null),
        Arguments.of(TestData.relChange, WHOLE_VALUE, TestData.relChange.value()),
        Arguments.of(TestData.relChange, ELEMENT_ID, TestData.REL_ELEMENT_ID),
        Arguments.of(TestData.relChange, ENTITY_KEYS, TestData.relKeys),
    )
  }
}

object TestData {

  private const val LABEL = "Label1"

  const val NODE_ELEMENT_ID: String = "node-element-id"

  const val REL_ELEMENT_ID: String = "rel-element-id"

  val elementIdSchema: Schema = Schema.STRING_SCHEMA

  private val propertySchema: Schema =
      SchemaBuilder.struct()
          .field("foo", PropertyType.schema)
          .field("bar", PropertyType.schema)
          .optional()
          .build()

  val nodeKeysSchema: Schema =
      SchemaBuilder.struct()
          .field(
              "keys",
              SchemaBuilder.struct()
                  .field(LABEL, SchemaBuilder.array(propertySchema).optional().build())
                  .optional()
                  .build())
          .optional()
          .build()

  val nodeKeys: Struct =
      Struct(nodeKeysSchema)
          .put(
              "keys",
              Struct(nodeKeysSchema.field("keys").schema())
                  .put(
                      LABEL,
                      listOf(
                          Struct(propertySchema)
                              .put("foo", PropertyType.toConnectValue("fighters"))
                              .put("bar", PropertyType.toConnectValue(42L)),
                      ),
                  ))

  val relKeysSchema: Schema =
      SchemaBuilder.struct()
          .field("keys", SchemaBuilder.array(propertySchema).optional().build())
          .optional()
          .build()

  val relKeys: Struct =
      Struct(relKeysSchema)
          .put(
              "keys",
              listOf(
                  Struct(propertySchema)
                      .put("foo", PropertyType.toConnectValue("fighters"))
                      .put("bar", PropertyType.toConnectValue(42L)),
              ))

  val nodeChange =
      ChangeEventConverter()
          .toConnectValue(
              ChangeEvent(
                  ChangeIdentifier("a-node-change-id"),
                  aTransactionId(),
                  aSequenceNumber(),
                  someMetadata(),
                  NodeEvent(
                      NODE_ELEMENT_ID,
                      EntityOperation.CREATE,
                      listOf(LABEL),
                      mapOf(LABEL to listOf(mapOf("foo" to "fighters", "bar" to 42L))),
                      NodeState(listOf(LABEL), mapOf()),
                      NodeState(listOf(LABEL), mapOf("foo" to "fighters", "bar" to 42L)))))

  val relChange =
      ChangeEventConverter()
          .toConnectValue(
              ChangeEvent(
                  ChangeIdentifier("a-rel-change-id"),
                  aTransactionId(),
                  aSequenceNumber(),
                  someMetadata(),
                  RelationshipEvent(
                      REL_ELEMENT_ID,
                      aRelationshipType(),
                      aStartNode(),
                      anEndNode(),
                      listOf(mapOf("foo" to "fighters", "bar" to 42L)),
                      EntityOperation.CREATE,
                      RelationshipState(mapOf()),
                      RelationshipState(mapOf("foo" to "fighters", "bar" to 42L)))))

  private fun aTransactionId(): Long {
    return 42
  }

  private fun aSequenceNumber(): Int {
    return 4242
  }

  private fun someMetadata(): Metadata {
    val txStartTime = ZonedDateTime.now()
    val txCommitTime = txStartTime.plusSeconds(5)
    return Metadata(
        "authenticated-user",
        "executing-user",
        "server-id",
        CaptureMode.FULL,
        "connection-type",
        "connection-client",
        "connection-server",
        txStartTime,
        txCommitTime,
        mapOf("tx" to "metadata"),
        mapOf("additional" to "entries"))
  }

  private fun aRelationshipType() = "A_RELATION_TO"

  private fun aStartNode() = Node("start-element-id", listOf("Start"), mapOf())

  private fun anEndNode() = Node("end-element-id", listOf("End"), mapOf())
}
