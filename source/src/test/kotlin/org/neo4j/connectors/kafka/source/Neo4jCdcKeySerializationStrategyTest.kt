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
package org.neo4j.connectors.kafka.source

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
import org.neo4j.connectors.kafka.source.Neo4jCdcKeySerializationStrategy.ELEMENT_ID
import org.neo4j.connectors.kafka.source.Neo4jCdcKeySerializationStrategy.ENTITY_KEYS
import org.neo4j.connectors.kafka.source.Neo4jCdcKeySerializationStrategy.SKIP
import org.neo4j.connectors.kafka.source.Neo4jCdcKeySerializationStrategy.WHOLE_VALUE

class Neo4jCdcKeySerializationStrategyTest {

  @ParameterizedTest
  @ArgumentsSource(KeySchemaSerializationArgument::class)
  fun `serializes key schema`(
      message: SchemaAndValue,
      strategy: Neo4jCdcKeySerializationStrategy,
      expectedKeySchema: Schema?
  ) {
    val actualKeySchema = strategy.schema(message)

    assertEquals(expectedKeySchema, actualKeySchema)
  }

  @ParameterizedTest
  @ArgumentsSource(KeyValueSerializationArgument::class)
  fun `serializes key value`(
      message: SchemaAndValue,
      strategy: Neo4jCdcKeySerializationStrategy,
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
        Arguments.of(TestData.nodeChange, WHOLE_VALUE, TestData.nodeSchema),
        Arguments.of(TestData.nodeChange, ELEMENT_ID, TestData.elementIdSchema),
        Arguments.of(TestData.nodeChange, ENTITY_KEYS, TestData.nodeKeysSchema),
        Arguments.of(TestData.relChange, SKIP, null),
        Arguments.of(TestData.relChange, WHOLE_VALUE, TestData.relSchema),
        Arguments.of(TestData.relChange, ELEMENT_ID, TestData.elementIdSchema),
        Arguments.of(TestData.relChange, ENTITY_KEYS, TestData.relKeysSchema),
    )
  }
}

class KeyValueSerializationArgument : ArgumentsProvider {
  override fun provideArguments(ignored: ExtensionContext?): Stream<out Arguments> {
    return Stream.of(
        Arguments.of(TestData.nodeChange, SKIP, null),
        Arguments.of(TestData.nodeChange, WHOLE_VALUE, TestData.nodeValue),
        Arguments.of(TestData.nodeChange, ELEMENT_ID, TestData.NODE_ELEMENT_ID),
        Arguments.of(TestData.nodeChange, ENTITY_KEYS, TestData.nodeKeys),
        Arguments.of(TestData.relChange, SKIP, null),
        Arguments.of(TestData.relChange, WHOLE_VALUE, TestData.relValue),
        Arguments.of(TestData.relChange, ELEMENT_ID, TestData.REL_ELEMENT_ID),
        Arguments.of(TestData.relChange, ENTITY_KEYS, TestData.relKeys),
    )
  }
}

object TestData {

  const val NODE_ELEMENT_ID: String = "node-element-id"

  const val REL_ELEMENT_ID: String = "rel-element-id"

  val elementIdSchema: Schema = Schema.OPTIONAL_STRING_SCHEMA

  private val fooBarKeySchema: Schema =
      SchemaBuilder.struct()
          .field("foo", Schema.OPTIONAL_STRING_SCHEMA)
          .field("bar", Schema.OPTIONAL_INT64_SCHEMA)
          .optional()
          .build()

  val nodeKeysSchema: Schema =
      SchemaBuilder.struct()
          .field("Label1", SchemaBuilder.array(fooBarKeySchema))
          .optional()
          .build()

  val nodeKeys: Struct =
      Struct(nodeKeysSchema)
          .put(
              "Label1",
              listOf(
                  Struct(fooBarKeySchema).put("foo", "fighters").put("bar", 42L),
              ),
          )

  private val propertiesSchema: Schema =
      SchemaBuilder.struct()
          .field("foo", Schema.OPTIONAL_STRING_SCHEMA)
          .field("bar", Schema.OPTIONAL_INT64_SCHEMA)
          .build()

  private val properties: Struct = Struct(propertiesSchema).put("foo", "fighters").put("bar", 42L)

  private val stateSchema: Schema =
      SchemaBuilder.struct().field("properties", propertiesSchema).build()

  private val nodeEventSchema: Schema =
      SchemaBuilder.struct()
          .field("keys", nodeKeysSchema)
          .field("eventType", Schema.STRING_SCHEMA)
          .field("elementId", elementIdSchema)
          .field("state", stateSchema)
          .build()

  val nodeSchema: Schema =
      SchemaBuilder.struct()
          .field("event", nodeEventSchema)
          .field("properties", propertiesSchema)
          .build()

  val nodeValue: Struct =
      with(Struct(nodeSchema)) {
        put(
            "event",
            Struct(nodeEventSchema)
                .put("elementId", NODE_ELEMENT_ID)
                .put("eventType", "n")
                .put("keys", nodeKeys)
                .put("state", Struct(stateSchema).put("properties", properties)))
      }

  val nodeChange = SchemaAndValue(nodeSchema, nodeValue)

  val relKeysSchema: Schema = SchemaBuilder.array(fooBarKeySchema).build()

  val relKeys: Any =
      listOf(
          Struct(fooBarKeySchema).put("foo", "fighters").put("bar", 42L),
      )

  private val relEventSchema: Schema =
      SchemaBuilder.struct()
          .field("keys", relKeysSchema)
          .field("eventType", Schema.STRING_SCHEMA)
          .field("elementId", elementIdSchema)
          .field("state", stateSchema)
          .build()

  val relSchema: Schema =
      SchemaBuilder.struct()
          .field("event", relEventSchema)
          .field("properties", propertiesSchema)
          .build()

  val relValue: Struct =
      with(Struct(relSchema)) {
        put(
            "event",
            Struct(relEventSchema)
                .put("elementId", REL_ELEMENT_ID)
                .put("eventType", "r")
                .put("keys", relKeys)
                .put("state", Struct(stateSchema).put("properties", properties)))
      }
  val relChange = SchemaAndValue(relSchema, relValue)
}
