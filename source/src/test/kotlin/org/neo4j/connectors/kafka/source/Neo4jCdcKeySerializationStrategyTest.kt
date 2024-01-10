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
    val schema =
        SchemaBuilder.struct()
            .field("foo", Schema.OPTIONAL_STRING_SCHEMA)
            .field("bar", Schema.OPTIONAL_INT32_SCHEMA)
            .build()

    val value =
        SchemaAndValue(
            schema,
            with(Struct(schema)) {
              put("foo", "fighters")
              put("bar", 16)
            },
        )
    return Stream.of(
        Arguments.of(value, SKIP, null),
        Arguments.of(value, WHOLE_VALUE, schema),
        Arguments.of(value, ELEMENT_ID, Schema.OPTIONAL_STRING_SCHEMA),
    )
  }
}

class KeyValueSerializationArgument : ArgumentsProvider {
  override fun provideArguments(ignored: ExtensionContext?): Stream<out Arguments> {
    val elementIdSchema = SchemaBuilder.struct().field("elementId", Schema.OPTIONAL_STRING_SCHEMA).build()
    val schema = SchemaBuilder.struct().field("event", elementIdSchema)
    val value = Struct(schema)
        .put("event", Struct(elementIdSchema).put("elementId", "some-element-id"))
    val message =
        SchemaAndValue(
            schema,
            value,
        )
    return Stream.of(
        Arguments.of(message, SKIP, null),
        Arguments.of(message, WHOLE_VALUE, value),
        Arguments.of(message, ELEMENT_ID, "some-element-id"),
    )
  }
}
