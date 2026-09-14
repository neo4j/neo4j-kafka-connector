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
package org.neo4j.connectors.kafka.sink.strategy

import io.kotest.matchers.shouldBe
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetTime
import java.time.ZonedDateTime
import java.util.UUID
import java.util.stream.Stream
import org.junit.jupiter.api.Named
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.ArgumentsProvider
import org.junit.jupiter.params.provider.ArgumentsSource
import org.junit.jupiter.params.support.ParameterDeclarations
import org.neo4j.connectors.kafka.sink.ChangeQuery
import org.neo4j.connectors.kafka.sink.SinkMessage
import org.neo4j.driver.Value
import org.neo4j.driver.Values
import org.neo4j.driver.types.TypeSystem

class SinkBatchStrategyTest {

  private val neo4jTypes = TypeSystem.getDefault()

  private val batchStrategyStub =
      object : SinkBatchStrategy {
        override fun handle(
            messages: Iterable<SinkMessage>,
            eventTransformer: (SinkMessage) -> SinkAction,
        ): Iterable<Iterable<ChangeQuery>> = error("Not implemented in test stub")
      }

  @Test
  fun `should stringify a UUID`() {
    val uuid = UUID.randomUUID()
    val input = Values.value(uuid)

    val result = batchStrategyStub.stringifyUuids(neo4jTypes, input)

    result.type() shouldBe neo4jTypes.STRING()
    result.asString() shouldBe uuid.toString()
  }

  @Test
  fun `should stringify every UUID in a list and preserve order`() {
    val uuid1 = UUID.randomUUID()
    val uuid2 = UUID.randomUUID()
    val input = Values.value(listOf(uuid1, uuid2))

    val result = batchStrategyStub.stringifyUuids(neo4jTypes, input)

    result.type() shouldBe neo4jTypes.LIST()
    result.asList { it.type() } shouldBe listOf(neo4jTypes.STRING(), neo4jTypes.STRING())
    result.asList { it.asString() } shouldBe listOf(uuid1.toString(), uuid2.toString())
  }

  @Test
  fun `should stringify UUIDs in nested batch parameters and preserve other contents`() {
    val uuid1 = UUID.randomUUID()
    val uuid2 = UUID.randomUUID()

    val params =
        Values.value(
            mapOf(
                "id" to uuid1,
                "nested" to
                    listOf(
                        mapOf(uuid1.toString() to uuid2), // list of map with uuid value
                        listOf(uuid1, null, 42L), // mixed type
                    ),
                "uuid-looking" to uuid2.toString(),
            )
        )

    val input =
        Values.value(
            mapOf(
                "events" to listOf(mapOf("offset" to 12L, "params" to params)),
                "topic" to "uuid-events",
            )
        )

    val expected =
        Values.value(
            mapOf(
                "events" to
                    listOf(
                        mapOf(
                            "offset" to 12L,
                            "params" to
                                mapOf(
                                    "id" to uuid1.toString(),
                                    "nested" to
                                        listOf(
                                            mapOf(uuid1.toString() to uuid2.toString()),
                                            listOf(uuid1.toString(), null, 42),
                                        ),
                                    "uuid-looking" to uuid2.toString(),
                                ),
                        )
                    ),
                "topic" to "uuid-events",
            )
        )

    val result = batchStrategyStub.stringifyUuids(neo4jTypes, input)

    result shouldBe expected

    // nested value -- deep check
    result.get("events").get(0).get("params").get("id").type() shouldBe neo4jTypes.STRING()

    // nested mixed type only touch uuid type -- deep check
    val nested = result.get("events").get(0).get("params").get("nested")
    nested.type() shouldBe neo4jTypes.LIST()
    nested.get(1).get(0).type() shouldBe neo4jTypes.STRING()
    nested.get(1).get(1).type() shouldBe neo4jTypes.NULL()
    nested.get(1).get(2).type() shouldBe neo4jTypes.INTEGER()
  }

  @ParameterizedTest(name = "{0}")
  @ArgumentsSource(NonUuidValues::class)
  fun `should preserve the type and contents of non UUID values`(input: Value) {
    val result = batchStrategyStub.stringifyUuids(neo4jTypes, input)

    result.type() shouldBe input.type()
    result shouldBe input
  }

  object NonUuidValues : ArgumentsProvider {
    override fun provideArguments(
        parameters: ParameterDeclarations,
        context: ExtensionContext,
    ): Stream<out Arguments> =
        Stream.of(
            Arguments.of(Named.of("null", Values.NULL)),
            Arguments.of(Named.of("boolean", Values.value(true))),
            Arguments.of(Named.of("integer", Values.value(42L))),
            Arguments.of(Named.of("float", Values.value(1.25))),
            Arguments.of(Named.of("string", Values.value("hello"))),
            Arguments.of(Named.of("uuid but string", Values.value(UUID.randomUUID().toString()))),
            Arguments.of(Named.of("bytes", Values.value(byteArrayOf(0, -1, 127)))),
            Arguments.of(Named.of("date", Values.value(LocalDate.of(2026, 9, 14)))),
            Arguments.of(Named.of("local time", Values.value(LocalTime.of(12, 34, 56, 123)))),
            Arguments.of(Named.of("time", Values.value(OffsetTime.parse("12:34:56.123+02:00")))),
            Arguments.of(
                Named.of(
                    "local date time",
                    Values.value(LocalDateTime.of(2026, 9, 14, 12, 34, 56, 123)),
                )
            ),
            Arguments.of(
                Named.of(
                    "date time",
                    Values.value(ZonedDateTime.parse("2026-09-14T12:34:56+02:00[Europe/Stockholm]")),
                )
            ),
            Arguments.of(Named.of("duration", Values.isoDuration(2, 3, 4, 5))),
            Arguments.of(Named.of("2D point", Values.point(4326, 18.0, 59.0))),
            Arguments.of(Named.of("3D point", Values.point(4979, 18.0, 59.0, 10.0))),
            Arguments.of(Named.of("empty list", Values.value(emptyList<Any?>()))),
            Arguments.of(Named.of("empty map", Values.value(emptyMap<String, Any?>()))),
        )
  }
}
