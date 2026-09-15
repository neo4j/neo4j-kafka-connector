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
import java.math.BigDecimal
import java.math.BigInteger
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.OffsetTime
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util.UUID
import java.util.stream.Stream
import org.apache.kafka.connect.data.Date
import org.apache.kafka.connect.data.Decimal
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Time
import org.apache.kafka.connect.data.Timestamp
import org.junit.jupiter.api.Named
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.ArgumentsProvider
import org.junit.jupiter.params.provider.ArgumentsSource
import org.junit.jupiter.params.support.ParameterDeclarations
import org.neo4j.connectors.kafka.data.DynamicTypes.MILLIS_PER_DAY

class DynamicTypesTest {
  @Test
  fun `kafka connect date values should be returned as local date`() {
    DynamicTypes.fromConnectValue(
        Date.SCHEMA,
        java.util.Date(LocalDate.of(2000, 1, 1).toEpochDay() * MILLIS_PER_DAY),
    ) shouldBe LocalDate.of(2000, 1, 1)
  }

  @Test
  fun `kafka connect time values should be returned as local time`() {
    DynamicTypes.fromConnectValue(
        Time.SCHEMA,
        java.util.Date(
            OffsetDateTime.of(
                    LocalDate.EPOCH,
                    LocalTime.of(23, 59, 59, 999_000_000),
                    ZoneOffset.UTC,
                )
                .toInstant()
                .toEpochMilli()
        ),
    ) shouldBe LocalTime.of(23, 59, 59, 999_000_000)
  }

  @Test
  fun `kafka connect timestamp values should be returned as local time`() {
    DynamicTypes.fromConnectValue(
        Timestamp.SCHEMA,
        java.util.Date(
            OffsetDateTime.of(
                    LocalDate.of(2000, 1, 1),
                    LocalTime.of(23, 59, 59, 999_000_000),
                    ZoneOffset.UTC,
                )
                .toInstant()
                .toEpochMilli()
        ),
    ) shouldBe LocalDateTime.of(2000, 1, 1, 23, 59, 59, 999_000_000)
  }

  @Test
  fun `kafka connect decimal values should be returned as string`() {
    DynamicTypes.fromConnectValue(
        Decimal.schema(4),
        BigDecimal(BigInteger("12345678901234567890"), 4),
    ) shouldBe "1234567890123456.7890"
    DynamicTypes.fromConnectValue(
        Decimal.schema(6),
        BigDecimal(BigInteger("12345678901234567890"), 6),
    ) shouldBe "12345678901234.567890"
    DynamicTypes.fromConnectValue(
        Decimal.schema(6),
        BigDecimal(BigInteger("123456000000"), 6),
    ) shouldBe "123456.000000"
  }

  @Test
  fun `should stringify UUID when not supported or support not provided`() {
    val uuidValue = UUID.randomUUID()
    val uuidString = uuidValue.toString()

    DynamicTypes.fromConnectValue(SimpleTypes.UUID.schema, uuidString) shouldBe uuidString

    DynamicTypes.fromConnectValue(
        SimpleTypes.UUID.schema,
        uuidString,
        supportsUuidType = false,
    ) shouldBe uuidString
  }

  @Test
  fun `should retain UUID when supported`() {
    val uuidValue = UUID.randomUUID()
    val uuidString = uuidValue.toString()

    val result =
        DynamicTypes.fromConnectValue(SimpleTypes.UUID.schema, uuidString, supportsUuidType = true)

    result shouldBe uuidValue
  }

  @ParameterizedTest(name = "{0}")
  @ArgumentsSource(StringBackedValues::class)
  fun `should convert other string-backed values no matter config`(schema: Schema, value: Any) {
    DynamicTypes.fromConnectValue(schema, value.toString()) shouldBe value
    DynamicTypes.fromConnectValue(schema, value.toString(), supportsUuidType = true) shouldBe value
  }

  object StringBackedValues : ArgumentsProvider {
    override fun provideArguments(
        parameters: ParameterDeclarations,
        context: ExtensionContext,
    ): Stream<out Arguments> {
      return Stream.of(
          Arguments.of(
              Named.of("local date", SimpleTypes.LOCALDATE.schema),
              LocalDate.of(2026, 9, 14),
          ),
          Arguments.of(
              Named.of("local time", SimpleTypes.LOCALTIME.schema),
              LocalTime.of(12, 34, 56, 123),
          ),
          Arguments.of(
              Named.of("local date time", SimpleTypes.LOCALDATETIME.schema),
              LocalDateTime.of(2026, 9, 14, 12, 34, 56, 123),
          ),
          Arguments.of(
              Named.of("zoned date time", SimpleTypes.ZONEDDATETIME.schema),
              ZonedDateTime.of(2026, 9, 14, 12, 34, 56, 0, ZoneId.of("Europe/Copenhagen")),
          ),
          Arguments.of(
              Named.of("offset time", SimpleTypes.OFFSETTIME.schema),
              OffsetTime.of(12, 34, 56, 123_000_000, ZoneOffset.ofHoursMinutes(1, 30)),
          ),
      )
    }
  }
}
