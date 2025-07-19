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
package org.neo4j.connectors.kafka.configuration.helpers

import kotlin.test.assertEquals
import kotlin.time.Duration
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.microseconds
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.nanoseconds
import kotlin.time.Duration.Companion.seconds
import org.junit.jupiter.api.Test

class DurationTest {

  @Test
  fun `should parse simple duration strings correctly`() {
    // basic syntax
    assertEquals(10.milliseconds, Duration.parseSimpleString("10ms"))
    assertEquals(10.seconds, Duration.parseSimpleString("10s"))
    assertEquals(10.minutes, Duration.parseSimpleString("10m"))
    assertEquals(10.hours, Duration.parseSimpleString("10h"))
    assertEquals(10.days, Duration.parseSimpleString("10d"))

    // compound
    assertEquals(10.seconds + 10.milliseconds, Duration.parseSimpleString("10s10ms"))
    assertEquals(65.seconds + 550.milliseconds, Duration.parseSimpleString("65s550ms"))
    assertEquals(
        1.days + 23.hours + 59.minutes + 59.seconds + 10.milliseconds,
        Duration.parseSimpleString("1d23h59m59s10ms"),
    )
    assertEquals(2.days, Duration.parseSimpleString("1d23h59m60s"))
  }

  @Test
  fun `should produce simple duration strings correctly`() {
    assertEquals("0s", 1.nanoseconds.toSimpleString())
    assertEquals("0s", 1.microseconds.toSimpleString())

    assertEquals("10ms", 10.milliseconds.toSimpleString())
    assertEquals("10s", 10.seconds.toSimpleString())
    assertEquals("10m", 10.minutes.toSimpleString())
    assertEquals("10h", 10.hours.toSimpleString())
    assertEquals("10d", 10.days.toSimpleString())

    assertEquals("1s", 1000.milliseconds.toSimpleString())
    assertEquals("1s10ms", 1010.milliseconds.toSimpleString())

    assertEquals("1d23h59m59s999ms", (2.days - 1.milliseconds).toSimpleString())
    assertEquals("1d23h59m59s999ms", (2.days - 1.microseconds).toSimpleString())
    assertEquals("1d23h59m59s999ms", (2.days - 1.nanoseconds).toSimpleString())
  }
}
