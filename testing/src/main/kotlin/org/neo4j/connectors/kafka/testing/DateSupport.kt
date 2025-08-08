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
package org.neo4j.connectors.kafka.testing

import java.util.Calendar
import java.util.Date
import java.util.TimeZone

object DateSupport {
  val UTC = TimeZone.getTimeZone("UTC")

  fun date(year: Int, month: Int, day: Int): Date {
    val calendar = Calendar.getInstance(UTC)
    calendar.set(year, month - 1, day, 0, 0, 0)
    calendar.set(Calendar.MILLISECOND, 0)
    return calendar.time
  }

  fun time(hour: Int, minute: Int, second: Int, millis: Int): Date {
    val calendar = Calendar.getInstance(UTC)
    calendar.set(1970, 0, 1, hour, minute, second)
    calendar.set(Calendar.MILLISECOND, millis)
    return calendar.time
  }

  fun timestamp(
      year: Int,
      month: Int,
      day: Int,
      hour: Int,
      minute: Int,
      second: Int,
      millis: Int,
  ): Date {
    val calendar = Calendar.getInstance(UTC)
    calendar.set(year, month - 1, day, hour, minute, second)
    calendar.set(Calendar.MILLISECOND, millis)
    return calendar.time
  }
}
