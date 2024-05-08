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

import java.util.Locale
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern
import kotlin.time.Duration
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

private const val VALID_UNITS_DESCRIPTION = "Valid units are: `ms`, `s`, `m`, `h` and `d`"

val SIMPLE_DURATION_PATTERN: Pattern = Pattern.compile("(\\d+(ms|s|m|h|d))+")

/** Parses a simple duration string up to millisecond precision. */
fun Duration.Companion.parseSimpleString(value: String): Duration {
  fun toNanos(unit: String, amount: Int): Long {
    return when (unit) {
      "ms" -> TimeUnit.MILLISECONDS.toNanos(amount.toLong())
      "s" -> TimeUnit.SECONDS.toNanos(amount.toLong())
      "m" -> TimeUnit.MINUTES.toNanos(amount.toLong())
      "h" -> TimeUnit.HOURS.toNanos(amount.toLong())
      "d" -> TimeUnit.DAYS.toNanos(amount.toLong())
      else -> throw IllegalArgumentException("Unrecognized unit `$unit`. $VALID_UNITS_DESCRIPTION")
    }
  }

  val unitIndex = value.indexOfFirst { !it.isDigit() }
  if (unitIndex == -1) {
    throw IllegalArgumentException(
        "Invalid duration string '$value'. No unit provided. $VALID_UNITS_DESCRIPTION")
  }
  require(unitIndex != 0) { "Missing numeric value: $value" }

  var input = value
  var timeInNanos = 0L
  while (input.isNotEmpty()) {
    val amount = input.takeWhile { it.isDigit() }
    val unit = input.substring(amount.length).takeWhile { !it.isDigit() }.lowercase(Locale.ROOT)
    timeInNanos += toNanos(unit, amount.toInt())

    input = input.substring(amount.length + unit.length)
  }

  return timeInNanos.nanoseconds
}

/** Converts a Duration value to a simple duration string up to millisecond precision */
fun Duration.toSimpleString(): String {
  var duration = this
  if (duration.isNegative()) {
    throw IllegalArgumentException("negative durations are not supported: $duration")
  }

  val timeString = StringBuilder()

  val days = duration.inWholeDays
  if (days > 0) {
    timeString.append(days).append("d")
    duration -= days.days
  }

  val hours = duration.inWholeHours
  if (hours > 0) {
    timeString.append(hours).append('h')
    duration -= hours.hours
  }

  val minutes = duration.inWholeMinutes
  if (minutes > 0) {
    timeString.append(minutes).append('m')
    duration -= minutes.minutes
  }

  val seconds = duration.inWholeSeconds
  if (seconds > 0) {
    timeString.append(seconds).append('s')
    duration -= seconds.seconds
  }

  val milliseconds = duration.inWholeMilliseconds
  if (milliseconds > 0) {
    timeString.append(milliseconds).append("ms")
    duration -= milliseconds.milliseconds
  }

  if (timeString.isEmpty()) {
    timeString.append("0s")
  }

  return timeString.toString()
}
