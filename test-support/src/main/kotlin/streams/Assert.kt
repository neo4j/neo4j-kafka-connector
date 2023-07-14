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
package streams

import java.util.concurrent.TimeUnit
import java.util.function.Supplier
import org.hamcrest.Matcher
import org.hamcrest.StringDescription

object Assert {
  fun <T> assertEventually(
    actual: Supplier<T>,
    matcher: Matcher<in T>,
    timeout: Long,
    timeUnit: TimeUnit
  ) {
    assertEventually({ _: T -> "" }, actual, matcher, timeout, timeUnit)
  }

  fun <T> assertEventually(
    reason: String,
    actual: Supplier<T>,
    matcher: Matcher<in T>,
    timeout: Long,
    timeUnit: TimeUnit
  ) {
    assertEventually({ _: T -> reason }, actual, matcher, timeout, timeUnit)
  }

  fun <T> assertEventually(
    reason: java.util.function.Function<T, String>,
    actual: Supplier<T>,
    matcher: Matcher<in T>,
    timeout: Long,
    timeUnit: TimeUnit
  ) {
    val endTimeMillis = System.currentTimeMillis() + timeUnit.toMillis(timeout)
    while (true) {
      val sampleTime = System.currentTimeMillis()
      val last: T = actual.get()
      val matched: Boolean = matcher.matches(last)
      if (matched || sampleTime > endTimeMillis) {
        if (!matched) {
          val description = StringDescription()
          description
            .appendText(reason.apply(last))
            .appendText("\nExpected: ")
            .appendDescriptionOf(matcher)
            .appendText("\n     but: ")
          matcher.describeMismatch(last, description)
          throw AssertionError(
            "Timeout hit (" +
              timeout +
              " " +
              timeUnit.toString().toLowerCase() +
              ") while waiting for condition to match: " +
              description.toString())
        } else {
          return
        }
      }
      Thread.sleep(100L)
    }
  }
}
