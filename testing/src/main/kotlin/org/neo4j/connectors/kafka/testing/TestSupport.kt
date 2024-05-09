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

import io.kotest.assertions.nondeterministic.eventually
import io.kotest.matchers.shouldBe
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test

object TestSupport {

  /**
   * kotest assertions package provide non-deterministic assertions like `eventually`,
   * `continually`, `until` or `retry` which expect the containing function to be `suspend`able.
   * However, JUnit does not allow us to mark the test functions `suspend`able and the workaround is
   * to wrap the test code inside a `runTest` block this function provides.
   *
   * @param context coroutine context to be used during block execution
   * @param block code block to be wrapped inside a `runBlocking` coroutine
   * @sample TestSupport.sample
   */
  fun runTest(
      context: CoroutineContext = EmptyCoroutineContext,
      block: suspend CoroutineScope.() -> Any
  ): Unit {
    runBlocking(context, block)
    Unit
  }

  private fun sample() {
    @Test
    fun myTest() = runTest {
      val iterator = sequenceOf('a'..'z').iterator()

      eventually(30.seconds) { iterator.next() shouldBe 'z' }
    }
  }
}
