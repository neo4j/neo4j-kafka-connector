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
package streams.utils

import java.io.IOException
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test

class CoroutineUtilsTest {

  @Test
  fun `should success after retry for known exception`() = runBlocking {
    var count = 0
    var executed = false

    retryForException(
        exceptions = arrayOf(RuntimeException::class.java), retries = 4, delayTime = 100) {
          if (count < 2) {
            ++count
            throw RuntimeException()
          }
          executed = true
        }

    assertEquals(2, count)
    assertTrue { executed }
  }

  @Test
  fun `should fail after retry for known exception`() {
    var retries = 3

    assertFailsWith(RuntimeException::class) {
      runBlocking {
        retryForException(
            exceptions = arrayOf(RuntimeException::class.java), retries = 3, delayTime = 100) {
              if (retries >= 0) {
                --retries
                throw RuntimeException()
              }
            }
      }
    }
  }

  @Test
  fun `should fail fast unknown exception`() {
    var iteration = 0

    assertFailsWith(IOException::class) {
      runBlocking {
        retryForException(
            exceptions = arrayOf(RuntimeException::class.java), retries = 3, delayTime = 100) {
              if (iteration >= 0) {
                ++iteration
                throw IOException()
              }
            }
      }
    }
    assertEquals(1, iteration)
  }
}
