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
import java.lang.RuntimeException
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue
import org.junit.jupiter.api.Test

class StreamsUtilsTest {

  private val foo = "foo"

  @Test
  fun shouldReturnValue() {
    val data = StreamsUtils.ignoreExceptions({ foo }, RuntimeException::class.java)
    assertTrue { data != null && data == foo }
  }

  @Test
  fun shouldIgnoreTheException() {
    val data =
        StreamsUtils.ignoreExceptions<Nothing>(
            { throw RuntimeException() }, RuntimeException::class.java)
    assertNull(data)
  }

  @Test
  fun shouldNotIgnoreTheException() {
    assertFailsWith(IOException::class) {
      StreamsUtils.ignoreExceptions<Nothing>({ throw IOException() }, RuntimeException::class.java)
    }
  }
}
