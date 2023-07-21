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

import kotlin.test.assertEquals
import kotlin.test.assertTrue
import org.junit.jupiter.api.Test
import org.testcontainers.containers.GenericContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

@Testcontainers
class ValidationUtilsTest {

  @Container val fakeWebServer: GenericContainer<*> = GenericContainer("httpd").withExposedPorts(80)

  @Test
  fun `should reach the server`() {
    assertTrue {
      ValidationUtils.checkServersUnreachable("http://localhost:${fakeWebServer.getMappedPort(80)}")
        .isEmpty()
    }
  }

  @Test
  fun `should not reach the server`() {
    val urls = "http://my.fake.host:1234,PLAINTEXT://my.fake.host1:1234,my.fake.host2:1234"
    val checkServersUnreachable = ValidationUtils.checkServersUnreachable(urls)
    assertTrue { checkServersUnreachable.isNotEmpty() }
    assertEquals(urls.split(",").toList(), checkServersUnreachable)
  }
}
