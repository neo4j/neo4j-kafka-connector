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
package org.neo4j.connectors.kafka.source

import kotlin.test.assertContains
import kotlin.test.assertTrue
import org.junit.jupiter.api.Test
import org.neo4j.connectors.kafka.configuration.Neo4jConfiguration

class Neo4jConnectorTest {

  @Test
  fun `should validate basic auth details`() {
    val connector = Neo4jConnector()
    val config = connector.validate(mutableMapOf(Neo4jConfiguration.AUTHENTICATION_TYPE to "BASIC"))

    assertContains(
        config
            .configValues()
            .first { it.name() == Neo4jConfiguration.AUTHENTICATION_BASIC_USERNAME }
            .errorMessages(),
        "Must be non-empty.")
    assertContains(
        config
            .configValues()
            .first { it.name() == Neo4jConfiguration.AUTHENTICATION_BASIC_PASSWORD }
            .errorMessages(),
        "Must be non-empty.")
    assertTrue {
      config
          .configValues()
          .first { it.name() == Neo4jConfiguration.AUTHENTICATION_BASIC_REALM }
          .errorMessages()
          .isEmpty()
    }
  }

  @Test
  fun `should validate kerberos auth details`() {
    val connector = Neo4jConnector()
    val config =
        connector.validate(mutableMapOf(Neo4jConfiguration.AUTHENTICATION_TYPE to "KERBEROS"))

    assertContains(
        config
            .configValues()
            .first { it.name() == Neo4jConfiguration.AUTHENTICATION_KERBEROS_TICKET }
            .errorMessages(),
        "Must be non-empty.")
  }

  @Test
  fun `should validate bearer auth details`() {
    val connector = Neo4jConnector()
    val config =
        connector.validate(mutableMapOf(Neo4jConfiguration.AUTHENTICATION_TYPE to "BEARER"))

    assertContains(
        config
            .configValues()
            .first { it.name() == Neo4jConfiguration.AUTHENTICATION_BEARER_TOKEN }
            .errorMessages(),
        "Must be non-empty.")
  }

  @Test
  fun `should validate custom auth details`() {
    val connector = Neo4jConnector()
    val config =
        connector.validate(mutableMapOf(Neo4jConfiguration.AUTHENTICATION_TYPE to "CUSTOM"))

    assertContains(
        config
            .configValues()
            .first { it.name() == Neo4jConfiguration.AUTHENTICATION_CUSTOM_SCHEME }
            .errorMessages(),
        "Must be non-empty.")
    assertContains(
        config
            .configValues()
            .first { it.name() == Neo4jConfiguration.AUTHENTICATION_CUSTOM_PRINCIPAL }
            .errorMessages(),
        "Must be non-empty.")
    assertContains(
        config
            .configValues()
            .first { it.name() == Neo4jConfiguration.AUTHENTICATION_CUSTOM_CREDENTIALS }
            .errorMessages(),
        "Must be non-empty.")
    assertTrue {
      config
          .configValues()
          .first { it.name() == Neo4jConfiguration.AUTHENTICATION_CUSTOM_REALM }
          .errorMessages()
          .isEmpty()
    }
  }
}
