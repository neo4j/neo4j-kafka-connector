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
package org.neo4j.connectors.kafka.sink

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
        "Invalid value for configuration neo4j.authentication.basic.username: Must not be blank.")
    assertContains(
        config
            .configValues()
            .first { it.name() == Neo4jConfiguration.AUTHENTICATION_BASIC_PASSWORD }
            .errorMessages(),
        "Invalid value for configuration neo4j.authentication.basic.password: Must not be blank.")
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
        "Invalid value for configuration neo4j.authentication.kerberos.ticket: Must not be blank.")
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
        "Invalid value for configuration neo4j.authentication.bearer.token: Must not be blank.")
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
        "Invalid value for configuration neo4j.authentication.custom.scheme: Must not be blank.")
    assertContains(
        config
            .configValues()
            .first { it.name() == Neo4jConfiguration.AUTHENTICATION_CUSTOM_PRINCIPAL }
            .errorMessages(),
        "Invalid value for configuration neo4j.authentication.custom.principal: Must not be blank.")
    assertContains(
        config
            .configValues()
            .first { it.name() == Neo4jConfiguration.AUTHENTICATION_CUSTOM_CREDENTIALS }
            .errorMessages(),
        "Invalid value for configuration neo4j.authentication.custom.credentials: Must not be blank.")
    assertTrue {
      config
          .configValues()
          .first { it.name() == Neo4jConfiguration.AUTHENTICATION_CUSTOM_REALM }
          .errorMessages()
          .isEmpty()
    }
  }
}
