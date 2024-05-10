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
package org.neo4j.connectors.kafka.sink

import io.kotest.matchers.collections.beEmpty
import io.kotest.matchers.collections.shouldContain
import io.kotest.matchers.should
import org.junit.jupiter.api.Test
import org.neo4j.connectors.kafka.configuration.Neo4jConfiguration

class Neo4jConnectorTest {

  @Test
  fun `should validate basic auth details`() {
    val connector = Neo4jConnector()
    val config = connector.validate(mutableMapOf(Neo4jConfiguration.AUTHENTICATION_TYPE to "BASIC"))

    config
        .configValues()
        .first { it.name() == Neo4jConfiguration.AUTHENTICATION_BASIC_USERNAME }
        .errorMessages() shouldContain
        "Invalid value for configuration neo4j.authentication.basic.username: Must not be blank."

    config
        .configValues()
        .first { it.name() == Neo4jConfiguration.AUTHENTICATION_BASIC_PASSWORD }
        .errorMessages() shouldContain
        "Invalid value for configuration neo4j.authentication.basic.password: Must not be blank."
    config
        .configValues()
        .first { it.name() == Neo4jConfiguration.AUTHENTICATION_BASIC_REALM }
        .errorMessages() should beEmpty<String>()
  }

  @Test
  fun `should validate kerberos auth details`() {
    val connector = Neo4jConnector()
    val config =
        connector.validate(mutableMapOf(Neo4jConfiguration.AUTHENTICATION_TYPE to "KERBEROS"))

    config
        .configValues()
        .first { it.name() == Neo4jConfiguration.AUTHENTICATION_KERBEROS_TICKET }
        .errorMessages() shouldContain
        "Invalid value for configuration neo4j.authentication.kerberos.ticket: Must not be blank."
  }

  @Test
  fun `should validate bearer auth details`() {
    val connector = Neo4jConnector()
    val config =
        connector.validate(mutableMapOf(Neo4jConfiguration.AUTHENTICATION_TYPE to "BEARER"))

    config
        .configValues()
        .first { it.name() == Neo4jConfiguration.AUTHENTICATION_BEARER_TOKEN }
        .errorMessages() shouldContain
        "Invalid value for configuration neo4j.authentication.bearer.token: Must not be blank."
  }

  @Test
  fun `should validate custom auth details`() {
    val connector = Neo4jConnector()
    val config =
        connector.validate(mutableMapOf(Neo4jConfiguration.AUTHENTICATION_TYPE to "CUSTOM"))

    config
        .configValues()
        .first { it.name() == Neo4jConfiguration.AUTHENTICATION_CUSTOM_SCHEME }
        .errorMessages() shouldContain
        "Invalid value for configuration neo4j.authentication.custom.scheme: Must not be blank."
    config
        .configValues()
        .first { it.name() == Neo4jConfiguration.AUTHENTICATION_CUSTOM_PRINCIPAL }
        .errorMessages() shouldContain
        "Invalid value for configuration neo4j.authentication.custom.principal: Must not be blank."
    config
        .configValues()
        .first { it.name() == Neo4jConfiguration.AUTHENTICATION_CUSTOM_CREDENTIALS }
        .errorMessages() shouldContain
        "Invalid value for configuration neo4j.authentication.custom.credentials: Must not be blank."
    config
        .configValues()
        .first { it.name() == Neo4jConfiguration.AUTHENTICATION_CUSTOM_REALM }
        .errorMessages() should beEmpty<String>()
  }

  @Test
  fun `should validate cypher strategy alias settings when no alias set`() {
    val connector = Neo4jConnector()
    val config =
        connector.validate(
            mutableMapOf(
                Neo4jConfiguration.AUTHENTICATION_TYPE to "NONE",
                SinkConfiguration.CYPHER_BIND_TIMESTAMP_AS to "",
                SinkConfiguration.CYPHER_BIND_HEADER_AS to "",
                SinkConfiguration.CYPHER_BIND_KEY_AS to "",
                SinkConfiguration.CYPHER_BIND_VALUE_AS to "",
                SinkConfiguration.CYPHER_BIND_VALUE_AS_EVENT to "false"))

    config
        .configValues()
        .filter {
          it.name() in
              listOf(
                  SinkConfiguration.CYPHER_BIND_TIMESTAMP_AS,
                  SinkConfiguration.CYPHER_BIND_HEADER_AS,
                  SinkConfiguration.CYPHER_BIND_KEY_AS,
                  SinkConfiguration.CYPHER_BIND_VALUE_AS,
                  SinkConfiguration.CYPHER_BIND_VALUE_AS_EVENT)
        }
        .forEach {
          it.errorMessages() shouldContain
              "At least one variable binding must be specified for Cypher strategies."
        }
  }

  @Test
  fun `should validate cypher strategy alias settings with value as event`() {
    val connector = Neo4jConnector()
    val config =
        connector.validate(
            mutableMapOf(
                Neo4jConfiguration.AUTHENTICATION_TYPE to "NONE",
                SinkConfiguration.CYPHER_BIND_HEADER_AS to "",
                SinkConfiguration.CYPHER_BIND_KEY_AS to "",
                SinkConfiguration.CYPHER_BIND_VALUE_AS to "",
                SinkConfiguration.CYPHER_BIND_VALUE_AS_EVENT to "true"))

    config
        .configValues()
        .filter {
          it.name() in
              listOf(
                  SinkConfiguration.CYPHER_BIND_HEADER_AS,
                  SinkConfiguration.CYPHER_BIND_KEY_AS,
                  SinkConfiguration.CYPHER_BIND_VALUE_AS,
                  SinkConfiguration.CYPHER_BIND_VALUE_AS_EVENT)
        }
        .forEach { it.errorMessages() should beEmpty<String>() }
  }

  @Test
  fun `should validate cypher strategy alias settings with value as __value`() {
    val connector = Neo4jConnector()
    val config =
        connector.validate(
            mutableMapOf(
                Neo4jConfiguration.AUTHENTICATION_TYPE to "NONE",
                SinkConfiguration.CYPHER_BIND_HEADER_AS to "",
                SinkConfiguration.CYPHER_BIND_KEY_AS to "",
                SinkConfiguration.CYPHER_BIND_VALUE_AS to "__value",
                SinkConfiguration.CYPHER_BIND_VALUE_AS_EVENT to "false"))

    config
        .configValues()
        .filter {
          it.name() in
              listOf(
                  SinkConfiguration.CYPHER_BIND_HEADER_AS,
                  SinkConfiguration.CYPHER_BIND_KEY_AS,
                  SinkConfiguration.CYPHER_BIND_VALUE_AS,
                  SinkConfiguration.CYPHER_BIND_VALUE_AS_EVENT)
        }
        .forEach { it.errorMessages() should beEmpty<String>() }
  }
}
