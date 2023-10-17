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

import io.kotest.matchers.collections.shouldContain
import io.kotest.matchers.collections.shouldHaveSingleElement
import io.kotest.matchers.shouldBe
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
        .errorMessages() shouldBe emptyList()
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
        .errorMessages() shouldBe emptyList()
  }

  @Test
  fun `should validate empty topic configuration with cdc strategy`() {
    val connector = Neo4jConnector()
    val config =
        connector.validate(
            mutableMapOf(
                Neo4jConfiguration.URI to "neo4j://localhost",
                Neo4jConfiguration.AUTHENTICATION_TYPE to "NONE",
                SourceConfiguration.STRATEGY to "CDC"))

    config
        .configValues()
        .first { it.name() == SourceConfiguration.STRATEGY }
        .errorMessages() shouldContain
        "At least one topic needs to be configured with pattern(s) describing the entities to query changes for. Please refer to documentation for more information."
  }

  @Test
  fun `should validate query with query strategy`() {
    val connector = Neo4jConnector()

    connector
        .validate(
            mutableMapOf(
                Neo4jConfiguration.URI to "neo4j://localhost",
                SourceConfiguration.TOPIC to "topic",
                SourceConfiguration.STRATEGY to "QUERY"))
        .apply {
          this.configValues()
              .first { it.name() == SourceConfiguration.QUERY }
              .errorMessages() shouldContain
              "Invalid value for configuration neo4j.query: Must not be blank."
        }

    connector
        .validate(
            mutableMapOf(
                Neo4jConfiguration.URI to "neo4j://localhost",
                SourceConfiguration.QUERY to "MATCH (n) RETURN n",
                SourceConfiguration.STRATEGY to "QUERY"))
        .apply {
          this.configValues()
              .first { it.name() == SourceConfiguration.TOPIC }
              .errorMessages() shouldContain
              "Invalid value for configuration topic: Must not be blank."
        }
  }

  @Test
  fun `should validate topic patterns with cdc strategy`() {
    val connector = Neo4jConnector()

    connector
        .validate(
            mutableMapOf(
                Neo4jConfiguration.URI to "neo4j://localhost",
                Neo4jConfiguration.AUTHENTICATION_TYPE to "NONE",
                SourceConfiguration.STRATEGY to "CDC",
                "neo4j.cdc.topic.topic-1" to ""))
        .apply {
          this.configValues()
              .first { it.name() == SourceConfiguration.STRATEGY }
              .errorMessages() shouldContain
              "Invalid value  for configuration neo4j.cdc.topic.topic-1: Must not be blank."
        }

    connector
        .validate(
            mutableMapOf(
                Neo4jConfiguration.URI to "neo4j://localhost",
                Neo4jConfiguration.AUTHENTICATION_TYPE to "NONE",
                SourceConfiguration.STRATEGY to "CDC",
                "neo4j.cdc.topic.topic-1" to "(;ABC]"))
        .apply {
          this.configValues()
              .first { it.name() == SourceConfiguration.STRATEGY }
              .errorMessages() shouldHaveSingleElement
              {
                it.startsWith("Invalid value (;ABC] for configuration neo4j.cdc.topic.topic-1:")
              }
        }

    connector
        .validate(
            mutableMapOf(
                Neo4jConfiguration.URI to "neo4j://localhost",
                Neo4jConfiguration.AUTHENTICATION_TYPE to "NONE",
                SourceConfiguration.STRATEGY to "CDC",
                "neo4j.cdc.topic.topic-1" to "(:Person),()-[:KNOWS]-()",
                "neo4j.cdc.topic.topic-2.patterns" to "(:Person),()-[:KNOWS]-(;Company)"))
        .apply {
          this.configValues()
              .first { it.name() == SourceConfiguration.STRATEGY }
              .errorMessages() shouldHaveSingleElement
              {
                it.startsWith("Invalid value (:Person),()-[:KNOWS]-(;Company)")
              }
        }
  }

  @Test
  fun `should return Neo4jQueryTask for query strategy`() {
    val connector = Neo4jConnector()

    connector.start(
        mutableMapOf(
            Neo4jConfiguration.URI to "neo4j://localhost",
            Neo4jConfiguration.AUTHENTICATION_TYPE to "NONE",
            SourceConfiguration.TOPIC to "my-topic",
            SourceConfiguration.STRATEGY to "QUERY",
            SourceConfiguration.QUERY to "MATCH (n) RETURN n.timestamp, n"))

    connector.taskClass() shouldBe Neo4jQueryTask::class.java
  }

  @Test
  fun `should return Neo4jCDCTask for cdc strategy`() {
    val connector = Neo4jConnector()

    connector.start(
        mutableMapOf(
            Neo4jConfiguration.URI to "neo4j://localhost",
            Neo4jConfiguration.AUTHENTICATION_TYPE to "NONE",
            SourceConfiguration.STRATEGY to "CDC",
            "neo4j.cdc.topic.topic-1" to "(:Person)"))

    connector.taskClass() shouldBe Neo4jCDCTask::class.java
  }
}
