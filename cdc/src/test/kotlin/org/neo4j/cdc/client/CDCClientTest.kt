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
package org.neo4j.cdc.client

import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.testcontainers.containers.Neo4jContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import reactor.test.StepVerifier

/**
 * Just some tests against a running instance with cdc enabled. Since I consider moving this into
 * some Java classes, this will be testcontainers-based.
 *
 * @author Gerrit Meier
 */
@Testcontainers
class CDCClientTest {

  companion object {
    private const val NEO4J_VERSION = "5.11"

    @Container
    val neo4j: Neo4jContainer<*> =
        Neo4jContainer("neo4j:$NEO4J_VERSION-enterprise")
            .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
            .withNeo4jConfig("internal.dbms.change_data_capture", "true")
            .withoutAuthentication()

    private lateinit var driver: Driver
    private lateinit var current: ChangeIdentifier

    @BeforeAll
    @JvmStatic
    fun setup() {
      driver = GraphDatabase.driver(neo4j.boltUrl, AuthTokens.none())

      driver.session().use {
        // enable CDC
        it.run(
                "ALTER DATABASE \$db SET OPTION txLogEnrichment \$mode",
                mapOf("db" to "neo4j", "mode" to CaptureMode.FULL.name))
            .consume()

        // capture current change identifier
        current = ChangeIdentifier(it.run("CALL cdc.current()").single()[0].asString())
      }
    }

    @AfterAll
    @JvmStatic
    fun cleanup() {
      driver.close()
    }
  }

  private val client = CDCClient(driver)

  @Test
  fun earliest() {
    StepVerifier.create(client.earliest())
        .assertNext { cv -> assertNotNull(cv.id) }
        .verifyComplete()
  }

  @Test
  fun current() {
    StepVerifier.create(client.current()).assertNext { cv -> assertNotNull(cv.id) }.verifyComplete()
  }

  @Test
  fun query() {
    driver.session().use { it.run("CREATE ()").consume() }

    StepVerifier.create(client.query(current)).assertNext { ce -> println(ce) }.verifyComplete()
  }
}
