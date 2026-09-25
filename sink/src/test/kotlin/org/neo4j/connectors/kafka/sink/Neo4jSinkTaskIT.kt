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

import io.kotest.assertions.throwables.shouldNotThrow
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.throwable.shouldHaveMessage
import java.util.UUID
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkConnector
import org.apache.kafka.connect.sink.SinkTaskContext
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.neo4j.connectors.kafka.configuration.Neo4jConfiguration
import org.neo4j.connectors.kafka.testing.DatabaseSupport.createDatabase
import org.neo4j.connectors.kafka.testing.DatabaseSupport.dropDatabase
import org.neo4j.connectors.kafka.testing.createNeo4jContainer
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.testcontainers.containers.Neo4jContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

@Testcontainers
class Neo4jSinkTaskIT {

  private lateinit var db: String
  private lateinit var task: Neo4jSinkTask

  @BeforeEach
  fun setUp() {
    db = "test-${UUID.randomUUID()}"
    driver.createDatabase(db)
    task = Neo4jSinkTask()
    task.initialize(mock<SinkTaskContext>())
  }

  @AfterEach
  fun tearDown() {
    if (this::task.isInitialized) task.stop()
    if (this::db.isInitialized) driver.dropDatabase(db)
  }

  @Test
  fun `should fail startup when EOS constraint is missing`() {
    val props =
        mapOf(
            Neo4jConfiguration.URI to container.boltUrl,
            Neo4jConfiguration.DATABASE to db,
            Neo4jConfiguration.AUTHENTICATION_TYPE to "NONE",
            SinkConnector.TOPICS_CONFIG to "my-topic",
            SinkConfiguration.CUD_TOPICS to "my-topic",
            SinkConfiguration.EOS_OFFSET_LABEL to "even when spaces",
            SinkConfiguration.EOS_OFFSET_AUTO_CONSTRAINT to "false",
        )

    shouldThrow<ConnectException> { task.start(props) } shouldHaveMessage
        """
        Missing EOS offset constraint for label `even when spaces`. Create it in the sink database by running:
        CREATE CONSTRAINT kafka_eos_offset_key IF NOT EXISTS
        FOR (n:`even when spaces`) REQUIRE (n.strategy, n.topic, n.partition) IS NODE KEY
        """
            .trimIndent()
  }

  @Test
  fun `should successfully create constraints for EOS when auto mode`() {
    val props =
        mapOf(
            Neo4jConfiguration.URI to container.boltUrl,
            Neo4jConfiguration.DATABASE to db,
            Neo4jConfiguration.AUTHENTICATION_TYPE to "NONE",
            SinkConnector.TOPICS_CONFIG to "my-topic",
            SinkConfiguration.CUD_TOPICS to "my-topic",
            SinkConfiguration.EOS_OFFSET_LABEL to "even when spaces",
            SinkConfiguration.EOS_OFFSET_AUTO_CONSTRAINT to "true",
        )

    shouldNotThrow<Exception> { task.start(props) }
  }

  companion object {
    @Container val container: Neo4jContainer<*> = createNeo4jContainer()

    private lateinit var driver: Driver

    @BeforeAll
    @JvmStatic
    fun setUpContainer() {
      driver = GraphDatabase.driver(container.boltUrl, AuthTokens.none())
    }

    @AfterAll
    @JvmStatic
    fun tearDownContainer() {
      if (this::driver.isInitialized) driver.close()
    }
  }
}
