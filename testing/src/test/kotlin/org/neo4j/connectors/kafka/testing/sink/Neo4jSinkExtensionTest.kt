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
package org.neo4j.connectors.kafka.testing.sink

import java.util.concurrent.atomic.AtomicBoolean
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertSame
import kotlin.test.assertTrue
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ConditionEvaluationResult
import org.junit.jupiter.api.extension.ExtensionConfigurationException
import org.junit.jupiter.api.extension.ExtensionContext
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.inOrder
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.neo4j.connectors.kafka.testing.JUnitSupport.extensionContextFor
import org.neo4j.connectors.kafka.testing.JUnitSupport.parameterContextForType
import org.neo4j.connectors.kafka.testing.KafkaConnectServer
import org.neo4j.driver.Driver
import org.neo4j.driver.Session

class Neo4jSinkExtensionTest {

  private val extension = Neo4jSinkExtension()

  private val kafkaConnectServer = KafkaConnectServer()

  @AfterEach
  fun cleanUp() {
    kafkaConnectServer.close()
  }

  @Test
  fun `continues execution evaluation if annotation is found and valid`() {
    val result = extension.evaluateExecutionCondition(extensionContextFor(::validMethod))

    assertIs<ConditionEvaluationResult>(result)
    assertFalse { result.isDisabled }
  }

  @Test
  fun `registers sink connector`() {
    val handlerCalled = AtomicBoolean()
    kafkaConnectServer.start(
        registrationHandler = { exchange ->
          if (!handlerCalled.compareAndSet(false, true)) {
            kafkaConnectServer.internalServerError(
                exchange, "expected handler flag to be initially false")
            return@start true
          }
          return@start false
        },
    )
    val environment =
        mapOf(
            "KAFKA_CONNECT_EXTERNAL_URI" to kafkaConnectServer.address(),
        )
    val extension = Neo4jSinkExtension(environment::get)
    val extensionContext = extensionContextFor(::onlyKafkaConnectExternalUriFromEnvMethod)
    extension.evaluateExecutionCondition(extensionContext)

    extension.beforeEach(extensionContext)

    assertTrue(handlerCalled.get(), "registration should be successful")
  }

  @Test
  fun `unregisters sink connector`() {
    val handlerCalled = AtomicBoolean()
    kafkaConnectServer.start(
        unregistrationHandler = { exchange ->
          if (!handlerCalled.compareAndSet(false, true)) {
            kafkaConnectServer.internalServerError(
                exchange, "expected handler flag to be initially false")
            return@start true
          }
          return@start false
        },
    )
    val environment =
        mapOf(
            "KAFKA_CONNECT_EXTERNAL_URI" to kafkaConnectServer.address(),
        )
    val extension = Neo4jSinkExtension(environment::get)
    val extensionContext = extensionContextFor(::onlyKafkaConnectExternalUriFromEnvMethod)
    extension.evaluateExecutionCondition(extensionContext)
    extension.beforeEach(extensionContext)

    extension.afterEach(extensionContext)

    assertTrue(handlerCalled.get(), "unregistration should be successful")
  }

  @Test
  fun `supports specific parameters`() {
    assertTrue(
        extension.supportsParameter(
            parameterContextForType(Session::class),
            mock<ExtensionContext>(),
        ),
        "session parameters should be resolvable",
    )
    assertFalse(
        extension.supportsParameter(
            parameterContextForType(Thread::class),
            mock<ExtensionContext>(),
        ),
        "arbitrary parameters should not be supported",
    )
  }

  @Test
  fun `resolves Session parameter`() {
    val session = mock<Session>()
    val driver = mock<Driver> { on { session() } doReturn session }
    val extension = Neo4jSinkExtension(driverFactory = { _, _ -> driver })
    val extensionContext = extensionContextFor(::validMethod)
    extension.evaluateExecutionCondition(extensionContext)

    val sessionParam =
        extension.resolveParameter(parameterContextForType(Session::class), extensionContext)

    assertIs<Session>(sessionParam)
    assertSame(session, sessionParam)
  }

  @Test
  fun `verifies connectivity if driver is initialized before each test`() {
    kafkaConnectServer.start()
    val session = mock<Session>()
    val environment =
        mapOf(
            "KAFKA_CONNECT_EXTERNAL_URI" to kafkaConnectServer.address(),
        )
    val driver =
        mock<Driver> {
          on { session() } doReturn session
          on { verifyConnectivity() } doAnswer {}
        }
    val extension =
        Neo4jSinkExtension(
            envAccessor = environment::get,
            driverFactory = { _, _ -> driver },
        )
    val extensionContext = extensionContextFor(::onlyKafkaConnectExternalUriFromEnvMethod)
    extension.evaluateExecutionCondition(extensionContext)
    extension.resolveParameter(parameterContextForType(Session::class), extensionContext)

    extension.beforeEach(extensionContext)

    verify(driver).verifyConnectivity()
  }

  @Test
  fun `closes Driver and Session after each test`() {
    kafkaConnectServer.start()
    val session = mock<Session>()
    val environment =
        mapOf(
            "KAFKA_CONNECT_EXTERNAL_URI" to kafkaConnectServer.address(),
        )
    val driver = mock<Driver> { on { session() } doReturn session }
    val extension =
        Neo4jSinkExtension(
            envAccessor = environment::get,
            driverFactory = { _, _ -> driver },
        )
    val extensionContext = extensionContextFor(::onlyKafkaConnectExternalUriFromEnvMethod)
    extension.evaluateExecutionCondition(extensionContext)
    extension.resolveParameter(parameterContextForType(Session::class), extensionContext)
    extension.beforeEach(extensionContext)

    extension.afterEach(extensionContext)

    inOrder(session, driver) {
      verify(session).close()
      verify(driver).close()
    }
  }

  @Test
  fun `stops execution evaluation if annotation is not found`() {
    val exception =
        assertFailsWith<ExtensionConfigurationException> {
          extension.evaluateExecutionCondition(extensionContextFor(::missingAnnotationMethod))
        }

    assertEquals(exception.message, "@Neo4jSink not found")
  }

  @Test
  fun `stops execution evaluation if kafka connect external URI is not specified`() {
    val environment =
        mapOf(
            "NEO4J_URI" to "neo4j://example",
            "NEO4J_USER" to "user",
            "NEO4J_PASSWORD" to "password",
        )
    val extension = Neo4jSinkExtension(environment::get)

    val exception =
        assertFailsWith<ExtensionConfigurationException> {
          extension.evaluateExecutionCondition(extensionContextFor(::envBackedMethod))
        }

    assertContains(
        exception.message!!,
        "Both annotation field kafkaConnectExternalUri and environment variable KAFKA_CONNECT_EXTERNAL_URI are unset. Please specify one",
    )
  }

  @Test
  fun `stops execution evaluation if neo4j URI is not specified`() {
    val environment =
        mapOf(
            "KAFKA_CONNECT_EXTERNAL_URI" to "http://example.com",
            "NEO4J_USER" to "user",
            "NEO4J_PASSWORD" to "password",
        )
    val extension = Neo4jSinkExtension(environment::get)

    val exception =
        assertFailsWith<ExtensionConfigurationException> {
          extension.evaluateExecutionCondition(extensionContextFor(::envBackedMethod))
        }

    assertContains(
        exception.message!!,
        "Both annotation field neo4jUri and environment variable NEO4J_URI are unset. Please specify one",
    )
  }

  @Test
  fun `stops execution evaluation if neo4j user is not specified`() {
    val environment =
        mapOf(
            "KAFKA_CONNECT_EXTERNAL_URI" to "http://example.com",
            "NEO4J_URI" to "neo4j://example.com",
            "NEO4J_PASSWORD" to "password",
        )
    val extension = Neo4jSinkExtension(environment::get)

    val exception =
        assertFailsWith<ExtensionConfigurationException> {
          extension.evaluateExecutionCondition(extensionContextFor(::envBackedMethod))
        }
    assertContains(
        exception.message!!,
        "Both annotation field neo4jUser and environment variable NEO4J_USER are unset. Please specify one",
    )
  }

  @Test
  fun `stops execution evaluation if neo4j password is not specified`() {
    val environment =
        mapOf(
            "KAFKA_CONNECT_EXTERNAL_URI" to "http://example.com",
            "NEO4J_URI" to "neo4j://example.com",
            "NEO4J_USER" to "user",
        )
    val extension = Neo4jSinkExtension(environment::get)

    val exception =
        assertFailsWith<ExtensionConfigurationException> {
          extension.evaluateExecutionCondition(extensionContextFor(::envBackedMethod))
        }
    assertContains(
        exception.message!!,
        "Both annotation field neo4jPassword and environment variable NEO4J_PASSWORD are unset. Please specify one",
    )
  }

  @Test
  fun `stops execution evaluation if number of queries does not match number of topics`() {
    val exception1 =
        assertFailsWith<ExtensionConfigurationException> {
          extension.evaluateExecutionCondition(extensionContextFor(::moreQueriesThanTopicsMethod))
        }
    val exception2 =
        assertFailsWith<ExtensionConfigurationException> {
          extension.evaluateExecutionCondition(extensionContextFor(::moreTopicsThanQueriesMethod))
        }

    assertContains(
        exception1.message!!,
        "Expected 1 query, but got 2. There must be as many topics (here: 1) as queries defined.",
    )
    assertContains(
        exception2.message!!,
        "Expected 2 queries, but got 1. There must be as many topics (here: 2) as queries defined.",
    )
  }

  @Neo4jSink(
      kafkaConnectExternalUri = "http://example.com",
      neo4jExternalUri = "neo4j://example.com",
      neo4jUri = "neo4j://example.com",
      neo4jUser = "user",
      neo4jPassword = "password",
      topics = ["topic1"],
      queries = ["MERGE ()"],
      schemaControlRegistryUri = "http://example.com")
  @Suppress("UNUSED")
  fun validMethod() {}

  @Neo4jSink(
      neo4jExternalUri = "neo4j://example.com",
      neo4jUri = "neo4j://example.com",
      neo4jUser = "user",
      neo4jPassword = "password",
      topics = ["topic1"],
      queries = ["MERGE ()"],
      schemaControlRegistryUri = "http://example.com")
  @Suppress("UNUSED")
  fun onlyKafkaConnectExternalUriFromEnvMethod() {}

  @Neo4jSink(topics = ["topic1"], queries = ["MERGE ()"])
  @Suppress("UNUSED")
  fun envBackedMethod() {}

  @Suppress("UNUSED") fun missingAnnotationMethod() {}

  @Neo4jSink(
      kafkaConnectExternalUri = "http://example.com",
      neo4jUri = "neo4j://example.com",
      neo4jUser = "user",
      neo4jPassword = "password",
      topics = ["topic1"],
      queries = ["MERGE ()", "CREATE ()"],
  )
  @Suppress("UNUSED")
  fun moreQueriesThanTopicsMethod() {}

  @Neo4jSink(
      kafkaConnectExternalUri = "http://example.com",
      neo4jUri = "neo4j://example.com",
      neo4jUser = "user",
      neo4jPassword = "password",
      topics = ["topic1", "topic2"],
      queries = ["MERGE ()"],
  )
  @Suppress("UNUSED")
  fun moreTopicsThanQueriesMethod() {}
}
