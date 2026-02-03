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
package org.neo4j.connectors.kafka.testing.source

import java.util.concurrent.atomic.AtomicBoolean
import kotlin.reflect.KFunction
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertSame
import kotlin.test.assertTrue
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ConditionEvaluationResult
import org.junit.jupiter.api.extension.ExtensionConfigurationException
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.params.ParameterizedInvocationConstants.DISPLAY_NAME_PLACEHOLDER
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.any
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.inOrder
import org.mockito.kotlin.mock
import org.neo4j.caniuse.Neo4j
import org.neo4j.caniuse.Neo4jDeploymentType
import org.neo4j.caniuse.Neo4jEdition
import org.neo4j.caniuse.Neo4jVersion
import org.neo4j.connectors.kafka.testing.JUnitSupport.annotatedParameterContextForType
import org.neo4j.connectors.kafka.testing.JUnitSupport.extensionContextFor
import org.neo4j.connectors.kafka.testing.JUnitSupport.parameterContextForType
import org.neo4j.connectors.kafka.testing.KafkaConnectServer
import org.neo4j.connectors.kafka.testing.kafka.ConvertingKafkaConsumer
import org.neo4j.driver.Driver
import org.neo4j.driver.Record
import org.neo4j.driver.Result
import org.neo4j.driver.Session
import org.neo4j.driver.SessionConfig
import org.neo4j.driver.Values

class Neo4jSourceExtensionTest {

  private val extension = Neo4jSourceExtension()

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
  fun `registers source connector`() {
    val handlerCalled = AtomicBoolean()
    kafkaConnectServer.start(
        registrationHandler = { exchange ->
          if (!handlerCalled.compareAndSet(false, true)) {
            kafkaConnectServer.internalServerError(
                exchange,
                "expected handler flag to be initially false",
            )
            return@start true
          }
          return@start false
        }
    )
    val environment = mapOf("KAFKA_CONNECT_EXTERNAL_URI" to kafkaConnectServer.address())
    val (driver, _) = setupDetectableDriver()
    val extension = Neo4jSourceExtension(environment::get, driverFactory = { _, _ -> driver })
    val extensionContext = extensionContextFor(::onlyKafkaConnectExternalUriFromEnvMethod)
    extension.evaluateExecutionCondition(extensionContext)

    extension.beforeEach(extensionContext)

    assertTrue(handlerCalled.get(), "registration should be successful")
  }

  @Test
  fun `unregisters source connector`() {
    val handlerCalled = AtomicBoolean()
    kafkaConnectServer.start(
        unregistrationHandler = { exchange ->
          if (!handlerCalled.compareAndSet(false, true)) {
            kafkaConnectServer.internalServerError(
                exchange,
                "expected handler flag to be initially false",
            )
            return@start true
          }
          return@start false
        }
    )
    val environment = mapOf("KAFKA_CONNECT_EXTERNAL_URI" to kafkaConnectServer.address())
    val (driver, _) = setupDetectableDriver()
    val extension = Neo4jSourceExtension(environment::get, driverFactory = { _, _ -> driver })
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
        "session parameter should be resolvable",
    )
    assertTrue(
        extension.supportsParameter(
            parameterContextForType(ConvertingKafkaConsumer::class),
            mock<ExtensionContext>(),
        ),
        "consumer parameter should be resolvable",
    )
    assertFalse(
        extension.supportsParameter(
            parameterContextForType(Thread::class),
            mock<ExtensionContext>(),
        ),
        "arbitrary parameters should not be supported",
    )
  }

  @ParameterizedTest(name = "$DISPLAY_NAME_PLACEHOLDER [{0}]")
  @MethodSource("validMethods")
  @Suppress("UNUSED_PARAMETER") // Kotlin compiler not smart enough to see name param is used
  fun `resolves Session parameter`(name: String, method: KFunction<Unit>) {
    val (driver, session) = setupDetectableDriver()
    val extension = Neo4jSourceExtension(driverFactory = { _, _ -> driver })
    val extensionContext = extensionContextFor(method)
    extension.evaluateExecutionCondition(extensionContext)

    val sessionParam =
        extension.resolveParameter(parameterContextForType(Session::class), extensionContext)

    assertIs<Session>(sessionParam)
    assertSame(session, sessionParam)
  }

  @ParameterizedTest(name = "$DISPLAY_NAME_PLACEHOLDER [{0}]")
  @MethodSource("validMethods")
  @Suppress("UNUSED_PARAMETER") // Kotlin compiler not smart enough to see name param is used
  fun `resolves consumer parameter`(name: String, method: KFunction<Unit>) {
    val consumer = mock<KafkaConsumer<ByteArray, ByteArray>>()
    val (driver, _) = setupDetectableDriver()
    val extension =
        Neo4jSourceExtension(
            consumerFactory = { _, _ -> consumer },
            driverFactory = { _, _ -> driver },
        )
    val extensionContext = extensionContextFor(method)
    extension.evaluateExecutionCondition(extensionContext)
    val consumerAnnotation = TopicConsumer(topic = "topic", offset = "earliest")

    val convertingKafkaConsumer =
        extension.resolveParameter(
            annotatedParameterContextForType(ConvertingKafkaConsumer::class, consumerAnnotation),
            extensionContext,
        )

    assertIs<ConvertingKafkaConsumer>(convertingKafkaConsumer)
    assertSame(consumer, convertingKafkaConsumer.kafkaConsumer)
  }

  @ParameterizedTest(name = "$DISPLAY_NAME_PLACEHOLDER [{0}]")
  @MethodSource("validMethods")
  @Suppress("UNUSED_PARAMETER") // Kotlin compiler not smart enough to see name param is used
  fun `resolves neo4j parameter`(name: String, method: KFunction<Unit>) {
    val (driver, _) = setupDetectableDriver()
    val extension = Neo4jSourceExtension(driverFactory = { _, _ -> driver })
    val extensionContext = extensionContextFor(method)
    extension.evaluateExecutionCondition(extensionContext)

    val neo4j = extension.resolveParameter(parameterContextForType(Neo4j::class), extensionContext)

    assertIs<Neo4j>(neo4j)
    assertEquals(
        Neo4j(Neo4jVersion(5, 26, 0), Neo4jEdition.ENTERPRISE, Neo4jDeploymentType.SELF_MANAGED),
        neo4j,
    )
  }

  private fun setupDetectableDriver(): Pair<Driver, Session> {
    val versionRecord =
        mock<Record> {
          on { get("version") } doReturn Values.value("5.26.0")
          on { get("edition") } doReturn Values.value("enterprise")
        }
    val versionResult = mock<Result> { on { single() } doReturn versionRecord }

    val statusRecord = mock<Record> { on { get("currentStatus") } doReturn Values.value("online") }
    val statusResult = mock<Result> { on { single() } doReturn statusRecord }

    val session =
        mock<Session> {
          on { run(ArgumentMatchers.contains("dbms.components"), any<Map<String, Any>>()) } doReturn
              versionResult
          on {
            run(ArgumentMatchers.contains("RETURN currentStatus"), any<Map<String, Any>>())
          } doReturn statusResult
        }
    val driver =
        mock<Driver> {
          on { session() } doReturn session
          on { session(any(SessionConfig::class.java)) } doReturn session
        }
    return Pair(driver, session)
  }

  @Test
  fun `closes Driver and Session after each test`() {
    kafkaConnectServer.start()
    val environment = mapOf("KAFKA_CONNECT_EXTERNAL_URI" to kafkaConnectServer.address())
    val (driver, session) = setupDetectableDriver()
    val extension =
        Neo4jSourceExtension(envAccessor = environment::get, driverFactory = { _, _ -> driver })
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

    assertEquals(exception.message, "@Neo4jSource not found")
  }

  @Test
  fun `stops execution evaluation if broker external host is not specified`() {
    val environment =
        mapOf(
            "SCHEMA_CONTROL_REGISTRY_URI" to "http://example.com",
            "SCHEMA_CONTROL_REGISTRY_EXTERNAL_URI" to "http://example.com",
            "KAFKA_CONNECT_EXTERNAL_URI" to "example.com",
            "NEO4J_URI" to "neo4j://example",
            "NEO4J_USER" to "user",
            "NEO4J_PASSWORD" to "password",
        )
    val extension = Neo4jSourceExtension(environment::get)

    val exception =
        assertFailsWith<ExtensionConfigurationException> {
          extension.evaluateExecutionCondition(extensionContextFor(::envBackedMethod))
        }

    assertContains(
        exception.message!!,
        "Both annotation field brokerExternalHost and environment variable BROKER_EXTERNAL_HOST are unset. Please specify one",
    )
  }

  @Test
  fun `stops execution evaluation if schema control registry URI is not specified`() {
    val environment =
        mapOf(
            "BROKER_EXTERNAL_HOST" to "example.com",
            "SCHEMA_CONTROL_REGISTRY_EXTERNAL_URI" to "http://example.com",
            "KAFKA_CONNECT_EXTERNAL_URI" to "example.com",
            "NEO4J_URI" to "neo4j://example",
            "NEO4J_USER" to "user",
            "NEO4J_PASSWORD" to "password",
        )
    val extension = Neo4jSourceExtension(environment::get)

    val exception =
        assertFailsWith<ExtensionConfigurationException> {
          extension.evaluateExecutionCondition(extensionContextFor(::envBackedMethod))
        }

    assertContains(
        exception.message!!,
        "Both annotation field schemaControlRegistryUri and environment variable SCHEMA_CONTROL_REGISTRY_URI are unset. Please specify one",
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
    val extension = Neo4jSourceExtension(environment::get)

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
    val extension = Neo4jSourceExtension(environment::get)

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
    val extension = Neo4jSourceExtension(environment::get)

    val exception =
        assertFailsWith<ExtensionConfigurationException> {
          extension.evaluateExecutionCondition(extensionContextFor(::envBackedMethod))
        }
    assertContains(
        exception.message!!,
        "Both annotation field neo4jPassword and environment variable NEO4J_PASSWORD are unset. Please specify one",
    )
  }

  @Neo4jSource(
      brokerExternalHost = "example.com",
      schemaControlRegistryUri = "http://example.com",
      schemaControlRegistryExternalUri = "http://example.com",
      kafkaConnectExternalUri = "http://example.com",
      neo4jExternalUri = "neo4j://example.com",
      neo4jUri = "neo4j://example.com",
      neo4jUser = "user",
      neo4jPassword = "password",
      topic = "topic",
      streamingProperty = "prop",
      startFrom = "ALL",
      query = "MERGE (:Example)",
  )
  @Suppress("UNUSED")
  fun validMethod() {}

  @Neo4jSource(
      brokerExternalHost = "example.com",
      schemaControlRegistryUri = "http://example.com",
      schemaControlRegistryExternalUri = "http://example.com",
      kafkaConnectExternalUri = "http://example.com",
      neo4jExternalUri = "neo4j://example.com",
      neo4jUri = "neo4j://example.com",
      neo4jUser = "user",
      neo4jPassword = "password",
      startFrom = "ALL",
      strategy = SourceStrategy.CDC,
      cdc =
          CdcSource(
              topics =
                  arrayOf(
                      CdcSourceTopic(
                          topic = "topic",
                          patterns =
                              arrayOf(
                                  CdcSourceParam("(:Person {+name})"),
                                  CdcSourceParam("(:Person)-[:WORKS_FOR]→(:Company)"),
                              ),
                      )
                  )
          ),
  )
  @Suppress("UNUSED")
  fun validCdcMethod() {}

  @Neo4jSource(
      brokerExternalHost = "example.com",
      schemaControlRegistryUri = "http://example.com",
      schemaControlRegistryExternalUri = "http://example.com",
      neo4jExternalUri = "neo4j://example.com",
      neo4jUri = "neo4j://example.com",
      neo4jUser = "user",
      neo4jPassword = "password",
      topic = "topic",
      streamingProperty = "prop",
      startFrom = "ALL",
      query = "MERGE (:Example)",
  )
  @Suppress("UNUSED")
  fun onlyKafkaConnectExternalUriFromEnvMethod() {}

  @Neo4jSource(
      topic = "topic",
      streamingProperty = "prop",
      startFrom = "ALL",
      query = "MERGE (:Example)",
  )
  @Suppress("UNUSED")
  fun envBackedMethod() {}

  @Suppress("UNUSED") fun missingAnnotationMethod() {}

  companion object {
    @JvmStatic
    fun validMethods(): Array<Array<Any>> =
        arrayOf(
            arrayOf("valid query settings", Neo4jSourceExtensionTest::validMethod),
            arrayOf("valid cdc settings", Neo4jSourceExtensionTest::validCdcMethod),
        )
  }
}
