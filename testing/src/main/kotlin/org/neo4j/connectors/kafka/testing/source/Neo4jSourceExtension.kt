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

import io.kotest.assertions.nondeterministic.eventually
import io.kotest.matchers.shouldBe
import java.util.*
import kotlin.jvm.optionals.getOrNull
import kotlin.reflect.KProperty1
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.junit.jupiter.api.extension.AfterEachCallback
import org.junit.jupiter.api.extension.BeforeEachCallback
import org.junit.jupiter.api.extension.ConditionEvaluationResult
import org.junit.jupiter.api.extension.ExecutionCondition
import org.junit.jupiter.api.extension.ExtensionConfigurationException
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.api.extension.ParameterContext
import org.junit.jupiter.api.extension.ParameterResolver
import org.junit.jupiter.api.extension.TestExecutionExceptionHandler
import org.neo4j.caniuse.CanIUse.canIUse
import org.neo4j.caniuse.Dbms
import org.neo4j.caniuse.Neo4j
import org.neo4j.caniuse.Neo4jDetector
import org.neo4j.connectors.kafka.testing.AnnotationSupport
import org.neo4j.connectors.kafka.testing.AnnotationValueResolver
import org.neo4j.connectors.kafka.testing.DatabaseSupport.createDatabase
import org.neo4j.connectors.kafka.testing.DatabaseSupport.dropDatabase
import org.neo4j.connectors.kafka.testing.ParameterResolvers
import org.neo4j.connectors.kafka.testing.format.KeyValueConverterResolver
import org.neo4j.connectors.kafka.testing.kafka.ConsumerResolver
import org.neo4j.connectors.kafka.testing.kafka.ConvertingKafkaConsumer
import org.neo4j.connectors.kafka.testing.kafka.TopicRegistry
import org.neo4j.driver.AuthToken
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.Session
import org.neo4j.driver.SessionConfig
import org.slf4j.Logger
import org.slf4j.LoggerFactory

internal class Neo4jSourceExtension(
    // visible for testing
    envAccessor: (String) -> String? = System::getenv,
    private val driverFactory: (String, AuthToken) -> Driver = GraphDatabase::driver,
    consumerFactory: (Properties, String) -> KafkaConsumer<ByteArray, ByteArray> =
        { properties, topic ->
          ConsumerResolver.getSubscribedConsumer(properties, topic)
        },
) :
    ExecutionCondition,
    BeforeEachCallback,
    AfterEachCallback,
    ParameterResolver,
    TestExecutionExceptionHandler {

  private val log: Logger = LoggerFactory.getLogger(Neo4jSourceExtension::class.java)

  private val paramResolvers =
      ParameterResolvers(
          mapOf(
              Session::class.java to ::resolveSession,
              ConvertingKafkaConsumer::class.java to ::resolveTopicConsumer,
              Neo4j::class.java to ::resolveNeo4j,
          ),
      )

  private lateinit var sourceAnnotation: Neo4jSource

  private lateinit var source: Neo4jSourceRegistration

  private lateinit var driver: Driver

  private lateinit var session: Session

  private lateinit var neo4jDatabase: String

  private var testFailed: Boolean = false

  private val brokerExternalHost =
      AnnotationValueResolver(Neo4jSource::brokerExternalHost, envAccessor)

  private val schemaRegistryUri =
      AnnotationValueResolver(Neo4jSource::schemaControlRegistryUri, envAccessor)

  private val schemaControlRegistryExternalUri =
      AnnotationValueResolver(Neo4jSource::schemaControlRegistryExternalUri, envAccessor)

  private val kafkaConnectExternalUri =
      AnnotationValueResolver(Neo4jSource::kafkaConnectExternalUri, envAccessor)

  private val neo4jUri = AnnotationValueResolver(Neo4jSource::neo4jUri, envAccessor)

  private val neo4jExternalUri = AnnotationValueResolver(Neo4jSource::neo4jExternalUri, envAccessor)

  private val neo4jUser = AnnotationValueResolver(Neo4jSource::neo4jUser, envAccessor)

  private val neo4jPassword = AnnotationValueResolver(Neo4jSource::neo4jPassword, envAccessor)

  private val mandatorySettings =
      listOf(
          brokerExternalHost,
          schemaRegistryUri,
          neo4jUri,
          neo4jUser,
          neo4jPassword,
      )

  private val topicRegistry = TopicRegistry()

  private val keyValueConverterResolver = KeyValueConverterResolver()

  private val consumerResolver =
      ConsumerResolver(
          keyValueConverterResolver,
          topicRegistry,
          brokerExternalHostProvider = { brokerExternalHost.resolve(sourceAnnotation) },
          schemaControlRegistryExternalUriProvider = {
            schemaControlRegistryExternalUri.resolve(sourceAnnotation)
          },
          consumerFactory)

  override fun evaluateExecutionCondition(context: ExtensionContext?): ConditionEvaluationResult {
    val metadata =
        AnnotationSupport.findAnnotation<Neo4jSource>(context)
            ?: throw ExtensionConfigurationException("@Neo4jSource not found")

    val errors = mutableListOf<String>()
    mandatorySettings.forEach {
      if (!it.isValid(metadata)) {
        errors.add(it.errorMessage())
      }
    }
    if (errors.isNotEmpty()) {
      throw ExtensionConfigurationException(
          "\nMissing settings, see details below:\n\t${errors.joinToString("\n\t")}",
      )
    }

    this.sourceAnnotation = metadata

    if (metadata.strategy == SourceStrategy.CDC) {
      createDriver().use {
        var version = Neo4jDetector.detect(it)
        if (!canIUse(Dbms.changeDataCapture()).withNeo4j(version)) {
          return ConditionEvaluationResult.disabled(
              "CDC is not available with this version of Neo4j: $version",
          )
        }
      }
    }

    return ConditionEvaluationResult.enabled("@Neo4jSource and environment properly configured")
  }

  override fun beforeEach(context: ExtensionContext?) {
    ensureDatabase(context)

    source =
        Neo4jSourceRegistration(
            schemaControlRegistryUri = schemaRegistryUri.resolve(sourceAnnotation),
            neo4jUri = neo4jUri.resolve(sourceAnnotation),
            neo4jUser = neo4jUser.resolve(sourceAnnotation),
            neo4jPassword = neo4jPassword.resolve(sourceAnnotation),
            neo4jDatabase = neo4jDatabase,
            topic = topicRegistry.resolveTopic(sourceAnnotation.topic),
            streamingProperty = sourceAnnotation.streamingProperty,
            startFrom = sourceAnnotation.startFrom,
            startFromValue = sourceAnnotation.startFromValue,
            query = sourceAnnotation.query,
            strategy = sourceAnnotation.strategy,
            keyConverter = keyValueConverterResolver.resolveKeyConverter(context),
            valueConverter = keyValueConverterResolver.resolveValueConverter(context),
            cdcPatternsIndexed = sourceAnnotation.cdc.patternsIndexed,
            cdcPatterns = sourceAnnotation.cdc.paramAsMap(CdcSourceTopic::patterns),
            cdcOperations = sourceAnnotation.cdc.paramAsMap(CdcSourceTopic::operations),
            cdcChangesTo = sourceAnnotation.cdc.paramAsMap(CdcSourceTopic::changesTo),
            cdcMetadata = sourceAnnotation.cdc.metadataAsMap(),
            cdcKeySerializations = sourceAnnotation.cdc.keySerializationsAsMap(),
            cdcValueSerializations = sourceAnnotation.cdc.valueSerializationsAsMap(),
            payloadMode = keyValueConverterResolver.resolvePayloadMode(context))

    source.register(kafkaConnectExternalUri.resolve(sourceAnnotation))
    log.info("registered source connector with name {}", source.name)
    topicRegistry.log()

    testFailed = false
  }

  override fun handleTestExecutionException(context: ExtensionContext?, throwable: Throwable) {
    // we do not drop database if the test fails
    testFailed = true

    throw throwable
  }

  override fun afterEach(context: ExtensionContext?) {
    source.unregister()
    if (this::driver.isInitialized) {
      if (!testFailed) {
        driver.session(SessionConfig.forDatabase("system")).use { it.dropDatabase(neo4jDatabase) }
      }
      session.close()
      driver.close()
    } else {
      if (!testFailed) {
        createDriver().use { dr ->
          dr.session(SessionConfig.forDatabase("system")).use { it.dropDatabase(neo4jDatabase) }
        }
      }
    }
  }

  private fun resolveSession(
      @Suppress("UNUSED_PARAMETER") parameterContext: ParameterContext?,
      extensionContext: ExtensionContext?
  ): Any {
    ensureDatabase(extensionContext)
    driver = createDriver()
    session = driver.session(SessionConfig.forDatabase(neo4jDatabase))
    return session
  }

  private fun createDriver(): Driver {
    val uri = neo4jExternalUri.resolve(sourceAnnotation)
    val username = neo4jUser.resolve(sourceAnnotation)
    val password = neo4jPassword.resolve(sourceAnnotation)
    return driverFactory(uri, AuthTokens.basic(username, password))
  }

  private fun ensureDatabase(context: ExtensionContext?) {
    if (this::neo4jDatabase.isInitialized) {
      return
    }
    neo4jDatabase =
        sourceAnnotation.neo4jDatabase.ifEmpty { "test-" + UUID.randomUUID().toString() }
    log.info(
        "Using database {} for test {}",
        neo4jDatabase,
        "${context?.testClass?.getOrNull()?.simpleName}#${context?.displayName}",
    )
    createDriver().use { driver ->
      driver.session(SessionConfig.forDatabase("system")).use { session ->
        session.createDatabase(
            neo4jDatabase, withCdc = sourceAnnotation.strategy == SourceStrategy.CDC)
      }

      // want to make sure that CDC is functional before running the test
      if (sourceAnnotation.strategy == SourceStrategy.CDC) {
        log.info("waiting cdc to be available")
        runBlocking {
          eventually(30.seconds) {
            driver.session(SessionConfig.forDatabase(neo4jDatabase)).use { session ->
              try {
                val earliest = session.run("CALL db.cdc.earliest").single().get(0).asString()
                val count =
                    session
                        .run("CALL db.cdc.query(${'$'}from)", mapOf("from" to earliest))
                        .list()
                        .count()

                count shouldBe 0
              } catch (e: Exception) {
                log.debug("error received while waiting for cdc to be available", e)
              }
            }
          }
        }
        log.info("cdc is available")
      }
    }
  }

  private fun resolveTopicConsumer(
      parameterContext: ParameterContext?,
      context: ExtensionContext?
  ): ConvertingKafkaConsumer {
    return consumerResolver.resolveGenericConsumer(parameterContext, context)
  }

  private fun resolveNeo4j(parameterContext: ParameterContext?, context: ExtensionContext?): Neo4j {
    if (!::driver.isInitialized) {
      driver = createDriver()
    }
    return Neo4jDetector.detect(driver)
  }

  private fun CdcSource.paramAsMap(
      property: KProperty1<CdcSourceTopic, Array<CdcSourceParam>>
  ): Map<String, List<String>> {
    val result = mutableMapOf<String, MutableList<String>>()
    this.topics.forEach { topic ->
      property.get(topic).forEach { param ->
        val actualTopic = topicRegistry.resolveTopic(topic.topic)
        result.computeIfAbsent(actualTopic) { mutableListOf() }.add(param.index, param.value)
      }
    }
    return result
  }

  private fun CdcSource.metadataAsMap(): Map<String, List<Map<String, String>>> {
    val result = mutableMapOf<String, MutableList<MutableMap<String, String>>>()
    this.topics.forEach { topic ->
      topic.metadata.forEach { metadata ->
        val actualTopic = topicRegistry.resolveTopic(topic.topic)
        val metadataForIndex = result.computeIfAbsent(actualTopic) { mutableListOf() }
        val metadataByKey = metadataForIndex.getOrElse(metadata.index) { mutableMapOf() }
        if (metadataByKey.isEmpty()) {
          metadataForIndex.add(metadata.index, metadataByKey)
        }
        metadataByKey[metadata.key] = metadata.value
      }
    }
    return result
  }

  private fun CdcSource.keySerializationsAsMap(): Map<String, String> {
    return this.topics
        .groupBy { it.topic }
        .mapKeys { entry -> topicRegistry.resolveTopic(entry.key) }
        .mapValues { entry -> entry.value.map { it.keySerializationStrategy }.single() }
  }

  private fun CdcSource.valueSerializationsAsMap(): Map<String, String> {
    return this.topics
        .groupBy { it.topic }
        .mapKeys { entry -> topicRegistry.resolveTopic(entry.key) }
        .mapValues { entry -> entry.value.map { it.valueSerializationStrategy }.single() }
  }

  override fun supportsParameter(
      parameterContext: ParameterContext?,
      extensionContext: ExtensionContext?
  ): Boolean {
    return paramResolvers.supportsParameter(parameterContext, extensionContext)
  }

  override fun resolveParameter(
      parameterContext: ParameterContext?,
      extensionContext: ExtensionContext?
  ): Any {
    return paramResolvers.resolveParameter(parameterContext, extensionContext)
  }
}
