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

import java.util.*
import kotlin.concurrent.atomics.AtomicBoolean
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.jvm.optionals.getOrNull
import kotlin.reflect.KProperty1
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
import org.neo4j.connectors.kafka.configuration.PayloadMode
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

internal class SourceAnnotationResolvers(envAccessor: (String) -> String?) {
  val brokerExternalHost = AnnotationValueResolver(Neo4jSource::brokerExternalHost, envAccessor)

  val schemaControlRegistryUri =
      AnnotationValueResolver(Neo4jSource::schemaControlRegistryUri, envAccessor)

  val schemaControlRegistryExternalUri =
      AnnotationValueResolver(Neo4jSource::schemaControlRegistryExternalUri, envAccessor)

  val kafkaConnectExternalUri =
      AnnotationValueResolver(Neo4jSource::kafkaConnectExternalUri, envAccessor)

  val neo4jUri = AnnotationValueResolver(Neo4jSource::neo4jUri, envAccessor)

  val neo4jExternalUri = AnnotationValueResolver(Neo4jSource::neo4jExternalUri, envAccessor)

  val neo4jUser = AnnotationValueResolver(Neo4jSource::neo4jUser, envAccessor)

  val neo4jPassword = AnnotationValueResolver(Neo4jSource::neo4jPassword, envAccessor)

  val keyValueConverterResolver = KeyValueConverterResolver()

  val mandatorySettings =
      listOf(brokerExternalHost, schemaControlRegistryUri, neo4jUri, neo4jUser, neo4jPassword)
}

internal class Neo4jSourceExtension(
    // visible for testing
    envAccessor: (String) -> String? = System::getenv,
    private val driverFactory: (String, AuthToken) -> Driver = GraphDatabase::driver,
    private val consumerFactory: (Properties, String) -> KafkaConsumer<ByteArray, ByteArray> =
        { properties, topic ->
          ConsumerResolver.getSubscribedConsumer(properties, topic)
        },
) :
    ExecutionCondition,
    BeforeEachCallback,
    AfterEachCallback,
    ParameterResolver,
    TestExecutionExceptionHandler {

  companion object {
    private val log: Logger = LoggerFactory.getLogger(Neo4jSourceExtension::class.java)
  }

  private val paramResolvers =
      ParameterResolvers(
          mapOf(
              Session::class.java to ::resolveSession,
              ConvertingKafkaConsumer::class.java to ::resolveTopicConsumer,
              Neo4j::class.java to ::resolveNeo4j,
              PayloadMode::class.java to ::resolvePayloadMode,
          )
      )

  private val annotationResolvers = SourceAnnotationResolvers(envAccessor)

  private data class TestState(
      val driverFactory: (String, AuthToken) -> Driver,
      val consumerFactory: (Properties, String) -> KafkaConsumer<ByteArray, ByteArray>,
      val sourceAnnotation: Neo4jSource,
      val resolvers: SourceAnnotationResolvers,
      private var driver: Driver? = null,
      var source: Neo4jSourceRegistration? = null,
      var testFailed: Boolean = false,
  ) : AutoCloseable {
    @OptIn(ExperimentalAtomicApi::class) val dbCreated = AtomicBoolean(false)
    val topicRegistry = TopicRegistry()
    val neo4jDatabase =
        sourceAnnotation.neo4jDatabase.ifEmpty { "test-" + UUID.randomUUID().toString() }

    @OptIn(ExperimentalAtomicApi::class)
    fun driver(ensureDatabase: Boolean = true): Driver {
      if (driver != null) {
        return driver!!.also {
          if (ensureDatabase) {
            ensureDatabase(it)
          }
        }
      }

      val uri = resolvers.neo4jExternalUri.resolve(sourceAnnotation)
      val username = resolvers.neo4jUser.resolve(sourceAnnotation)
      val password = resolvers.neo4jPassword.resolve(sourceAnnotation)
      driver =
          driverFactory(uri, AuthTokens.basic(username, password)).also {
            it.verifyConnectivity()
            if (ensureDatabase) {
              ensureDatabase(it)
            }
          }
      return driver!!
    }

    @OptIn(ExperimentalAtomicApi::class)
    private fun ensureDatabase(driver: Driver) {
      if (dbCreated.compareAndSet(expectedValue = false, newValue = true)) {
        driver.createDatabase(
            neo4jDatabase,
            withCdc = sourceAnnotation.strategy == SourceStrategy.CDC,
        )
      }
    }

    fun session(): Session {
      return driver().session(SessionConfig.forDatabase(neo4jDatabase))
    }

    fun consumerResolver(): ConsumerResolver {
      return ConsumerResolver(
          resolvers.keyValueConverterResolver,
          topicRegistry,
          brokerExternalHostProvider = { resolvers.brokerExternalHost.resolve(sourceAnnotation) },
          schemaControlRegistryExternalUriProvider = {
            resolvers.schemaControlRegistryExternalUri.resolve(sourceAnnotation)
          },
          consumerFactory,
      )
    }

    @OptIn(ExperimentalAtomicApi::class)
    override fun close() {
      source?.unregister()
      if (!testFailed) {
        if (dbCreated.load()) {
          driver?.dropDatabase(neo4jDatabase)
        }
      }
      driver?.close()

      source = null
      driver = null
    }
  }

  private fun getStore(context: ExtensionContext): ExtensionContext.Store {
    return context.getStore(
        ExtensionContext.Namespace.create(
            javaClass,
            context.requiredTestClass,
            context.requiredTestMethod,
        )
    )
  }

  private fun getState(context: ExtensionContext): TestState {
    return getStore(context)
        .getOrComputeIfAbsent(
            "state",
            {
              TestState(
                  driverFactory,
                  consumerFactory,
                  AnnotationSupport.findAnnotation<Neo4jSource>(context)
                      ?: throw ExtensionConfigurationException("@Neo4jSource not found"),
                  annotationResolvers,
              )
            },
            TestState::class.java,
        )
  }

  override fun evaluateExecutionCondition(context: ExtensionContext): ConditionEvaluationResult {
    val state = getState(context)
    val metadata = state.sourceAnnotation

    val errors = mutableListOf<String>()
    annotationResolvers.mandatorySettings.forEach {
      if (!it.isValid(metadata)) {
        errors.add(it.errorMessage())
      }
    }
    if (errors.isNotEmpty()) {
      throw ExtensionConfigurationException(
          "\nMissing settings, see details below:\n\t${errors.joinToString("\n\t")}"
      )
    }

    if (metadata.strategy == SourceStrategy.CDC) {
      state.driver(false).also {
        val version = Neo4jDetector.detect(it)
        if (!canIUse(Dbms.changeDataCapture()).withNeo4j(version)) {
          return ConditionEvaluationResult.disabled(
              "CDC is not available with this version of Neo4j: $version"
          )
        }
      }
    }

    return ConditionEvaluationResult.enabled("@Neo4jSource and environment properly configured")
  }

  override fun beforeEach(context: ExtensionContext) {
    val state = getState(context)
    log.info(
        "Using database {} for test {}",
        state.neo4jDatabase,
        "${context.testClass?.getOrNull()?.simpleName}#${context.displayName}",
    )
    state.driver().verifyConnectivity()

    state.source =
        Neo4jSourceRegistration(
            schemaControlRegistryUri =
                annotationResolvers.schemaControlRegistryUri.resolve(state.sourceAnnotation),
            neo4jUri = annotationResolvers.neo4jUri.resolve(state.sourceAnnotation),
            neo4jUser = annotationResolvers.neo4jUser.resolve(state.sourceAnnotation),
            neo4jPassword = annotationResolvers.neo4jPassword.resolve(state.sourceAnnotation),
            neo4jDatabase = state.neo4jDatabase,
            topic = state.topicRegistry.resolveTopic(state.sourceAnnotation.topic),
            streamingProperty = state.sourceAnnotation.streamingProperty,
            startFrom = state.sourceAnnotation.startFrom,
            startFromValue = state.sourceAnnotation.startFromValue,
            query = state.sourceAnnotation.query,
            forceMapsAsStruct = state.sourceAnnotation.forceMapsAsStruct,
            strategy = state.sourceAnnotation.strategy,
            keyConverter =
                annotationResolvers.keyValueConverterResolver.resolveKeyConverter(context),
            valueConverter =
                annotationResolvers.keyValueConverterResolver.resolveValueConverter(context),
            cdcPatternsIndexed = state.sourceAnnotation.cdc.patternsIndexed,
            cdcPatterns =
                state.sourceAnnotation.cdc.paramAsMap(
                    state.topicRegistry,
                    CdcSourceTopic::patterns,
                ),
            cdcOperations =
                state.sourceAnnotation.cdc.paramAsMap(
                    state.topicRegistry,
                    CdcSourceTopic::operations,
                ),
            cdcChangesTo =
                state.sourceAnnotation.cdc.paramAsMap(
                    state.topicRegistry,
                    CdcSourceTopic::changesTo,
                ),
            cdcMetadata = state.sourceAnnotation.cdc.metadataAsMap(state.topicRegistry),
            cdcKeySerializations =
                state.sourceAnnotation.cdc.keySerializationsAsMap(state.topicRegistry),
            cdcValueSerializations =
                state.sourceAnnotation.cdc.valueSerializationsAsMap(state.topicRegistry),
            payloadMode = annotationResolvers.keyValueConverterResolver.resolvePayloadMode(context),
        )

    state.source!!.register(
        annotationResolvers.kafkaConnectExternalUri.resolve(state.sourceAnnotation)
    )
    log.info("registered source connector with name {}", state.source!!.name)
    state.topicRegistry.log()

    state.testFailed = false
  }

  override fun handleTestExecutionException(context: ExtensionContext, throwable: Throwable) {
    val state = getState(context)
    state.testFailed = true
    throw throwable
  }

  override fun afterEach(context: ExtensionContext) {
    val state = getState(context)
    state.close()
  }

  override fun supportsParameter(
      parameterContext: ParameterContext,
      extensionContext: ExtensionContext,
  ): Boolean {
    return paramResolvers.supportsParameter(parameterContext, extensionContext)
  }

  override fun resolveParameter(
      parameterContext: ParameterContext,
      extensionContext: ExtensionContext,
  ): Any {
    return paramResolvers.resolveParameter(parameterContext, extensionContext)
  }

  private fun resolveSession(
      @Suppress("UNUSED_PARAMETER") parameterContext: ParameterContext,
      extensionContext: ExtensionContext,
  ): Any {
    val state = getState(extensionContext)
    return state.session()
  }

  private fun resolveNeo4j(
      @Suppress("UNUSED_PARAMETER") parameterContext: ParameterContext,
      extensionContext: ExtensionContext,
  ): Neo4j {
    val state = getState(extensionContext)
    return Neo4jDetector.detect(state.driver())
  }

  private fun resolveTopicConsumer(
      parameterContext: ParameterContext,
      extensionContext: ExtensionContext,
  ): ConvertingKafkaConsumer {
    return getState(extensionContext)
        .consumerResolver()
        .resolveGenericConsumer(parameterContext, extensionContext)
  }

  private fun resolvePayloadMode(
      @Suppress("UNUSED_PARAMETER") parameterContext: ParameterContext,
      extensionContext: ExtensionContext,
  ): PayloadMode {
    return annotationResolvers.keyValueConverterResolver.resolvePayloadMode(extensionContext)
  }

  private fun CdcSource.paramAsMap(
      topicRegistry: TopicRegistry,
      property: KProperty1<CdcSourceTopic, Array<CdcSourceParam>>,
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

  private fun CdcSource.metadataAsMap(
      topicRegistry: TopicRegistry
  ): Map<String, List<Map<String, String>>> {
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

  private fun CdcSource.keySerializationsAsMap(topicRegistry: TopicRegistry): Map<String, String> {
    return this.topics
        .groupBy { it.topic }
        .mapKeys { entry -> topicRegistry.resolveTopic(entry.key) }
        .mapValues { entry -> entry.value.map { it.keySerializationStrategy }.single() }
  }

  private fun CdcSource.valueSerializationsAsMap(
      topicRegistry: TopicRegistry
  ): Map<String, String> {
    return this.topics
        .groupBy { it.topic }
        .mapKeys { entry -> topicRegistry.resolveTopic(entry.key) }
        .mapValues { entry -> entry.value.map { it.valueSerializationStrategy }.single() }
  }
}
