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
package org.neo4j.connectors.kafka.testing.sink

import java.util.*
import kotlin.concurrent.atomics.AtomicBoolean
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.jvm.optionals.getOrNull
import kotlin.text.ifEmpty
import org.junit.jupiter.api.extension.AfterEachCallback
import org.junit.jupiter.api.extension.BeforeEachCallback
import org.junit.jupiter.api.extension.ConditionEvaluationResult
import org.junit.jupiter.api.extension.ExecutionCondition
import org.junit.jupiter.api.extension.ExtensionConfigurationException
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.api.extension.ParameterContext
import org.junit.jupiter.api.extension.ParameterResolver
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
import org.neo4j.connectors.kafka.testing.kafka.ConvertingKafkaProducer
import org.neo4j.connectors.kafka.testing.kafka.ProducerResolver
import org.neo4j.connectors.kafka.testing.kafka.TopicRegistry
import org.neo4j.driver.AuthToken
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Config
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.Logging
import org.neo4j.driver.Session
import org.neo4j.driver.SessionConfig
import org.slf4j.Logger
import org.slf4j.LoggerFactory

internal class SinkAnnotationResolvers(envAccessor: (String) -> String?) {
  val brokerExternalHost = AnnotationValueResolver(Neo4jSink::brokerExternalHost, envAccessor)

  val schemaControlRegistryUri =
      AnnotationValueResolver(Neo4jSink::schemaControlRegistryUri, envAccessor)

  val schemaControlRegistryExternalUri =
      AnnotationValueResolver(Neo4jSink::schemaControlRegistryExternalUri, envAccessor)

  val kafkaConnectExternalUri =
      AnnotationValueResolver(Neo4jSink::kafkaConnectExternalUri, envAccessor)

  val neo4jUri = AnnotationValueResolver(Neo4jSink::neo4jUri, envAccessor)

  val neo4jExternalUri = AnnotationValueResolver(Neo4jSink::neo4jExternalUri, envAccessor)

  val neo4jUser = AnnotationValueResolver(Neo4jSink::neo4jUser, envAccessor)

  val neo4jPassword = AnnotationValueResolver(Neo4jSink::neo4jPassword, envAccessor)

  val errorTolerance = AnnotationValueResolver(Neo4jSink::errorTolerance, envAccessor)

  val errorDlqTopic = AnnotationValueResolver(Neo4jSink::errorDlqTopic, envAccessor)

  val keyValueConverterResolver = KeyValueConverterResolver()

  val mandatorySettings =
      listOf(kafkaConnectExternalUri, schemaControlRegistryUri, neo4jUri, neo4jUser, neo4jPassword)
}

internal class Neo4jSinkExtension(
    // visible for testing
    envAccessor: (String) -> String? = System::getenv,
    private val driverFactory: (String, AuthToken) -> Driver = { url, token ->
      GraphDatabase.driver(url, token, Config.builder().withLogging(Logging.slf4j()).build())
    },
) : ExecutionCondition, BeforeEachCallback, AfterEachCallback, ParameterResolver {

  private val log: Logger = LoggerFactory.getLogger(Neo4jSinkExtension::class.java)

  private val paramResolvers =
      ParameterResolvers(
          mapOf(
              Session::class.java to ::resolveSession,
              ConvertingKafkaProducer::class.java to ::resolveGenericProducer,
              ConvertingKafkaConsumer::class.java to ::resolveGenericConsumer,
              Neo4jSinkRegistration::class.java to ::resolveSinkRegistration,
              Neo4j::class.java to ::resolveNeo4j,
          )
      )

  private val annotationResolvers = SinkAnnotationResolvers(envAccessor)

  private data class TestState(
      val driverFactory: (String, AuthToken) -> Driver,
      val sinkAnnotation: Neo4jSink,
      val resolvers: SinkAnnotationResolvers,
      private var driver: Driver? = null,
      var sink: Neo4jSinkRegistration? = null,
  ) : AutoCloseable {
    @OptIn(ExperimentalAtomicApi::class) val dbCreated = AtomicBoolean(false)
    val topicRegistry = TopicRegistry()
    val neo4jDatabase =
        sinkAnnotation.neo4jDatabase.ifEmpty { "test-" + UUID.randomUUID().toString() }

    fun driver(ensureDatabase: Boolean = true): Driver {
      if (driver != null) {
        return driver!!.also {
          if (ensureDatabase) {
            ensureDatabase(it)
          }
        }
      }

      val uri = resolvers.neo4jExternalUri.resolve(sinkAnnotation)
      val username = resolvers.neo4jUser.resolve(sinkAnnotation)
      val password = resolvers.neo4jPassword.resolve(sinkAnnotation)
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
        driver.createDatabase(neo4jDatabase)
      }
    }

    fun session(): Session {
      return driver().session(SessionConfig.forDatabase(neo4jDatabase))
    }

    fun producerResolver(): ProducerResolver {
      return ProducerResolver(
          resolvers.keyValueConverterResolver,
          topicRegistry,
          brokerExternalHostProvider = { resolvers.brokerExternalHost.resolve(sinkAnnotation) },
          schemaControlRegistryExternalUriProvider = {
            resolvers.schemaControlRegistryExternalUri.resolve(sinkAnnotation)
          },
          schemaControlKeyCompatibilityProvider = { sinkAnnotation.schemaControlKeyCompatibility },
          schemaControlValueCompatibilityProvider = {
            sinkAnnotation.schemaControlValueCompatibility
          },
      )
    }

    fun consumerResolver(): ConsumerResolver {
      return ConsumerResolver(
          resolvers.keyValueConverterResolver,
          topicRegistry,
          brokerExternalHostProvider = { resolvers.brokerExternalHost.resolve(sinkAnnotation) },
          schemaControlRegistryExternalUriProvider = {
            resolvers.schemaControlRegistryExternalUri.resolve(sinkAnnotation)
          },
          consumerFactory = ConsumerResolver::getSubscribedConsumer,
      )
    }

    @OptIn(ExperimentalAtomicApi::class)
    override fun close() {
      sink?.unregister()
      driver
          ?.also {
            if (dbCreated.load()) {
              it.dropDatabase(neo4jDatabase)
            }
          }
          ?.close()

      sink = null
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
                  AnnotationSupport.findAnnotation<Neo4jSink>(context)
                      ?: throw ExtensionConfigurationException("@Neo4jSink not found"),
                  annotationResolvers,
              )
            },
            TestState::class.java,
        )
  }

  override fun evaluateExecutionCondition(context: ExtensionContext): ConditionEvaluationResult {
    val state = getState(context)
    val metadata = state.sinkAnnotation

    val errors = mutableListOf<String>()
    annotationResolvers.mandatorySettings.forEach {
      if (!it.isValid(metadata)) {
        errors.add(it.errorMessage())
      }
    }

    val topicCount =
        metadata.cypher.size +
            metadata.cdcSchema.size +
            metadata.cdcSourceId.size +
            metadata.nodePattern.size +
            metadata.relationshipPattern.size +
            metadata.cud.size
    if (topicCount == 0) {
      errors.add("Expected at least one strategy to be defined")
    }

    val (topics, _) = buildStrategies(state)
    if (topics.distinct().size != topics.size) {
      errors.add("Same topic alias has been used within multiple sink strategies")
    }

    if (errors.isNotEmpty()) {
      throw ExtensionConfigurationException(
          "\nMissing settings, see details below:\n\t${errors.joinToString("\n\t")}"
      )
    }

    return ConditionEvaluationResult.enabled("@Neo4jSink and environment properly configured")
  }

  private fun buildStrategies(state: TestState): Pair<List<String>, Map<String, Any>> {
    val topics = mutableListOf<String>()
    val strategies = mutableMapOf<String, Any>()
    val metadata = state.sinkAnnotation

    metadata.cypher.forEach {
      val resolved = state.topicRegistry.resolveTopic(it.topic)
      topics.add(resolved)
      strategies["neo4j.cypher.topic.$resolved"] = it.query
      strategies["neo4j.cypher.bind-timestamp-as"] = it.bindTimestampAs
      strategies["neo4j.cypher.bind-header-as"] = it.bindHeaderAs
      strategies["neo4j.cypher.bind-key-as"] = it.bindKeyAs
      strategies["neo4j.cypher.bind-value-as"] = it.bindValueAs
      strategies["neo4j.cypher.bind-value-as-event"] = it.bindValueAsEvent
    }

    if (metadata.cdcSourceId.isNotEmpty()) {
      val resolved = metadata.cdcSourceId.map { state.topicRegistry.resolveTopic(it.topic) }
      topics.addAll(resolved)
      strategies["neo4j.cdc.source-id.topics"] = resolved.joinToString(",")

      val labelName = metadata.cdcSourceId.first().labelName
      if (labelName.isNotBlank()) {
        strategies["neo4j.cdc.source-id.label-name"] = labelName
      }
      val propertyName = metadata.cdcSourceId.first().propertyName
      if (propertyName.isNotBlank()) {
        strategies["neo4j.cdc.source-id.property-name"] = propertyName
      }
    }

    if (metadata.cdcSchema.isNotEmpty()) {
      val resolved = metadata.cdcSchema.map { state.topicRegistry.resolveTopic(it.topic) }
      topics.addAll(resolved)
      strategies["neo4j.cdc.schema.topics"] = resolved.joinToString(",")
    }

    metadata.nodePattern.forEach {
      val resolved = state.topicRegistry.resolveTopic(it.topic)
      topics.add(resolved)
      strategies["neo4j.pattern.topic.$resolved"] = it.pattern
      strategies["neo4j.pattern.merge-node-properties"] = it.mergeNodeProperties
    }

    metadata.relationshipPattern.forEach {
      val resolved = state.topicRegistry.resolveTopic(it.topic)
      topics.add(resolved)
      strategies["neo4j.pattern.topic.$resolved"] = it.pattern
      strategies["neo4j.pattern.merge-node-properties"] = it.mergeNodeProperties
      strategies["neo4j.pattern.merge-relationship-properties"] = it.mergeRelationshipProperties
    }

    if (metadata.cud.isNotEmpty()) {
      val resolved = metadata.cud.map { state.topicRegistry.resolveTopic(it.topic) }
      topics.addAll(resolved)
      strategies["neo4j.cud.topics"] = resolved.joinToString(",")
    }

    return topics to strategies
  }

  override fun beforeEach(context: ExtensionContext) {
    val state = getState(context)
    log.info(
        "Using database {} for test {}",
        state.neo4jDatabase,
        "${context.testClass?.getOrNull()?.simpleName}#${context.displayName}",
    )
    state.driver().verifyConnectivity()

    val (topics, strategies) = buildStrategies(state)

    state.sink =
        Neo4jSinkRegistration(
            neo4jUri = annotationResolvers.neo4jUri.resolve(state.sinkAnnotation),
            neo4jUser = annotationResolvers.neo4jUser.resolve(state.sinkAnnotation),
            neo4jPassword = annotationResolvers.neo4jPassword.resolve(state.sinkAnnotation),
            neo4jDatabase = state.neo4jDatabase,
            schemaControlRegistryUri =
                annotationResolvers.schemaControlRegistryUri.resolve(state.sinkAnnotation),
            keyConverter =
                annotationResolvers.keyValueConverterResolver.resolveKeyConverter(context),
            valueConverter =
                annotationResolvers.keyValueConverterResolver.resolveValueConverter(context),
            topics = topics.distinct(),
            strategies = strategies,
            excludeErrorHandling = state.sinkAnnotation.excludeErrorHandling,
            errorTolerance = annotationResolvers.errorTolerance.resolve(state.sinkAnnotation),
            errorDlqTopic =
                state.topicRegistry.resolveTopic(
                    annotationResolvers.errorDlqTopic.resolve(state.sinkAnnotation)
                ),
            enableErrorHeaders = state.sinkAnnotation.enableErrorHeaders,
        )
    state.sink!!.register(annotationResolvers.kafkaConnectExternalUri.resolve(state.sinkAnnotation))
    state.topicRegistry.log()
  }

  override fun afterEach(extensionContent: ExtensionContext) {
    val state = getState(extensionContent)
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

  private fun resolveNeo4j(parameterContext: ParameterContext, context: ExtensionContext): Neo4j {
    val state = getState(context)
    return Neo4jDetector.detect(state.driver())
  }

  private fun resolveGenericProducer(
      parameterContext: ParameterContext,
      extensionContext: ExtensionContext,
  ): Any {
    return getState(extensionContext)
        .producerResolver()
        .resolveGenericProducer(parameterContext, extensionContext)
  }

  private fun resolveGenericConsumer(
      parameterContext: ParameterContext,
      extensionContext: ExtensionContext,
  ): Any {
    return getState(extensionContext)
        .consumerResolver()
        .resolveGenericConsumer(parameterContext, extensionContext)
  }

  private fun resolveSinkRegistration(
      parameterContext: ParameterContext,
      extensionContext: ExtensionContext,
  ): Any {
    return getState(extensionContext).sink!!
  }
}
