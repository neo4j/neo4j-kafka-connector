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

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import java.net.URI
import java.util.*
import kotlin.jvm.optionals.getOrNull
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.junit.jupiter.api.extension.AfterEachCallback
import org.junit.jupiter.api.extension.BeforeEachCallback
import org.junit.jupiter.api.extension.ConditionEvaluationResult
import org.junit.jupiter.api.extension.ExecutionCondition
import org.junit.jupiter.api.extension.ExtensionConfigurationException
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.api.extension.ParameterContext
import org.junit.jupiter.api.extension.ParameterResolver
import org.neo4j.connectors.kafka.testing.AnnotationSupport
import org.neo4j.connectors.kafka.testing.AnnotationValueResolver
import org.neo4j.connectors.kafka.testing.DatabaseSupport.createDatabase
import org.neo4j.connectors.kafka.testing.DatabaseSupport.dropDatabase
import org.neo4j.connectors.kafka.testing.ParameterResolvers
import org.neo4j.connectors.kafka.testing.format.KeyValueConverterResolver
import org.neo4j.connectors.kafka.testing.kafka.ConvertingKafkaProducer
import org.neo4j.connectors.kafka.testing.kafka.TopicRegistry
import org.neo4j.driver.AuthToken
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.Session
import org.neo4j.driver.SessionConfig
import org.slf4j.Logger
import org.slf4j.LoggerFactory

internal class Neo4jSinkExtension(
    // visible for testing
    envAccessor: (String) -> String? = System::getenv,
    private val driverFactory: (String, AuthToken) -> Driver = GraphDatabase::driver
) : ExecutionCondition, BeforeEachCallback, AfterEachCallback, ParameterResolver {

  private val log: Logger = LoggerFactory.getLogger(Neo4jSinkExtension::class.java)

  private val paramResolvers =
      ParameterResolvers(
          mapOf(
              Session::class.java to ::resolveSession,
              ConvertingKafkaProducer::class.java to ::resolveGenericProducer,
          ))

  private lateinit var sinkAnnotation: Neo4jSink

  private lateinit var sink: Neo4jSinkRegistration

  private var driver: Driver? = null

  private var session: Session? = null

  private var neo4jDatabase: String? = null

  private val brokerExternalHost =
      AnnotationValueResolver(Neo4jSink::brokerExternalHost, envAccessor)

  private val schemaControlRegistryUri =
      AnnotationValueResolver(Neo4jSink::schemaControlRegistryUri, envAccessor)

  private val schemaControlRegistryExternalUri =
      AnnotationValueResolver(Neo4jSink::schemaControlRegistryExternalUri, envAccessor)

  private val kafkaConnectExternalUri =
      AnnotationValueResolver(Neo4jSink::kafkaConnectExternalUri, envAccessor)

  private val neo4jUri = AnnotationValueResolver(Neo4jSink::neo4jUri, envAccessor)

  private val neo4jExternalUri = AnnotationValueResolver(Neo4jSink::neo4jExternalUri, envAccessor)

  private val neo4jUser = AnnotationValueResolver(Neo4jSink::neo4jUser, envAccessor)

  private val neo4jPassword = AnnotationValueResolver(Neo4jSink::neo4jPassword, envAccessor)

  private val mandatorySettings =
      listOf(
          kafkaConnectExternalUri,
          schemaControlRegistryUri,
          neo4jUri,
          neo4jUser,
          neo4jPassword,
      )

  private val topicRegistry = TopicRegistry()

  private val keyValueConverterResolver = KeyValueConverterResolver()

  override fun evaluateExecutionCondition(context: ExtensionContext?): ConditionEvaluationResult {
    val metadata =
        AnnotationSupport.findAnnotation<Neo4jSink>(context)
            ?: throw ExtensionConfigurationException("@Neo4jSink not found")

    val errors = mutableListOf<String>()
    mandatorySettings.forEach {
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

    val (topics, _) = buildStrategies(metadata)
    if (topics.distinct().size != topics.size) {
      errors.add("Same topic alias has been used within multiple sink strategies")
    }

    if (errors.isNotEmpty()) {
      throw ExtensionConfigurationException(
          "\nMissing settings, see details below:\n\t${errors.joinToString("\n\t")}",
      )
    }
    this.sinkAnnotation = metadata

    return ConditionEvaluationResult.enabled("@Neo4jSink and environment properly configured")
  }

  fun buildStrategies(metadata: Neo4jSink): Pair<List<String>, Map<String, Any>> {
    val topics = mutableListOf<String>()
    val strategies = mutableMapOf<String, Any>()

    metadata.cypher.forEach {
      val resolved = topicRegistry.resolveTopic(it.topic)
      topics.add(resolved)
      strategies["neo4j.cypher.topic.$resolved"] = it.query
      strategies["neo4j.cypher.bind-timestamp-as"] = it.bindTimestampAs
      strategies["neo4j.cypher.bind-header-as"] = it.bindHeaderAs
      strategies["neo4j.cypher.bind-key-as"] = it.bindKeyAs
      strategies["neo4j.cypher.bind-value-as"] = it.bindValueAs
      strategies["neo4j.cypher.bind-value-as-event"] = it.bindValueAsEvent
    }

    if (metadata.cdcSourceId.isNotEmpty()) {
      val resolved = metadata.cdcSourceId.map { topicRegistry.resolveTopic(it.topic) }
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
      val resolved = metadata.cdcSchema.map { topicRegistry.resolveTopic(it.topic) }
      topics.addAll(resolved)
      strategies["neo4j.cdc.schema.topics"] = resolved.joinToString(",")
    }

    metadata.nodePattern.forEach {
      val resolved = topicRegistry.resolveTopic(it.topic)
      topics.add(resolved)
      strategies["neo4j.pattern.node.topic.$resolved"] = it.pattern
      strategies["neo4j.pattern.node.merge-properties"] = it.mergeNodeProperties
    }

    metadata.relationshipPattern.forEach {
      val resolved = topicRegistry.resolveTopic(it.topic)
      topics.add(resolved)
      strategies["neo4j.pattern.relationship.topic.$resolved"] = it.pattern
      strategies["neo4j.pattern.node.merge-properties"] = it.mergeNodeProperties
      strategies["neo4j.pattern.relationship.merge-properties"] = it.mergeRelationshipProperties
    }

    if (metadata.cud.isNotEmpty()) {
      val resolved = metadata.cud.map { topicRegistry.resolveTopic(it.topic) }
      topics.addAll(resolved)
      strategies["neo4j.cud.topics"] = resolved.joinToString(",")
    }

    return topics to strategies
  }

  override fun beforeEach(context: ExtensionContext?) {
    ensureDatabase(context)

    val (topics, strategies) = buildStrategies(sinkAnnotation)

    sink =
        Neo4jSinkRegistration(
            neo4jUri = neo4jUri.resolve(sinkAnnotation),
            neo4jUser = neo4jUser.resolve(sinkAnnotation),
            neo4jPassword = neo4jPassword.resolve(sinkAnnotation),
            neo4jDatabase = neo4jDatabase!!,
            schemaControlRegistryUri = schemaControlRegistryUri.resolve(sinkAnnotation),
            keyConverter = keyValueConverterResolver.resolveKeyConverter(context),
            valueConverter = keyValueConverterResolver.resolveValueConverter(context),
            topics = topics.distinct(),
            strategies = strategies)
    sink.register(kafkaConnectExternalUri.resolve(sinkAnnotation))
    topicRegistry.log()
  }

  override fun afterEach(extensionContent: ExtensionContext?) {
    if (driver != null) {
      if (sinkAnnotation.dropDatabase) {
        session?.dropDatabase(neo4jDatabase!!)?.close()
      }
      driver!!.close()
    } else if (sinkAnnotation.dropDatabase) {
      createDriver().use { dr -> dr.session().use { it.dropDatabase(neo4jDatabase!!) } }
    }
    sink.unregister()

    topicRegistry.clear()
    neo4jDatabase = null
    driver = null
    session = null
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

  private fun resolveSession(
      @Suppress("UNUSED_PARAMETER") parameterContext: ParameterContext?,
      extensionContext: ExtensionContext?
  ): Any {
    ensureDatabase(extensionContext)
    driver = driver ?: createDriver()
    session = driver!!.session(SessionConfig.forDatabase(neo4jDatabase))
    return session!!
  }

  private fun createDriver(): Driver {
    val uri = neo4jExternalUri.resolve(sinkAnnotation)
    val username = neo4jUser.resolve(sinkAnnotation)
    val password = neo4jPassword.resolve(sinkAnnotation)
    return driverFactory(uri, AuthTokens.basic(username, password))
  }

  private fun ensureDatabase(context: ExtensionContext?) {
    if (neo4jDatabase != null) {
      return
    }
    neo4jDatabase = sinkAnnotation.neo4jDatabase.ifEmpty { "test-" + UUID.randomUUID().toString() }
    log.debug(
        "Using database {} for test {}",
        neo4jDatabase,
        "${context?.testClass?.getOrNull()?.simpleName}#${context?.displayName}",
    )
    driver = driver ?: createDriver()
    driver?.apply {
      this.verifyConnectivity()
      this.session(SessionConfig.forDatabase("system")).use { session ->
        session.createDatabase(neo4jDatabase!!)
      }
    }
  }

  private fun resolveProducer(
      @Suppress("UNUSED_PARAMETER") parameterContext: ParameterContext?,
      extensionContext: ExtensionContext?
  ): KafkaProducer<Any, Any> {
    val properties = Properties()
    properties.setProperty(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
        brokerExternalHost.resolve(sinkAnnotation),
    )
    val keyConverter = keyValueConverterResolver.resolveKeyConverter(extensionContext)
    val valueConverter = keyValueConverterResolver.resolveValueConverter(extensionContext)
    if (keyConverter.supportsSchemaRegistry || valueConverter.supportsSchemaRegistry) {
      properties.setProperty(
          KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
          schemaControlRegistryExternalUri.resolve(sinkAnnotation),
      )
    }
    properties.setProperty(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        keyConverter.serializerClass.name,
    )
    properties.setProperty(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        valueConverter.serializerClass.name,
    )
    return KafkaProducer<Any, Any>(properties)
  }

  private fun resolveGenericProducer(
      parameterContext: ParameterContext?,
      extensionContext: ExtensionContext?
  ): Any {
    val producerAnnotation = parameterContext?.parameter?.getAnnotation(TopicProducer::class.java)!!
    return ConvertingKafkaProducer(
        schemaRegistryURI = URI(schemaControlRegistryExternalUri.resolve(sinkAnnotation)),
        keyConverter = keyValueConverterResolver.resolveKeyConverter(extensionContext),
        keyCompatibilityMode = sinkAnnotation.schemaControlKeyCompatibility,
        valueConverter = keyValueConverterResolver.resolveValueConverter(extensionContext),
        valueCompatibilityMode = sinkAnnotation.schemaControlValueCompatibility,
        kafkaProducer = resolveProducer(parameterContext, extensionContext),
        topic = topicRegistry.resolveTopic(producerAnnotation.topic))
  }
}
