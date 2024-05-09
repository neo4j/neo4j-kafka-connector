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

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import java.util.*
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
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
import org.neo4j.connectors.kafka.testing.ParameterResolvers
import org.neo4j.connectors.kafka.testing.format.KeyValueConverterResolver
import org.neo4j.connectors.kafka.testing.kafka.ConvertingKafkaConsumer
import org.neo4j.connectors.kafka.testing.kafka.TopicRegistry
import org.neo4j.driver.AuthToken
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.Session

internal class LegacyNeo4jSourceExtension(
    // visible for testing
    envAccessor: (String) -> String? = System::getenv,
    private val driverFactory: (String, AuthToken) -> Driver = GraphDatabase::driver,
    private val consumerFactory: (Properties, String) -> KafkaConsumer<*, *> =
        ::getSubscribedConsumer,
) : ExecutionCondition, BeforeEachCallback, AfterEachCallback, ParameterResolver {

  private val paramResolvers =
      ParameterResolvers(
          mapOf(
              Session::class.java to ::resolveSession,
              ConvertingKafkaConsumer::class.java to ::resolveTopicConsumer,
          ))

  private lateinit var sourceAnnotation: LegacyNeo4jSource

  private lateinit var source: LegacyNeo4jSourceRegistration

  private lateinit var driver: Driver

  private lateinit var session: Session

  private val brokerExternalHost =
      AnnotationValueResolver(LegacyNeo4jSource::brokerExternalHost, envAccessor)

  private val schemaRegistryUri =
      AnnotationValueResolver(LegacyNeo4jSource::schemaControlRegistryUri, envAccessor)

  private val schemaControlRegistryExternalUri =
      AnnotationValueResolver(LegacyNeo4jSource::schemaControlRegistryExternalUri, envAccessor)

  private val kafkaConnectExternalUri =
      AnnotationValueResolver(LegacyNeo4jSource::kafkaConnectExternalUri, envAccessor)

  private val neo4jUri = AnnotationValueResolver(LegacyNeo4jSource::neo4jUri, envAccessor)

  private val neo4jExternalUri =
      AnnotationValueResolver(LegacyNeo4jSource::neo4jExternalUri, envAccessor)

  private val neo4jUser = AnnotationValueResolver(LegacyNeo4jSource::neo4jUser, envAccessor)

  private val neo4jPassword = AnnotationValueResolver(LegacyNeo4jSource::neo4jPassword, envAccessor)

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

  override fun evaluateExecutionCondition(context: ExtensionContext?): ConditionEvaluationResult {
    val metadata =
        AnnotationSupport.findAnnotation<LegacyNeo4jSource>(context)
            ?: throw ExtensionConfigurationException("@LegacyNeo4jSource not found")

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
    return ConditionEvaluationResult.enabled(
        "@LegacyNeo4jSource and environment properly configured")
  }

  override fun beforeEach(context: ExtensionContext?) {
    if (this::driver.isInitialized) {
      driver.verifyConnectivity()
    }
    source =
        LegacyNeo4jSourceRegistration(
            schemaControlRegistryUri = schemaRegistryUri.resolve(sourceAnnotation),
            neo4jUri = neo4jUri.resolve(sourceAnnotation),
            neo4jUser = neo4jUser.resolve(sourceAnnotation),
            neo4jPassword = neo4jPassword.resolve(sourceAnnotation),
            topic = topicRegistry.resolveTopic(sourceAnnotation.topic),
            streamingProperty = sourceAnnotation.streamingProperty,
            streamingFrom = sourceAnnotation.streamingFrom,
            streamingQuery = sourceAnnotation.streamingQuery,
            keyConverter = keyValueConverterResolver.resolveKeyConverter(context),
            valueConverter = keyValueConverterResolver.resolveValueConverter(context))
    source.register(kafkaConnectExternalUri.resolve(sourceAnnotation))
    topicRegistry.log()
  }

  override fun afterEach(context: ExtensionContext?) {
    source.unregister()
    if (this::driver.isInitialized) {
      session.close()
      driver.close()
    }
  }

  private fun resolveConsumer(
      parameterContext: ParameterContext?,
      extensionContext: ExtensionContext?
  ): KafkaConsumer<*, *> {
    val consumerAnnotation = parameterContext?.parameter?.getAnnotation(TopicConsumer::class.java)!!
    val topic = topicRegistry.resolveTopic(consumerAnnotation.topic)
    val properties = Properties()
    properties.setProperty(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
        brokerExternalHost.resolve(sourceAnnotation),
    )
    val keyConverter = keyValueConverterResolver.resolveKeyConverter(extensionContext)
    val valueConverter = keyValueConverterResolver.resolveValueConverter(extensionContext)
    if (keyConverter.supportsSchemaRegistry || valueConverter.supportsSchemaRegistry) {
      properties.setProperty(
          KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
          schemaControlRegistryExternalUri.resolve(sourceAnnotation),
      )
    }
    properties.setProperty(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        keyConverter.deserializerClass.name,
    )
    properties.setProperty(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        valueConverter.deserializerClass.name,
    )
    properties.setProperty(
        ConsumerConfig.GROUP_ID_CONFIG,
        // note: ExtensionContext#getUniqueId() returns null in the CLI
        "${topic}@${extensionContext?.testClass?: ""}#${extensionContext?.displayName}")
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, consumerAnnotation.offset)
    return consumerFactory(properties, topic)
  }

  private fun resolveSession(
      @Suppress("UNUSED_PARAMETER") parameterContext: ParameterContext?,
      @Suppress("UNUSED_PARAMETER") extensionContext: ExtensionContext?
  ): Any {
    val uri = neo4jExternalUri.resolve(sourceAnnotation)
    val username = neo4jUser.resolve(sourceAnnotation)
    val password = neo4jPassword.resolve(sourceAnnotation)
    driver = driverFactory(uri, AuthTokens.basic(username, password))
    session = driver.session()
    return session
  }

  private fun resolveTopicConsumer(
      parameterContext: ParameterContext?,
      context: ExtensionContext?
  ): ConvertingKafkaConsumer {
    val kafkaConsumer = resolveConsumer(parameterContext, context)
    return ConvertingKafkaConsumer(
        keyConverter = keyValueConverterResolver.resolveKeyConverter(context),
        valueConverter = keyValueConverterResolver.resolveValueConverter(context),
        kafkaConsumer = kafkaConsumer)
  }

  companion object {
    private fun getSubscribedConsumer(
        properties: Properties,
        topic: String
    ): KafkaConsumer<Any, Any> {
      val consumer = KafkaConsumer<Any, Any>(properties)
      consumer.subscribe(listOf(topic))
      return consumer
    }
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
