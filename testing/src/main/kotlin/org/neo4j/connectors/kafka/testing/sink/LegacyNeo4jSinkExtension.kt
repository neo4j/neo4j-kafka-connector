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

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import java.util.*
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
import org.neo4j.connectors.kafka.testing.ParameterResolvers
import org.neo4j.connectors.kafka.testing.WordSupport.pluralize
import org.neo4j.connectors.kafka.testing.format.KafkaConverter
import org.neo4j.connectors.kafka.testing.format.KeyValueConverter
import org.neo4j.connectors.kafka.testing.kafka.GenericKafkaProducer
import org.neo4j.connectors.kafka.testing.kafka.TopicRegistry
import org.neo4j.driver.AuthToken
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.Session

internal class LegacyNeo4jSinkExtension(
    // visible for testing
    envAccessor: (String) -> String? = System::getenv,
    private val driverFactory: (String, AuthToken) -> Driver = GraphDatabase::driver
) : ExecutionCondition, BeforeEachCallback, AfterEachCallback, ParameterResolver {

  private val paramResolvers =
      ParameterResolvers(
          mapOf(
              Session::class.java to ::resolveSession,
              KafkaProducer::class.java to ::resolveProducer,
              GenericKafkaProducer::class.java to ::resolveGenericProducer,
          ))

  private lateinit var sinkAnnotation: LegacyNeo4jSink

  private lateinit var sink: LegacyNeo4jSinkRegistration

  private lateinit var driver: Driver

  private lateinit var session: Session

  private lateinit var keyConverter: KafkaConverter
  private lateinit var valueConverter: KafkaConverter

  private val brokerExternalHost =
      AnnotationValueResolver(LegacyNeo4jSink::brokerExternalHost, envAccessor)

  private val schemaControlRegistryUri =
      AnnotationValueResolver(LegacyNeo4jSink::schemaControlRegistryUri, envAccessor)

  private val schemaControlRegistryExternalUri =
      AnnotationValueResolver(LegacyNeo4jSink::schemaControlRegistryExternalUri, envAccessor)

  private val kafkaConnectExternalUri =
      AnnotationValueResolver(LegacyNeo4jSink::kafkaConnectExternalUri, envAccessor)

  private val neo4jUri = AnnotationValueResolver(LegacyNeo4jSink::neo4jUri, envAccessor)

  private val neo4jExternalUri =
      AnnotationValueResolver(LegacyNeo4jSink::neo4jExternalUri, envAccessor)

  private val neo4jUser = AnnotationValueResolver(LegacyNeo4jSink::neo4jUser, envAccessor)

  private val neo4jPassword = AnnotationValueResolver(LegacyNeo4jSink::neo4jPassword, envAccessor)

  private val mandatorySettings =
      listOf(
          kafkaConnectExternalUri,
          schemaControlRegistryUri,
          neo4jUri,
          neo4jUser,
          neo4jPassword,
      )

  private val topicRegistry = TopicRegistry()

  override fun evaluateExecutionCondition(context: ExtensionContext?): ConditionEvaluationResult {
    val metadata =
        AnnotationSupport.findAnnotation<LegacyNeo4jSink>(context)
            ?: throw ExtensionConfigurationException("@LegacyNeo4jSink not found")

    val errors = mutableListOf<String>()
    mandatorySettings.forEach {
      if (!it.isValid(metadata)) {
        errors.add(it.errorMessage())
      }
    }
    val topicCount = metadata.topics.size
    if (topicCount != metadata.queries.size) {
      errors.add(
          "Expected $topicCount ${pluralize(topicCount, "query", "queries")}, but got ${metadata.queries.size}. There must be as many topics (here: ${topicCount}) as queries defined.")
    }
    if (errors.isNotEmpty()) {
      throw ExtensionConfigurationException(
          "\nMissing settings, see details below:\n\t${errors.joinToString("\n\t")}",
      )
    }
    this.sinkAnnotation = metadata

    return ConditionEvaluationResult.enabled("@LegacyNeo4jSink and environment properly configured")
  }

  override fun beforeEach(extensionContext: ExtensionContext?) {
    initialiseKeyValueConverter(extensionContext)
    if (::driver.isInitialized) {
      driver.verifyConnectivity()
    }

    sink =
        LegacyNeo4jSinkRegistration(
            topicQuerys =
                sinkAnnotation.topics.zip(sinkAnnotation.queries).toMap().mapKeys {
                  topicRegistry.resolveTopic(it.key)
                },
            neo4jUri = neo4jUri.resolve(sinkAnnotation),
            neo4jUser = neo4jUser.resolve(sinkAnnotation),
            neo4jPassword = neo4jPassword.resolve(sinkAnnotation),
            schemaControlRegistryUri = schemaControlRegistryUri.resolve(sinkAnnotation),
            keyConverter = keyConverter,
            valueConverter = valueConverter)
    sink.register(kafkaConnectExternalUri.resolve(sinkAnnotation))
    topicRegistry.log()
  }

  override fun afterEach(extensionContent: ExtensionContext?) {
    if (::driver.isInitialized) {
      session.close()
      driver.close()
    }
    sink.unregister()
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
      @Suppress("UNUSED_PARAMETER") extensionContext: ExtensionContext?
  ): Any {
    val uri = neo4jExternalUri.resolve(sinkAnnotation)
    val username = neo4jUser.resolve(sinkAnnotation)
    val password = neo4jPassword.resolve(sinkAnnotation)
    driver = driverFactory(uri, AuthTokens.basic(username, password))
    session = driver.session()
    return session
  }

  private fun resolveProducer(
      @Suppress("UNUSED_PARAMETER") parameterContext: ParameterContext?,
      extensionContext: ExtensionContext?
  ): KafkaProducer<Any, Any> {
    initialiseKeyValueConverter(extensionContext)
    val properties = Properties()
    properties.setProperty(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
        brokerExternalHost.resolve(sinkAnnotation),
    )
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

  private fun initialiseKeyValueConverter(context: ExtensionContext?) {
    if (this::keyConverter.isInitialized && this::valueConverter.isInitialized) {
      return
    }
    val annotation: KeyValueConverter? =
        AnnotationSupport.findAnnotation<KeyValueConverter>(context)
    if (annotation == null) {
      keyConverter = KafkaConverter.AVRO
      valueConverter = KafkaConverter.AVRO
    } else {
      keyConverter = annotation.key
      valueConverter = annotation.value
    }
  }

  private fun resolveGenericProducer(
      parameterContext: ParameterContext?,
      extensionContext: ExtensionContext?
  ): Any {
    return GenericKafkaProducer(
        keyConverter = keyConverter,
        valueConverter = valueConverter,
        kafkaProducer = resolveProducer(parameterContext, extensionContext),
        topicRegistry = topicRegistry)
  }
}
