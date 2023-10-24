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
package org.neo4j.connectors.kafka.testing.source

import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import java.util.*
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
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
import org.neo4j.driver.AuthToken
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.Session

internal class Neo4jSourceExtension(
    // visible for testing
    envAccessor: (String) -> String? = System::getenv,
    private val driverFactory: (String, AuthToken) -> Driver = GraphDatabase::driver,
    private val consumerFactory: (Properties, String) -> KafkaConsumer<String, GenericRecord> =
        ::getSubscribedConsumer,
) : ExecutionCondition, BeforeEachCallback, AfterEachCallback, ParameterResolver {

  private val paramResolvers =
      ParameterResolvers(
          mapOf(
              Session::class.java to ::resolveSession,
              KafkaConsumer::class.java to ::resolveConsumer,
          ))

  private lateinit var sourceAnnotation: Neo4jSource

  private lateinit var source: Neo4jSourceRegistration

  private lateinit var driver: Driver

  private lateinit var session: Session

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
    return ConditionEvaluationResult.enabled("@Neo4jSource and environment properly configured")
  }

  override fun beforeEach(context: ExtensionContext?) {
    if (this::driver.isInitialized) {
      driver.verifyConnectivity()
    }
    source =
        Neo4jSourceRegistration(
            schemaControlRegistryUri = schemaRegistryUri.resolve(sourceAnnotation),
            neo4jUri = neo4jUri.resolve(sourceAnnotation),
            neo4jUser = neo4jUser.resolve(sourceAnnotation),
            neo4jPassword = neo4jPassword.resolve(sourceAnnotation),
            topic = sourceAnnotation.topic,
            streamingProperty = sourceAnnotation.streamingProperty,
            streamingFrom = sourceAnnotation.streamingFrom,
            streamingQuery = sourceAnnotation.streamingQuery,
        )
    source.register(kafkaConnectExternalUri.resolve(sourceAnnotation))
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
  ): KafkaConsumer<String, GenericRecord> {
    val properties = Properties()
    properties.setProperty(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
        brokerExternalHost.resolve(sourceAnnotation),
    )
    properties.setProperty(
        KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
        schemaControlRegistryExternalUri.resolve(sourceAnnotation),
    )
    properties.setProperty(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer::class.java.getName(),
    )
    properties.setProperty(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        KafkaAvroDeserializer::class.java.getName(),
    )
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, extensionContext?.displayName)
    val consumerAnnotation = parameterContext?.parameter?.getAnnotation(TopicConsumer::class.java)!!
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, consumerAnnotation.offset)
    return consumerFactory(properties, consumerAnnotation.topic)
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

  companion object {
    private fun getSubscribedConsumer(
        properties: Properties,
        topic: String
    ): KafkaConsumer<String, GenericRecord> {
      val consumer = KafkaConsumer<String, GenericRecord>(properties)
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
