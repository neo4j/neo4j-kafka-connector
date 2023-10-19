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
import org.junit.jupiter.api.extension.ParameterResolutionException
import org.junit.jupiter.api.extension.ParameterResolver
import org.neo4j.connectors.kafka.testing.AnnotationSupport
import org.neo4j.connectors.kafka.testing.EnvBackedSetting
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.Session

internal class Neo4jSourceExtension(
    private val consumerSupplier: ConsumerSupplier<String, GenericRecord> = DefaultConsumerSupplier
) : ExecutionCondition, BeforeEachCallback, AfterEachCallback, ParameterResolver {

  companion object {
    val PARAMETER_RESOLVERS:
        Map<Class<*>, (Neo4jSourceExtension, ParameterContext?, ExtensionContext?) -> Any> =
        mapOf(
            KafkaConsumer::class.java to Neo4jSourceExtension::resolveConsumer,
            Session::class.java to Neo4jSourceExtension::resolveSession,
        )
  }

  /*visible for testing */ internal lateinit var sourceAnnotation: Neo4jSource
  private lateinit var source: Neo4jSourceRegistration

  private lateinit var driver: Driver

  private lateinit var session: Session

  private val brokerExternalHost =
      EnvBackedSetting<Neo4jSource>("brokerExternalHost", { it.brokerExternalHost })

  private val schemaRegistryUri =
      EnvBackedSetting<Neo4jSource>("schemaControlRegistryUri", { it.schemaControlRegistryUri })

  private val schemaRegistryExternalUri =
      EnvBackedSetting<Neo4jSource>(
          "schemaControlRegistryExternalUri", { it.schemaControlRegistryExternalUri })

  private val kafkaConnectExternalUri =
      EnvBackedSetting<Neo4jSource>("kafkaConnectExternalUri", { it.kafkaConnectExternalUri })

  private val neo4jUri = EnvBackedSetting<Neo4jSource>("neo4jUri", { it.neo4jUri })

  private val neo4jExternalUri =
      EnvBackedSetting<Neo4jSource>("neo4jExternalUri", { it.neo4jExternalUri })

  private val neo4jUser = EnvBackedSetting<Neo4jSource>("neo4jUser", { it.neo4jUser })

  private val neo4jPassword = EnvBackedSetting<Neo4jSource>("neo4jPassword", { it.neo4jPassword })

  private val envSettings =
      listOf(
          brokerExternalHost,
          schemaRegistryUri,
          schemaRegistryExternalUri,
          kafkaConnectExternalUri,
          neo4jUri,
          neo4jExternalUri,
          neo4jUser,
          neo4jPassword,
      )

  override fun evaluateExecutionCondition(context: ExtensionContext?): ConditionEvaluationResult {
    val metadata =
        AnnotationSupport.findAnnotation<Neo4jSource>(context)
            ?: throw ExtensionConfigurationException("@Neo4jSource not found")

    val errors = mutableListOf<String>()
    envSettings.forEach {
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
            schemaControlRegistryUri = schemaRegistryUri.read(sourceAnnotation),
            neo4jUri = neo4jUri.read(sourceAnnotation),
            neo4jUser = neo4jUser.read(sourceAnnotation),
            neo4jPassword = neo4jPassword.read(sourceAnnotation),
            topic = sourceAnnotation.topic,
            streamingProperty = sourceAnnotation.streamingProperty,
            streamingFrom = sourceAnnotation.streamingFrom,
            streamingQuery = sourceAnnotation.streamingQuery,
        )
    source.register(kafkaConnectExternalUri.read(sourceAnnotation))
  }

  override fun afterEach(context: ExtensionContext?) {
    source.unregister()
    if (this::driver.isInitialized) {
      session.close()
      driver.close()
    }
  }

  override fun supportsParameter(
      parameterContext: ParameterContext?,
      extensionContext: ExtensionContext?
  ): Boolean {
    return PARAMETER_RESOLVERS.contains(parameterContext?.parameter?.type)
  }

  override fun resolveParameter(
      parameterContext: ParameterContext?,
      extensionContext: ExtensionContext?
  ): Any {
    val parameterType = parameterContext?.parameter?.type
    val resolver =
        PARAMETER_RESOLVERS[parameterType]
            ?: throw ParameterResolutionException(
                "@Neo4jSource does not support injection of parameters typed $parameterType",
            )
    return resolver(this, parameterContext, extensionContext)
  }

  private fun resolveConsumer(
      parameterContext: ParameterContext?,
      extensionContext: ExtensionContext?
  ): KafkaConsumer<String, GenericRecord> {
    val properties = Properties()
    properties.setProperty(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
        brokerExternalHost.read(sourceAnnotation),
    )
    properties.setProperty(
        KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
        schemaRegistryExternalUri.read(sourceAnnotation),
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
    return consumerSupplier.getSubscribed(properties, consumerAnnotation.topic)
  }

  private fun resolveSession(
      parameterContext: ParameterContext?,
      extensionContext: ExtensionContext?
  ): Any {
    val uri = neo4jExternalUri.read(sourceAnnotation)
    val username = neo4jUser.read(sourceAnnotation)
    val password = neo4jPassword.read(sourceAnnotation)
    driver = GraphDatabase.driver(uri, AuthTokens.basic(username, password))
    // TODO: handle multiple parameter injection
    session = driver.session()
    return session
  }
}

internal interface ConsumerSupplier<K, V> {
  fun getSubscribed(properties: Properties, topic: String): KafkaConsumer<K, V>
}

internal object DefaultConsumerSupplier : ConsumerSupplier<String, GenericRecord> {
  override fun getSubscribed(
      properties: Properties,
      topic: String
  ): KafkaConsumer<String, GenericRecord> {
    val consumer = KafkaConsumer<String, GenericRecord>(properties)
    consumer.subscribe(listOf(topic))
    return consumer
  }
}
