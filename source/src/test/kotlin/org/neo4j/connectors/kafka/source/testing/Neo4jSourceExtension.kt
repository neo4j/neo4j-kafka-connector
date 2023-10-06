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
package org.neo4j.connectors.kafka.source.testing

import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import java.util.*
import kotlin.jvm.optionals.getOrNull
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
import org.junit.jupiter.api.extension.support.TypeBasedParameterResolver
import org.junit.platform.commons.support.AnnotationSupport
import streams.kafka.connect.source.testing.Neo4jSourceRegistration

class Neo4jSourceExtension :
    ExecutionCondition,
    BeforeEachCallback,
    AfterEachCallback,
    TypeBasedParameterResolver<KafkaConsumer<String, GenericRecord>>() {

  private lateinit var metadata: Neo4jSource
  private lateinit var source: Neo4jSourceRegistration

  private val brokerExternalHost =
      EnvBackedSetting("brokerExternalHost", "BROKER_EXTERNAL_HOST") { it.brokerExternalHost }
  private val schemaRegistryUri =
      EnvBackedSetting(
          "schemaControlRegistryUri",
          "SCHEMA_CONTROL_REGISTRY_URI",
      ) {
        it.schemaControlRegistryUri
      }
  private val schemaRegistryExternalUri =
      EnvBackedSetting(
          "schemaControlRegistryExternalUri",
          "SCHEMA_CONTROL_REGISTRY_EXTERNAL_URI",
      ) {
        it.schemaControlRegistryExternalUri
      }
  private val kafkaConnectExternalUri =
      EnvBackedSetting("kafkaConnectExternalUri", "KAFKA_CONNECT_EXTERNAL_URI") {
        it.kafkaConnectExternalUri
      }
  private val neo4jUri = EnvBackedSetting("neo4jUri", "NEO4J_URI") { it.neo4jUri }
  private val neo4jUser = EnvBackedSetting("neo4jUser", "NEO4J_USER") { it.neo4jUser }
  private val neo4jPassword =
      EnvBackedSetting("neo4jPassword", "NEO4J_PASSWORD") { it.neo4jPassword }

  private val envSettings =
      listOf(
          brokerExternalHost,
          schemaRegistryUri,
          schemaRegistryExternalUri,
          kafkaConnectExternalUri,
          neo4jUri,
          neo4jUser,
          neo4jPassword,
      )

  override fun evaluateExecutionCondition(context: ExtensionContext?): ConditionEvaluationResult {
    val metadata =
        findAnnotation(context) ?: throw ExtensionConfigurationException("@Neo4jSource not found")

    val errors = mutableListOf<String>()
    envSettings.forEach {
      if (!it.isValid(metadata)) {
        errors.add(it.errorMessage())
      }
    }
    if (errors.isNotEmpty()) {
      throw ExtensionConfigurationException(
          "\nMissing settings, see details below:\n\t${errors.joinToString("\n\t")}")
    }

    this.metadata = metadata
    return ConditionEvaluationResult.enabled("@Neo4jSource and environment properly configured")
  }

  override fun beforeEach(context: ExtensionContext?) {
    source =
        Neo4jSourceRegistration(
            schemaControlRegistryUri = schemaRegistryUri.readAnnotationOrEnv(metadata),
            neo4jUri = neo4jUri.readAnnotationOrEnv(metadata),
            neo4jUser = neo4jUser.readAnnotationOrEnv(metadata),
            neo4jPassword = neo4jPassword.readAnnotationOrEnv(metadata),
            topic = metadata.topic,
            streamingProperty = metadata.streamingProperty,
            streamingFrom = metadata.streamingFrom,
            streamingQuery = metadata.streamingQuery,
        )
    source.register(kafkaConnectExternalUri.readAnnotationOrEnv(metadata))
  }

  override fun afterEach(context: ExtensionContext?) {
    source.unregister()
  }

  private fun findAnnotation(context: ExtensionContext?): Neo4jSource? {
    var current = context
    while (current != null) {
      val annotation =
          AnnotationSupport.findAnnotation(
              current.requiredTestMethod,
              Neo4jSource::class.java,
          )
      if (annotation.isPresent) {
        return annotation.get()
      }
      current = current.parent.getOrNull()
    }
    return null
  }

  override fun resolveParameter(
      parameterContext: ParameterContext?,
      extensionContext: ExtensionContext?
  ): KafkaConsumer<String, GenericRecord> {
    val properties = Properties()
    properties.setProperty(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerExternalHost.readAnnotationOrEnv(metadata))
    properties.setProperty(
        KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
        schemaRegistryExternalUri.readAnnotationOrEnv(metadata),
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
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, metadata.consumerOffset)
    val consumer = KafkaConsumer<String, GenericRecord>(properties)
    consumer.subscribe(listOf(metadata.topic))
    return consumer
  }
}

class EnvBackedSetting(
    private val name: String,
    private val envVarName: String,
    private val getter: (Neo4jSource) -> String
) {

  fun isValid(annotation: Neo4jSource): Boolean {
    return getter(annotation) != UNSET_VALUE || System.getenv(envVarName) != null
  }

  fun readAnnotationOrEnv(annotation: Neo4jSource): String {
    val field = getter(annotation)
    if (field != UNSET_VALUE) {
      return field
    }
    return System.getenv(envVarName)
  }

  fun errorMessage(): String {
    return "Both @Neo4jSource.$name and environment variable $envVarName are unset. Please specify one"
  }
}
