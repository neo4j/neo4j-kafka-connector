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
package org.neo4j.connectors.kafka.testing.kafka

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import java.net.URI
import java.util.*
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.api.extension.ParameterContext
import org.neo4j.connectors.kafka.testing.format.KeyValueConverterResolver
import org.neo4j.connectors.kafka.testing.sink.SchemaCompatibilityMode
import org.neo4j.connectors.kafka.testing.sink.TopicProducer

class ProducerResolver(
    private val keyValueConverterResolver: KeyValueConverterResolver,
    private val topicRegistry: TopicRegistry,
    private val brokerExternalHostProvider: () -> String,
    private val schemaControlRegistryExternalUriProvider: () -> String,
    private val schemaControlKeyCompatibilityProvider: () -> SchemaCompatibilityMode,
    private val schemaControlValueCompatibilityProvider: () -> SchemaCompatibilityMode,
) {

  fun resolveGenericProducer(
      parameterContext: ParameterContext?,
      extensionContext: ExtensionContext?,
  ): Any {
    val producerAnnotation = parameterContext?.parameter?.getAnnotation(TopicProducer::class.java)!!
    return ConvertingKafkaProducer(
        schemaRegistryURI = URI(schemaControlRegistryExternalUriProvider()),
        keyConverter = keyValueConverterResolver.resolveKeyConverter(extensionContext),
        keyCompatibilityMode = schemaControlKeyCompatibilityProvider(),
        valueConverter = keyValueConverterResolver.resolveValueConverter(extensionContext),
        valueCompatibilityMode = schemaControlValueCompatibilityProvider(),
        kafkaProducer = resolveProducer(parameterContext, extensionContext),
        topic = topicRegistry.resolveTopic(producerAnnotation.topic),
    )
  }

  private fun resolveProducer(
      @Suppress("UNUSED_PARAMETER") parameterContext: ParameterContext?,
      extensionContext: ExtensionContext?,
  ): KafkaProducer<Any, Any> {
    val properties = Properties()
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerExternalHostProvider())
    val keyConverter = keyValueConverterResolver.resolveKeyConverter(extensionContext)
    val valueConverter = keyValueConverterResolver.resolveValueConverter(extensionContext)
    if (keyConverter.supportsSchemaRegistry || valueConverter.supportsSchemaRegistry) {
      properties.setProperty(
          KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
          schemaControlRegistryExternalUriProvider(),
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
    properties.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, UUID.randomUUID().toString())
    val producer = KafkaProducer<Any, Any>(properties)
    producer.initTransactions()
    return producer
  }
}
