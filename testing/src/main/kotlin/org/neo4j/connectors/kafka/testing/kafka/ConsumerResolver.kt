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
import java.util.*
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.api.extension.ParameterContext
import org.neo4j.connectors.kafka.testing.format.KeyValueConverterResolver
import org.neo4j.connectors.kafka.testing.source.TopicConsumer

internal class ConsumerResolver(
    private val keyValueConverterResolver: KeyValueConverterResolver,
    private val topicRegistry: TopicRegistry,
    private val brokerExternalHostProvider: () -> String,
    private val schemaControlRegistryExternalUriProvider: () -> String,
    private val consumerFactory: (Properties, String) -> KafkaConsumer<*, *>
) {

  fun resolveGenericConsumer(
      parameterContext: ParameterContext?,
      context: ExtensionContext?
  ): ConvertingKafkaConsumer {
    val kafkaConsumer = resolveConsumer(parameterContext, context)
    return ConvertingKafkaConsumer(
        keyConverter = keyValueConverterResolver.resolveKeyConverter(context),
        valueConverter = keyValueConverterResolver.resolveValueConverter(context),
        kafkaConsumer = kafkaConsumer)
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
        brokerExternalHostProvider(),
    )
    val keyConverter = keyValueConverterResolver.resolveKeyConverter(extensionContext)
    val valueConverter = keyValueConverterResolver.resolveValueConverter(extensionContext)
    if (keyConverter.supportsSchemaRegistry || valueConverter.supportsSchemaRegistry) {
      properties.setProperty(
          KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
          schemaControlRegistryExternalUriProvider(),
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
        "${topic}@${extensionContext?.testClass ?: ""}#${extensionContext?.displayName}",
    )

    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, consumerAnnotation.offset)
    return consumerFactory(properties, topic)
  }

  companion object {
    internal fun getSubscribedConsumer(properties: Properties, topic: String): KafkaConsumer<*, *> {
      val consumer = KafkaConsumer<Any, Any>(properties)
      consumer.subscribe(listOf(topic))
      return consumer
    }
  }
}
