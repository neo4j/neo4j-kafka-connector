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
package org.neo4j.connectors.kafka.utils

import java.lang.reflect.Modifier
import java.util.Properties
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.config.TopicConfig

object KafkaValidationUtils {
  fun getInvalidTopicsError(invalidTopics: List<String>) =
      "The BROKER config `auto.create.topics.enable` is false, the following topics need to be created into the Kafka cluster otherwise the messages will be discarded: $invalidTopics"

  fun getInvalidTopics(kafkaProps: Properties, allTopics: List<String>): List<String> =
      try {
        getInvalidTopics(AdminClient.create(kafkaProps), allTopics)
      } catch (e: Exception) {
        emptyList()
      }

  fun getInvalidTopics(client: AdminClient, allTopics: List<String>): List<String> =
      try {
        val kafkaTopics = client.listTopics().names().get()
        val invalidTopics = allTopics.filter { !kafkaTopics.contains(it) }
        if (invalidTopics.isNotEmpty() && isAutoCreateTopicsEnabled(client)) {
          emptyList()
        } else {
          invalidTopics
        }
      } catch (e: Exception) {
        emptyList()
      }

  fun isAutoCreateTopicsEnabled(kafkaProps: Properties): Boolean =
      try {
        isAutoCreateTopicsEnabled(AdminClient.create(kafkaProps))
      } catch (e: Exception) {
        false
      }

  fun isAutoCreateTopicsEnabled(client: AdminClient): Boolean =
      try {
        val firstNodeId = client.describeCluster().nodes().get().first().id()
        val configResources =
            listOf(ConfigResource(ConfigResource.Type.BROKER, firstNodeId.toString()))
        val configs = client.describeConfigs(configResources).all().get()
        configs.values
            .flatMap { it.entries() }
            .find { it.name() == "auto.create.topics.enable" }
            ?.value()
            ?.toBoolean() ?: false
      } catch (e: Exception) {
        false
      }

  private fun getConfigProperties(clazz: Class<*>) =
      clazz.declaredFields
          .filter { Modifier.isStatic(it.modifiers) && it.name.endsWith("_CONFIG") }
          .map { it.get(null).toString() }
          .toSet()

  private fun getBaseConfigs() =
      (getConfigProperties(CommonClientConfigs::class.java) +
          AdminClientConfig.configNames() +
          getConfigProperties(SaslConfigs::class.java) +
          getConfigProperties(TopicConfig::class.java) +
          getConfigProperties(SslConfigs::class.java))

  fun getProducerProperties() = ProducerConfig.configNames() - getBaseConfigs()

  fun getConsumerProperties() = ConsumerConfig.configNames() - getBaseConfigs()
}
