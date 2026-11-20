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

import java.util.Properties
import org.apache.kafka.common.KafkaException
import org.neo4j.connectors.kafka.configuration.Neo4jConfiguration
import org.neo4j.connectors.kafka.configuration.helpers.VersionUtil
import org.neo4j.driver.Config
import org.slf4j.LoggerFactory

interface EnvironmentProvider {
  fun get(env: String): String?
}

object Telemetry {
  internal const val CONFLUENT_ENV: String = "CONFLUENT_ENV"
  internal const val CONFLUENT_ENV_VALUE: String = "CCLOUD_CUSTOM_CONNECTOR"

  private val log = LoggerFactory.getLogger(Telemetry::class.java)
  private const val KAFKA_VERSION_RESOURCE = "/kafka/kafka-version.properties"
  private const val FALLBACK_VERSION = "unknown"

  // mirrors Kafka's internal AppInfoParser, which reads the same resource bundled in kafka-clients.
  // resolved through a public kafka-clients class so that we read the worker's own kafka-clients
  // jar.
  private val kafkaVersion: String by lazy {
    val props = Properties()
    try {
      KafkaException::class.java.getResourceAsStream(KAFKA_VERSION_RESOURCE)?.use { props.load(it) }
          ?: log.warn("Could not find {} on the classpath", KAFKA_VERSION_RESOURCE)
    } catch (e: Exception) {
      log.warn("Error while loading {}: {}", KAFKA_VERSION_RESOURCE, e.message)
    }
    props.getProperty("version", FALLBACK_VERSION).trim()
  }

  private object SystemEnvironmentProvider : EnvironmentProvider {
    override fun get(env: String): String? {
      return System.getenv(env)
    }
  }

  fun userAgent(
      type: String,
      comment: String = "",
      provider: EnvironmentProvider = SystemEnvironmentProvider,
  ): String {
    return String.format(
        "%s %s (%s) %s %s",
        connectorInformation(
            type,
            VersionUtil.version(Neo4jConfiguration::class.java),
            comment,
            provider,
        ),
        kafkaConnectInformation(),
        platform(),
        neo4jDriverVersion(),
        jreInformation(),
    )
  }

  private fun runningInConfluentCloud(
      provider: EnvironmentProvider = SystemEnvironmentProvider
  ): Boolean {
    val value = provider.get(CONFLUENT_ENV)
    if (value.isNullOrEmpty()) {
      return false
    }
    return value.contentEquals(CONFLUENT_ENV_VALUE, true)
  }

  internal fun connectorInformation(
      type: String,
      version: String = "",
      comment: String = "",
      provider: EnvironmentProvider = SystemEnvironmentProvider,
  ): String {
    return String.format(
        "%s-%s%s%s",
        if (runningInConfluentCloud(provider)) "confluent-cloud" else "kafka",
        type,
        if (version.isEmpty()) "" else "/$version",
        if (comment.isEmpty()) "" else " ($comment)",
    )
  }

  internal fun kafkaConnectInformation(): String {
    return String.format("kafka-connect/%s", kafkaVersion)
  }

  internal fun platform(): String {
    return String.format(
        "%s; %s; %s",
        System.getProperty("os.name"),
        System.getProperty("os.version"),
        System.getProperty("os.arch"),
    )
  }

  internal fun neo4jDriverVersion(): String {
    return Config.defaultConfig().userAgent()
  }

  internal fun jreInformation(): String {
    // this format loosely follows the Java driver's Bolt Agent format
    return String.format(
        "Java/%s (%s; %s; %s)",
        System.getProperty("java.version"),
        System.getProperty("java.vm.vendor"),
        System.getProperty("java.vm.name"),
        System.getProperty("java.vm.version"),
    )
  }
}
