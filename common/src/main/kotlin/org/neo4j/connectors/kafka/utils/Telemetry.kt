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
package org.neo4j.connectors.kafka.utils

import org.apache.kafka.common.utils.AppInfoParser
import org.neo4j.connectors.kafka.configuration.Neo4jConfiguration
import org.neo4j.connectors.kafka.configuration.helpers.VersionUtil
import org.neo4j.driver.Config

interface EnvironmentProvider {
  fun get(env: String): String?
}

object Telemetry {
  internal const val CONFLUENT_ENV: String = "CONFLUENT_ENV"
  internal const val CONFLUENT_ENV_VALUE: String = "CCLOUD_CUSTOM_CONNECTOR"

  private object SystemEnvironmentProvider : EnvironmentProvider {
    override fun get(env: String): String? {
      return System.getenv(env)
    }
  }

  fun userAgent(
      type: String,
      legacy: Boolean = false,
      comment: String = "",
      provider: EnvironmentProvider = SystemEnvironmentProvider
  ): String {
    return String.format(
        "%s %s (%s) %s %s",
        connectorInformation(
            type, legacy, VersionUtil.version(Neo4jConfiguration::class.java), comment, provider),
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
      legacy: Boolean,
      version: String = "",
      comment: String = "",
      provider: EnvironmentProvider = SystemEnvironmentProvider
  ): String {
    return String.format(
        "%s-%s%s%s",
        if (runningInConfluentCloud(provider)) "confluent-cloud" else "kafka",
        if (legacy) "legacy-$type" else type,
        if (version.isEmpty()) "" else "/$version",
        if (comment.isEmpty()) "" else " ($comment)")
  }

  internal fun kafkaConnectInformation(): String {
    return String.format("kafka-connect/%s", AppInfoParser.getVersion())
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
