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

import java.util.*
import org.slf4j.LoggerFactory

class PropertiesUtil {

  companion object {
    private val LOGGER = LoggerFactory.getLogger(PropertiesUtil::class.java)
    private const val DEFAULT_VERSION = "unknown"
    private var properties: Properties = Properties()
    private var VERSION: String

    init {
      properties.load(
          PropertiesUtil::class.java.getResourceAsStream("/neo4j-configuration.properties"))
      properties.load(
          PropertiesUtil::class.java.getResourceAsStream("/neo4j-source-configuration.properties"))
      properties.load(
          PropertiesUtil::class.java.getResourceAsStream("/neo4j-sink-configuration.properties"))
      properties.load(
          PropertiesUtil::class.java.getResourceAsStream("/kafka-connect-version.properties"))
      properties.load(
          PropertiesUtil::class.java.getResourceAsStream("/kafka-connect-neo4j.properties"))
      VERSION =
          try {
            properties.getProperty("version", DEFAULT_VERSION).trim()
          } catch (e: Exception) {
            LOGGER.warn("error while loading version:", e)
            DEFAULT_VERSION
          }
    }

    fun getVersion(): String {
      return VERSION
    }

    fun getProperty(key: String): String {
      return properties.getProperty(key)
    }
  }
}
