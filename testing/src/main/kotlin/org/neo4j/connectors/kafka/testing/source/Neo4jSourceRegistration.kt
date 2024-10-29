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
package org.neo4j.connectors.kafka.testing.source

import java.net.URI
import java.time.Duration
import org.neo4j.connectors.kafka.configuration.PayloadMode
import org.neo4j.connectors.kafka.testing.RegistrationSupport.randomizedName
import org.neo4j.connectors.kafka.testing.RegistrationSupport.registerConnector
import org.neo4j.connectors.kafka.testing.RegistrationSupport.unregisterConnector
import org.neo4j.connectors.kafka.testing.format.KafkaConverter
import org.neo4j.connectors.kafka.testing.source.SourceStrategy.CDC
import org.neo4j.connectors.kafka.testing.source.SourceStrategy.QUERY

internal class Neo4jSourceRegistration(
    topic: String,
    neo4jUri: String,
    neo4jUser: String = "neo4j",
    neo4jPassword: String,
    neo4jDatabase: String,
    pollInterval: Duration = Duration.ofMillis(1000),
    pollDuration: Duration = Duration.ofMillis(5000),
    streamingProperty: String,
    startFrom: String,
    startFromValue: String,
    query: String,
    keyConverter: KafkaConverter,
    valueConverter: KafkaConverter,
    schemaControlRegistryUri: String,
    strategy: SourceStrategy,
    cdcPatterns: Map<String, List<String>>,
    cdcPatternsIndexed: Boolean = false,
    cdcOperations: Map<String, List<String>>,
    cdcChangesTo: Map<String, List<String>>,
    cdcMetadata: Map<String, List<Map<String, String>>>,
    cdcKeySerializations: Map<String, String>,
    cdcValueSerializations: Map<String, String>,
    payloadMode: PayloadMode
) {

  private val name: String = randomizedName("Neo4jSourceConnector")
  private val payload: Map<String, Any>

  init {
    val config = buildMap {
      put("connector.class", "org.neo4j.connectors.kafka.source.Neo4jConnector")
      put("key.converter", keyConverter.className)
      if (keyConverter == KafkaConverter.JSON_EMBEDDED) {
        put("key.converter.schemas.enable", true)
      }
      put("value.converter", valueConverter.className)
      if (valueConverter == KafkaConverter.JSON_EMBEDDED) {
        put("value.converter.schemas.enable", true)
      }
      put("neo4j.uri", neo4jUri)
      put("neo4j.authentication.type", "BASIC")
      put("neo4j.authentication.basic.username", neo4jUser)
      put("neo4j.authentication.basic.password", neo4jPassword)
      put("neo4j.database", neo4jDatabase)
      put("neo4j.start-from", startFrom)
      if (startFromValue.isNotBlank()) {
        put("neo4j.start-from.value", startFromValue)
      }
      put("neo4j.source-strategy", strategy.name.uppercase())

      put("neo4j.payload-mode", payloadMode.name)

      if (keyConverter.supportsSchemaRegistry) {
        put("key.converter.schema.registry.url", schemaControlRegistryUri)
      }
      putAll(keyConverter.additionalProperties.mapKeys { "key.converter.${it.key}" })

      if (valueConverter.supportsSchemaRegistry) {
        put("value.converter.schema.registry.url", schemaControlRegistryUri)
      }
      putAll(valueConverter.additionalProperties.mapKeys { "value.converter.${it.key}" })

      if (strategy == QUERY) {
        put("neo4j.query.topic", topic)
        put("neo4j.query", query)
        put("neo4j.query.streaming-property", streamingProperty)
        put("neo4j.query.poll-interval", "${pollInterval.toMillis()}ms")
        put("neo4j.query.poll-duration", "${pollDuration.toMillis()}ms")
      }

      if (strategy == CDC) {
        put("neo4j.cdc.poll-interval", "${pollInterval.toMillis()}ms")
        put("neo4j.cdc.poll-duration", "${pollDuration.toMillis()}ms")
        putCdcParameters("neo4j.cdc.topic.%s.patterns.%s.operation", cdcOperations)
        putCdcParameters("neo4j.cdc.topic.%s.patterns.%s.changesTo", cdcChangesTo)
        putCdcPatterns(cdcPatterns, cdcPatternsIndexed)
        putCdcMetadata(cdcMetadata)
        putCdcKeyValueSerializations("neo4j.cdc.topic.%s.key-strategy", cdcKeySerializations)
        putCdcKeyValueSerializations("neo4j.cdc.topic.%s.value-strategy", cdcValueSerializations)
      }
    }

    payload = mapOf("name" to name, "config" to config)
  }

  private lateinit var connectBaseUri: String

  fun register(connectBaseUri: String) {
    this.connectBaseUri = connectBaseUri
    registerConnector(URI("${this.connectBaseUri}/connectors"), payload)
  }

  fun unregister() {
    unregisterConnector(URI("$connectBaseUri/connectors/$name/"))
  }

  companion object {

    fun MutableMap<String, Any>.putCdcParameters(
        keyPattern: String,
        values: Map<String, List<String>>
    ): MutableMap<String, Any> {
      if (values.isEmpty()) {
        return this
      }

      values.forEach { (topic, params) ->
        params.forEachIndexed { ind, value ->
          val key = keyPattern.format(topic, ind)
          this[key] = value
        }
      }
      return this
    }

    fun MutableMap<String, Any>.putCdcPatterns(
        patterns: Map<String, List<String>>,
        indexed: Boolean
    ): MutableMap<String, Any> {
      if (patterns.isEmpty()) {
        return this
      }
      if (indexed) {
        putCdcParameters("neo4j.cdc.topic.%s.patterns.%s.pattern", patterns)
      } else {
        patterns
            .filterValues { it.isNotEmpty() }
            .forEach { (topic, params) ->
              this["neo4j.cdc.topic.$topic.patterns"] = params.joinToString(",")
            }
      }
      return this
    }

    fun MutableMap<String, Any>.putCdcMetadata(
        metadata: Map<String, List<Map<String, String>>>
    ): MutableMap<String, Any> {
      if (metadata.isEmpty()) {
        return this
      }
      metadata.forEach { (topic, params) ->
        params.forEachIndexed { ind, dict ->
          dict.forEach { (key, value) ->
            this["neo4j.cdc.topic.$topic.patterns.$ind.metadata.$key"] = value
          }
        }
      }
      return this
    }

    fun MutableMap<String, Any>.putCdcKeyValueSerializations(
        pattern: String,
        strategies: Map<String, String>
    ): MutableMap<String, Any> {
      strategies.forEach { (topic, strategy) -> this[pattern.format(topic)] = strategy }
      return this
    }
  }
}
