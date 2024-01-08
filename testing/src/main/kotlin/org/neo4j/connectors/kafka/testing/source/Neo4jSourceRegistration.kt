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

import java.net.URI
import java.time.Duration
import org.neo4j.connectors.kafka.testing.MapSupport.putConditionally
import org.neo4j.connectors.kafka.testing.RegistrationSupport.randomizedName
import org.neo4j.connectors.kafka.testing.RegistrationSupport.registerConnector
import org.neo4j.connectors.kafka.testing.RegistrationSupport.unregisterConnector
import org.neo4j.connectors.kafka.testing.source.SourceStrategy.CDC
import org.neo4j.connectors.kafka.testing.source.SourceStrategy.QUERY

internal class Neo4jSourceRegistration(
    topic: String,
    neo4jUri: String,
    neo4jUser: String = "neo4j",
    neo4jPassword: String,
    neo4jDatabase: String,
    pollInterval: Duration = Duration.ofMillis(5000),
    pollDuration: Duration = Duration.ofMillis(1000),
    enforceSchema: Boolean = true,
    streamingProperty: String,
    startFrom: String,
    query: String,
    schemaControlRegistryUri: String,
    strategy: SourceStrategy,
    cdcPatterns: Map<String, List<String>>,
    cdcPatternsIndexed: Boolean = false,
    cdcOperations: Map<String, List<String>>,
    cdcChangesTo: Map<String, List<String>>,
    cdcMetadata: Map<String, List<Map<String, String>>>
) {

  private val name: String = randomizedName("Neo4jSourceConnector")
  private val payload: Map<String, Any>

  init {
    val config =
        mutableMapOf<String, Any>(
                "connector.class" to "org.neo4j.connectors.kafka.source.Neo4jConnector",
                "key.converter" to "io.confluent.connect.avro.AvroConverter",
                "key.converter.schema.registry.url" to schemaControlRegistryUri,
                "value.converter" to "io.confluent.connect.avro.AvroConverter",
                "value.converter.schema.registry.url" to schemaControlRegistryUri,
                "neo4j.uri" to neo4jUri,
                "neo4j.authentication.type" to "BASIC",
                "neo4j.authentication.basic.username" to neo4jUser,
                "neo4j.authentication.basic.password" to neo4jPassword,
                "neo4j.database" to neo4jDatabase,
                "neo4j.start-from" to startFrom,
                "neo4j.source-strategy" to strategy.name.uppercase())
            .putConditionally("topic", topic, String::isNotEmpty)
            .putConditionally("neo4j.query", query, String::isNotEmpty)
            .putConditionally(
                "neo4j.query.streaming-property", streamingProperty, String::isNotEmpty)
            .putConditionally("neo4j.query.poll-interval", "${pollInterval.toMillis()}ms") {
              strategy == QUERY
            }
            .putConditionally("neo4j.enforce-schema", enforceSchema) { strategy == QUERY }
            .putConditionally("neo4j.cdc.poll-interval", "${pollInterval.toMillis()}ms") {
              strategy == CDC
            }
            .putConditionally("neo4j.cdc.poll-duration", "${pollDuration.toMillis()}ms") {
              strategy == CDC
            }
            .putCdcParameters("neo4j.cdc.topic.%s.patterns.%s.operation", cdcOperations)
            .putCdcParameters("neo4j.cdc.topic.%s.patterns.%s.changesTo", cdcChangesTo)
            .putCdcPatterns(cdcPatterns, cdcPatternsIndexed)
            .putCdcMetadata(cdcMetadata)
            .toMap()

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
  }
}
