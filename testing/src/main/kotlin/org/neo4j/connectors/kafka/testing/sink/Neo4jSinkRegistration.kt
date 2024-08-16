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
package org.neo4j.connectors.kafka.testing.sink

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import org.neo4j.connectors.kafka.testing.RegistrationSupport
import org.neo4j.connectors.kafka.testing.RegistrationSupport.randomizedName
import org.neo4j.connectors.kafka.testing.format.KafkaConverter

class Neo4jSinkRegistration(
    neo4jUri: String,
    neo4jUser: String,
    neo4jPassword: String,
    neo4jDatabase: String,
    retryTimeout: Duration = (-1).milliseconds,
    retryMaxDelay: Duration = 1000.milliseconds,
    errorTolerance: String = "none",
    errorDlqTopic: String = "",
    enableErrorHeaders: Boolean = false,
    enableErrorLog: Boolean = true,
    includeMessagesInErrorLog: Boolean = true,
    schemaControlRegistryUri: String,
    keyConverter: KafkaConverter,
    valueConverter: KafkaConverter,
    topics: List<String>,
    strategies: Map<String, Any>
) {

  private val name: String = randomizedName("Neo4jSinkConnector")
  private val payload: Map<String, Any>

  companion object {
    private const val DLQ_TOPIC_REPLICATION_FACTOR = 1
  }

  init {
    payload =
        mutableMapOf(
                "name" to name,
                "config" to
                    buildMap {
                      put("topics", topics.joinToString(","))
                      put("connector.class", "org.neo4j.connectors.kafka.sink.Neo4jConnector")
                      put("key.converter", keyConverter.className)
                      if (keyConverter == KafkaConverter.JSON_EMBEDDED) {
                        put("key.converter.schemas.enable", true)
                      }
                      put("value.converter", valueConverter.className)
                      if (valueConverter == KafkaConverter.JSON_EMBEDDED) {
                        put("value.converter.schemas.enable", true)
                      }
                      put("errors.retry.timeout", retryTimeout.inWholeMilliseconds)
                      put("errors.retry.delay.max.ms", retryMaxDelay.inWholeMilliseconds)
                      put("errors.tolerance", errorTolerance)
                      if (errorDlqTopic.trim().isNotEmpty()) {
                        put("errors.deadletterqueue.topic.name", errorDlqTopic)
                        put(
                            "errors.deadletterqueue.topic.replication.factor",
                            DLQ_TOPIC_REPLICATION_FACTOR)
                      }
                      put("errors.deadletterqueue.context.headers.enable", enableErrorHeaders)
                      put("errors.log.enable", enableErrorLog)
                      put("errors.log.include.messages", includeMessagesInErrorLog)
                      put("neo4j.uri", neo4jUri)
                      put("neo4j.authentication.type", "BASIC")
                      put("neo4j.authentication.basic.username", neo4jUser)
                      put("neo4j.authentication.basic.password", neo4jPassword)
                      put("neo4j.database", neo4jDatabase)

                      if (keyConverter.supportsSchemaRegistry) {
                        put("key.converter.schema.registry.url", schemaControlRegistryUri)
                      }
                      putAll(
                          keyConverter.additionalProperties.mapKeys { "key.converter.${it.key}" })

                      if (valueConverter.supportsSchemaRegistry) {
                        put("value.converter.schema.registry.url", schemaControlRegistryUri)
                      }
                      putAll(
                          valueConverter.additionalProperties.mapKeys {
                            "value.converter.${it.key}"
                          })

                      putAll(strategies)
                    })
            .toMap()
  }

  private lateinit var connectBaseUri: String

  fun register(baseUri: String) {
    this.connectBaseUri = baseUri
    RegistrationSupport.registerConnector(URI("${this.connectBaseUri}/connectors"), payload)
  }

  fun unregister() {
    RegistrationSupport.unregisterConnector(URI("$connectBaseUri/connectors/$name/"))
  }

  fun getConnectorTasksForStatusCheck(): JsonNode {
    val request =
        HttpRequest.newBuilder(URI("$connectBaseUri/connectors/$name/status"))
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
            .GET()
            .build()

    val response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString())
    if (response.statusCode() != 200) {
      throw RuntimeException(
          "Could not get connector status, response code: ${response.statusCode()}")
    }

    val connectorTasks = ObjectMapper().readTree(response.body()).get("tasks")
    return connectorTasks
  }

  internal fun getPayload(): Map<String, Any> {
    return payload
  }
}
