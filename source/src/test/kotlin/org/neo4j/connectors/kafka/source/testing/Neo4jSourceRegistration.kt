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

import com.fasterxml.jackson.databind.ObjectMapper
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpRequest.BodyPublishers
import java.net.http.HttpResponse.BodyHandlers
import java.time.Duration
import java.util.concurrent.ThreadLocalRandom
import kotlin.streams.asSequence
import org.neo4j.connectors.kafka.source.StreamingFrom
import streams.kafka.connect.source.Neo4jSourceConnector

class Neo4jSourceRegistration(
    topic: String,
    neo4jUri: String,
    neo4jUser: String = "neo4j",
    neo4jPassword: String,
    pollInterval: Duration = Duration.ofMillis(5000),
    enforceSchema: Boolean = true,
    streamingProperty: String,
    streamingFrom: StreamingFrom,
    streamingQuery: String,
    schemaControlRegistryUri: String
) {

  private val name: String = randomized("Neo4jSourceConnector")
  private val payload: Map<String, Any>

  init {
    payload =
        mapOf(
            "name" to name,
            "config" to
                mapOf(
                    "topic" to topic,
                    "connector.class" to Neo4jSourceConnector::class.java.name,
                    "key.converter" to "io.confluent.connect.avro.AvroConverter",
                    "key.converter.schema.registry.url" to "http://schema-registry:8081",
                    "value.converter" to "io.confluent.connect.avro.AvroConverter",
                    "value.converter.schema.registry.url" to schemaControlRegistryUri,
                    "neo4j.server.uri" to neo4jUri,
                    "neo4j.authentication.basic.username" to neo4jUser,
                    "neo4j.authentication.basic.password" to neo4jPassword,
                    "neo4j.streaming.poll.interval.msecs" to pollInterval.toMillis(),
                    "neo4j.streaming.property" to streamingProperty,
                    "neo4j.streaming.from" to streamingFrom.name,
                    "neo4j.enforce.schema" to enforceSchema,
                    "neo4j.source.query" to streamingQuery,
                ),
        )
  }

  private lateinit var connectBaseUri: String

  fun register(connectBaseUri: String) {
    this.connectBaseUri = connectBaseUri
    val uri = URI("${this.connectBaseUri}/connectors")
    val requestBody = registrationJson()
    val registration =
        HttpRequest.newBuilder(uri)
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
            .POST(BodyPublishers.ofString(requestBody))
            .build()
    val response = HttpClient.newHttpClient().send(registration, BodyHandlers.ofString())
    if (response.statusCode() != 201) {
      val error =
          String.format(
              "Could not register source, expected 201, got: %s\n%s",
              response.statusCode(),
              response.body(),
          )
      throw RuntimeException(error)
    }
  }

  fun unregister() {
    val uri = URI("$connectBaseUri/connectors/$name/")
    val deregistration =
        HttpRequest.newBuilder(uri).header("Accept", "application/json").DELETE().build()
    val response = HttpClient.newHttpClient().send(deregistration, BodyHandlers.ofString())
    if (response.statusCode() != 204) {
      val error =
          String.format(
              "Could not unregister source, expected 204, got: %s\n%s",
              response.statusCode(),
              response.body(),
          )
      throw RuntimeException(error)
    }
  }

  private fun registrationJson(): String {
    return ObjectMapper().writeValueAsString(payload)
  }

  companion object {
    private fun randomized(baseName: String): String {
      val charPool: List<Char> = ('a'..'z') + ('A'..'Z') + ('0'..'9')
      val suffix =
          ThreadLocalRandom.current()
              .ints(8, 0, charPool.size)
              .asSequence()
              .map(charPool::get)
              .joinToString("")
      return "${baseName}_$suffix"
    }
  }
}
