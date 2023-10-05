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
package streams.kafka.connect.source.testing

import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpRequest.BodyPublishers
import java.net.http.HttpResponse.BodyHandlers
import java.time.Duration
import java.util.concurrent.ThreadLocalRandom
import kotlin.streams.asSequence
import streams.kafka.connect.source.StreamingFrom

data class Neo4jSourceRegistration(
    private val topic: String,
    private val neo4jUri: String,
    private val neo4jUser: String = "neo4j",
    private val neo4jPassword: String,
    private val pollInterval: Duration = Duration.ofMillis(5000),
    private val enforceSchema: Boolean = true,
    private val streamingProperty: String,
    private val streamingFrom: StreamingFrom,
    private val streamingQuery: String,
    private val schemaControlRegistry: String
) {

  private val name: String = randomized("Neo4jSourceConnector")
  private lateinit var controlCenterBaseUri: String

  fun register(controlCenterUri: String) {
    controlCenterBaseUri = controlCenterUri
    val uri = URI("$controlCenterBaseUri/connectors")
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
    val uri = URI("$controlCenterBaseUri/connectors/$name/")
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
    return java.lang
        .StringBuilder()
        .append("{")
        .append("\"name\": ")
        .append(quoted(name))
        .append(",")
        .append("\"config\": {")
        .append("\"topic\": ")
        .append(quoted(topic))
        .append(",")
        .append("\"connector.class\": \"streams.kafka.connect.source.Neo4jSourceConnector\",")
        .append("\"key.converter\": \"io.confluent.connect.avro.AvroConverter\",")
        .append("\"key.converter.schema.registry.url\": ")
        .append(quoted(schemaControlRegistry))
        .append(",")
        .append("\"value.converter\": \"io.confluent.connect.avro.AvroConverter\",")
        .append("\"value.converter.schema.registry.url\": ")
        .append(quoted(schemaControlRegistry))
        .append(",")
        .append("\"neo4j.server.uri\": ")
        .append(quoted(neo4jUri))
        .append(",")
        .append("\"neo4j.authentication.basic.username\": ")
        .append(quoted(neo4jUser))
        .append(",")
        .append("\"neo4j.authentication.basic.password\": ")
        .append(quoted(neo4jPassword))
        .append(",")
        .append("\"neo4j.streaming.poll.interval.msecs\": ")
        .append(pollInterval.toMillis())
        .append(",")
        .append("\"neo4j.streaming.property\": ")
        .append(quoted(streamingProperty))
        .append(",")
        .append("\"neo4j.streaming.from\": ")
        .append(quoted(streamingFrom.name))
        .append(",")
        .append("\"neo4j.enforce.schema\": ")
        .append(enforceSchema)
        .append(",")
        .append("\"neo4j.source.query\": ")
        .append(quoted(streamingQuery))
        .append("}")
        .append("}")
        .toString()
  }

  companion object {
    private fun quoted(string: String): String {
      return String.format("\"%s\"", string)
    }

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
