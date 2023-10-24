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
package org.neo4j.connectors.kafka.testing

import com.fasterxml.jackson.databind.ObjectMapper
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.concurrent.ThreadLocalRandom
import kotlin.streams.asSequence

internal object RegistrationSupport {

  private val mapper = ObjectMapper()

  fun randomizedName(baseName: String): String {
    val charPool: List<Char> = ('a'..'z') + ('A'..'Z') + ('0'..'9')
    val suffix =
        ThreadLocalRandom.current()
            .ints(8, 0, charPool.size)
            .asSequence()
            .map(charPool::get)
            .joinToString("")
    return "${baseName}_$suffix"
  }

  fun registerConnector(uri: URI, payload: Map<String, Any>) {
    val creation =
        HttpRequest.newBuilder(uri)
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(convertToJson(payload)))
            .build()
    val response = HttpClient.newHttpClient().send(creation, HttpResponse.BodyHandlers.ofString())
    if (response.statusCode() != 201) {
      val error =
          String.format(
              "Could not register connector, expected 201, got: %s\n%s",
              response.statusCode(),
              response.body(),
          )
      throw RuntimeException(error)
    }
  }

  fun unregisterConnector(uri: URI) {
    val deletion = HttpRequest.newBuilder(uri).header("Accept", "application/json").DELETE().build()
    val response = HttpClient.newHttpClient().send(deletion, HttpResponse.BodyHandlers.ofString())
    if (response.statusCode() != 204) {
      val error =
          String.format(
              "Could not unregister connector, expected 204, got: %s\n%s",
              response.statusCode(),
              response.body(),
          )
      throw RuntimeException(error)
    }
  }

  private fun convertToJson(payload: Map<String, Any>): String {
    return mapper.writeValueAsString(payload)
  }
}
