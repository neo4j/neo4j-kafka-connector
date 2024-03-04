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
import org.neo4j.connectors.kafka.testing.sink.SchemaCompatibilityMode

internal object SchemaRegistrySupport {

  private val mapper = ObjectMapper()

  fun setCompatibilityMode(registryUri: URI, subject: String, mode: SchemaCompatibilityMode) {
    val update =
        HttpRequest.newBuilder(registryUri.resolve("/config/$subject"))
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
            .PUT(
                HttpRequest.BodyPublishers.ofString(
                    convertToJson(mapOf("compatibility" to mode.name))))
            .build()
    val response = HttpClient.newHttpClient().send(update, HttpResponse.BodyHandlers.ofString())
    if (response.statusCode() != 200) {
      val error =
          String.format(
              "Could not update compatibility mode, expected 200, got: %s\n%s",
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
