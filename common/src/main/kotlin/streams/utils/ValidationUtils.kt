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
package streams.utils

import java.io.IOException
import java.net.Socket
import java.net.URI

object ValidationUtils {

  fun isServerReachable(url: String, port: Int): Boolean =
    try {
      Socket(url, port).use { true }
    } catch (e: IOException) {
      false
    }

  fun checkServersUnreachable(urls: String, separator: String = ","): List<String> =
    urls
      .split(separator)
      .map {
        val uri = URI.create(it)
        when (uri.host.isNullOrBlank()) {
          true -> {
            val splitted = it.split(":")
            URI("fake-scheme", "", splitted.first(), splitted.last().toInt(), "", "", "")
          }
          else -> uri
        }
      }
      .filter { uri -> !isServerReachable(uri.host, uri.port) }
      .map { if (it.scheme == "fake-scheme") "${it.host}:${it.port}" else it.toString() }

  fun validateConnection(url: String, kafkaPropertyKey: String, checkReachable: Boolean = true) {
    if (url.isBlank()) {
      throw RuntimeException("The `kafka.$kafkaPropertyKey` property is empty")
    } else if (checkReachable) {
      val unreachableServers = checkServersUnreachable(url)
      if (unreachableServers.isNotEmpty()) {
        throw RuntimeException(
          "The servers defined into the property `kafka.$kafkaPropertyKey` are not reachable: $unreachableServers")
      }
    }
  }
}
