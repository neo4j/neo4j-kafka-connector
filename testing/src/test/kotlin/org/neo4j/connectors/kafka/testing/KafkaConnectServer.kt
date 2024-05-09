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
package org.neo4j.connectors.kafka.testing

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import java.net.InetSocketAddress

class KafkaConnectServer : AutoCloseable {

  companion object {
    val CANDIDATE_ACCEPT_HEADERS = setOf("*/*", "application/*", "*/json", "application/json")
  }

  private var started = false
  private val httpServer: HttpServer = HttpServer.create(InetSocketAddress(0), 0)

  /**
   * Starts the fake Kafka Connect server
   *
   * @param registrationHandler an optional handler to act on the ongoing HTTP exchange for
   *   connector registration
   * @param unregistrationHandler an optional handler to act on the ongoing HTTP exchange for
   *   connector registration Note that both handlers should return true if
   *   HttpExchange.sendResponseHeaders is called and false otherwise
   */
  fun start(
      registrationHandler: (HttpExchange) -> Boolean = {
        it.sendResponseHeaders(201, -1)
        true
      },
      unregistrationHandler: (HttpExchange) -> Boolean = {
        it.sendResponseHeaders(204, -1)
        true
      }
  ) {

    httpServer.createContext("/connectors") { exchange ->
      val path = exchange.requestURI.path.replaceFirst("/connectors/?([^/]*)/?".toRegex(), "$1")

      when {
        path.isEmpty() -> handleRegistration(exchange, registrationHandler)
        !path.contains("/") -> handleUnregistration(exchange, unregistrationHandler)
        else -> exchange.sendResponseHeaders(404, 0)
      }
      exchange.close()
    }
    this.started = true
    this.httpServer.start()
  }

  fun address(): String {
    return "http://localhost:${httpServer.address.port}"
  }

  fun internalServerError(exchange: HttpExchange, error: String) {
    exchange.sendResponseHeaders(500, -1)
    exchange.responseBody.bufferedWriter().write(error)
  }

  override fun close() {
    if (this.started) {
      this.started = false
      httpServer.stop(0)
    }
  }

  private fun handleRegistration(exchange: HttpExchange, handler: (HttpExchange) -> Boolean) {
    if (!validateMethod(exchange, "POST") ||
        !validateJsonIn(exchange) ||
        !validateJsonOut(exchange)) {
      return
    }
    if (!handler(exchange)) {
      exchange.sendResponseHeaders(201, -1)
    }
  }

  private fun handleUnregistration(exchange: HttpExchange, handler: (HttpExchange) -> Boolean) {
    if (!validateMethod(exchange, "DELETE") || !validateJsonOut(exchange)) {
      return
    }
    if (!handler(exchange)) {
      exchange.sendResponseHeaders(204, -1)
    }
  }

  private fun validateMethod(exchange: HttpExchange, method: String): Boolean {
    if (exchange.requestMethod != method) {
      exchange.sendResponseHeaders(405, -1)
      return false
    }
    return true
  }

  private fun validateJsonIn(exchange: HttpExchange): Boolean {
    val contentType = exchange.requestHeaders["Content-Type"]!!
    if (contentType.size != 1) {
      exchange.sendResponseHeaders(400, -1)
      exchange.responseBody.bufferedWriter().write("unparseable Content-Type")
      return false
    }
    if (contentType[0] != "application/json") {
      exchange.sendResponseHeaders(415, -1)
      return false
    }
    return true
  }

  private fun validateJsonOut(exchange: HttpExchange): Boolean {
    val acceptedMedia = exchange.requestHeaders["Accept"]!!
    if (acceptedMedia.isNotEmpty() &&
        acceptedMedia.none { CANDIDATE_ACCEPT_HEADERS.contains(it) }) {
      exchange.sendResponseHeaders(406, -1)
      return false
    }
    return true
  }
}
