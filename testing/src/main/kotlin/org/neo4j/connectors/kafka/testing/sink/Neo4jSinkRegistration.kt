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
package org.neo4j.connectors.kafka.testing.sink

import java.net.URI
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import org.neo4j.connectors.kafka.testing.MapSupport.nestUnder
import org.neo4j.connectors.kafka.testing.RegistrationSupport
import org.neo4j.connectors.kafka.testing.RegistrationSupport.randomizedName

class Neo4jSinkRegistration(
    topicQuerys: Map<String, String>,
    neo4jUri: String,
    neo4jUsername: String,
    neo4jPassword: String,
    enableKeySchemas: Boolean = false,
    enableValueSchemas: Boolean = false,
    retryTimeout: Duration = (-1).milliseconds,
    retryMaxDelay: Duration = 1000.milliseconds,
    errorTolerance: String = "all",
    enableErrorLog: Boolean = true,
    includeMessagesInErrorLog: Boolean = true,
) {

  private val name: String = randomizedName("Neo4jSinkConnector")
  private val payload: Map<String, Any>

  init {
    val queries = topicQuerys.mapKeys { "neo4j.topic.cypher.${it.key}" }
    payload =
        mutableMapOf(
                "name" to name,
                "config" to
                    mutableMapOf(
                        "topics" to topicQuerys.keys.joinToString(","),
                        "connector.class" to "streams.kafka.connect.sink.Neo4jSinkConnector",
                        "key.converter" to "org.apache.kafka.connect.json.JsonConverter",
                        "key.converter.schemas.enable" to enableKeySchemas,
                        "value.converter" to "org.apache.kafka.connect.json.JsonConverter",
                        "value.converter.schemas.enable" to enableValueSchemas,
                        "errors.retry.timeout" to retryTimeout.inWholeMilliseconds,
                        "errors.retry.delay.max.ms" to retryMaxDelay.inWholeMilliseconds,
                        "errors.tolerance" to errorTolerance,
                        "errors.log.enable" to enableErrorLog,
                        "errors.log.include.messages" to includeMessagesInErrorLog,
                        "neo4j.server.uri" to neo4jUri,
                        "neo4j.authentication.basic.username" to neo4jUsername,
                        "neo4j.authentication.basic.password" to neo4jPassword,
                    ))
            .nestUnder("config", queries)
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

  internal fun getPayload(): Map<String, Any> {
    return payload
  }
}
