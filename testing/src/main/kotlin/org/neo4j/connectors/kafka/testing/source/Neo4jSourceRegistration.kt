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
import org.neo4j.connectors.kafka.testing.RegistrationSupport.randomizedName
import org.neo4j.connectors.kafka.testing.RegistrationSupport.registerConnector
import org.neo4j.connectors.kafka.testing.RegistrationSupport.unregisterConnector

internal class Neo4jSourceRegistration(
    topic: String,
    neo4jUri: String,
    neo4jUser: String = "neo4j",
    neo4jPassword: String,
    pollInterval: Duration = Duration.ofMillis(5000),
    enforceSchema: Boolean = true,
    streamingProperty: String,
    streamingFrom: String,
    streamingQuery: String,
    schemaControlRegistryUri: String
) {

  private val name: String = randomizedName("Neo4jSourceConnector")
  private val payload: Map<String, Any>

  init {
    payload =
        mapOf(
            "name" to name,
            "config" to
                mapOf(
                    "topic" to topic,
                    "connector.class" to "streams.kafka.connect.source.Neo4jSourceConnector",
                    "key.converter" to "io.confluent.connect.avro.AvroConverter",
                    "key.converter.schema.registry.url" to "http://schema-registry:8081",
                    "value.converter" to "io.confluent.connect.avro.AvroConverter",
                    "value.converter.schema.registry.url" to schemaControlRegistryUri,
                    "neo4j.server.uri" to neo4jUri,
                    "neo4j.authentication.basic.username" to neo4jUser,
                    "neo4j.authentication.basic.password" to neo4jPassword,
                    "neo4j.streaming.poll.interval.msecs" to pollInterval.toMillis(),
                    "neo4j.streaming.property" to streamingProperty,
                    "neo4j.streaming.from" to streamingFrom,
                    "neo4j.enforce.schema" to enforceSchema,
                    "neo4j.source.query" to streamingQuery,
                ),
        )
  }

  private lateinit var connectBaseUri: String

  fun register(connectBaseUri: String) {
    this.connectBaseUri = connectBaseUri
    registerConnector(URI("${this.connectBaseUri}/connectors"), payload)
  }

  fun unregister() {
    unregisterConnector(URI("$connectBaseUri/connectors/$name/"))
  }
}
