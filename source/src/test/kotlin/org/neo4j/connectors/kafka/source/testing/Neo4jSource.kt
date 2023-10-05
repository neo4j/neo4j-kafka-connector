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

import org.junit.jupiter.api.extension.ExtendWith
import streams.kafka.connect.source.StreamingFrom

@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
@ExtendWith(Neo4jSourceExtension::class)
annotation class Neo4jSource(
    val schemaControlRegistryUri: String,
    val kafkaConnectUri: String,
    val topic: String,
    val neo4jUri: String,
    val neo4jUser: String = "neo4j",
    val neo4jPassword: String,
    val streamingProperty: String,
    val streamingFrom: StreamingFrom,
    val streamingQuery: String,
    val brokerExternalHost: String,
    val schemaControlRegistryExternalUri: String,
    val consumerOffset: String = "latest"
)
