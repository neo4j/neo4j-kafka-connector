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

import org.junit.jupiter.api.extension.ExtendWith
import org.neo4j.connectors.kafka.testing.DEFAULT_TO_ENV

@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
@ExtendWith(LegacyNeo4jSinkExtension::class)
annotation class LegacyNeo4jSink(
    val brokerExternalHost: String = DEFAULT_TO_ENV,
    val kafkaConnectExternalUri: String = DEFAULT_TO_ENV,
    val schemaControlRegistryUri: String = DEFAULT_TO_ENV,
    val schemaControlRegistryExternalUri: String = DEFAULT_TO_ENV,
    val neo4jUri: String = DEFAULT_TO_ENV,
    val neo4jExternalUri: String = DEFAULT_TO_ENV,
    val neo4jUser: String = DEFAULT_TO_ENV,
    val neo4jPassword: String = DEFAULT_TO_ENV,
    val topics: Array<String>,
    val queries: Array<String>
)
