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
@ExtendWith(Neo4jSinkExtension::class)
annotation class Neo4jSink(
    val brokerExternalHost: String = DEFAULT_TO_ENV,
    val kafkaConnectExternalUri: String = DEFAULT_TO_ENV,
    val schemaControlRegistryUri: String = DEFAULT_TO_ENV,
    val schemaControlRegistryExternalUri: String = DEFAULT_TO_ENV,
    val schemaControlKeyCompatibility: SchemaCompatibilityMode = SchemaCompatibilityMode.BACKWARD,
    val schemaControlValueCompatibility: SchemaCompatibilityMode = SchemaCompatibilityMode.BACKWARD,
    val neo4jUri: String = DEFAULT_TO_ENV,
    val neo4jExternalUri: String = DEFAULT_TO_ENV,
    val neo4jUser: String = DEFAULT_TO_ENV,
    val neo4jPassword: String = DEFAULT_TO_ENV,
    val neo4jDatabase: String = "",
    val dropDatabase: Boolean = true,
    val cypher: Array<CypherStrategy> = [],
    val cdcSchema: Array<CdcSchemaStrategy> = [],
    val cdcSourceId: Array<CdcSourceIdStrategy> = [],
    val nodePattern: Array<NodePatternStrategy> = [],
    val relationshipPattern: Array<RelationshipPatternStrategy> = [],
    val cud: Array<CudStrategy> = [],
    val errorTolerance: String = "all",
    val errorDlqTopic: String = "",
    val enableErrorHeaders: Boolean = false,
)

enum class SchemaCompatibilityMode {
  BACKWARD,
  BACKWARD_TRANSITIVE,
  FORWARD,
  FORWARD_TRANSITIVE,
  FULL,
  FULL_TRANSITIVE,
  NONE
}

annotation class CypherStrategy(
    val topic: String = "",
    val query: String,
    val bindTimestampAs: String = "__timestamp",
    val bindHeaderAs: String = "__header",
    val bindKeyAs: String = "__key",
    val bindValueAs: String = "__value",
    val bindValueAsEvent: Boolean = true
)

annotation class CdcSourceIdStrategy(
    val topic: String,
    val labelName: String = "",
    val propertyName: String = ""
)

annotation class CdcSchemaStrategy(
    val topic: String,
)

annotation class NodePatternStrategy(
    val topic: String,
    val pattern: String,
    val mergeNodeProperties: Boolean
)

annotation class RelationshipPatternStrategy(
    val topic: String,
    val pattern: String,
    val mergeNodeProperties: Boolean,
    val mergeRelationshipProperties: Boolean
)

annotation class CudStrategy(val topic: String)
