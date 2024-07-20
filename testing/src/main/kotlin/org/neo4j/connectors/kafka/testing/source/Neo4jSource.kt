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
package org.neo4j.connectors.kafka.testing.source

import org.junit.jupiter.api.extension.ExtendWith
import org.neo4j.connectors.kafka.TemporalDataSchemaType
import org.neo4j.connectors.kafka.testing.DEFAULT_TO_ENV

@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
@ExtendWith(Neo4jSourceExtension::class)
annotation class Neo4jSource(
    val brokerExternalHost: String = DEFAULT_TO_ENV,
    val schemaControlRegistryUri: String = DEFAULT_TO_ENV,
    val schemaControlRegistryExternalUri: String = DEFAULT_TO_ENV,
    val kafkaConnectExternalUri: String = DEFAULT_TO_ENV,
    val neo4jUri: String = DEFAULT_TO_ENV,
    val neo4jExternalUri: String = DEFAULT_TO_ENV,
    val neo4jUser: String = DEFAULT_TO_ENV,
    val neo4jPassword: String = DEFAULT_TO_ENV,
    val neo4jDatabase: String = "",
    val startFrom: String = "NOW",
    val startFromValue: String = "",
    val strategy: SourceStrategy = SourceStrategy.QUERY,
    val temporalDataSchemaType: TemporalDataSchemaType = TemporalDataSchemaType.STRUCT,

    // QUERY strategy
    val topic: String = "",
    val streamingProperty: String = "",
    val query: String = "",

    // CDC strategy
    val cdc: CdcSource = CdcSource()
)

enum class SourceStrategy {
  QUERY,
  CDC
}

annotation class CdcSource(
    val patternsIndexed: Boolean = false,
    val topics: Array<CdcSourceTopic> = []
)

annotation class CdcSourceTopic(
    val topic: String,
    val patterns: Array<CdcSourceParam> = [],
    val operations: Array<CdcSourceParam> = [],
    val changesTo: Array<CdcSourceParam> = [],
    val metadata: Array<CdcMetadata> = [],
    val keySerialization: String = "WHOLE_VALUE",
)

annotation class CdcSourceParam(val value: String, val index: Int = 0)

annotation class CdcMetadata(val key: String, val value: String, val index: Int = 0)
