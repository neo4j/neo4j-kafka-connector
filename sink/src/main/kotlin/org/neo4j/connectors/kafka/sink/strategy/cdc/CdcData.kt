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
package org.neo4j.connectors.kafka.sink.strategy.cdc

import org.neo4j.cdc.client.model.EntityOperation

const val EVENT = "e"

interface CdcData {}

data class CdcNodeData(
    val operation: EntityOperation,
    val matchLabels: Set<String>,
    val matchProperties: Map<String, Any?>,
    val setProperties: Map<String, Any?>,
    val addLabels: Set<String>,
    val removeLabels: Set<String>,
) : CdcData

data class CdcRelationshipData(
    val operation: EntityOperation,
    val startMatchLabels: Set<String>,
    val startMatchProperties: Map<String, Any?>,
    val endMatchLabels: Set<String>,
    val endMatchProperties: Map<String, Any?>,
    val matchType: String,
    val matchProperties: Map<String, Any?>,
    val hasKeys: Boolean,
    val setProperties: Map<String, Any?>,
) : CdcData
