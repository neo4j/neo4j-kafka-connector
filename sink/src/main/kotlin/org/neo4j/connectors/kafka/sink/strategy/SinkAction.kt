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
package org.neo4j.connectors.kafka.sink.strategy

const val EVENT = "e"

sealed class SinkAction

data class CreateNodeSinkAction(val labels: Set<String>, val properties: Map<String, Any?>) :
    SinkAction()

data class UpdateNodeSinkAction(
    val matchLabels: Set<String>,
    val matchProperties: Map<String, Any?>,
    val setProperties: Map<String, Any?>,
    val addLabels: Set<String>,
    val removeLabels: Set<String>,
) : SinkAction()

data class MergeNodeSinkAction(
    val matchLabels: Set<String>,
    val matchProperties: Map<String, Any?>,
    val setProperties: Map<String, Any?>,
    val addLabels: Set<String>,
    val removeLabels: Set<String>,
) : SinkAction()

data class DeleteNodeSinkAction(
    val matchLabels: Set<String>,
    val matchProperties: Map<String, Any?>,
) : SinkAction()

enum class LookupMode {
  MATCH,
  MERGE,
}

data class SinkActionNodeReference(
    val labels: Set<String>,
    val properties: Map<String, Any?>,
    val lookupMode: LookupMode,
) {
  companion object {
    val EMPTY = SinkActionNodeReference(emptySet(), emptyMap(), LookupMode.MATCH)
  }
}

data class CreateRelationshipSinkAction(
    val startNode: SinkActionNodeReference,
    val endNode: SinkActionNodeReference,
    val type: String,
    val properties: Map<String, Any?>,
) : SinkAction()

data class UpdateRelationshipSinkAction(
    val startNode: SinkActionNodeReference,
    val endNode: SinkActionNodeReference,
    val matchType: String,
    val matchProperties: Map<String, Any?>,
    val setProperties: Map<String, Any?>,
    val hasKeys: Boolean,
) : SinkAction()

data class MergeRelationshipSinkAction(
    val startNode: SinkActionNodeReference,
    val endNode: SinkActionNodeReference,
    val matchType: String,
    val matchProperties: Map<String, Any?>,
    val setProperties: Map<String, Any?>,
    val hasKeys: Boolean,
) : SinkAction()

data class DeleteRelationshipSinkAction(
    val startNode: SinkActionNodeReference,
    val endNode: SinkActionNodeReference,
    val matchType: String,
    val matchProperties: Map<String, Any?>,
    val hasKeys: Boolean,
) : SinkAction()
