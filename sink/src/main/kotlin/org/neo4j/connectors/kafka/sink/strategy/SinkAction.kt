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

sealed class Matcher

sealed class NodeMatcher : Matcher() {
  data class ByLabelsAndProperties(val labels: Set<String>, val properties: Map<String, Any?>) :
      NodeMatcher() {
    init {
      require(properties.isNotEmpty()) { "properties can not be empty for node matchers" }
    }
  }

  data class ById(val id: Long) : NodeMatcher()

  data class ByElementId(val elementId: String) : NodeMatcher()
}

sealed class RelationshipMatcher : Matcher() {
  data class ByTypeAndProperties(
      val type: String,
      val properties: Map<String, Any?>,
      val hasKeys: Boolean,
  ) : RelationshipMatcher() {
    init {
      require(type.isNotBlank()) { "type can not be blank." }
    }
  }

  data class ById(val id: Long) : RelationshipMatcher()

  data class ByElementId(val elementId: String) : RelationshipMatcher()
}

data class CreateNodeSinkAction(val labels: Set<String>, val properties: Map<String, Any?>) :
    SinkAction()

data class UpdateNodeSinkAction(
    val matcher: NodeMatcher,
    val setProperties: Map<String, Any?>,
    val addLabels: Set<String>,
    val removeLabels: Set<String>,
) : SinkAction()

data class MergeNodeSinkAction(
    val matcher: NodeMatcher,
    val setProperties: Map<String, Any?>,
    val addLabels: Set<String>,
    val removeLabels: Set<String>,
) : SinkAction() {
  init {
    require(matcher is NodeMatcher.ByLabelsAndProperties) {
      "can only use labels and properties as a matcher for merge node action."
    }
  }
}

data class DeleteNodeSinkAction(val matcher: NodeMatcher) : SinkAction()

enum class LookupMode {
  MATCH,
  MERGE,
}

data class SinkActionNodeReference(val matcher: NodeMatcher, val lookupMode: LookupMode) {
  init {
    if (lookupMode == LookupMode.MERGE) {
      require(matcher is NodeMatcher.ByLabelsAndProperties) {
        "can only use labels and properties as a matcher for MERGE lookup mode."
      }
    }
  }
}

data class CreateRelationshipSinkAction(
    val startNode: SinkActionNodeReference,
    val endNode: SinkActionNodeReference,
    val type: String,
    val properties: Map<String, Any?>,
) : SinkAction() {
  init {
    require(type.isNotBlank()) { "type can not be blank." }
  }
}

data class UpdateRelationshipSinkAction(
    val startNode: SinkActionNodeReference,
    val endNode: SinkActionNodeReference,
    val matcher: RelationshipMatcher,
    val setProperties: Map<String, Any?>,
) : SinkAction() {
  init {
    require(startNode.lookupMode == LookupMode.MATCH) {
      "start node must use MATCH lookup mode for update relationship action."
    }
    require(endNode.lookupMode == LookupMode.MATCH) {
      "start node must use MATCH lookup mode for update relationship action."
    }
  }
}

data class MergeRelationshipSinkAction(
    val startNode: SinkActionNodeReference,
    val endNode: SinkActionNodeReference,
    val matcher: RelationshipMatcher,
    val setProperties: Map<String, Any?>,
) : SinkAction()

data class DeleteRelationshipSinkAction(
    val startNode: SinkActionNodeReference,
    val endNode: SinkActionNodeReference,
    val matcher: RelationshipMatcher,
) : SinkAction() {
  init {
    require(startNode.lookupMode == LookupMode.MATCH) {
      "start node must use MATCH lookup mode for delete relationship action."
    }
    require(endNode.lookupMode == LookupMode.MATCH) {
      "start node must use MATCH lookup mode for delete relationship action."
    }
  }
}
