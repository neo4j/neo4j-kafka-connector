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

import kotlin.collections.buildMap
import org.neo4j.caniuse.CanIUse.canIUse
import org.neo4j.caniuse.Cypher
import org.neo4j.caniuse.Neo4j
import org.neo4j.cypherdsl.core.internal.SchemaNames
import org.neo4j.driver.Query

interface SinkActionStatementGenerator {

  fun buildStatement(data: SinkAction, eventVariable: String = "${'$'}$EVENT"): Query
}

class DefaultSinkActionStatementGenerator(neo4j: Neo4j) : SinkActionStatementGenerator {
  private val supportsDynamicLabelsWithPropertyIndices =
      canIUse(Cypher.dynamicLabelsAndTypesCanLeveragePropertyIndices()).withNeo4j(neo4j)
  private val setDynamicLabels = canIUse(Cypher.setDynamicLabels()).withNeo4j(neo4j)
  private val removeDynamicLabels = canIUse(Cypher.removeDynamicLabels()).withNeo4j(neo4j)

  override fun buildStatement(data: SinkAction, eventVariable: String): Query {
    return when (data) {
      is CreateNodeSinkAction -> buildNodeStatement(data, eventVariable)
      is UpdateNodeSinkAction -> buildNodeStatement(data, eventVariable)
      is MergeNodeSinkAction -> buildNodeStatement(data, eventVariable)
      is DeleteNodeSinkAction -> buildNodeStatement(data, eventVariable)
      is CreateRelationshipSinkAction -> buildRelationshipStatement(data, eventVariable)
      is UpdateRelationshipSinkAction -> buildRelationshipStatement(data, eventVariable)
      is MergeRelationshipSinkAction -> buildRelationshipStatement(data, eventVariable)
      is DeleteRelationshipSinkAction -> buildRelationshipStatement(data, eventVariable)
    }
  }

  private fun buildNodeStatement(action: CreateNodeSinkAction, eventVariable: String): Query {
    val labels = buildLabelPattern(action.labels, "_e", "labels")
    val stmt = "WITH $eventVariable AS _e CREATE (n$labels) SET n += _e.properties"

    val params = buildMap {
      if (supportsDynamicLabelsWithPropertyIndices) {
        this["labels"] = action.labels
      }
      this["properties"] = action.properties
    }

    return buildQuery(stmt, eventVariable, params)
  }

  private fun buildNodeStatement(action: UpdateNodeSinkAction, eventVariable: String): Query {
    return buildNodeUpdateStatement(
        LookupMode.MATCH,
        action.matcher,
        action.setProperties,
        action.addLabels,
        action.removeLabels,
        eventVariable,
    )
  }

  private fun buildNodeStatement(action: MergeNodeSinkAction, eventVariable: String): Query {
    return buildNodeUpdateStatement(
        LookupMode.MERGE,
        action.matcher,
        action.setProperties,
        action.addLabels,
        action.removeLabels,
        eventVariable,
    )
  }

  private fun buildNodeUpdateStatement(
      mode: LookupMode,
      matcher: NodeMatcher,
      setProperties: Map<String, Any?>,
      addLabels: Set<String>,
      removeLabels: Set<String>,
      eventVariable: String,
  ): Query {
    val matchFragment = buildNodeFragment(matcher, mode, "n", "_e")
    val setLabelsClause =
        if (setDynamicLabels) {
          " SET n:\$(_e.addLabels)"
        } else if (addLabels.isNotEmpty()) {
          " SET n" + buildLabels(addLabels)
        } else {
          ""
        }
    val removeLabelsClause =
        if (removeDynamicLabels) {
          " REMOVE n:\$(_e.removeLabels)"
        } else if (removeLabels.isNotEmpty()) {
          " REMOVE n" + buildLabels(removeLabels)
        } else {
          ""
        }
    val stmt =
        "WITH $eventVariable AS _e ${matchFragment.clause} SET n += _e.setProperties$setLabelsClause$removeLabelsClause"
    val params = buildMap {
      this.putAll(matchFragment.params)

      this["setProperties"] = setProperties
      if (setDynamicLabels) {
        this["addLabels"] = addLabels
      }
      if (removeDynamicLabels) {
        this["removeLabels"] = removeLabels
      }
    }

    return buildQuery(stmt, eventVariable, params)
  }

  private fun buildNodeStatement(action: DeleteNodeSinkAction, eventVariable: String): Query {
    val matchFragment = buildNodeFragment(action.matcher, LookupMode.MATCH, "n", "_e")
    val stmt = "WITH $eventVariable AS _e ${matchFragment.clause} DELETE n"
    val params = matchFragment.params

    return buildQuery(stmt, eventVariable, params)
  }

  private fun buildRelationshipStatement(
      action: CreateRelationshipSinkAction,
      eventVariable: String,
  ): Query {
    val nodeFragments = buildNodeFragments(action.startNode, action.endNode, "_e")
    val typePattern = buildTypePattern(action.type, "_e", "type")

    val stmt =
        "WITH $eventVariable AS _e ${nodeFragments.start.clause} ${nodeFragments.end.clause} CREATE (start)-[r:$typePattern]->(end) SET r += _e.properties"
    val params = buildMap {
      if (nodeFragments.start.params.isNotEmpty()) {
        this["start"] = nodeFragments.start.params
      }
      if (nodeFragments.end.params.isNotEmpty()) {
        this["end"] = nodeFragments.end.params
      }
      if (supportsDynamicLabelsWithPropertyIndices) {
        this["type"] = action.type
      }
      this["properties"] = action.properties
    }
    return buildQuery(stmt, eventVariable, params)
  }

  private fun buildRelationshipStatement(
      action: UpdateRelationshipSinkAction,
      eventVariable: String,
  ): Query {
    return buildRelationshipUpdateStatement(
        LookupMode.MATCH,
        action.startNode,
        action.endNode,
        action.matcher,
        action.setProperties,
        eventVariable,
    )
  }

  private fun buildRelationshipStatement(
      action: MergeRelationshipSinkAction,
      eventVariable: String,
  ): Query {
    return buildRelationshipUpdateStatement(
        LookupMode.MERGE,
        action.startNode,
        action.endNode,
        action.matcher,
        action.setProperties,
        eventVariable,
    )
  }

  private fun buildRelationshipUpdateStatement(
      mode: LookupMode,
      startNode: SinkActionNodeReference,
      endNode: SinkActionNodeReference,
      matcher: RelationshipMatcher,
      setProperties: Map<String, Any?>,
      eventVariable: String,
  ): Query {
    val matchFragment = buildRelationshipFragment(matcher, mode, startNode, endNode, "r", "_e")
    val stmt =
        buildRelationshipStatementWithKeylessHandling(
            matcher,
            eventVariable,
            matchFragment.clause,
            "SET r += _e.setProperties",
        )
    val params = buildMap {
      putAll(matchFragment.params)
      this["setProperties"] = setProperties
    }

    return buildQuery(stmt, eventVariable, params)
  }

  private fun buildRelationshipStatement(
      action: DeleteRelationshipSinkAction,
      eventVariable: String,
  ): Query {
    val matchFragment =
        buildRelationshipFragment(
            action.matcher,
            LookupMode.MATCH,
            action.startNode,
            action.endNode,
            "r",
            "_e",
        )
    val stmt =
        buildRelationshipStatementWithKeylessHandling(
            action.matcher,
            eventVariable,
            matchFragment.clause,
            "DELETE r",
        )
    val params = matchFragment.params

    return buildQuery(stmt, eventVariable, params)
  }

  data class Fragment(val clause: String, val params: Map<String, Any>)

  private fun buildNodeFragment(
      matcher: NodeMatcher,
      mode: LookupMode,
      alias: String,
      eventVariable: String,
  ): Fragment {
    return when (matcher) {
      is NodeMatcher.ByLabelsAndProperties ->
          buildByLabelsAndPropertiesFragment(matcher, mode, alias, eventVariable)
      is NodeMatcher.ById -> buildByIdFragment(matcher, mode, alias, eventVariable)
      is NodeMatcher.ByElementId -> buildByElementIdFragment(matcher, mode, alias, eventVariable)
    }
  }

  private fun buildByLabelsAndPropertiesFragment(
      matcher: NodeMatcher.ByLabelsAndProperties,
      mode: LookupMode,
      alias: String,
      eventVariable: String,
  ): Fragment {
    val matchLabelsPattern = buildLabelPattern(matcher.labels, eventVariable, "matchLabels")
    val matchPropsPattern = buildMatchProps(matcher.properties, eventVariable, "matchProperties")

    return Fragment(
        "$mode ($alias$matchLabelsPattern$matchPropsPattern)",
        buildMap {
          if (supportsDynamicLabelsWithPropertyIndices) {
            this["matchLabels"] = matcher.labels
          }
          this["matchProperties"] = matcher.properties
        },
    )
  }

  private fun buildByIdFragment(
      matcher: NodeMatcher.ById,
      mode: LookupMode,
      alias: String,
      eventVariable: String,
  ): Fragment {
    return Fragment(
        "$mode ($alias) WHERE id($alias) = $eventVariable.matchId",
        mapOf("matchId" to matcher.id),
    )
  }

  private fun buildByElementIdFragment(
      matcher: NodeMatcher.ByElementId,
      mode: LookupMode,
      alias: String,
      eventVariable: String,
  ): Fragment {
    return Fragment(
        "$mode ($alias) WHERE elementId($alias) = $eventVariable.matchElementId",
        mapOf("matchElementId" to matcher.elementId),
    )
  }

  @Suppress("SameParameterValue")
  private fun buildRelationshipFragment(
      matcher: RelationshipMatcher,
      mode: LookupMode,
      startNode: SinkActionNodeReference,
      endNode: SinkActionNodeReference,
      alias: String,
      eventVariable: String,
  ): Fragment {
    return when (matcher) {
      is RelationshipMatcher.ByTypeAndProperties ->
          buildByTypeAndPropertiesFragment(matcher, mode, startNode, endNode, alias, eventVariable)
      is RelationshipMatcher.ById ->
          buildByIdFragment(matcher, mode, startNode, endNode, alias, eventVariable)
      is RelationshipMatcher.ByElementId ->
          buildByElementIdFragment(matcher, mode, startNode, endNode, alias, eventVariable)
    }
  }

  private fun buildRelationshipFragmentWithNodes(
      startNode: SinkActionNodeReference,
      endNode: SinkActionNodeReference,
      eventVariable: String,
      relationshipPattern: String,
      additionalParams: Map<String, Any>,
  ): Fragment {
    val nodeFragments = buildNodeFragments(startNode, endNode, eventVariable)
    return Fragment(
        buildString {
          append(nodeFragments.start.clause).append(" ")
          append(nodeFragments.end.clause).append(" ")
          append(relationshipPattern)
        },
        buildMap {
          this["start"] = nodeFragments.start.params
          this["end"] = nodeFragments.end.params
          putAll(additionalParams)
        },
    )
  }

  private fun buildByTypeAndPropertiesFragment(
      matcher: RelationshipMatcher.ByTypeAndProperties,
      mode: LookupMode,
      startNode: SinkActionNodeReference,
      endNode: SinkActionNodeReference,
      alias: String,
      eventVariable: String,
  ): Fragment {
    val matchTypePattern = buildTypePattern(matcher.type, eventVariable, "matchType")
    val matchPropsPattern = buildMatchProps(matcher.properties, eventVariable, "matchProperties")
    val relationshipPattern = "$mode (start)-[$alias:$matchTypePattern$matchPropsPattern]->(end)"

    val additionalParams = buildMap {
      if (supportsDynamicLabelsWithPropertyIndices) {
        this["matchType"] = matcher.type
      }
      if (matcher.properties.isNotEmpty()) {
        this["matchProperties"] = matcher.properties
      }
    }

    return buildRelationshipFragmentWithNodes(
        startNode,
        endNode,
        eventVariable,
        relationshipPattern,
        additionalParams,
    )
  }

  private fun buildByIdFragment(
      matcher: RelationshipMatcher.ById,
      mode: LookupMode,
      startNode: SinkActionNodeReference,
      endNode: SinkActionNodeReference,
      alias: String,
      eventVariable: String,
  ): Fragment {
    val relationshipPattern =
        "$mode (start)-[$alias]->(end) WHERE id($alias) = $eventVariable.matchId"
    return buildRelationshipFragmentWithNodes(
        startNode,
        endNode,
        eventVariable,
        relationshipPattern,
        mapOf("matchId" to matcher.id),
    )
  }

  private fun buildByElementIdFragment(
      matcher: RelationshipMatcher.ByElementId,
      mode: LookupMode,
      startNode: SinkActionNodeReference,
      endNode: SinkActionNodeReference,
      alias: String,
      eventVariable: String,
  ): Fragment {
    val relationshipPattern =
        "$mode (start)-[$alias]->(end) WHERE elementId($alias) = $eventVariable.matchElementId"
    return buildRelationshipFragmentWithNodes(
        startNode,
        endNode,
        eventVariable,
        relationshipPattern,
        mapOf("matchElementId" to matcher.elementId),
    )
  }

  private data class NodeFragments(val start: Fragment, val end: Fragment)

  private fun buildNodeFragments(
      startNode: SinkActionNodeReference,
      endNode: SinkActionNodeReference,
      eventVariable: String,
  ): NodeFragments {
    return NodeFragments(
        start =
            buildNodeFragment(
                startNode.matcher,
                startNode.lookupMode,
                "start",
                "$eventVariable.start",
            ),
        end = buildNodeFragment(endNode.matcher, endNode.lookupMode, "end", "$eventVariable.end"),
    )
  }

  private fun wrapParams(eventVariable: String, params: Map<String, Any?>): Map<String, Any?> =
      if (eventVariable == "\$$EVENT") mapOf(EVENT to params) else params

  private fun buildQuery(stmt: String, eventVariable: String, params: Map<String, Any?>): Query =
      Query(stmt, wrapParams(eventVariable, params))

  private fun buildLabelPattern(
      labels: Set<String>,
      eventVariable: String,
      paramName: String,
  ): String =
      if (supportsDynamicLabelsWithPropertyIndices) ":\$($eventVariable.$paramName)"
      else buildLabels(labels)

  private fun buildTypePattern(type: String, eventVariable: String, paramName: String): String =
      if (supportsDynamicLabelsWithPropertyIndices) "\$($eventVariable.$paramName)"
      else SchemaNames.sanitize(type, true).orElseThrow()

  private fun buildRelationshipStatementWithKeylessHandling(
      matcher: RelationshipMatcher,
      eventVariable: String,
      matchClause: String,
      operation: String,
  ): String {
    val needsLimit = matcher is RelationshipMatcher.ByTypeAndProperties && !matcher.hasKeys
    return if (needsLimit) "WITH $eventVariable AS _e $matchClause WITH _e, r LIMIT 1 $operation"
    else "WITH $eventVariable AS _e $matchClause $operation"
  }

  companion object {
    private fun buildMatchProps(
        matchProperties: Map<String, Any?>,
        eventVariable: String,
        paramsPath: String,
    ): String =
        if (matchProperties.isEmpty()) ""
        else
            matchProperties
                .map { SchemaNames.sanitize(it.key, true).orElseThrow() }
                .sorted()
                .joinToString(", ", " {", "}") { "$it: $eventVariable.${paramsPath}.$it" }

    private fun buildLabels(labels: Set<String>): String =
        if (labels.isEmpty()) ""
        else labels.sorted().joinToString(":", ":") { SchemaNames.sanitize(it, true).orElseThrow() }
  }
}
