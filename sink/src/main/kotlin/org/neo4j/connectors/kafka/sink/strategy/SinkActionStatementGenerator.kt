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
    val labels =
        if (supportsDynamicLabelsWithPropertyIndices) {
          ":${'$'}(_e.labels)"
        } else buildLabels(action.labels)
    val stmt = "WITH $eventVariable AS _e CREATE (n$labels) SET n += _e.properties"

    val params = buildMap {
      if (supportsDynamicLabelsWithPropertyIndices) {
        this["labels"] = action.labels
      }
      this["properties"] = action.properties
    }

    return Query(stmt, if (eventVariable == "${'$'}$EVENT") mapOf(EVENT to params) else params)
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
    val matchFragment = matcher.buildFragment(mode, "n", "_e")
    val setLabelsClause =
        if (setDynamicLabels) {
          " SET n:${'$'}(_e.addLabels)"
        } else if (addLabels.isNotEmpty()) {
          " SET n" + buildLabels(addLabels)
        } else {
          ""
        }
    val removeLabelsClause =
        if (removeDynamicLabels) {
          " REMOVE n:${'$'}(_e.removeLabels)"
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

    return Query(stmt, if (eventVariable == "${'$'}$EVENT") mapOf(EVENT to params) else params)
  }

  private fun buildNodeStatement(action: DeleteNodeSinkAction, eventVariable: String): Query {
    val matchFragment = action.matcher.buildFragment(LookupMode.MATCH, "n", "_e")
    val stmt = "WITH $eventVariable AS _e ${matchFragment.clause} DELETE n"
    val params = matchFragment.params

    return Query(stmt, if (eventVariable == "${'$'}$EVENT") mapOf(EVENT to params) else params)
  }

  private fun buildRelationshipStatement(
      action: CreateRelationshipSinkAction,
      eventVariable: String,
  ): Query {
    val startFragment =
        action.startNode.matcher.buildFragment(action.startNode.lookupMode, "start", "_e.start")
    val endFragment =
        action.endNode.matcher.buildFragment(action.endNode.lookupMode, "end", "_e.end")

    val typePattern =
        if (supportsDynamicLabelsWithPropertyIndices) "${'$'}(_e.type)"
        else SchemaNames.sanitize(action.type, true).orElseThrow()

    val stmt =
        "WITH $eventVariable AS _e ${startFragment.clause} ${endFragment.clause} CREATE (start)-[r:$typePattern]->(end) SET r += _e.properties"
    val params = buildMap {
      if (startFragment.params.isNotEmpty()) {
        this["start"] = startFragment.params
      }
      if (endFragment.params.isNotEmpty()) {
        this["end"] = endFragment.params
      }
      if (supportsDynamicLabelsWithPropertyIndices) {
        this["type"] = action.type
      }
      this["properties"] = action.properties
    }
    return Query(stmt, if (eventVariable == "${'$'}$EVENT") mapOf(EVENT to params) else params)
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
    val matchFragment = matcher.buildFragment(mode, startNode, endNode, "r", "_e")
    val stmt =
        if (matcher is RelationshipMatcher.ByTypeAndProperties && !matcher.hasKeys)
            "WITH $eventVariable AS _e ${matchFragment.clause} WITH _e, r LIMIT 1 SET r += _e.setProperties"
        else "WITH $eventVariable AS _e ${matchFragment.clause} SET r += _e.setProperties"
    val params = buildMap {
      putAll(matchFragment.params)
      this["setProperties"] = setProperties
    }

    return Query(stmt, if (eventVariable == "${'$'}$EVENT") mapOf(EVENT to params) else params)
  }

  private fun buildRelationshipStatement(
      action: DeleteRelationshipSinkAction,
      eventVariable: String,
  ): Query {
    val matchFragment =
        action.matcher.buildFragment(LookupMode.MATCH, action.startNode, action.endNode, "r", "_e")
    val stmt =
        if (action.matcher is RelationshipMatcher.ByTypeAndProperties && !action.matcher.hasKeys)
            "WITH $eventVariable AS _e ${matchFragment.clause} WITH _e, r LIMIT 1 DELETE r"
        else "WITH $eventVariable AS _e ${matchFragment.clause} DELETE r"
    val params = matchFragment.params

    return Query(stmt, if (eventVariable == "${'$'}$EVENT") mapOf(EVENT to params) else params)
  }

  data class Fragment(val clause: String, val params: Map<String, Any>)

  fun NodeMatcher.buildFragment(mode: LookupMode, alias: String, eventVariable: String): Fragment {
    return when (this) {
      is NodeMatcher.ByLabelsAndProperties -> this.buildFragment(mode, alias, eventVariable)
      is NodeMatcher.ById -> this.buildFragment(mode, alias, eventVariable)
      is NodeMatcher.ByElementId -> this.buildFragment(mode, alias, eventVariable)
    }
  }

  fun NodeMatcher.ByLabelsAndProperties.buildFragment(
      mode: LookupMode,
      alias: String,
      eventVariable: String,
  ): Fragment {
    val matchLabelsPattern =
        if (supportsDynamicLabelsWithPropertyIndices) {
          ":${'$'}($eventVariable.matchLabels)"
        } else buildLabels(this.labels)
    val matchPropsPattern = buildMatchProps(this.properties, eventVariable, "matchProperties")

    return Fragment(
        "$mode ($alias$matchLabelsPattern$matchPropsPattern)",
        buildMap {
          if (supportsDynamicLabelsWithPropertyIndices) {
            this["matchLabels"] = this@buildFragment.labels
          }
          this["matchProperties"] = this@buildFragment.properties
        },
    )
  }

  fun NodeMatcher.ById.buildFragment(
      mode: LookupMode,
      alias: String,
      eventVariable: String,
  ): Fragment {
    return Fragment(
        "$mode ($alias) WHERE id($alias) = $eventVariable.matchId",
        buildMap { this["matchId"] = this@buildFragment.id },
    )
  }

  fun NodeMatcher.ByElementId.buildFragment(
      mode: LookupMode,
      alias: String,
      eventVariable: String,
  ): Fragment {
    return Fragment(
        "$mode ($alias) WHERE elementId($alias) = $eventVariable.matchElementId",
        buildMap { this["matchElementId"] = this@buildFragment.elementId },
    )
  }

  fun RelationshipMatcher.buildFragment(
      mode: LookupMode,
      startNode: SinkActionNodeReference,
      endNode: SinkActionNodeReference,
      alias: String,
      eventVariable: String,
  ): Fragment {
    return when (this) {
      is RelationshipMatcher.ByTypeAndProperties ->
          this.buildFragment(mode, startNode, endNode, alias, eventVariable)
      is RelationshipMatcher.ById ->
          this.buildFragment(mode, startNode, endNode, alias, eventVariable)
      is RelationshipMatcher.ByElementId ->
          this.buildFragment(mode, startNode, endNode, alias, eventVariable)
    }
  }

  fun RelationshipMatcher.ByTypeAndProperties.buildFragment(
      mode: LookupMode,
      startNode: SinkActionNodeReference,
      endNode: SinkActionNodeReference,
      alias: String,
      eventVariable: String,
  ): Fragment {
    val startFragment =
        startNode.matcher.buildFragment(startNode.lookupMode, "start", "$eventVariable.start")
    val endFragment = endNode.matcher.buildFragment(endNode.lookupMode, "end", "$eventVariable.end")
    val matchTypePattern =
        if (supportsDynamicLabelsWithPropertyIndices) "${'$'}($eventVariable.matchType)"
        else SchemaNames.sanitize(this.type, true).orElseThrow()
    val matchPropsPattern = buildMatchProps(this.properties, eventVariable, "matchProperties")

    return Fragment(
        buildString {
          append(startFragment.clause).append(" ")
          append(endFragment.clause).append(" ")
          append("$mode (start)-[$alias:$matchTypePattern$matchPropsPattern]->(end)")
        },
        buildMap {
          this["start"] = startFragment.params
          this["end"] = endFragment.params
          if (supportsDynamicLabelsWithPropertyIndices) {
            this["matchType"] = this@buildFragment.type
          }
          if (this@buildFragment.properties.isNotEmpty()) {
            this["matchProperties"] = this@buildFragment.properties
          }
        },
    )
  }

  fun RelationshipMatcher.ById.buildFragment(
      mode: LookupMode,
      startNode: SinkActionNodeReference,
      endNode: SinkActionNodeReference,
      alias: String,
      eventVariable: String,
  ): Fragment {
    val startFragment =
        startNode.matcher.buildFragment(startNode.lookupMode, "start", "$eventVariable.start")
    val endFragment = endNode.matcher.buildFragment(endNode.lookupMode, "end", "$eventVariable.end")

    return Fragment(
        buildString {
          append(startFragment.clause).append(" ")
          append(endFragment.clause).append(" ")
          append("$mode (start)-[$alias]->(end) WHERE id($alias) = $eventVariable.matchId")
        },
        buildMap {
          this["start"] = startFragment.params
          this["end"] = endFragment.params
          this["matchId"] = this@buildFragment.id
        },
    )
  }

  fun RelationshipMatcher.ByElementId.buildFragment(
      mode: LookupMode,
      startNode: SinkActionNodeReference,
      endNode: SinkActionNodeReference,
      alias: String,
      eventVariable: String,
  ): Fragment {
    val startFragment =
        startNode.matcher.buildFragment(startNode.lookupMode, "start", "$eventVariable.start")
    val endFragment = endNode.matcher.buildFragment(endNode.lookupMode, "end", "$eventVariable.end")

    return Fragment(
        buildString {
          append(startFragment.clause).append(" ")
          append(endFragment.clause).append(" ")
          append(
              "$mode (start)-[$alias]->(end) WHERE elementId($alias) = $eventVariable.matchElementId"
          )
        },
        buildMap {
          this["start"] = startFragment.params
          this["end"] = endFragment.params
          this["matchElementId"] = this@buildFragment.elementId
        },
    )
  }

  companion object {
    @Suppress("SameParameterValue")
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
