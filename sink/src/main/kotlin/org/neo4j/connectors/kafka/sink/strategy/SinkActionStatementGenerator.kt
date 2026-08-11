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
import org.neo4j.caniuse.Cypher as CanIUseCypher
import org.neo4j.caniuse.Neo4j
import org.neo4j.cypherdsl.core.Condition
import org.neo4j.cypherdsl.core.Cypher
import org.neo4j.cypherdsl.core.Expression
import org.neo4j.cypherdsl.core.MapExpression
import org.neo4j.cypherdsl.core.Node
import org.neo4j.cypherdsl.core.PatternElement
import org.neo4j.cypherdsl.core.Statement
import org.neo4j.cypherdsl.core.StatementBuilder
import org.neo4j.cypherdsl.core.SymbolicName
import org.neo4j.cypherdsl.core.internal.SchemaNames
import org.neo4j.driver.Query

interface SinkActionStatementGenerator {

  fun buildStatement(data: SinkAction, eventVariable: String = "${'$'}$EVENT"): Query
}

/**
 * Generates per-record Cypher statements via the Cypher-DSL, still gated by the same
 * [org.neo4j.caniuse.CanIUse] feature checks as before, now also used to pick a rendering
 * [org.neo4j.cypherdsl.core.renderer.Dialect] (see [CypherRenderer]).
 *
 * The DSL cannot express a `WHERE` clause following a `MERGE` (real Cypher does not support that
 * either), so [matchOrMergeClause] renders those as a `MATCH` internally and swaps the keyword
 * afterwards. That combination is unreachable for nodes - [NodeMatcher.ById]/[NodeMatcher
 * .ByElementId] never coexist with [LookupMode.MERGE], see [SinkAction.kt] - but relationships
 * allow [RelationshipMatcher.ById]/[RelationshipMatcher.ByElementId] with a `MERGE` lookup mode, so
 * the keyword swap is what actually reproduces that (pre-existing) behaviour there.
 */
class DefaultSinkActionStatementGenerator(neo4j: Neo4j) : SinkActionStatementGenerator {
  private val supportsDynamicLabelsWithPropertyIndices = false
  private val setDynamicLabels = canIUse(CanIUseCypher.setDynamicLabels()).withNeo4j(neo4j)
  private val removeDynamicLabels = canIUse(CanIUseCypher.removeDynamicLabels()).withNeo4j(neo4j)
  private val renderer = CypherRenderer(neo4j)

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
      is CypherSinkAction -> buildCypherStatement(data, eventVariable)
    }
  }

  private fun buildNodeStatement(action: CreateNodeSinkAction, eventVariable: String): Query {
    val node = namedNode(action.labels, "n", "_e", "labels")
    val stmt =
        Cypher.with(rawEvent(eventVariable).`as`(Cypher.name("_e")))
            .create(node)
            .set(Cypher.mutate(node.requiredSymbolicName, rawEvent("_e").property("properties")))
            .build()

    val params = buildMap {
      if (supportsDynamicLabelsWithPropertyIndices) {
        this["labels"] = action.labels
      }
      this["properties"] = action.properties
    }

    return buildQuery(renderer.render(stmt), eventVariable, params)
  }

  private fun buildNodeStatement(action: UpdateNodeSinkAction, eventVariable: String): Query {
    return buildNodeUpdateStatement(
        LookupMode.MATCH,
        action.matcher,
        action.setProperties,
        action.mutateProperties,
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
        action.mutateProperties,
        action.addLabels,
        action.removeLabels,
        eventVariable,
    )
  }

  private fun buildNodeUpdateStatement(
      mode: LookupMode,
      matcher: NodeMatcher,
      setProperties: Map<String, Any?>?,
      mutateProperties: Map<String, Any?>,
      addLabels: Set<String>,
      removeLabels: Set<String>,
      eventVariable: String,
  ): Query {
    val matchFragment = buildNodeFragment(matcher, mode, "n", "_e", setProperties, mutateProperties)
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
        "WITH $eventVariable AS _e ${matchFragment.clause}$setLabelsClause$removeLabelsClause"
    val params = buildMap {
      this.putAll(matchFragment.params)
      if (setProperties != null) {
        this["setProperties"] = setProperties
      }
      this["mutateProperties"] = mutateProperties
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
    val deleteClause = if (action.detach) "DETACH DELETE n" else "DELETE n"
    val stmt = "WITH $eventVariable AS _e ${matchFragment.clause} $deleteClause"
    val params = matchFragment.params

    return buildQuery(stmt, eventVariable, params)
  }

  private fun buildRelationshipStatement(
      action: CreateRelationshipSinkAction,
      eventVariable: String,
  ): Query {
    val nodeFragments = buildNodeFragments(action.startNode, action.endNode, "_e")
    val createClause =
        if (supportsDynamicLabelsWithPropertyIndices) {
          "CREATE (start)-[r:${dynamicPlaceholder("_e", "type")}]->(end) SET r += _e.properties"
        } else {
          val rel =
              Cypher.anyNode()
                  .named("start")
                  .relationshipTo(Cypher.anyNode().named("end"), action.type)
                  .named("r")
          renderer.render(
              Cypher.create(rel)
                  .set(
                      Cypher.mutate(rel.requiredSymbolicName, rawEvent("_e").property("properties"))
                  )
                  .build()
          )
        }

    val stmt =
        "WITH $eventVariable AS _e ${nodeFragments.start.clause} WITH _e, start ${nodeFragments.end.clause} WITH _e, start, end $createClause"
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
        action.mutateProperties,
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
        action.mutateProperties,
        eventVariable,
    )
  }

  private fun buildRelationshipUpdateStatement(
      mode: LookupMode,
      startNode: SinkActionNodeReference,
      endNode: SinkActionNodeReference,
      matcher: RelationshipMatcher,
      setProperties: Map<String, Any?>?,
      mutateProperties: Map<String, Any?>,
      eventVariable: String,
  ): Query {
    val matchFragment = buildRelationshipFragment(matcher, mode, startNode, endNode, "r", "_e")
    val operation = buildString {
      if (setProperties != null) {
        append("SET r = _e.setProperties ")
      }
      append("SET r += _e.mutateProperties")
    }
    val stmt =
        buildRelationshipStatementWithKeylessHandling(
            matcher,
            eventVariable,
            matchFragment.clause,
            operation,
        )
    val params = buildMap {
      putAll(matchFragment.params)
      if (setProperties != null) {
        this["setProperties"] = setProperties
      }
      this["mutateProperties"] = mutateProperties
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

  private fun buildCypherStatement(action: CypherSinkAction, eventVariable: String): Query {
    // action.query is arbitrary user-supplied Cypher text - there is no version-dependent syntax
    // here for the Cypher-DSL to help with, and nothing to generate beyond the WITH-projection
    // prefix, whose alias still goes through the same sanitizer the DSL itself uses for every
    // other identifier in this file.
    val projection =
        action.aliasProjection.joinToString(", ") { (alias, source) ->
          "$eventVariable.$source AS ${SchemaNames.sanitize(alias, true).orElseThrow()}"
        }
    val stmt = "WITH $projection ${action.query}"

    return buildQuery(stmt, eventVariable, action.params)
  }

  data class Fragment(val clause: String, val params: Map<String, Any>)

  private fun buildNodeFragment(
      matcher: NodeMatcher,
      mode: LookupMode,
      alias: String,
      eventVariable: String,
      setProperties: Map<String, Any?>? = null,
      mutateProperties: Map<String, Any?>? = null,
  ): Fragment {
    return when (matcher) {
      is NodeMatcher.ByLabelsAndProperties ->
          buildByLabelsAndPropertiesFragment(
              matcher,
              mode,
              alias,
              eventVariable,
              setProperties,
              mutateProperties,
          )

      is NodeMatcher.ById ->
          buildByIdFragment(matcher, mode, alias, eventVariable, setProperties, mutateProperties)

      is NodeMatcher.ByElementId ->
          buildByElementIdFragment(
              matcher,
              mode,
              alias,
              eventVariable,
              setProperties,
              mutateProperties,
          )
    }
  }

  private fun buildByLabelsAndPropertiesFragment(
      matcher: NodeMatcher.ByLabelsAndProperties,
      mode: LookupMode,
      alias: String,
      eventVariable: String,
      setProperties: Map<String, Any?>? = null,
      mutateProperties: Map<String, Any?>? = null,
  ): Fragment {
    val node = namedNode(matcher.labels, alias, eventVariable, "matchLabels")
    val propsMap = propsMapExpression(matcher.properties, eventVariable, "matchProperties")
    val pattern = if (propsMap != null) node.withProperties(propsMap) else node

    // NodeMatcher.ByLabelsAndProperties never carries a WHERE condition, so a real MATCH/MERGE
    // clause always applies cleanly here - unlike the ById/ByElementId overloads below.
    val clause =
        matchOrMergeClause(
            mode,
            pattern,
            null,
            node.requiredSymbolicName,
            eventVariable,
            setProperties,
            mutateProperties,
        )

    return Fragment(
        clause,
        buildMap {
          if (supportsDynamicLabelsWithPropertyIndices) {
            this["matchLabels"] = matcher.labels
          }
          this["matchProperties"] = matcher.properties
          if (setProperties != null) {
            this["setProperties"] = setProperties
          }
          if (mutateProperties != null) {
            this["mutateProperties"] = mutateProperties
          }
        },
    )
  }

  private fun buildByIdFragment(
      matcher: NodeMatcher.ById,
      mode: LookupMode,
      alias: String,
      eventVariable: String,
      setProperties: Map<String, Any?>? = null,
      mutateProperties: Map<String, Any?>? = null,
  ): Fragment {
    // NodeMatcher.ById never coexists with LookupMode.MERGE (see the class-level comment), so the
    // MERGE+WHERE combination the Cypher-DSL can't express is unreachable here.
    val node = Cypher.anyNode().named(alias)
    val condition = idCondition(node.requiredSymbolicName, eventVariable)

    val clause =
        matchOrMergeClause(
            mode,
            node,
            condition,
            node.requiredSymbolicName,
            eventVariable,
            setProperties,
            mutateProperties,
        )

    return Fragment(
        clause,
        buildMap {
          this["matchId"] = matcher.id
          if (setProperties != null) {
            this["setProperties"] = setProperties
          }
          if (mutateProperties != null) {
            this["mutateProperties"] = mutateProperties
          }
        },
    )
  }

  private fun buildByElementIdFragment(
      matcher: NodeMatcher.ByElementId,
      mode: LookupMode,
      alias: String,
      eventVariable: String,
      setProperties: Map<String, Any?>? = null,
      mutateProperties: Map<String, Any?>? = null,
  ): Fragment {
    // See the comment in the ById overload above: this combination is always LookupMode.MATCH.
    val node = Cypher.anyNode().named(alias)
    val condition = elementIdCondition(node.requiredSymbolicName, eventVariable)

    val clause =
        matchOrMergeClause(
            mode,
            node,
            condition,
            node.requiredSymbolicName,
            eventVariable,
            setProperties,
            mutateProperties,
        )

    return Fragment(
        clause,
        buildMap {
          this["matchElementId"] = matcher.elementId
          if (setProperties != null) {
            this["setProperties"] = setProperties
          }
          if (mutateProperties != null) {
            this["mutateProperties"] = mutateProperties
          }
        },
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
          append(nodeFragments.start.clause).append(" WITH _e, start ")
          append(nodeFragments.end.clause).append(" WITH _e, start, end ")
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
    val start = Cypher.anyNode().named("start")
    val end = Cypher.anyNode().named("end")
    val rel = start.relationshipTo(end, matcher.type).named(alias)
    val propsMap = propsMapExpression(matcher.properties, eventVariable, "matchProperties")

    val relationshipPattern =
        if (supportsDynamicLabelsWithPropertyIndices) {
          "$mode (start)-[$alias:${dynamicPlaceholder(eventVariable, "matchType")}${buildMatchProps(matcher.properties, eventVariable, "matchProperties")}]->(end)"
        } else {
          val pattern = if (propsMap != null) rel.withProperties(propsMap) else rel
          matchOrMergeClause(
              mode,
              pattern,
              null,
              rel.requiredSymbolicName,
              eventVariable,
              null,
              null,
          )
        }

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
    val start = Cypher.anyNode().named("start")
    val end = Cypher.anyNode().named("end")
    val rel = start.relationshipTo(end).named(alias)
    val condition = idCondition(rel.requiredSymbolicName, eventVariable)
    val relationshipPattern =
        matchOrMergeClause(
            mode,
            rel,
            condition,
            rel.requiredSymbolicName,
            eventVariable,
            null,
            null,
        )

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
    val start = Cypher.anyNode().named("start")
    val end = Cypher.anyNode().named("end")
    val rel = start.relationshipTo(end).named(alias)
    val condition = elementIdCondition(rel.requiredSymbolicName, eventVariable)
    val relationshipPattern =
        matchOrMergeClause(
            mode,
            rel,
            condition,
            rel.requiredSymbolicName,
            eventVariable,
            null,
            null,
        )

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
                startNode.setProperties,
                startNode.mutateProperties,
            ),
        end =
            buildNodeFragment(
                endNode.matcher,
                endNode.lookupMode,
                "end",
                "$eventVariable.end",
                endNode.setProperties,
                endNode.mutateProperties,
            ),
    )
  }

  private fun wrapParams(eventVariable: String, params: Map<String, Any?>): Map<String, Any?> =
      if (eventVariable == "\$$EVENT") mapOf(EVENT to params) else params

  private fun buildQuery(stmt: String, eventVariable: String, params: Map<String, Any?>): Query =
      Query(stmt, wrapParams(eventVariable, params))

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

  // ---------- Cypher-DSL helpers ----------

  /**
   * A raw expression referencing [eventVariable] verbatim. [eventVariable] is not always a simple
   * identifier - it can be a top-level parameter reference like `$e`, or a nested property path
   * like `_e.start` built up by [buildNodeFragments] - so it is lifted into the DSL as-is rather
   * than parsed.
   */
  private fun rawEvent(eventVariable: String): Expression = Cypher.raw(eventVariable)

  /**
   * A sorted map-expression for a `matchProperties`/`properties`-style property bag, or `null` when
   * there is nothing to render (matching the pre-refactor convention of omitting the `{...}` suffix
   * entirely rather than rendering `{}`).
   */
  private fun propsMapExpression(
      properties: Map<String, Any?>,
      eventVariable: String,
      paramsPath: String,
  ): MapExpression? {
    if (properties.isEmpty()) return null
    val event = rawEvent(eventVariable)
    val keysAndValues =
        properties.keys.flatMap { key -> listOf(key, event.property(paramsPath, key)) }
    return Cypher.sortedMapOf(*keysAndValues.toTypedArray())
  }

  private fun namedNode(
      labels: Set<String>,
      alias: String,
      eventVariable: String,
      paramName: String,
  ): Node {
    return when {
      supportsDynamicLabelsWithPropertyIndices ->
          Cypher.node(Cypher.allLabels(rawEvent(eventVariable).property(paramName))).named(alias)
      labels.isEmpty() -> Cypher.anyNode().named(alias)
      else -> {
        val sorted = labels.sorted()
        Cypher.node(sorted.first(), sorted.drop(1)).named(alias)
      }
    }
  }

  private fun idCondition(target: SymbolicName, eventVariable: String): Condition =
      Cypher.raw("id(\$E)", target).eq(rawEvent(eventVariable).property("matchId"))

  private fun elementIdCondition(target: SymbolicName, eventVariable: String): Condition =
      Cypher.raw("elementId(\$E)", target).eq(rawEvent(eventVariable).property("matchElementId"))

  private fun dynamicPlaceholder(eventVariable: String, paramName: String): String =
      "\$($eventVariable.$paramName)"

  /**
   * Renders `mode (pattern)[ WHERE condition][ SET ...]`. A real `MERGE` clause never supports a
   * `WHERE` (see the class-level comment), so when [mode] is [LookupMode.MERGE] and [condition] is
   * non-null this renders as `MATCH` internally and swaps the keyword afterwards.
   */
  private fun matchOrMergeClause(
      mode: LookupMode,
      pattern: PatternElement,
      condition: Condition?,
      target: SymbolicName,
      eventVariable: String,
      setProperties: Map<String, Any?>?,
      mutateProperties: Map<String, Any?>?,
  ): String {
    val event = rawEvent(eventVariable)
    val renderAsMatch = mode == LookupMode.MATCH || condition != null

    val statement =
        if (renderAsMatch) {
          val matched = Cypher.match(pattern)
          val reading: StatementBuilder.OngoingReading =
              if (condition != null) matched.where(condition) else matched
          finishReading(reading, target, event, setProperties, mutateProperties)
        } else {
          finishMerge(Cypher.merge(pattern), target, event, setProperties, mutateProperties)
        }

    val text = renderer.render(statement)
    val stripped = text.removePrefix("MATCH ").removePrefix("MERGE ").removeSuffix(" RETURN true")
    return "$mode $stripped"
  }

  private fun finishReading(
      reading: StatementBuilder.OngoingReading,
      target: SymbolicName,
      event: Expression,
      setProperties: Map<String, Any?>?,
      mutateProperties: Map<String, Any?>?,
  ): Statement {
    if (setProperties == null && mutateProperties == null) {
      return reading.returning(Cypher.literalTrue()).build()
    }
    var current: StatementBuilder.BuildableMatchAndUpdate =
        if (setProperties != null) reading.set(Cypher.set(target, event.property("setProperties")))
        else reading.set(Cypher.mutate(target, event.property("mutateProperties")))
    if (setProperties != null && mutateProperties != null) {
      current = current.set(Cypher.mutate(target, event.property("mutateProperties")))
    }
    return current.returning(Cypher.literalTrue()).build()
  }

  private fun finishMerge(
      merged: StatementBuilder.OngoingMerge,
      target: SymbolicName,
      event: Expression,
      setProperties: Map<String, Any?>?,
      mutateProperties: Map<String, Any?>?,
  ): Statement {
    if (setProperties == null && mutateProperties == null) {
      return merged.returning(Cypher.literalTrue()).build()
    }
    var current: StatementBuilder.BuildableMatchAndUpdate =
        if (setProperties != null) merged.set(Cypher.set(target, event.property("setProperties")))
        else merged.set(Cypher.mutate(target, event.property("mutateProperties")))
    if (setProperties != null && mutateProperties != null) {
      current = current.set(Cypher.mutate(target, event.property("mutateProperties")))
    }
    return current.returning(Cypher.literalTrue()).build()
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
