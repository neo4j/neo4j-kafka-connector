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
import org.neo4j.connectors.kafka.utils.CypherRenderer
import org.neo4j.cypherdsl.core.AliasedExpression
import org.neo4j.cypherdsl.core.Condition
import org.neo4j.cypherdsl.core.Cypher
import org.neo4j.cypherdsl.core.Expression
import org.neo4j.cypherdsl.core.MapExpression
import org.neo4j.cypherdsl.core.Node
import org.neo4j.cypherdsl.core.PatternElement
import org.neo4j.cypherdsl.core.Relationship
import org.neo4j.cypherdsl.core.Statement
import org.neo4j.cypherdsl.core.StatementBuilder
import org.neo4j.cypherdsl.core.SymbolicName
import org.neo4j.cypherdsl.core.internal.SchemaNames
import org.neo4j.driver.Query

interface SinkActionStatementGenerator {

  fun buildStatement(data: SinkAction, eventVariable: String = "${'$'}$EVENT"): Query
}

/**
 * Generates per-record Cypher statements via the Cypher-DSL, gated by the same
 * [org.neo4j.caniuse.CanIUse] feature checks as before, which also pick the rendering
 * [org.neo4j.cypherdsl.core.renderer.Dialect] (see [CypherRenderer]). Every action is composed as a
 * single [Statement] and rendered exactly once.
 *
 * Neither Cypher nor the DSL allows a `WHERE` to follow a `MERGE`, so a lookup that carries a
 * condition is always read with `MATCH`, whatever its [LookupMode]. For nodes that is a no-op:
 * [SinkActionNodeReference] and [MergeNodeSinkAction] both reject anything but
 * [NodeMatcher.ByLabelsAndProperties] under [LookupMode.MERGE], so a condition never coexists with
 * one. For relationships the combination is reachable - via the CUD strategy with `op: merge` and
 * an `_id`/`_elementId` matcher - and reading it as a `MATCH` is a deliberate behaviour change:
 * those records used to be sent as `MERGE (start)-[r]->(end) WHERE ...`, which every server version
 * rejects with a syntax error. There is no realizable `MERGE` there anyway, since a relationship
 * cannot be created with a caller-chosen internal id.
 */
class DefaultSinkActionStatementGenerator(neo4j: Neo4j) : SinkActionStatementGenerator {
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
    val node = namedNode(action.labels, "n")
    val statement =
        Cypher.with(eventAlias(eventVariable))
            .create(node)
            .set(Cypher.mutate(node.requiredSymbolicName, rawEvent("_e").property("properties")))
            .build()

    val params = buildMap { this["properties"] = action.properties }

    return buildQuery(statement, eventVariable, params)
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
    val lookup = buildNodeLookup(matcher, mode, "n", "_e", setProperties, mutateProperties)
    val node = Cypher.anyNode().named("n")
    val event = rawEvent("_e")

    // mutateProperties is non-null here, so the lookup always contributes at least one SET and the
    // label operations below always have a BuildableMatchAndUpdate to chain onto.
    var update = lookup.applyTo(Cypher.with(eventAlias(eventVariable))).set(lookup.operations)
    if (setDynamicLabels) {
      update = update.set(node, Cypher.allLabels(event.property("addLabels")))
    } else if (addLabels.isNotEmpty()) {
      update = update.set(node, addLabels.sorted())
    }
    if (removeDynamicLabels) {
      update = update.remove(node, Cypher.allLabels(event.property("removeLabels")))
    } else if (removeLabels.isNotEmpty()) {
      update = update.remove(node, removeLabels.sorted())
    }

    val params = buildMap {
      putAll(lookup.params)
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

    return buildQuery(update.build(), eventVariable, params)
  }

  private fun buildNodeStatement(action: DeleteNodeSinkAction, eventVariable: String): Query {
    val lookup = buildNodeLookup(action.matcher, LookupMode.MATCH, "n", "_e")
    val statement =
        lookup
            .applyTo(Cypher.with(eventAlias(eventVariable)))
            .delete(action.detach, lookup.target)
            .build()

    return buildQuery(statement, eventVariable, lookup.params)
  }

  private fun buildRelationshipStatement(
      action: CreateRelationshipSinkAction,
      eventVariable: String,
  ): Query {
    val nodes = buildNodeLookups(action.startNode, action.endNode, "_e")
    val rel = relationship("r", action.type)
    val statement =
        readNodes(Cypher.with(eventAlias(eventVariable)), nodes)
            .create(rel)
            .set(Cypher.mutate(rel.requiredSymbolicName, rawEvent("_e").property("properties")))
            .build()

    val params = buildMap {
      if (nodes.start.params.isNotEmpty()) {
        this["start"] = nodes.start.params
      }
      if (nodes.end.params.isNotEmpty()) {
        this["end"] = nodes.end.params
      }
      this["properties"] = action.properties
    }

    return buildQuery(statement, eventVariable, params)
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
    val read = readRelationship(matcher, mode, startNode, endNode, eventVariable)
    val statement =
        read.point
            .set(propertyOperations(read.target, rawEvent("_e"), setProperties, mutateProperties))
            .build()

    val params = buildMap {
      putAll(read.params)
      if (setProperties != null) {
        this["setProperties"] = setProperties
      }
      this["mutateProperties"] = mutateProperties
    }

    return buildQuery(statement, eventVariable, params)
  }

  private fun buildRelationshipStatement(
      action: DeleteRelationshipSinkAction,
      eventVariable: String,
  ): Query {
    val read =
        readRelationship(
            action.matcher,
            LookupMode.MATCH,
            action.startNode,
            action.endNode,
            eventVariable,
        )
    val statement = read.point.delete(false, read.target).build()

    return buildQuery(statement, eventVariable, read.params)
  }

  private fun buildCypherStatement(action: CypherSinkAction, eventVariable: String): Query {
    // action.query is opaque, operator-authored Cypher; the only generated part is the
    // WITH-projection in front of it. The Cypher-DSL cannot build that prefix either - Cypher.with
    // returns a builder with no build(), so a WITH-only statement can never be rendered on its own
    // and prepended - so it is assembled as text, with the alias run through the same sanitizer the
    // DSL uses for every other identifier in this file.
    val projection =
        action.aliasProjection.joinToString(", ") { (alias, source) ->
          "$eventVariable.$source AS ${SchemaNames.sanitize(alias, true).orElseThrow()}"
        }
    val stmt = "WITH $projection ${action.query}"

    return buildQuery(stmt, eventVariable, action.params)
  }

  // ---------- lookups ----------

  /**
   * A `MATCH`/`MERGE` of a single pattern, held as Cypher-DSL objects so that callers compose it
   * into a larger [Statement] instead of concatenating rendered text.
   */
  private data class Lookup(
      val mode: LookupMode,
      val pattern: PatternElement,
      /** Non-null only for the `ById`/`ByElementId` matchers. */
      val condition: Condition?,
      val target: SymbolicName,
      /** The `SET`s belonging to this lookup, from its `setProperties`/`mutateProperties`. */
      val operations: List<Expression>,
      val params: Map<String, Any>,
  )

  private fun buildNodeLookup(
      matcher: NodeMatcher,
      mode: LookupMode,
      alias: String,
      eventVariable: String,
      setProperties: Map<String, Any?>? = null,
      mutateProperties: Map<String, Any?>? = null,
  ): Lookup {
    return when (matcher) {
      is NodeMatcher.ByLabelsAndProperties ->
          buildByLabelsAndPropertiesLookup(
              matcher,
              mode,
              alias,
              eventVariable,
              setProperties,
              mutateProperties,
          )

      is NodeMatcher.ById ->
          buildByIdLookup(matcher, mode, alias, eventVariable, setProperties, mutateProperties)

      is NodeMatcher.ByElementId ->
          buildByElementIdLookup(
              matcher,
              mode,
              alias,
              eventVariable,
              setProperties,
              mutateProperties,
          )
    }
  }

  private fun buildByLabelsAndPropertiesLookup(
      matcher: NodeMatcher.ByLabelsAndProperties,
      mode: LookupMode,
      alias: String,
      eventVariable: String,
      setProperties: Map<String, Any?>? = null,
      mutateProperties: Map<String, Any?>? = null,
  ): Lookup {
    val node = namedNode(matcher.labels, alias)
    val propsMap = propsMapExpression(matcher.properties, eventVariable, "matchProperties")

    return Lookup(
        mode = mode,
        pattern = if (propsMap != null) node.withProperties(propsMap) else node,
        // This is the only node matcher that carries no WHERE, and so the only one whose
        // LookupMode.MERGE is actually read as a MERGE.
        condition = null,
        target = node.requiredSymbolicName,
        operations =
            propertyOperations(
                node.requiredSymbolicName,
                rawEvent(eventVariable),
                setProperties,
                mutateProperties,
            ),
        params =
            buildMap {
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

  private fun buildByIdLookup(
      matcher: NodeMatcher.ById,
      mode: LookupMode,
      alias: String,
      eventVariable: String,
      setProperties: Map<String, Any?>? = null,
      mutateProperties: Map<String, Any?>? = null,
  ): Lookup {
    // NodeMatcher.ById never coexists with LookupMode.MERGE (see the class-level comment), so the
    // MERGE-read-as-MATCH fallback is unreachable here - [mode] is always LookupMode.MATCH.
    val node = Cypher.anyNode().named(alias)

    return Lookup(
        mode = mode,
        pattern = node,
        condition = idCondition(node.requiredSymbolicName, eventVariable),
        target = node.requiredSymbolicName,
        operations =
            propertyOperations(
                node.requiredSymbolicName,
                rawEvent(eventVariable),
                setProperties,
                mutateProperties,
            ),
        params =
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

  private fun buildByElementIdLookup(
      matcher: NodeMatcher.ByElementId,
      mode: LookupMode,
      alias: String,
      eventVariable: String,
      setProperties: Map<String, Any?>? = null,
      mutateProperties: Map<String, Any?>? = null,
  ): Lookup {
    // See the comment in the ById overload above: this combination is always LookupMode.MATCH.
    val node = Cypher.anyNode().named(alias)

    return Lookup(
        mode = mode,
        pattern = node,
        condition = elementIdCondition(node.requiredSymbolicName, eventVariable),
        target = node.requiredSymbolicName,
        operations =
            propertyOperations(
                node.requiredSymbolicName,
                rawEvent(eventVariable),
                setProperties,
                mutateProperties,
            ),
        params =
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
  private fun buildRelationshipLookup(
      matcher: RelationshipMatcher,
      mode: LookupMode,
      alias: String,
      eventVariable: String,
  ): Lookup {
    return when (matcher) {
      is RelationshipMatcher.ByTypeAndProperties ->
          buildByTypeAndPropertiesLookup(matcher, mode, alias, eventVariable)

      is RelationshipMatcher.ById -> buildByIdLookup(matcher, mode, alias, eventVariable)

      is RelationshipMatcher.ByElementId ->
          buildByElementIdLookup(matcher, mode, alias, eventVariable)
    }
  }

  private fun buildByTypeAndPropertiesLookup(
      matcher: RelationshipMatcher.ByTypeAndProperties,
      mode: LookupMode,
      alias: String,
      eventVariable: String,
  ): Lookup {
    val rel = relationship(alias, matcher.type)
    val propsMap = propsMapExpression(matcher.properties, eventVariable, "matchProperties")

    return Lookup(
        mode = mode,
        pattern = if (propsMap != null) rel.withProperties(propsMap) else rel,
        condition = null,
        target = rel.requiredSymbolicName,
        operations = emptyList(),
        params =
            buildMap {
              if (matcher.properties.isNotEmpty()) {
                this["matchProperties"] = matcher.properties
              }
            },
    )
  }

  private fun buildByIdLookup(
      matcher: RelationshipMatcher.ById,
      mode: LookupMode,
      alias: String,
      eventVariable: String,
  ): Lookup {
    val rel = relationship(alias, null)

    return Lookup(
        mode = mode,
        pattern = rel,
        condition = idCondition(rel.requiredSymbolicName, eventVariable),
        target = rel.requiredSymbolicName,
        operations = emptyList(),
        params = mapOf("matchId" to matcher.id),
    )
  }

  private fun buildByElementIdLookup(
      matcher: RelationshipMatcher.ByElementId,
      mode: LookupMode,
      alias: String,
      eventVariable: String,
  ): Lookup {
    val rel = relationship(alias, null)

    return Lookup(
        mode = mode,
        pattern = rel,
        condition = elementIdCondition(rel.requiredSymbolicName, eventVariable),
        target = rel.requiredSymbolicName,
        operations = emptyList(),
        params = mapOf("matchElementId" to matcher.elementId),
    )
  }

  private data class NodeLookups(val start: Lookup, val end: Lookup)

  private fun buildNodeLookups(
      startNode: SinkActionNodeReference,
      endNode: SinkActionNodeReference,
      eventVariable: String,
  ): NodeLookups {
    return NodeLookups(
        start =
            buildNodeLookup(
                startNode.matcher,
                startNode.lookupMode,
                "start",
                "$eventVariable.start",
                startNode.setProperties,
                startNode.mutateProperties,
            ),
        end =
            buildNodeLookup(
                endNode.matcher,
                endNode.lookupMode,
                "end",
                "$eventVariable.end",
                endNode.setProperties,
                endNode.mutateProperties,
            ),
    )
  }

  // ---------- composition ----------

  /**
   * A point in a chain where `SET`, `WITH` or `DELETE` can be attached. Both the after-`MATCH`
   * builder ([StatementBuilder.OngoingReading]) and the after-`MERGE` one
   * ([StatementBuilder.OngoingMerge]) accept all three, but reach them through separate `Exposes*`
   * interfaces with no common supertype, and Kotlin cannot name their intersection - so it is
   * spelled out here, rather than duplicating every chain once per [LookupMode].
   */
  private sealed class UpdatePoint {

    /**
     * One `SET` clause per operation. Handing the whole list to the DSL at once would instead
     * render a single `SET n = ..., n += ...`, which is equivalent Cypher but not what this
     * generator has ever emitted.
     */
    fun set(operations: List<Expression>): StatementBuilder.BuildableMatchAndUpdate =
        operations.drop(1).fold(setOne(operations.first())) { update, operation ->
          update.set(operation)
        }

    protected abstract fun setOne(operation: Expression): StatementBuilder.BuildableMatchAndUpdate

    abstract fun with(
        vararg variables: SymbolicName
    ): StatementBuilder.OrderableOngoingReadingAndWithWithoutWhere

    abstract fun delete(detach: Boolean, target: SymbolicName): StatementBuilder.OngoingUpdate

    /** Also covers everything reached through a `WITH`, which reads as an ongoing `MATCH`. */
    class AfterReading(private val reading: StatementBuilder.OngoingReading) : UpdatePoint() {

      override fun setOne(operation: Expression) = reading.set(operation)

      override fun with(vararg variables: SymbolicName) = reading.with(*variables)

      override fun delete(detach: Boolean, target: SymbolicName) =
          if (detach) reading.detachDelete(target) else reading.delete(target)
    }

    class AfterMerge(private val merge: StatementBuilder.OngoingMerge) : UpdatePoint() {

      override fun setOne(operation: Expression) = merge.set(operation)

      override fun with(vararg variables: SymbolicName) = merge.with(*variables)

      override fun delete(detach: Boolean, target: SymbolicName) =
          if (detach) merge.detachDelete(target) else merge.delete(target)
    }
  }

  /**
   * Appends this lookup to [reading]. A lookup carrying a [Lookup.condition] is always read with
   * `MATCH`, whatever its [Lookup.mode] - see the class-level comment.
   */
  private fun Lookup.applyTo(reading: StatementBuilder.OngoingReading): UpdatePoint =
      if (mode == LookupMode.MATCH || condition != null) {
        val matched = reading.match(pattern)
        UpdatePoint.AfterReading(if (condition != null) matched.where(condition) else matched)
      } else {
        UpdatePoint.AfterMerge(reading.merge(pattern))
      }

  /** [lookup], its own `SET`s, then a `WITH` narrowing the scope down to [scope]. */
  private fun readNode(
      reading: StatementBuilder.OngoingReading,
      lookup: Lookup,
      vararg scope: SymbolicName,
  ): StatementBuilder.OrderableOngoingReadingAndWithWithoutWhere {
    val point = lookup.applyTo(reading)
    return if (lookup.operations.isEmpty()) point.with(*scope)
    else point.set(lookup.operations).with(*scope)
  }

  private fun readNodes(
      reading: StatementBuilder.OngoingReading,
      nodes: NodeLookups,
  ): StatementBuilder.OrderableOngoingReadingAndWithWithoutWhere =
      readNode(
          readNode(reading, nodes.start, EVENT_VAR, START_VAR),
          nodes.end,
          EVENT_VAR,
          START_VAR,
          END_VAR,
      )

  private class RelationshipRead(
      val point: UpdatePoint,
      val target: SymbolicName,
      val params: Map<String, Any>,
  )

  /**
   * The whole read side of a relationship update or delete: the event projection, both endpoint
   * lookups separated by their `WITH`s, the relationship lookup itself, and - for a keyless
   * relationship matcher - the `WITH _e, r LIMIT 1` that keeps the following write single-rowed.
   */
  private fun readRelationship(
      matcher: RelationshipMatcher,
      mode: LookupMode,
      startNode: SinkActionNodeReference,
      endNode: SinkActionNodeReference,
      eventVariable: String,
  ): RelationshipRead {
    val nodes = buildNodeLookups(startNode, endNode, "_e")
    val lookup = buildRelationshipLookup(matcher, mode, "r", "_e")

    var point = lookup.applyTo(readNodes(Cypher.with(eventAlias(eventVariable)), nodes))
    if (matcher is RelationshipMatcher.ByTypeAndProperties && !matcher.hasKeys) {
      point = UpdatePoint.AfterReading(point.with(EVENT_VAR, lookup.target).limit(1))
    }

    return RelationshipRead(
        point,
        lookup.target,
        buildMap {
          this["start"] = nodes.start.params
          this["end"] = nodes.end.params
          putAll(lookup.params)
        },
    )
  }

  private fun wrapParams(eventVariable: String, params: Map<String, Any?>): Map<String, Any?> =
      if (eventVariable == "\$$EVENT") mapOf(EVENT to params) else params

  private fun buildQuery(
      statement: Statement,
      eventVariable: String,
      params: Map<String, Any?>,
  ): Query = buildQuery(renderer.render(statement), eventVariable, params)

  private fun buildQuery(stmt: String, eventVariable: String, params: Map<String, Any?>): Query =
      Query(stmt, wrapParams(eventVariable, params))

  // ---------- Cypher-DSL helpers ----------

  /**
   * A raw expression referencing [eventVariable] verbatim. [eventVariable] is not always a simple
   * identifier - it can be a top-level parameter reference like `$e`, or a nested property path
   * like `_e.start` built up by [buildNodeLookups] - so it is lifted into the DSL as-is rather than
   * parsed.
   */
  private fun rawEvent(eventVariable: String): Expression = Cypher.raw(eventVariable)

  /** The `<eventVariable> AS _e` projection every generated statement opens with. */
  private fun eventAlias(eventVariable: String): AliasedExpression =
      rawEvent(eventVariable).`as`(EVENT_VAR)

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

  private fun namedNode(labels: Set<String>, alias: String): Node {
    val sorted = labels.sorted()
    return if (sorted.isEmpty()) Cypher.anyNode().named(alias)
    else Cypher.node(sorted.first(), sorted.drop(1)).named(alias)
  }

  /** `(start)-[<alias>:<type>]->(end)`, untyped when [type] is `null`. */
  private fun relationship(alias: String, type: String?): Relationship {
    val start = Cypher.anyNode().named("start")
    val end = Cypher.anyNode().named("end")
    val rel = if (type != null) start.relationshipTo(end, type) else start.relationshipTo(end)
    return rel.named(alias)
  }

  private fun propertyOperations(
      target: SymbolicName,
      event: Expression,
      setProperties: Map<String, Any?>?,
      mutateProperties: Map<String, Any?>?,
  ): List<Expression> = buildList {
    if (setProperties != null) {
      add(Cypher.set(target, event.property("setProperties")))
    }
    if (mutateProperties != null) {
      add(Cypher.mutate(target, event.property("mutateProperties")))
    }
  }

  private fun idCondition(target: SymbolicName, eventVariable: String): Condition =
      Cypher.raw("id(\$E)", target).eq(rawEvent(eventVariable).property("matchId"))

  private fun elementIdCondition(target: SymbolicName, eventVariable: String): Condition =
      Cypher.raw("elementId(\$E)", target).eq(rawEvent(eventVariable).property("matchElementId"))

  private companion object {
    val EVENT_VAR: SymbolicName = Cypher.name("_e")
    val START_VAR: SymbolicName = Cypher.name("start")
    val END_VAR: SymbolicName = Cypher.name("end")
  }
}
