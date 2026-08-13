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
 * Builds the Cypher statement for a single [SinkAction]. Server capabilities are resolved once per
 * instance through [org.neo4j.caniuse.CanIUse], both to choose between available syntaxes and to
 * pick the rendering [org.neo4j.cypherdsl.core.renderer.Dialect] (see [CypherRenderer]).
 *
 * An action is composed as one [Statement] and rendered once: [buildStatement] runs per record, on
 * the hot path of every batch strategy.
 *
 * Cypher does not allow a `WHERE` to follow a `MERGE`, so a lookup carrying a condition is read
 * with `MATCH` regardless of its [LookupMode]. Nodes never reach that case - only the id matchers
 * produce a condition, and both [SinkActionNodeReference] and [MergeNodeSinkAction] reject anything
 * but [NodeMatcher.ByLabelsAndProperties] under [LookupMode.MERGE]. Relationships do reach it,
 * through [RelationshipMatcher.ById]/[RelationshipMatcher.ByElementId] under [LookupMode.MERGE],
 * where `MATCH` is also the only meaningful reading: a relationship cannot be created with a
 * caller-chosen internal id, so there is nothing for a `MERGE` to create.
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
            .set(Cypher.mutate(node.requiredSymbolicName, EVENT_REF.property("properties")))
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

    // An update always carries mutateProperties, so the lookup always contributes at least one
    // SET - which is what leaves a BuildableMatchAndUpdate for the label operations to chain onto.
    var update = lookup.applyTo(Cypher.with(eventAlias(eventVariable))).set(lookup.operations)
    if (setDynamicLabels) {
      update = update.set(node, Cypher.allLabels(EVENT_REF.property("addLabels")))
    } else if (addLabels.isNotEmpty()) {
      update = update.set(node, addLabels.sorted())
    }
    if (removeDynamicLabels) {
      update = update.remove(node, Cypher.allLabels(EVENT_REF.property("removeLabels")))
    } else if (removeLabels.isNotEmpty()) {
      update = update.remove(node, removeLabels.sorted())
    }

    val params = buildMap {
      putAll(lookup.params)
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
    val reading = lookup.matchIn(Cypher.with(eventAlias(eventVariable)))
    val statement =
        if (action.detach) reading.detachDelete(lookup.target).build()
        else reading.delete(lookup.target).build()

    return buildQuery(statement, eventVariable, lookup.params)
  }

  private fun buildRelationshipStatement(
      action: CreateRelationshipSinkAction,
      eventVariable: String,
  ): Query {
    val nodes = buildNodeLookups(action.startNode, action.endNode)
    val rel = relationship(action.type)
    val statement =
        readEndpoints(eventVariable, nodes)
            .create(rel)
            .set(Cypher.mutate(rel.requiredSymbolicName, EVENT_REF.property("properties")))
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
    val nodes = buildNodeLookups(startNode, endNode)
    val lookup = buildRelationshipLookup(matcher, mode, setProperties, mutateProperties)

    var point = lookup.applyTo(readEndpoints(eventVariable, nodes))
    if (matcher.isKeyless) {
      point = UpdatePoint.AfterReading(point.with(EVENT_VAR, lookup.target).limit(1))
    }

    return buildQuery(
        point.set(lookup.operations).build(),
        eventVariable,
        relationshipParams(nodes, lookup),
    )
  }

  private fun buildRelationshipStatement(
      action: DeleteRelationshipSinkAction,
      eventVariable: String,
  ): Query {
    val nodes = buildNodeLookups(action.startNode, action.endNode)
    val lookup = buildRelationshipLookup(action.matcher, LookupMode.MATCH)

    var reading = lookup.matchIn(readEndpoints(eventVariable, nodes))
    if (action.matcher.isKeyless) {
      reading = reading.with(EVENT_VAR, lookup.target).limit(1)
    }

    return buildQuery(
        reading.delete(lookup.target).build(),
        eventVariable,
        relationshipParams(nodes, lookup),
    )
  }

  private fun buildCypherStatement(action: CypherSinkAction, eventVariable: String): Query {
    // action.query is opaque, operator-authored Cypher, so the only generated part is the
    // WITH-projection in front of it. That projection cannot come from the Cypher-DSL: Cypher.with
    // returns a builder with no build(), so a WITH-only statement is not renderable on its own.
    // It is assembled as text instead, with each alias run through the same sanitizer the DSL
    // applies to every other identifier here.
    val projection =
        action.aliasProjection.joinToString(", ") { (alias, source) ->
          "$eventVariable.$source AS ${SchemaNames.sanitize(alias, true).orElseThrow()}"
        }
    val stmt = "WITH $projection ${action.query}"

    return buildQuery(stmt, eventVariable, action.params)
  }

  // ---------- lookups ----------

  /**
   * How one node or relationship is looked up, as unrendered Cypher-DSL objects. Keeping the parts
   * addressable lets a caller splice them into a surrounding chain - a `WITH` between two lookups,
   * a `LIMIT` before the write - which rendered text would not allow.
   */
  private data class Lookup(
      val mode: LookupMode,
      val pattern: PatternElement,
      /** Non-null only for the `ById`/`ByElementId` matchers. */
      val condition: Condition?,
      val target: SymbolicName,
      /** The `SET`s belonging to this lookup, from its `setProperties`/`mutateProperties`. */
      val operations: List<Expression> = emptyList(),
      /** The slice of the event payload this lookup reads, keyed as the statement addresses it. */
      val params: Map<String, Any>,
  )

  /**
   * Attaches the property writes a matched node or relationship carries. Split from the matcher
   * itself because both are identical for every matcher, while the pattern and condition are not.
   */
  private fun Lookup.withWrites(
      event: Expression,
      setProperties: Map<String, Any?>?,
      mutateProperties: Map<String, Any?>?,
  ): Lookup =
      copy(
          operations =
              buildList {
                if (setProperties != null) {
                  add(Cypher.set(target, event.property("setProperties")))
                }
                if (mutateProperties != null) {
                  add(Cypher.mutate(target, event.property("mutateProperties")))
                }
              },
          params =
              buildMap {
                putAll(params)
                if (setProperties != null) {
                  this["setProperties"] = setProperties
                }
                if (mutateProperties != null) {
                  this["mutateProperties"] = mutateProperties
                }
              },
      )

  private fun buildNodeLookup(
      matcher: NodeMatcher,
      mode: LookupMode,
      alias: String,
      eventVariable: String,
      setProperties: Map<String, Any?>? = null,
      mutateProperties: Map<String, Any?>? = null,
  ): Lookup {
    val event = rawEvent(eventVariable)

    return when (matcher) {
      is NodeMatcher.ByLabelsAndProperties -> {
        val node = namedNode(matcher.labels, alias)
        val properties = matchPropertiesExpression(matcher.properties, event)
        Lookup(
            mode = mode,
            pattern = if (properties != null) node.withProperties(properties) else node,
            // Matching on the pattern needs no WHERE, which makes this the only node matcher whose
            // LookupMode.MERGE can be honoured as a MERGE.
            condition = null,
            target = node.requiredSymbolicName,
            params = mapOf("matchProperties" to matcher.properties),
        )
      }

      // The id matchers only ever arrive with LookupMode.MATCH (see the class-level comment), so
      // for nodes their condition never has to override the requested mode.
      is NodeMatcher.ById -> {
        val node = Cypher.anyNode().named(alias)
        Lookup(
            mode = mode,
            pattern = node,
            condition = idCondition(node.requiredSymbolicName, event),
            target = node.requiredSymbolicName,
            params = mapOf("matchId" to matcher.id),
        )
      }

      is NodeMatcher.ByElementId -> {
        val node = Cypher.anyNode().named(alias)
        Lookup(
            mode = mode,
            pattern = node,
            condition = elementIdCondition(node.requiredSymbolicName, event),
            target = node.requiredSymbolicName,
            params = mapOf("matchElementId" to matcher.elementId),
        )
      }
    }.withWrites(event, setProperties, mutateProperties)
  }

  private fun buildRelationshipLookup(
      matcher: RelationshipMatcher,
      mode: LookupMode,
      setProperties: Map<String, Any?>? = null,
      mutateProperties: Map<String, Any?>? = null,
  ): Lookup {
    return when (matcher) {
      is RelationshipMatcher.ByTypeAndProperties -> {
        val rel = relationship(matcher.type)
        val properties = matchPropertiesExpression(matcher.properties, EVENT_REF)
        Lookup(
            mode = mode,
            pattern = if (properties != null) rel.withProperties(properties) else rel,
            condition = null,
            target = rel.requiredSymbolicName,
            params =
                if (properties == null) emptyMap()
                else mapOf("matchProperties" to matcher.properties),
        )
      }

      is RelationshipMatcher.ById -> {
        val rel = relationship(null)
        Lookup(
            mode = mode,
            pattern = rel,
            condition = idCondition(rel.requiredSymbolicName, EVENT_REF),
            target = rel.requiredSymbolicName,
            params = mapOf("matchId" to matcher.id),
        )
      }

      is RelationshipMatcher.ByElementId -> {
        val rel = relationship(null)
        Lookup(
            mode = mode,
            pattern = rel,
            condition = elementIdCondition(rel.requiredSymbolicName, EVENT_REF),
            target = rel.requiredSymbolicName,
            params = mapOf("matchElementId" to matcher.elementId),
        )
      }
    }.withWrites(EVENT_REF, setProperties, mutateProperties)
  }

  private data class NodeLookups(val start: Lookup, val end: Lookup)

  private fun buildNodeLookups(
      startNode: SinkActionNodeReference,
      endNode: SinkActionNodeReference,
  ): NodeLookups =
      NodeLookups(
          start = buildNodeLookup(startNode, "start"),
          end = buildNodeLookup(endNode, "end"),
      )

  /**
   * An endpoint of a relationship action. The alias doubles as the path into the event payload, so
   * `start` is matched from `_e.start` and `end` from `_e.end`.
   */
  private fun buildNodeLookup(reference: SinkActionNodeReference, alias: String): Lookup =
      buildNodeLookup(
          reference.matcher,
          reference.lookupMode,
          alias,
          "_e.$alias",
          reference.setProperties,
          reference.mutateProperties,
      )

  private fun relationshipParams(nodes: NodeLookups, relationship: Lookup): Map<String, Any> =
      buildMap {
        this["start"] = nodes.start.params
        this["end"] = nodes.end.params
        putAll(relationship.params)
      }

  /**
   * Whether the matcher has no key properties to identify a single relationship by. Such a matcher
   * can match many relationships, so the write that follows it is capped with a `WITH ... LIMIT 1`
   * to keep one record from updating an unbounded number of rows.
   */
  private val RelationshipMatcher.isKeyless: Boolean
    get() = this is RelationshipMatcher.ByTypeAndProperties && !hasKeys

  // ---------- composition ----------

  /**
   * A point in a chain where `SET` or `WITH` can be attached, whichever clause got the chain here.
   *
   * Both the after-`MATCH` builder ([StatementBuilder.OngoingReading]) and the after-`MERGE` one
   * ([StatementBuilder.OngoingMerge]) accept the two, but inherit them from separate `Exposes*`
   * interfaces with no common supertype, and Kotlin cannot express that intersection as a type.
   * Naming it here keeps a chain that reads either way written once instead of once per
   * [LookupMode].
   *
   * `DELETE` is absent because no chain needs it here: every deleting action pins its lookups to
   * [LookupMode.MATCH], so those chains stay in [StatementBuilder.OngoingReading] throughout and
   * can call the DSL directly.
   */
  private sealed class UpdatePoint {

    abstract fun set(operations: List<Expression>): StatementBuilder.BuildableMatchAndUpdate

    abstract fun with(
        vararg variables: SymbolicName
    ): StatementBuilder.OrderableOngoingReadingAndWithWithoutWhere

    /** Covers a `WITH` as well as a `MATCH`: the DSL treats both as an ongoing reading. */
    class AfterReading(private val reading: StatementBuilder.OngoingReading) : UpdatePoint() {

      override fun set(operations: List<Expression>) = reading.set(operations)

      override fun with(vararg variables: SymbolicName) = reading.with(*variables)
    }

    class AfterMerge(private val merge: StatementBuilder.OngoingMerge) : UpdatePoint() {

      override fun set(operations: List<Expression>) = merge.set(operations)

      override fun with(vararg variables: SymbolicName) = merge.with(*variables)
    }
  }

  /**
   * Reads this lookup with `MATCH`, applying its [Lookup.condition] as a `WHERE`. Callers that must
   * keep the chain in [StatementBuilder.OngoingReading] - the deleting ones - use this directly;
   * [applyTo] also routes here for any lookup a `MERGE` could not express.
   */
  private fun Lookup.matchIn(
      reading: StatementBuilder.OngoingReading
  ): StatementBuilder.OngoingReading {
    val matched = reading.match(pattern)
    return if (condition != null) matched.where(condition) else matched
  }

  /**
   * Appends this lookup to [reading], as a `MERGE` when its [Lookup.mode] asks for one and no
   * [Lookup.condition] rules it out - see the class-level comment.
   */
  private fun Lookup.applyTo(reading: StatementBuilder.OngoingReading): UpdatePoint =
      if (mode == LookupMode.MATCH || condition != null) UpdatePoint.AfterReading(matchIn(reading))
      else UpdatePoint.AfterMerge(reading.merge(pattern))

  /**
   * [lookup], its own `SET`s, then a `WITH` narrowing the scope down to [scope]. The `WITH` is what
   * lets the next clause be another `MATCH`/`MERGE`, which the DSL will not accept straight after a
   * `SET`.
   */
  private fun readNode(
      reading: StatementBuilder.OngoingReading,
      lookup: Lookup,
      vararg scope: SymbolicName,
  ): StatementBuilder.OrderableOngoingReadingAndWithWithoutWhere {
    val point = lookup.applyTo(reading)
    return if (lookup.operations.isEmpty()) point.with(*scope)
    else point.set(lookup.operations).with(*scope)
  }

  /**
   * The read side shared by every relationship action: the event projection, then each endpoint
   * looked up in turn, each one widening the scope carried forward by one variable.
   */
  private fun readEndpoints(
      eventVariable: String,
      nodes: NodeLookups,
  ): StatementBuilder.OrderableOngoingReadingAndWithWithoutWhere =
      readNode(
          readNode(Cypher.with(eventAlias(eventVariable)), nodes.start, EVENT_VAR, START_VAR),
          nodes.end,
          EVENT_VAR,
          START_VAR,
          END_VAR,
      )

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
   * like `_e.start` - so it is lifted into the DSL as-is rather than parsed.
   */
  private fun rawEvent(eventVariable: String): Expression = Cypher.raw(eventVariable)

  /** The `<eventVariable> AS _e` projection every generated statement opens with. */
  private fun eventAlias(eventVariable: String): AliasedExpression =
      rawEvent(eventVariable).`as`(EVENT_VAR)

  /**
   * A map-expression selecting a `matchProperties` bag out of [event], or `null` when the bag is
   * empty - an empty map constrains nothing, so the pattern is left without a `{...}` suffix rather
   * than carrying an empty one. Keys are sorted so that equal actions render to equal text.
   */
  private fun matchPropertiesExpression(
      properties: Map<String, Any?>,
      event: Expression,
  ): MapExpression? {
    if (properties.isEmpty()) return null
    val keysAndValues =
        properties.keys.flatMap { key -> listOf(key, event.property("matchProperties", key)) }
    return Cypher.sortedMapOf(*keysAndValues.toTypedArray())
  }

  /** `(<alias>:<labels>)`, or an unlabelled `(<alias>)` when [labels] is empty. */
  private fun namedNode(labels: Set<String>, alias: String): Node {
    val sorted = labels.sorted()
    return if (sorted.isEmpty()) Cypher.anyNode().named(alias)
    else Cypher.node(sorted.first(), sorted.drop(1)).named(alias)
  }

  /** `(start)-[r:<type>]->(end)`, untyped when [type] is `null`. */
  private fun relationship(type: String?): Relationship {
    val start = Cypher.anyNode().named("start")
    val end = Cypher.anyNode().named("end")
    val rel = if (type != null) start.relationshipTo(end, type) else start.relationshipTo(end)
    return rel.named("r")
  }

  private fun idCondition(target: SymbolicName, event: Expression): Condition =
      Cypher.raw("id(\$E)", target).eq(event.property("matchId"))

  private fun elementIdCondition(target: SymbolicName, event: Expression): Condition =
      Cypher.raw("elementId(\$E)", target).eq(event.property("matchElementId"))

  private companion object {
    /**
     * The variables every generated statement binds. `_e` is the event projection opened by
     * [eventAlias] and needs both forms: [EVENT_VAR] to name it in a `WITH`, [EVENT_REF] to read
     * properties off it.
     */
    val EVENT_VAR: SymbolicName = Cypher.name("_e")
    val EVENT_REF: Expression = Cypher.raw("_e")

    val START_VAR: SymbolicName = Cypher.name("start")
    val END_VAR: SymbolicName = Cypher.name("end")
  }
}
