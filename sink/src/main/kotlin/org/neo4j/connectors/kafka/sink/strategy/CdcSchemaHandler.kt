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

import org.neo4j.cdc.client.model.NodeEvent
import org.neo4j.cdc.client.model.RelationshipEvent
import org.neo4j.connectors.kafka.exceptions.InvalidDataException
import org.neo4j.connectors.kafka.sink.SinkStrategy
import org.neo4j.cypherdsl.core.Cypher
import org.neo4j.cypherdsl.core.Node
import org.neo4j.cypherdsl.core.Relationship
import org.neo4j.cypherdsl.core.renderer.Renderer
import org.neo4j.driver.Query

class CdcSchemaHandler(val topic: String, private val renderer: Renderer) : CdcHandler() {

  override fun strategy() = SinkStrategy.CDC_SCHEMA

  override fun transformCreate(event: NodeEvent): Query {
    if (event.after == null) {
      throw InvalidDataException("create operation requires 'after' field in the event object")
    }

    val node = buildNode(event.keys, "n")
    val stmt =
        Cypher.create(node)
            .set(node, Cypher.parameter("nProps", event.after.properties))
            .let {
              val labels = event.after.labels.minus(event.keys.keys)
              if (labels.isNotEmpty()) {
                it.set(node, labels)
              } else {
                it
              }
            }
            .build()

    return Query(renderer.render(stmt), stmt.parameters)
  }

  override fun transformUpdate(event: NodeEvent): Query {
    if (event.before == null) {
      throw InvalidDataException("update operation requires 'before' field in the event object")
    }
    if (event.after == null) {
      throw InvalidDataException("update operation requires 'after' field in the event object")
    }

    val node = buildNode(event.keys, "n")
    val stmt =
        Cypher.match(node)
            .mutate(node, Cypher.parameter("nProps", event.mutatedProperties()))
            .let {
              val addedLabels = event.addedLabels()
              if (addedLabels.isNotEmpty()) {
                it.set(node, addedLabels)
              } else {
                it
              }
            }
            .let {
              val removedLabels = event.removedLabels()
              if (removedLabels.isNotEmpty()) {
                it.remove(node, removedLabels)
              } else {
                it
              }
            }
            .build()

    return Query(renderer.render(stmt), stmt.parameters)
  }

  override fun transformDelete(event: NodeEvent): Query {
    val node = buildNode(event.keys, "n")
    val stmt = Cypher.match(node).detachDelete(node).build()

    return Query(renderer.render(stmt), stmt.parameters)
  }

  override fun transformCreate(event: RelationshipEvent): Query {
    if (event.after == null) {
      throw InvalidDataException("create operation requires 'after' field in the event object")
    }

    val (start, end, rel) = buildRelationship(event, "r", true)
    val stmt =
        Cypher.match(start)
            .match(end)
            .create(rel)
            .set(rel, Cypher.parameter("rProps", event.after.properties))
            .build()

    return Query(renderer.render(stmt), stmt.parameters)
  }

  override fun transformUpdate(event: RelationshipEvent): Query {
    if (event.before == null) {
      throw InvalidDataException("update operation requires 'before' field in the event object")
    }
    if (event.after == null) {
      throw InvalidDataException("update operation requires 'after' field in the event object")
    }

    val (start, end, rel, matchNodes) = buildRelationship(event, "r", false)
    val stmt =
        (if (matchNodes) Cypher.match(start).match(end).match(rel) else Cypher.match(rel))
            .mutate(rel, Cypher.parameter("rProps", event.mutatedProperties()))
            .build()

    return Query(renderer.render(stmt), stmt.parameters)
  }

  override fun transformDelete(event: RelationshipEvent): Query {
    val (start, end, rel, matchNodes) = buildRelationship(event, "r", false)
    val stmt =
        (if (matchNodes) Cypher.match(start).match(end).match(rel) else Cypher.match(rel))
            .delete(rel)
            .build()

    return Query(renderer.render(stmt), stmt.parameters)
  }

  private fun buildNode(
      keys: Map<String, List<Map<String, Any>>>,
      named: String,
  ): Node {
    val validKeys =
        keys
            .mapValues { kvp -> kvp.value.filter { it.isNotEmpty() } }
            .filterValues { it.isNotEmpty() }

    if (validKeys.isEmpty()) {
      throw InvalidDataException(
          "schema strategy requires at least one node key with valid properties on node aliased '$named'.",
      )
    }

    val node =
        Cypher.node(validKeys.keys.first(), validKeys.keys.drop(1))
            .withProperties(
                validKeys
                    .flatMap { it.value }
                    .asSequence()
                    .flatMap { it.asSequence() }
                    .associate { e ->
                      Pair(
                          e.key,
                          Cypher.parameter(
                              "${named}${e.key.replaceFirstChar { c -> c.uppercaseChar() }}",
                              e.value,
                          ),
                      )
                    },
            )
            .named(named)

    return node
  }

  data class RelationshipOutput(
      val start: Node,
      val end: Node,
      val relationship: Relationship,
      val matchNodes: Boolean
  )

  @Suppress("SameParameterValue")
  private fun buildRelationship(
      event: RelationshipEvent,
      named: String,
      forCreate: Boolean
  ): RelationshipOutput {
    val relationshipHasKeys = event.keys.filter { it.isNotEmpty() }.any()

    // if this is a create event, we enforce start and end key properties
    // else we check whether relationship has its own keys or not
    val start =
        if (!forCreate && relationshipHasKeys) Cypher.anyNode("start")
        else
            buildNode(
                event.start.keys,
                "start",
            )
    val end =
        if (!forCreate && relationshipHasKeys) Cypher.anyNode("end")
        else
            buildNode(
                event.end.keys,
                "end",
            )
    val rel =
        start
            .relationshipTo(end, event.type)
            .withProperties(
                event.keys
                    .asSequence()
                    .flatMap { it.asSequence() }
                    .associate { e ->
                      Pair(
                          e.key,
                          Cypher.parameter(
                              "${named}${e.key.replaceFirstChar { c -> c.uppercaseChar() }}",
                              e.value,
                          ),
                      )
                    },
            )
            .named(named)

    return RelationshipOutput(start, end, rel, forCreate || !relationshipHasKeys)
  }
}
