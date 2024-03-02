/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
import org.neo4j.connectors.kafka.sink.SinkStrategy
import org.neo4j.cypherdsl.core.Cypher
import org.neo4j.cypherdsl.core.Node
import org.neo4j.cypherdsl.core.Relationship
import org.neo4j.cypherdsl.core.renderer.Renderer
import org.neo4j.driver.Query

class CdcSourceIdHandler(
    val topic: String,
    private val renderer: Renderer,
    private val labelName: String,
    private val propertyName: String
) : CdcHandler() {

  override fun strategy() = SinkStrategy.CDC_SOURCE_ID

  override fun transformCreate(event: NodeEvent): Query {
    val node = buildNode(event.elementId, "n")
    val stmt =
        Cypher.merge(node)
            .mutate(node, Cypher.parameter("nProps", event.after.properties))
            .let {
              if (event.after.labels.isNotEmpty()) {
                it.set(node, event.after.labels)
              } else {
                it
              }
            }
            .build()

    return Query(renderer.render(stmt), stmt.parameters)
  }

  override fun transformUpdate(event: NodeEvent): Query {
    val node = buildNode(event.elementId, "n")
    val stmt =
        Cypher.merge(node)
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
    val node = buildNode(event.elementId, "n")
    val stmt = Cypher.match(node).detachDelete(node).build()

    return Query(renderer.render(stmt), stmt.parameters)
  }

  override fun transformCreate(event: RelationshipEvent): Query {
    val (start, end, rel) = buildRelationship(event, "r")
    val stmt =
        Cypher.merge(start)
            .merge(end)
            .merge(rel)
            .mutate(rel, Cypher.parameter("rProps", event.after.properties))
            .build()

    return Query(renderer.render(stmt), stmt.parameters)
  }

  override fun transformUpdate(event: RelationshipEvent): Query {
    val (start, end, rel) = buildRelationship(event, "r")
    val stmt =
        Cypher.merge(start)
            .merge(end)
            .merge(rel)
            .mutate(rel, Cypher.parameter("rProps", event.mutatedProperties()))
            .build()

    return Query(renderer.render(stmt), stmt.parameters)
  }

  override fun transformDelete(event: RelationshipEvent): Query {
    val rel =
        Cypher.anyNode()
            .relationshipTo(Cypher.anyNode(), event.type)
            .withProperties(propertyName, Cypher.parameter("rElementId", event.elementId))
            .named("r")
    val stmt = Cypher.match(rel).delete(rel).build()

    return Query(renderer.render(stmt), stmt.parameters)
  }

  private fun buildNode(elementId: String, named: String): Node {
    val node =
        Cypher.node(labelName)
            .withProperties(propertyName, Cypher.parameter("${named}ElementId", elementId))
            .named(named)
    return node
  }

  @Suppress("SameParameterValue")
  private fun buildRelationship(
      event: RelationshipEvent,
      named: String
  ): Triple<Node, Node, Relationship> {
    val start = buildNode(event.start.elementId, "start")
    val end = buildNode(event.end.elementId, "end")
    val rel =
        start
            .relationshipTo(end, event.type)
            .withProperties(propertyName, Cypher.parameter("${named}ElementId", event.elementId))
            .named(named)
    return Triple(start, end, rel)
  }
}
