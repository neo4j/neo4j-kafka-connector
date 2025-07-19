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
package org.neo4j.connectors.kafka.sink.strategy.cud

import org.neo4j.cypherdsl.core.Cypher
import org.neo4j.cypherdsl.core.ExposesWhere
import org.neo4j.cypherdsl.core.Expression
import org.neo4j.cypherdsl.core.Node
import org.neo4j.cypherdsl.core.StatementBuilder.OrderableOngoingReadingAndWithWithoutWhere

fun buildNode(labels: Set<String>, ids: Map<String, Any?>, keys: Expression): Node {
  return when {
    ids.containsKey(Keys.PHYSICAL_ID) || ids.containsKey(Keys.ELEMENT_ID) -> Cypher.anyNode()
    labels.isEmpty() ->
        Cypher.anyNode()
            .withProperties(ids.entries.associate { (key, _) -> key to keys.property(key) })
    else ->
        Cypher.node(labels.first(), labels.drop(1))
            .withProperties(ids.entries.associate { (key, _) -> key to keys.property(key) })
  }
}

@Suppress("UNCHECKED_CAST")
fun <T : ExposesWhere> T.applyFilter(node: Node, ids: Map<String, Any?>, keys: Expression): T {
  return if (ids.containsKey(Keys.PHYSICAL_ID)) {
    this.where(
        Cypher.raw("id(${'$'}E)", node.requiredSymbolicName).eq(keys.property(Keys.PHYSICAL_ID))
    ) as T
  } else if (ids.containsKey(Keys.ELEMENT_ID)) {
    this.where(
        Cypher.raw("elementId(${'$'}E)", node.requiredSymbolicName)
            .eq(keys.property(Keys.ELEMENT_ID))
    ) as T
  } else {
    this
  }
}

fun lookupNodes(
    start: NodeReference,
    end: NodeReference,
    startParam: Expression,
    endParam: Expression,
): Triple<Node, Node, OrderableOngoingReadingAndWithWithoutWhere> {
  val startNode = buildNode(start.labels, start.ids, startParam.property("keys")).named("start")
  val endNode = buildNode(end.labels, end.ids, endParam.property("keys")).named("end")

  return Triple(
      startNode,
      endNode,
      when (start.op) {
        LookupMode.MATCH ->
            Cypher.match(startNode)
                .applyFilter(startNode, start.ids, startParam.property("keys"))
                .with(startNode)
        LookupMode.MERGE -> Cypher.merge(startNode).with(startNode)
      }.let {
        when (end.op) {
          LookupMode.MATCH ->
              it.match(endNode)
                  .applyFilter(endNode, end.ids, endParam.property("keys"))
                  .with(startNode, endNode)
          LookupMode.MERGE -> it.merge(endNode).with(startNode, endNode)
        }
      },
  )
}
