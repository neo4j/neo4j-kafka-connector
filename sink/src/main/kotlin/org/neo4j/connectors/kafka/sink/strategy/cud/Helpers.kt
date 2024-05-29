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
        Cypher.raw("id(${'$'}E)", node.requiredSymbolicName).eq(keys.property(Keys.PHYSICAL_ID)))
        as T
  } else if (ids.containsKey(Keys.ELEMENT_ID)) {
    this.where(
        Cypher.raw("elementId(${'$'}E)", node.requiredSymbolicName)
            .eq(keys.property(Keys.ELEMENT_ID))) as T
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
        LookupMode.MATCH -> Cypher.match(startNode).with(startNode)
        LookupMode.MERGE -> Cypher.merge(startNode).with(startNode)
      }.let {
        when (end.op) {
          LookupMode.MATCH -> it.match(endNode).with(startNode, endNode)
          LookupMode.MERGE -> it.merge(endNode).with(startNode, endNode)
        }
      })
}
