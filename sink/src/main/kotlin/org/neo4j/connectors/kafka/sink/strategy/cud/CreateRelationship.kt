package org.neo4j.connectors.kafka.sink.strategy.cud

import com.fasterxml.jackson.annotation.JsonProperty
import org.neo4j.connectors.kafka.sink.strategy.InvalidDataException
import org.neo4j.cypherdsl.core.Cypher
import org.neo4j.cypherdsl.core.renderer.Renderer
import org.neo4j.driver.Query

data class CreateRelationship(
    @JsonProperty(Keys.RELATION_TYPE) val type: String,
    @JsonProperty(Keys.FROM) val start: NodeReference,
    @JsonProperty(Keys.TO) val end: NodeReference,
    @JsonProperty(Keys.PROPERTIES) val properties: Map<String, Any?>
) : Operation {
  override fun toQuery(renderer: Renderer): Query {
    if (type.isEmpty()) {
      throw InvalidDataException("'${Keys.RELATION_TYPE}' must be specified.")
    }

    if (start.ids.isEmpty() || end.ids.isEmpty()) {
      throw InvalidDataException(
          "'${Keys.FROM}' and '${Keys.TO}' must contain at least one ID property.")
    }

    val startParam = Cypher.parameter("start")
    val endParam = Cypher.parameter("end")
    val propertiesParam = Cypher.parameter("properties")
    val (startNode, endNode, lookup) = lookupNodes(start, end, startParam, endParam)
    val relationship = startNode.relationshipTo(endNode, type).named("r")

    val stmt =
        renderer.render(lookup.create(relationship).set(relationship, propertiesParam).build())

    return Query(
        stmt,
        mapOf(
            "start" to mapOf("keys" to start.ids),
            "end" to mapOf("keys" to end.ids),
            "properties" to properties))
  }
}
