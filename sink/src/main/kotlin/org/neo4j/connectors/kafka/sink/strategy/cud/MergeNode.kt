package org.neo4j.connectors.kafka.sink.strategy.cud

import com.fasterxml.jackson.annotation.JsonProperty
import org.neo4j.connectors.kafka.sink.strategy.InvalidDataException
import org.neo4j.cypherdsl.core.Cypher
import org.neo4j.cypherdsl.core.renderer.Renderer
import org.neo4j.driver.Query

data class MergeNode(
    @JsonProperty(Keys.LABELS) val labels: Set<String>,
    @JsonProperty(Keys.IDS) val ids: Map<String, Any?>,
    @JsonProperty(Keys.PROPERTIES) val properties: Map<String, Any?>
) : Operation {
  override fun toQuery(renderer: Renderer): Query {
    if (ids.isEmpty()) {
      throw InvalidDataException("Node must contain at least one ID property.")
    }

    val keysParam = Cypher.parameter("keys")
    val propertiesParam = Cypher.parameter("properties")
    val node = buildNode(labels, ids, keysParam).named("n")
    val stmt = renderer.render(Cypher.merge(node).mutate(node, propertiesParam).build())

    return Query(stmt, mapOf("keys" to ids, "properties" to properties))
  }
}
