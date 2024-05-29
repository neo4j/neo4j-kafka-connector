package org.neo4j.connectors.kafka.sink.strategy.cud

import com.fasterxml.jackson.annotation.JsonProperty
import org.neo4j.cypherdsl.core.Cypher
import org.neo4j.cypherdsl.core.renderer.Renderer
import org.neo4j.driver.Query

data class CreateNode(
    @JsonProperty(Keys.LABELS) val labels: Set<String>,
    @JsonProperty(Keys.PROPERTIES) val properties: Map<String, Any?>
) : Operation {
  override fun toQuery(renderer: Renderer): Query {
    val node = buildNode(labels, emptyMap(), Cypher.parameter("keys")).named("n")
    val stmt =
        renderer.render(Cypher.create(node).set(node, Cypher.parameter("properties")).build())

    return Query(stmt, mapOf("properties" to properties))
  }
}
