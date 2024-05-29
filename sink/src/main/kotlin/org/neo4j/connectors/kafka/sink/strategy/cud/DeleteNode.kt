package org.neo4j.connectors.kafka.sink.strategy.cud

import com.fasterxml.jackson.annotation.JsonProperty
import org.neo4j.connectors.kafka.sink.strategy.InvalidDataException
import org.neo4j.cypherdsl.core.Cypher
import org.neo4j.cypherdsl.core.renderer.Renderer
import org.neo4j.driver.Query

data class DeleteNode(
    @JsonProperty(Keys.LABELS) val labels: Set<String>,
    @JsonProperty(Keys.IDS) val ids: Map<String, Any?>,
    @JsonProperty(Keys.DETACH, defaultValue = "false") val detach: Boolean = false
) : Operation {
  override fun toQuery(renderer: Renderer): Query {
    if (ids.isEmpty()) {
      throw InvalidDataException("Node must contain at least one ID property.")
    }

    val keysParam = Cypher.parameter("keys")
    val node = buildNode(labels, ids, keysParam).named("n")
    val stmt =
        renderer.render(
            Cypher.match(node)
                .applyFilter(node, ids, keysParam)
                .let {
                  if (detach) {
                    it.detachDelete(node)
                  } else {
                    it.delete(node)
                  }
                }
                .build())

    return Query(stmt, mapOf("keys" to ids))
  }
}
