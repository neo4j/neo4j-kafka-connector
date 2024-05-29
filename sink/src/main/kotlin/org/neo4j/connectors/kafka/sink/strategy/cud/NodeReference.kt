package org.neo4j.connectors.kafka.sink.strategy.cud

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty

enum class LookupMode {
  MATCH,
  MERGE;

  companion object {
    @JvmStatic
    @JsonCreator
    fun fromString(key: String?): LookupMode? {
      for (mode in entries) {
        if (mode.name.equals(key, true)) {
          return mode
        }
      }
      return null
    }
  }
}

data class NodeReference(
    @JsonProperty(Keys.LABELS) val labels: Set<String>,
    @JsonProperty(Keys.IDS) val ids: Map<String, Any?>,
    @JsonProperty(Keys.OPERATION, defaultValue = "MATCH", required = false)
    val op: LookupMode = LookupMode.MATCH
)
