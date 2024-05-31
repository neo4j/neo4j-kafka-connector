package org.neo4j.connectors.kafka.sink.strategy.cud

import com.fasterxml.jackson.annotation.JsonCreator

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
