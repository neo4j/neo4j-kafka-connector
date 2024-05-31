package org.neo4j.connectors.kafka.sink.strategy.cud

import com.fasterxml.jackson.annotation.JsonCreator

enum class OperationType {
  CREATE,
  UPDATE,
  MERGE,
  DELETE;

  companion object {
    @JvmStatic
    @JsonCreator
    fun fromString(key: String?): OperationType? {
      for (type in entries) {
        if (type.name.equals(key, true)) {
          return type
        }
      }
      return null
    }
  }
}
