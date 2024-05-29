package org.neo4j.connectors.kafka.sink.strategy.cud

internal object Keys {
  const val TYPE = "type"
  const val OPERATION = "op"
  const val LABELS = "labels"
  const val PROPERTIES = "properties"
  const val IDS = "ids"
  const val RELATION_TYPE = "rel_type"
  const val FROM = "from"
  const val TO = "to"
  const val DETACH = "detach"
  const val PHYSICAL_ID = "_id"
  const val ELEMENT_ID = "_elementId"
}

internal object Values {
  const val NODE = "node"
  const val RELATIONSHIP = "relationship"
  const val CREATE = "create"
  const val UPDATE = "update"
  const val MERGE = "merge"
  const val DELETE = "delete"
}
