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
package org.neo4j.connectors.kafka.sink.strategy

import kotlin.collections.buildMap
import org.neo4j.caniuse.CanIUse.canIUse
import org.neo4j.caniuse.Cypher
import org.neo4j.caniuse.Neo4j
import org.neo4j.cypherdsl.core.internal.SchemaNames
import org.neo4j.driver.Query

interface SinkActionStatementGenerator {

  fun buildStatement(data: SinkAction, eventVariable: String = "${'$'}$EVENT"): Query
}

class DefaultSinkActionStatementGenerator(neo4j: Neo4j) : SinkActionStatementGenerator {
  private val supportsDynamicLabelsWithPropertyIndices =
      canIUse(Cypher.dynamicLabelsAndTypesCanLeveragePropertyIndices()).withNeo4j(neo4j)
  private val setDynamicLabels = canIUse(Cypher.setDynamicLabels()).withNeo4j(neo4j)
  private val removeDynamicLabels = canIUse(Cypher.removeDynamicLabels()).withNeo4j(neo4j)

  override fun buildStatement(data: SinkAction, eventVariable: String): Query {
    return when (data) {
      is CreateNodeSinkAction -> buildNodeStatement(data, eventVariable)
      is UpdateNodeSinkAction -> buildNodeStatement(data, eventVariable)
      is MergeNodeSinkAction -> buildNodeStatement(data, eventVariable)
      is DeleteNodeSinkAction -> buildNodeStatement(data, eventVariable)
      is CreateRelationshipSinkAction -> buildRelationshipStatement(data, eventVariable)
      is UpdateRelationshipSinkAction -> buildRelationshipStatement(data, eventVariable)
      is MergeRelationshipSinkAction -> buildRelationshipStatement(data, eventVariable)
      is DeleteRelationshipSinkAction -> buildRelationshipStatement(data, eventVariable)
    }
  }

  private fun buildNodeStatement(action: CreateNodeSinkAction, eventVariable: String): Query {
    val labels =
        if (supportsDynamicLabelsWithPropertyIndices) {
          ":${'$'}(_e.labels)"
        } else buildLabels(action.labels)
    val stmt = "WITH $eventVariable AS _e CREATE (n$labels) SET n += _e.properties"

    val params = buildMap {
      if (supportsDynamicLabelsWithPropertyIndices) {
        this["labels"] = action.labels
      }
      this["properties"] = action.properties
    }

    return Query(stmt, if (eventVariable == "${'$'}$EVENT") mapOf(EVENT to params) else params)
  }

  private fun buildNodeStatement(action: UpdateNodeSinkAction, eventVariable: String): Query {
    return buildNodeUpdateStatement(
        "MATCH",
        action.matchLabels,
        action.matchProperties,
        action.setProperties,
        action.addLabels,
        action.removeLabels,
        eventVariable,
    )
  }

  private fun buildNodeStatement(action: MergeNodeSinkAction, eventVariable: String): Query {
    return buildNodeUpdateStatement(
        "MERGE",
        action.matchLabels,
        action.matchProperties,
        action.setProperties,
        action.addLabels,
        action.removeLabels,
        eventVariable,
    )
  }

  private fun buildNodeUpdateStatement(
      action: String,
      matchLabels: Set<String>,
      matchProperties: Map<String, Any?>,
      setProperties: Map<String, Any?>,
      addLabels: Set<String>,
      removeLabels: Set<String>,
      eventVariable: String,
  ): Query {
    val matchLabelsPattern =
        if (supportsDynamicLabelsWithPropertyIndices) {
          ":${'$'}(_e.matchLabels)"
        } else buildLabels(matchLabels)
    val matchPropsPattern = buildMatchProps(matchProperties, "_e", "matchProperties")
    val setLabelsClause =
        if (setDynamicLabels) {
          " SET n:${'$'}(_e.addLabels)"
        } else if (addLabels.isNotEmpty()) {
          " SET n" + buildLabels(addLabels)
        } else {
          ""
        }
    val removeLabelsClause =
        if (removeDynamicLabels) {
          " REMOVE n:${'$'}(_e.removeLabels)"
        } else if (removeLabels.isNotEmpty()) {
          " REMOVE n" + buildLabels(removeLabels)
        } else {
          ""
        }
    val stmt =
        "WITH $eventVariable AS _e $action (n$matchLabelsPattern$matchPropsPattern) SET n += _e.setProperties$setLabelsClause$removeLabelsClause"
    val params = buildMap {
      if (supportsDynamicLabelsWithPropertyIndices) {
        this["matchLabels"] = matchLabels
      }
      this["matchProperties"] = matchProperties
      this["setProperties"] = setProperties
      if (setDynamicLabels) {
        this["addLabels"] = addLabels
      }
      if (removeDynamicLabels) {
        this["removeLabels"] = removeLabels
      }
    }

    return Query(stmt, if (eventVariable == "${'$'}$EVENT") mapOf(EVENT to params) else params)
  }

  private fun buildNodeStatement(action: DeleteNodeSinkAction, eventVariable: String): Query {
    val matchLabels =
        if (supportsDynamicLabelsWithPropertyIndices) {
          ":${'$'}(_e.matchLabels)"
        } else buildLabels(action.matchLabels)
    val matchProps = buildMatchProps(action.matchProperties, "_e", "matchProperties")
    val stmt = "WITH $eventVariable AS _e MATCH (n$matchLabels$matchProps) DELETE n"
    val params = buildMap {
      if (supportsDynamicLabelsWithPropertyIndices) {
        this["matchLabels"] = action.matchLabels
      }
      this["matchProperties"] = action.matchProperties
    }

    return Query(stmt, if (eventVariable == "${'$'}$EVENT") mapOf(EVENT to params) else params)
  }

  private fun buildRelationshipStatement(
      action: CreateRelationshipSinkAction,
      eventVariable: String,
  ): Query {
    val startClause = action.startNode.buildClause("start")
    val endClause = action.endNode.buildClause("end")

    val typePattern =
        if (supportsDynamicLabelsWithPropertyIndices) "${'$'}(_e.type)"
        else SchemaNames.sanitize(action.type, true).orElseThrow()

    val stmt =
        "WITH $eventVariable AS _e$startClause$endClause CREATE (start)-[r:$typePattern]->(end) SET r += _e.properties"
    val params = buildMap {
      if (action.startNode.properties.isNotEmpty()) {
        this["start"] = buildMap {
          if (supportsDynamicLabelsWithPropertyIndices) {
            this["matchLabels"] = action.startNode.labels
          }
          this["matchProperties"] = action.startNode.properties
        }
      }
      if (action.endNode.properties.isNotEmpty()) {
        this["end"] = buildMap {
          if (supportsDynamicLabelsWithPropertyIndices) {
            this["matchLabels"] = action.endNode.labels
          }
          this["matchProperties"] = action.endNode.properties
        }
      }
      if (supportsDynamicLabelsWithPropertyIndices) {
        this["type"] = action.type
      }
      this["properties"] = action.properties
    }
    return Query(stmt, if (eventVariable == "${'$'}$EVENT") mapOf(EVENT to params) else params)
  }

  private fun buildRelationshipStatement(
      action: UpdateRelationshipSinkAction,
      eventVariable: String,
  ): Query {
    return buildRelationshipUpdateStatement(
        "MATCH",
        action.startNode,
        action.endNode,
        action.matchType,
        action.matchProperties,
        action.setProperties,
        action.hasKeys,
        eventVariable,
    )
  }

  private fun buildRelationshipStatement(
      action: MergeRelationshipSinkAction,
      eventVariable: String,
  ): Query {
    return buildRelationshipUpdateStatement(
        "MERGE",
        action.startNode,
        action.endNode,
        action.matchType,
        action.matchProperties,
        action.setProperties,
        action.hasKeys,
        eventVariable,
    )
  }

  private fun buildRelationshipUpdateStatement(
      action: String,
      startNode: SinkActionNodeReference,
      endNode: SinkActionNodeReference,
      matchType: String,
      matchProperties: Map<String, Any?>,
      setProperties: Map<String, Any?>,
      hasKeys: Boolean,
      eventVariable: String,
  ): Query {
    val startClause = startNode.buildClause("start")
    val endClause = endNode.buildClause("end")

    val matchTypePattern =
        if (supportsDynamicLabelsWithPropertyIndices) "${'$'}(_e.matchType)"
        else SchemaNames.sanitize(matchType, true).orElseThrow()
    val matchPropsPattern = buildMatchProps(matchProperties, "_e", "matchProperties")

    val stmt =
        if (!hasKeys)
            "WITH $eventVariable AS _e$startClause$endClause $action (start)-[r:$matchTypePattern$matchPropsPattern]->(end) WITH _e, r LIMIT 1 SET r += _e.setProperties"
        else
            "WITH $eventVariable AS _e$startClause$endClause $action (start)-[r:$matchTypePattern$matchPropsPattern]->(end) SET r += _e.setProperties"
    val params = buildMap {
      if (startNode.properties.isNotEmpty() || !hasKeys) {
        this["start"] = buildMap {
          if (supportsDynamicLabelsWithPropertyIndices) {
            this["matchLabels"] = startNode.labels
          }
          this["matchProperties"] = startNode.properties
        }
      }
      if (endNode.properties.isNotEmpty() || !hasKeys) {
        this["end"] = buildMap {
          if (supportsDynamicLabelsWithPropertyIndices) {
            this["matchLabels"] = endNode.labels
          }
          this["matchProperties"] = endNode.properties
        }
      }
      if (supportsDynamicLabelsWithPropertyIndices) {
        this["matchType"] = matchType
      }
      if (matchProperties.isNotEmpty()) {
        this["matchProperties"] = matchProperties
      }
      this["setProperties"] = setProperties
    }

    return Query(stmt, if (eventVariable == "${'$'}$EVENT") mapOf(EVENT to params) else params)
  }

  private fun buildRelationshipStatement(
      action: DeleteRelationshipSinkAction,
      eventVariable: String,
  ): Query {
    val startClause = action.startNode.buildClause("start")
    val endClause = action.endNode.buildClause("end")

    val matchTypePattern =
        if (supportsDynamicLabelsWithPropertyIndices) "${'$'}(_e.matchType)"
        else SchemaNames.sanitize(action.matchType, true).orElseThrow()
    val matchPropsPattern = buildMatchProps(action.matchProperties, "_e", "matchProperties")

    val stmt =
        if (!action.hasKeys)
            "WITH $eventVariable AS _e$startClause$endClause MATCH (start)-[r:$matchTypePattern$matchPropsPattern]->(end) WITH _e, r LIMIT 1 DELETE r"
        else
            "WITH $eventVariable AS _e MATCH ()-[r:$matchTypePattern$matchPropsPattern]->() DELETE r"
    val params = buildMap {
      if (action.startNode.properties.isNotEmpty() && !action.hasKeys) {
        this["start"] = buildMap {
          if (supportsDynamicLabelsWithPropertyIndices) {
            this["matchLabels"] = action.startNode.labels
          }
          this["matchProperties"] = action.startNode.properties
        }
      }
      if (action.endNode.properties.isNotEmpty() && !action.hasKeys) {
        this["end"] = buildMap {
          if (supportsDynamicLabelsWithPropertyIndices) {
            this["matchLabels"] = action.endNode.labels
          }
          this["matchProperties"] = action.endNode.properties
        }
      }
      if (supportsDynamicLabelsWithPropertyIndices) {
        this["matchType"] = action.matchType
      }
      if (action.matchProperties.isNotEmpty()) {
        this["matchProperties"] = action.matchProperties
      }
    }

    return Query(stmt, if (eventVariable == "${'$'}$EVENT") mapOf(EVENT to params) else params)
  }

  private fun SinkActionNodeReference.buildClause(alias: String): String {
    val matchLabels =
        if (supportsDynamicLabelsWithPropertyIndices && this.labels.isNotEmpty())
            ":${'$'}(_e.$alias.matchLabels)"
        else buildLabels(this.labels)
    val matchProps = buildMatchProps(this.properties, "_e", "$alias.matchProperties")
    val op = this.lookupMode.name

    return if (matchLabels.isEmpty() && matchProps.isEmpty()) ""
    else " $op ($alias$matchLabels$matchProps)"
  }

  companion object {
    @Suppress("SameParameterValue")
    private fun buildMatchProps(
        matchProperties: Map<String, Any?>,
        eventVariable: String,
        paramsPath: String,
    ): String =
        if (matchProperties.isEmpty()) ""
        else
            matchProperties
                .map { SchemaNames.sanitize(it.key, true).orElseThrow() }
                .sorted()
                .joinToString(", ", " {", "}") { "$it: $eventVariable.${paramsPath}.$it" }

    private fun buildLabels(labels: Set<String>): String =
        if (labels.isEmpty()) ""
        else labels.sorted().joinToString(":", ":") { SchemaNames.sanitize(it, true).orElseThrow() }
  }
}
