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

interface SinkDataStatementGenerator {

  fun buildStatement(data: SinkData, eventVariable: String = "${'$'}$EVENT"): Query
}

class DefaultSinkDataStatementGenerator(neo4j: Neo4j) : SinkDataStatementGenerator {
  private val supportsDynamicLabelsWithPropertyIndices =
      canIUse(Cypher.dynamicLabelsAndTypesCanLeveragePropertyIndices()).withNeo4j(neo4j)
  private val setDynamicLabels = canIUse(Cypher.setDynamicLabels()).withNeo4j(neo4j)
  private val removeDynamicLabels = canIUse(Cypher.removeDynamicLabels()).withNeo4j(neo4j)

  override fun buildStatement(data: SinkData, eventVariable: String): Query {
    return when (data) {
      is SinkNodeData -> buildNodeStatement(data, eventVariable)
      is SinkRelationshipData -> buildRelationshipStatement(data, eventVariable)
      else -> throw IllegalArgumentException("unknown cdc data type ${data::class}")
    }
  }

  private fun buildNodeStatement(data: SinkNodeData, eventVariable: String): Query {
    val matchLabels =
        if (supportsDynamicLabelsWithPropertyIndices) {
          ":${'$'}(_e.matchLabels)"
        } else buildMatchLabels(data.matchLabels)
    val matchProps = buildMatchProps(data.matchProperties, "_e", "matchProperties")
    val setLabels =
        if (setDynamicLabels) {
          " SET n:${'$'}(_e.addLabels)"
        } else if (data.addLabels.isNotEmpty()) {
          " SET n" +
              data.addLabels.sorted().joinToString(":", ":") {
                SchemaNames.sanitize(it, true).orElseThrow()
              }
        } else {
          ""
        }
    val removeLabels =
        if (removeDynamicLabels) {
          " REMOVE n:${'$'}(_e.removeLabels)"
        } else if (data.removeLabels.isNotEmpty()) {
          " REMOVE n" +
              data.removeLabels.sorted().joinToString(":", ":") {
                SchemaNames.sanitize(it, true).orElseThrow()
              }
        } else {
          ""
        }
    val stmt =
        when (data.operation) {
          OperationType.CREATE ->
              "WITH $eventVariable AS _e CREATE (n$matchLabels$matchProps) SET n += _e.setProperties$setLabels$removeLabels"
          OperationType.UPDATE ->
              "WITH $eventVariable AS _e MATCH (n$matchLabels$matchProps) SET n += _e.setProperties$setLabels$removeLabels"
          OperationType.MERGE ->
              "WITH $eventVariable AS _e MERGE (n$matchLabels$matchProps) SET n += _e.setProperties$setLabels$removeLabels"
          OperationType.DELETE -> {
            "WITH $eventVariable AS _e MATCH (n$matchLabels$matchProps) DELETE n"
          }
        }
    val params = buildMap {
      if (supportsDynamicLabelsWithPropertyIndices) {
        this["matchLabels"] = data.matchLabels
      }
      this["matchProperties"] = data.matchProperties
      if (data.operation != OperationType.DELETE) {
        this["setProperties"] = data.setProperties
      }
      if (setDynamicLabels && data.operation != OperationType.DELETE) {
        this["addLabels"] = data.addLabels
      }
      if (removeDynamicLabels && data.operation != OperationType.DELETE) {
        this["removeLabels"] = data.removeLabels
      }
    }

    return Query(stmt, if (eventVariable == "${'$'}$EVENT") mapOf(EVENT to params) else params)
  }

  private fun buildRelationshipStatement(data: SinkRelationshipData, eventVariable: String): Query {
    val startMatchLabels =
        if (supportsDynamicLabelsWithPropertyIndices && data.startNode.labels.isNotEmpty())
            ":${'$'}(_e.start.matchLabels)"
        else buildMatchLabels(data.startNode.labels)
    val startMatchProps = buildMatchProps(data.startNode.properties, "_e", "start.matchProperties")
    val startOp = data.startNode.lookupMode.name
    val startClause =
        if (startMatchLabels.isEmpty() && startMatchProps.isEmpty()) ""
        else " $startOp (start$startMatchLabels$startMatchProps)"
    val endMatchLabels =
        if (supportsDynamicLabelsWithPropertyIndices && data.endNode.labels.isNotEmpty())
            ":${'$'}(_e.end.matchLabels)"
        else buildMatchLabels(data.endNode.labels)
    val endMatchProps = buildMatchProps(data.endNode.properties, "_e", "end.matchProperties")
    val endOp = data.endNode.lookupMode.name
    val endClause =
        if (endMatchLabels.isEmpty() && endMatchProps.isEmpty()) ""
        else " $endOp (end$endMatchLabels$endMatchProps)"
    val matchType =
        if (supportsDynamicLabelsWithPropertyIndices) "${'$'}(_e.matchType)"
        else SchemaNames.sanitize(data.matchType, true).orElseThrow()
    val matchProps = buildMatchProps(data.matchProperties, "_e", "matchProperties")

    val stmt =
        when (data.operation) {
          OperationType.CREATE ->
              "WITH $eventVariable AS _e$startClause$endClause CREATE (start)-[r:$matchType$matchProps]->(end) SET r += _e.setProperties"
          OperationType.UPDATE ->
              if (!data.hasKeys)
                  "WITH $eventVariable AS _e$startClause$endClause MATCH (start)-[r:$matchType$matchProps]->(end) WITH _e, r LIMIT 1 SET r += _e.setProperties"
              else
                  "WITH $eventVariable AS _e$startClause$endClause MATCH (start)-[r:$matchType$matchProps]->(end) SET r += _e.setProperties"
          OperationType.MERGE ->
              if (!data.hasKeys)
                  "WITH $eventVariable AS _e$startClause$endClause MERGE (start)-[r:$matchType$matchProps]->(end) WITH _e, r LIMIT 1 SET r += _e.setProperties"
              else
                  "WITH $eventVariable AS _e$startClause$endClause MERGE (start)-[r:$matchType$matchProps]->(end) SET r += _e.setProperties"
          OperationType.DELETE ->
              if (!data.hasKeys)
                  "WITH $eventVariable AS _e$startClause$endClause MATCH (start)-[r:$matchType$matchProps]->(end) WITH _e, r LIMIT 1 DELETE r"
              else "WITH $eventVariable AS _e MATCH ()-[r:$matchType$matchProps]->() DELETE r"
        }
    val params = buildMap {
      if (
          data.startNode.properties.isNotEmpty() &&
              (data.operation != OperationType.DELETE || !data.hasKeys)
      ) {
        this["start"] = buildMap {
          if (supportsDynamicLabelsWithPropertyIndices) {
            this["matchLabels"] = data.startNode.labels
          }
          this["matchProperties"] = data.startNode.properties
        }
      }
      if (
          data.endNode.properties.isNotEmpty() &&
              (data.operation != OperationType.DELETE || !data.hasKeys)
      ) {
        this["end"] = buildMap {
          if (supportsDynamicLabelsWithPropertyIndices) {
            this["matchLabels"] = data.endNode.labels
          }
          this["matchProperties"] = data.endNode.properties
        }
      }
      if (supportsDynamicLabelsWithPropertyIndices) {
        this["matchType"] = data.matchType
      }
      if (data.matchProperties.isNotEmpty()) {
        this["matchProperties"] = data.matchProperties
      }
      if (data.operation != OperationType.DELETE) {
        this["setProperties"] = data.setProperties
      }
    }
    return Query(stmt, if (eventVariable == "${'$'}$EVENT") mapOf(EVENT to params) else params)
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

    private fun buildMatchLabels(labels: Set<String>): String =
        if (labels.isEmpty()) ""
        else labels.sorted().joinToString(":", ":") { SchemaNames.sanitize(it, true).orElseThrow() }
  }
}
