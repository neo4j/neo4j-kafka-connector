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
package org.neo4j.connectors.kafka.sink.strategy.cdc

import kotlin.collections.buildMap
import org.neo4j.caniuse.CanIUse.canIUse
import org.neo4j.caniuse.Cypher
import org.neo4j.caniuse.Neo4j
import org.neo4j.cdc.client.model.EntityOperation
import org.neo4j.cypherdsl.core.internal.SchemaNames
import org.neo4j.driver.Query

interface CdcStatementGenerator {

  fun buildStatement(data: CdcData, eventVariable: String = "${'$'}$EVENT"): Query
}

class DefaultCdcStatementGenerator(neo4j: Neo4j) : CdcStatementGenerator {
  private val supportsDynamicLabelsWithPropertyIndices =
      canIUse(Cypher.dynamicLabelsAndTypesCanLeveragePropertyIndices()).withNeo4j(neo4j)
  private val setDynamicLabels = canIUse(Cypher.setDynamicLabels()).withNeo4j(neo4j)
  private val removeDynamicLabels = canIUse(Cypher.removeDynamicLabels()).withNeo4j(neo4j)

  override fun buildStatement(data: CdcData, eventVariable: String): Query {
    return when (data) {
      is CdcNodeData -> buildNodeStatement(data, eventVariable)
      is CdcRelationshipData -> buildRelationshipStatement(data, eventVariable)
      else -> throw IllegalArgumentException("unknown cdc data type ${data::class}")
    }
  }

  private fun buildNodeStatement(data: CdcNodeData, eventVariable: String): Query {
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
          EntityOperation.CREATE,
          EntityOperation.UPDATE -> {
            "WITH $eventVariable AS _e MERGE (n$matchLabels$matchProps) SET n += _e.setProperties$setLabels$removeLabels"
          }
          EntityOperation.DELETE -> {
            "WITH $eventVariable AS _e MATCH (n$matchLabels$matchProps) DELETE n"
          }
        }
    val params = buildMap {
      if (supportsDynamicLabelsWithPropertyIndices) {
        this["matchLabels"] = data.matchLabels
      }
      this["matchProperties"] = data.matchProperties
      if (data.operation != EntityOperation.DELETE) {
        this["setProperties"] = data.setProperties
      }
      if (setDynamicLabels && data.operation != EntityOperation.DELETE) {
        this["addLabels"] = data.addLabels
      }
      if (removeDynamicLabels && data.operation != EntityOperation.DELETE) {
        this["removeLabels"] = data.removeLabels
      }
    }

    return Query(stmt, if (eventVariable == "${'$'}$EVENT") mapOf(EVENT to params) else params)
  }

  private fun buildRelationshipStatement(data: CdcRelationshipData, eventVariable: String): Query {
    val startMatchLabels =
        if (supportsDynamicLabelsWithPropertyIndices && data.startMatchLabels.isNotEmpty())
            ":${'$'}(_e.start.matchLabels)"
        else buildMatchLabels(data.startMatchLabels)
    val startMatchProps = buildMatchProps(data.startMatchProperties, "_e", "start.matchProperties")
    val endMatchLabels =
        if (supportsDynamicLabelsWithPropertyIndices && data.endMatchLabels.isNotEmpty())
            ":${'$'}(_e.end.matchLabels)"
        else buildMatchLabels(data.endMatchLabels)
    val endMatchProps = buildMatchProps(data.endMatchProperties, "_e", "end.matchProperties")
    val matchType =
        if (supportsDynamicLabelsWithPropertyIndices) "${'$'}(_e.matchType)"
        else SchemaNames.sanitize(data.matchType, true).orElseThrow()
    val matchProps = buildMatchProps(data.matchProperties, "_e", "matchProperties")

    val stmt =
        when (data.operation) {
          EntityOperation.CREATE -> {
            "WITH $eventVariable AS _e MATCH (start$startMatchLabels$startMatchProps) MATCH (end$endMatchLabels$endMatchProps) MERGE (start)-[r:$matchType$matchProps]->(end) SET r += _e.setProperties"
          }
          EntityOperation.UPDATE -> {
            if (!data.hasKeys) {
              "WITH $eventVariable AS _e MATCH (start$startMatchLabels$startMatchProps) MATCH (end$endMatchLabels$endMatchProps) MATCH (start)-[r:$matchType$matchProps]->(end) WITH _e, r LIMIT 1 SET r += _e.setProperties"
            } else {
              "WITH $eventVariable AS _e MATCH (start$startMatchLabels$startMatchProps)-[r:$matchType$matchProps]->(end$endMatchLabels$endMatchProps) SET r += _e.setProperties"
            }
          }
          EntityOperation.DELETE -> {
            if (!data.hasKeys) {
              "WITH $eventVariable AS _e MATCH (start$startMatchLabels$startMatchProps) MATCH (end$endMatchLabels$endMatchProps) MATCH (start)-[r:$matchType$matchProps]->(end) WITH _e, r LIMIT 1 DELETE r"
            } else {
              "WITH $eventVariable AS _e MATCH ()-[r:$matchType$matchProps]->() DELETE r"
            }
          }
        }
    val params = buildMap {
      if (
          data.startMatchProperties.isNotEmpty() &&
              (data.operation != EntityOperation.DELETE || !data.hasKeys)
      ) {
        this["start"] = buildMap {
          if (supportsDynamicLabelsWithPropertyIndices) {
            this["matchLabels"] = data.startMatchLabels
          }
          this["matchProperties"] = data.startMatchProperties
        }
      }
      if (
          data.endMatchProperties.isNotEmpty() &&
              (data.operation != EntityOperation.DELETE || !data.hasKeys)
      ) {
        this["end"] = buildMap {
          if (supportsDynamicLabelsWithPropertyIndices) {
            this["matchLabels"] = data.endMatchLabels
          }
          this["matchProperties"] = data.endMatchProperties
        }
      }
      if (supportsDynamicLabelsWithPropertyIndices) {
        this["matchType"] = data.matchType
      }
      if (data.matchProperties.isNotEmpty()) {
        this["matchProperties"] = data.matchProperties
      }
      if (data.operation != EntityOperation.DELETE) {
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
