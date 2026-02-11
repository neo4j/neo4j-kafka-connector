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

  fun buildStatement(data: CdcData): Query
}

class DefaultCdcStatementGenerator(neo4j: Neo4j) : CdcStatementGenerator {
  private val setDynamicLabels = canIUse(Cypher.setDynamicLabels()).withNeo4j(neo4j)
  private val removeDynamicLabels = canIUse(Cypher.removeDynamicLabels()).withNeo4j(neo4j)

  override fun buildStatement(data: CdcData): Query {
    return when (data) {
      is CdcNodeData -> buildNodeStatement(data)
      is CdcRelationshipData -> buildRelationshipStatement(data)
      else -> throw IllegalArgumentException("unknown cdc data type ${data::class}")
    }
  }

  private fun buildNodeStatement(data: CdcNodeData): Query {
    val matchLabels = buildMatchLabels(data.matchLabels)
    val matchProps = buildMatchProps(data.matchProperties, "matchProperties")
    val setLabels =
        if (setDynamicLabels) {
          "SET n:\$($EVENT.addLabels)"
        } else {
          "SET n" +
              data.addLabels.sorted().joinToString(":", ":") {
                SchemaNames.sanitize(it, true).orElseThrow()
              }
        }
    val removeLabels =
        if (removeDynamicLabels) {
          "REMOVE n:\$($EVENT.removeLabels)"
        } else {
          "REMOVE n" +
              data.removeLabels.sorted().joinToString(":", ":") {
                SchemaNames.sanitize(it, true).orElseThrow()
              }
        }
    val stmt =
        when (data.operation) {
          EntityOperation.CREATE,
          EntityOperation.UPDATE -> {
            "MERGE (n$matchLabels$matchProps) SET n += ${'$'}$EVENT.setProperties $setLabels $removeLabels"
          }
          EntityOperation.DELETE -> {
            "MATCH (n$matchLabels$matchProps) DELETE n"
          }
        }

    return Query(
        stmt,
        mapOf(
            EVENT to
                buildMap {
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
        ),
    )
  }

  private fun buildRelationshipStatement(data: CdcRelationshipData): Query {
    val startMatchLabels = buildMatchLabels(data.startMatchLabels)
    val startMatchProps = buildMatchProps(data.startMatchProperties, "start.matchProperties")
    val endMatchLabels = buildMatchLabels(data.endMatchLabels)
    val endMatchProps = buildMatchProps(data.endMatchProperties, "end.matchProperties")
    val matchType = SchemaNames.sanitize(data.matchType, true).orElseThrow()
    val matchProps = buildMatchProps(data.matchProperties, "matchProperties")

    val stmt =
        when (data.operation) {
          EntityOperation.CREATE -> {
            "MATCH (start$startMatchLabels$startMatchProps) MATCH (end$endMatchLabels$endMatchProps) MERGE (start)-[r:$matchType$matchProps]->(end) SET r += ${'$'}$EVENT.setProperties"
          }
          EntityOperation.UPDATE -> {
            if (!data.hasKeys) {
              "MATCH (start$startMatchLabels$startMatchProps) MATCH (end$endMatchLabels$endMatchProps) MATCH (start)-[r:$matchType$matchProps]->(end) WITH r LIMIT 1 SET r += ${'$'}$EVENT.setProperties"
            } else {
              "MATCH ($startMatchLabels$startMatchProps)-[r:$matchType$matchProps]->($endMatchLabels$endMatchProps) SET r += ${'$'}$EVENT.setProperties"
            }
          }
          EntityOperation.DELETE -> {
            if (!data.hasKeys) {
              "MATCH (start$startMatchLabels$startMatchProps) MATCH (end$endMatchLabels$endMatchProps) MATCH (start)-[r:$matchType$matchProps]->(end) WITH r LIMIT 1 DELETE r"
            } else {
              "MATCH ()-[r:$matchType$matchProps]->() DELETE r"
            }
          }
        }

    return Query(
        stmt,
        mapOf(
            EVENT to
                buildMap {
                  if (data.startMatchProperties.isNotEmpty()) {
                    this["start"] = mapOf("matchProperties" to data.startMatchProperties)
                  }
                  if (data.endMatchProperties.isNotEmpty()) {
                    this["end"] = mapOf("matchProperties" to data.endMatchProperties)
                  }
                  if (data.matchProperties.isNotEmpty()) {
                    this["matchProperties"] = data.matchProperties
                  }
                  if (data.operation != EntityOperation.DELETE) {
                    this["setProperties"] = data.setProperties
                  }
                }
        ),
    )
  }

  companion object {
    private fun buildMatchProps(matchProperties: Map<String, Any?>, paramsPath: String): String =
        if (matchProperties.isEmpty()) ""
        else
            matchProperties
                .map { SchemaNames.sanitize(it.key, true).orElseThrow() }
                .sorted()
                .joinToString(", ", " {", "}") { "$it: ${'$'}$EVENT.${paramsPath}.$it" }

    private fun buildMatchLabels(labels: Set<String>): String =
        if (labels.isEmpty()) ""
        else labels.sorted().joinToString(":", ":") { SchemaNames.sanitize(it, true).orElseThrow() }
  }
}
