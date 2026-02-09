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
package org.neo4j.connectors.kafka.sink.strategy.cdc.apoc

import org.apache.kafka.connect.sink.SinkRecord
import org.neo4j.cdc.client.model.EntityOperation
import org.neo4j.cypherdsl.core.internal.SchemaNames

const val EVENT = "e"

interface CdcData {

  fun toParams(r: SinkRecord): Map<String, Any>

  fun buildStatement(): String
}

data class CdcNodeData(
    val operation: EntityOperation,
    val matchLabels: Set<String>,
    val matchProperties: Map<String, Any?>,
    val setProperties: Map<String, Any?>,
    val addLabels: Set<String>,
    val removeLabels: Set<String>,
) : CdcData {

  override fun toParams(r: SinkRecord): Map<String, Any> {
    return mapOf(
        "offset" to r.kafkaOffset(),
        "stmt" to buildStatement(),
        "params" to
            mapOf(
                EVENT to
                    mapOf(
                        "matchProperties" to matchProperties,
                        "setProperties" to setProperties,
                        "addLabels" to addLabels,
                        "removeLabels" to removeLabels,
                    )
            ),
    )
  }

  override fun buildStatement(): String {
    val matchLabels =
        if (matchLabels.isEmpty()) ""
        else matchLabels.joinToString(":", ":") { SchemaNames.sanitize(it).orElseThrow() }
    val matchProps =
        if (matchProperties.isEmpty()) ""
        else
            matchProperties
                .map { SchemaNames.sanitize(it.key).orElseThrow() }
                .joinToString(", ", " {", "}") { "$it: ${'$'}$EVENT.matchProperties.$it" }

    return when (operation) {
      EntityOperation.CREATE,
      EntityOperation.UPDATE -> {
        "MERGE (n$matchLabels$matchProps) SET n += ${'$'}$EVENT.setProperties SET n:\$(${'$'}$EVENT.addLabels) REMOVE n:\$(${'$'}$EVENT.removeLabels)"
      }
      EntityOperation.DELETE -> {
        "MATCH (n$matchLabels$matchProps) DELETE n"
      }
    }
  }
}

data class CdcRelationshipData(
    val operation: EntityOperation,
    val startMatchLabels: Set<String>,
    val startMatchProperties: Map<String, Any?>,
    val endMatchLabels: Set<String>,
    val endMatchProperties: Map<String, Any?>,
    val matchType: String,
    val matchProperties: Map<String, Any?>,
    val hasKeys: Boolean,
    val setProperties: Map<String, Any?>,
) : CdcData {

  override fun toParams(r: SinkRecord): Map<String, Any> {
    return mapOf(
        "offset" to r.kafkaOffset(),
        "stmt" to buildStatement(),
        "params" to
            mapOf(
                EVENT to
                    mapOf(
                        "start" to mapOf("matchProperties" to startMatchProperties),
                        "end" to mapOf("matchProperties" to endMatchProperties),
                        "matchProperties" to matchProperties,
                        "setProperties" to setProperties,
                    )
            ),
    )
  }

  override fun buildStatement(): String {
    val startMatchLabels =
        if (startMatchLabels.isEmpty()) ""
        else startMatchLabels.joinToString(":", ":") { SchemaNames.sanitize(it).orElseThrow() }
    val startMatchProps =
        if (startMatchProperties.isEmpty()) ""
        else
            startMatchProperties
                .map { SchemaNames.sanitize(it.key).orElseThrow() }
                .joinToString(", ", " {", "}") { "$it: ${'$'}$EVENT.start.matchProperties.$it" }
    val endMatchLabels =
        if (endMatchLabels.isEmpty()) ""
        else endMatchLabels.joinToString(":", ":") { SchemaNames.sanitize(it).orElseThrow() }
    val endMatchProps =
        if (endMatchProperties.isEmpty()) ""
        else
            endMatchProperties
                .map { SchemaNames.sanitize(it.key).orElseThrow() }
                .joinToString(", ", " {", "}") { "$it: ${'$'}$EVENT.end.matchProperties.$it" }
    val matchType = SchemaNames.sanitize(matchType).orElseThrow()
    val matchProps =
        if (matchProperties.isEmpty()) ""
        else
            matchProperties
                .map { SchemaNames.sanitize(it.key).orElseThrow() }
                .joinToString(", ", " {", "}") { "$it: ${'$'}$EVENT.matchProperties.$it" }

    return when (operation) {
      EntityOperation.CREATE -> {
        "MATCH (start$startMatchLabels$startMatchProps) MATCH (end$endMatchLabels$endMatchProps) MERGE (start)-[r:$matchType$matchProps]->(end) SET r += ${'$'}$EVENT.setProperties"
      }
      EntityOperation.UPDATE -> {
        if (!hasKeys) {
          "MATCH (start$startMatchLabels$startMatchProps) MATCH (end$endMatchLabels$endMatchProps) MATCH (start)-[r:$matchType$matchProps]->(end) WITH r LIMIT 1 SET r += ${'$'}$EVENT.setProperties"
        } else {
          "MATCH ($startMatchLabels$startMatchProps)-[r:$matchType$matchProps]->($endMatchLabels$endMatchProps) SET r += ${'$'}$EVENT.setProperties"
        }
      }
      EntityOperation.DELETE -> {
        if (!hasKeys) {
          "MATCH (start$startMatchLabels$startMatchProps) MATCH (end$endMatchLabels$endMatchProps) MATCH (start)-[r:$matchType$matchProps]->(end) WITH r LIMIT 1 DELETE r"
        } else {
          "MATCH ()-[r:$matchType$matchProps]->() DELETE r"
        }
      }
    }
  }
}
