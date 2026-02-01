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

import org.neo4j.cdc.client.model.EntityOperation
import org.neo4j.connectors.kafka.events.EntityType
import org.neo4j.cypherdsl.core.internal.SchemaNames

const val EVENT = "e"

interface CdcData {

  fun groupingBasedOn(): GroupingKey

  fun toParams(queryId: Int): Map<String, Any>

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
  override fun groupingBasedOn(): GroupingKey {
    return GroupingKey(EntityType.node, operation, matchProperties.keys)
  }

  override fun toParams(queryId: Int): Map<String, Any> {
    return mapOf(
        "q" to queryId,
        "matchLabels" to matchLabels,
        "matchProperties" to matchProperties,
        "setProperties" to setProperties,
        "addLabels" to addLabels,
        "removeLabels" to removeLabels,
    )
  }

  override fun buildStatement(): String {
    val matchProps =
        matchProperties
            .map { SchemaNames.sanitize(it.key).orElseThrow() }
            .joinToString(", ") { "$it: $EVENT.matchProperties.$it" }

    return when (operation) {
      EntityOperation.CREATE,
      EntityOperation.UPDATE -> {
        "MERGE (n:\$($EVENT.matchLabels) {$matchProps}) SET n += $EVENT.setProperties SET n:\$($EVENT.addLabels) REMOVE n:\$($EVENT.removeLabels)"
      }
      EntityOperation.DELETE -> {
        "MATCH (n:\$($EVENT.matchLabels) {$matchProps}) DETACH DELETE n"
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
    val setProperties: Map<String, Any?>,
) : CdcData {
  override fun groupingBasedOn(): GroupingKey {
    return if (matchProperties.isEmpty()) {
      GroupingKey(
          EntityType.relationship,
          operation,
          startMatchProperties.keys,
          endMatchProperties.keys,
          matchProperties.keys,
      )
    } else {
      GroupingKey(EntityType.relationship, operation, matchProperties.keys)
    }
  }

  override fun toParams(queryId: Int): Map<String, Any> {
    return mapOf(
        "q" to queryId,
        "start" to
            mapOf("matchLabels" to startMatchLabels, "matchProperties" to startMatchProperties),
        "end" to mapOf("matchLabels" to endMatchLabels, "matchProperties" to endMatchProperties),
        "matchType" to matchType,
        "matchProperties" to matchProperties,
        "setProperties" to setProperties,
    )
  }

  override fun buildStatement(): String {
    val startMatchProps =
        startMatchProperties
            .map { SchemaNames.sanitize(it.key).orElseThrow() }
            .joinToString(", ") { "$it: $EVENT.start.matchProperties.$it" }
    val endMatchProps =
        endMatchProperties
            .map { SchemaNames.sanitize(it.key).orElseThrow() }
            .joinToString(", ") { "$it: $EVENT.end.matchProperties.$it" }
    val matchProps =
        matchProperties
            .map { SchemaNames.sanitize(it.key).orElseThrow() }
            .joinToString(", ") { "$it: $EVENT.matchProperties.$it" }

    return when (operation) {
      EntityOperation.CREATE -> {
        val operation = if (matchProperties.isEmpty()) "CREATE" else "MERGE"
        "MERGE (start:\$($EVENT.start.matchLabels) {$startMatchProps}) MERGE (end:\$($EVENT.end.matchLabels) {$endMatchProps}) $operation (start)-[r:\$($EVENT.matchType) {$matchProps}]->(end) SET r += $EVENT.setProperties"
      }
      EntityOperation.UPDATE -> {
        if (matchProperties.isEmpty()) {
          "MERGE (start:\$($EVENT.start.matchLabels) {$startMatchProps}) MERGE (end:\$($EVENT.end.matchLabels) {$endMatchProps}) MERGE (start)-[r:\$($EVENT.matchType)]->(end) SET r += $EVENT.setProperties"
        } else {
          "MATCH (:\$($EVENT.start.matchLabels) {$startMatchProps})-[r:\$($EVENT.matchType) {$matchProps}]->(:\$($EVENT.end.matchLabels) {$endMatchProps}) SET r += $EVENT.setProperties"
        }
      }
      EntityOperation.DELETE -> {
        if (matchProperties.isEmpty()) {
          "MATCH (start:\$($EVENT.start.matchLabels) {$startMatchProps}) MATCH (end:\$($EVENT.end.matchLabels) {$endMatchProps}) MATCH (start)-[r:\$($EVENT.matchType) {$matchProps}]->(end) DELETE r"
        } else {
          "MATCH ()-[r:\$($EVENT.matchType) {$matchProps}]->() DELETE r"
        }
      }
    }
  }
}
