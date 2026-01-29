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
            .map { "${SchemaNames.sanitize(it.key).orElseThrow()}: e.matchProperties.${it.key}" }
            .joinToString(", ")

    return when (operation) {
      EntityOperation.CREATE,
      EntityOperation.UPDATE -> {
        "MERGE (n:\$(e.matchLabels) {$matchProps}) SET n += e.setProperties SET n:\$(e.addLabels) REMOVE n:\$(e.removeLabels)"
      }
      EntityOperation.DELETE -> {
        "MATCH (n:\$(e.matchLabels) {$matchProps}) DETACH DELETE n"
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
            .map {
              "${SchemaNames.sanitize(it.key).orElseThrow()}: e.start.matchProperties.${it.key}"
            }
            .joinToString(", ")
    val endMatchProps =
        endMatchProperties
            .map {
              "${SchemaNames.sanitize(it.key).orElseThrow()}: e.end.matchProperties.${it.key}"
            }
            .joinToString(", ")
    val matchProps =
        matchProperties
            .map { "${SchemaNames.sanitize(it.key).orElseThrow()}: e.matchProperties.${it.key}" }
            .joinToString(", ")

    return when (operation) {
      EntityOperation.CREATE -> {
        "MERGE (start:\$(e.start.matchLabels) {$startMatchProps}) MERGE (end:\$(e.end.matchLabels) {$endMatchProps}) MERGE (start)-[r:\$(e.matchType) {$matchProps}]->(end) SET r += e.setProperties"
      }
      EntityOperation.UPDATE -> {
        if (matchProperties.isEmpty()) {
          "MERGE (start:\$(e.start.matchLabels) {$startMatchProps}) MERGE (end:\$(e.end.matchLabels) {$endMatchProps}) MERGE (start)-[r:\$(e.matchType)]->(end) SET r += e.setProperties"
        } else {
          "MATCH (:\$(e.start.matchLabels) {$startMatchProps})-[r:\$(e.matchType) {$matchProps}]->(:\$(e.end.matchLabels) {$endMatchProps}) SET r += e.setProperties"
        }
      }
      EntityOperation.DELETE -> {
        if (matchProperties.isEmpty()) {
          "MATCH (start:\$(e.start.matchLabels) {$startMatchProps}) MATCH (end:\$(e.end.matchLabels) {$endMatchProps}) MATCH (start)-[r:\$(e.matchType) {$matchProps}]->(end) DELETE r"
        } else {
          "MATCH ()-[r:\$(e.matchType) {$matchProps}]->() DELETE r"
        }
      }
    }
  }
}
