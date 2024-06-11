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
package org.neo4j.connectors.kafka.sink.strategy.legacy

import org.neo4j.connectors.kafka.sink.strategy.legacy.IngestionUtils.containsProp
import org.neo4j.connectors.kafka.sink.strategy.legacy.IngestionUtils.getLabelsAsString
import org.neo4j.connectors.kafka.sink.strategy.legacy.IngestionUtils.getNodeMergeKeys
import org.neo4j.connectors.kafka.sink.strategy.pattern.NodePattern
import org.neo4j.connectors.kafka.utils.JSONUtils
import org.neo4j.connectors.kafka.utils.StreamsUtils

class NodePatternIngestionStrategy(private val pattern: NodePattern, mergeProperties: Boolean) :
    IngestionStrategy {

  private val mergeNodeTemplate: String =
      """
                |${StreamsUtils.UNWIND}
                |MERGE (n${getLabelsAsString(pattern.labels)}{${
                    getNodeMergeKeys("keys", pattern.keyProperties.map { it.from }.toSet())
                }})
                |SET n ${if (mergeProperties) "+" else ""}= event.properties
                |SET n += event.keys
            """
          .trimMargin()

  private val deleteNodeTemplate: String =
      """
                |${StreamsUtils.UNWIND}
                |MATCH (n${getLabelsAsString(pattern.labels)}{${
                    getNodeMergeKeys("keys", pattern.keyProperties.map { it.from }.toSet())
                }})
                |DETACH DELETE n
            """
          .trimMargin()

  override fun mergeNodeEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents> {
    val data =
        events
            .mapNotNull { if (it.value != null) JSONUtils.asMap(it.value) else null }
            .mapNotNull { toData(pattern, it) }
    return if (data.isEmpty()) {
      emptyList()
    } else {
      listOf(QueryEvents(mergeNodeTemplate, data))
    }
  }

  override fun deleteNodeEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents> {
    val data =
        events
            .filter { it.value == null && it.key != null }
            .mapNotNull { if (it.key != null) JSONUtils.asMap(it.key) else null }
            .mapNotNull { toData(pattern, it, false) }
    return if (data.isEmpty()) {
      emptyList()
    } else {
      listOf(QueryEvents(deleteNodeTemplate, data))
    }
  }

  override fun mergeRelationshipEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents> {
    return emptyList()
  }

  override fun deleteRelationshipEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents> {
    return emptyList()
  }

  companion object {
    fun toData(
        pattern: NodePattern,
        props: Map<String, Any?>,
        withProperties: Boolean = true
    ): Map<String, Map<String, Any?>>? {
      val properties = props.flatten()
      val keyProperties = pattern.keyProperties.map { it.from }.toSet()
      val containsKeys = keyProperties.all { properties.containsKey(it) }
      return if (containsKeys) {
        val filteredProperties =
            buildMap<String, Any?> {
              if (pattern.includeAllValueProperties) {
                putAll(properties.filterKeys { key -> !keyProperties.contains(key) })
              }

              if (pattern.includeProperties.isNotEmpty()) {
                putAll(
                    properties.filterKeys { key ->
                      val containsProp =
                          containsProp(
                              key,
                              pattern.includeProperties.map { include -> include.from }.toSet())
                      !keyProperties.contains(key) && containsProp
                    })
              }

              if (pattern.excludeProperties.isNotEmpty()) {
                keys
                    .filter { key ->
                      val excluded = containsProp(key, pattern.excludeProperties)
                      !keyProperties.contains(key) && excluded
                    }
                    .forEach { remove(it) }
              }
            }

        if (withProperties) {
          mapOf(
              "keys" to properties.filterKeys { keyProperties.contains(it) },
              "properties" to filteredProperties)
        } else {
          mapOf("keys" to properties.filterKeys { keyProperties.contains(it) })
        }
      } else {
        null
      }
    }
  }
}
