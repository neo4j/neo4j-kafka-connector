/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.connectors.kafka.service.sink.strategy

import org.neo4j.connectors.kafka.extensions.flatten
import org.neo4j.connectors.kafka.service.StreamsSinkEntity
import org.neo4j.connectors.kafka.utils.IngestionUtils.containsProp
import org.neo4j.connectors.kafka.utils.IngestionUtils.getLabelsAsString
import org.neo4j.connectors.kafka.utils.IngestionUtils.getNodeMergeKeys
import org.neo4j.connectors.kafka.utils.JSONUtils
import org.neo4j.connectors.kafka.utils.StreamsUtils

class NodePatternIngestionStrategy(private val nodePatternConfiguration: NodePatternConfiguration) :
    IngestionStrategy {

  private val mergeNodeTemplate: String =
      """
                |${StreamsUtils.UNWIND}
                |MERGE (n${getLabelsAsString(nodePatternConfiguration.labels)}{${
                    getNodeMergeKeys("keys", nodePatternConfiguration.keys)
                }})
                |SET n ${if (nodePatternConfiguration.mergeProperties) "+" else ""}= event.properties
                |SET n += event.keys
            """
          .trimMargin()

  private val deleteNodeTemplate: String =
      """
                |${StreamsUtils.UNWIND}
                |MATCH (n${getLabelsAsString(nodePatternConfiguration.labels)}{${
                    getNodeMergeKeys("keys", nodePatternConfiguration.keys)
                }})
                |DETACH DELETE n
            """
          .trimMargin()

  override fun mergeNodeEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents> {
    val data =
        events
            .mapNotNull { if (it.value != null) JSONUtils.asMap(it.value) else null }
            .mapNotNull { toData(nodePatternConfiguration, it) }
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
            .mapNotNull { toData(nodePatternConfiguration, it, false) }
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
        nodePatternConfiguration: NodePatternConfiguration,
        props: Map<String, Any?>,
        withProperties: Boolean = true
    ): Map<String, Map<String, Any?>>? {
      val properties = props.flatten()
      val containsKeys = nodePatternConfiguration.keys.all { properties.containsKey(it) }
      return if (containsKeys) {
        val filteredProperties =
            when (nodePatternConfiguration.type) {
              PatternConfigurationType.ALL ->
                  properties.filterKeys { !nodePatternConfiguration.keys.contains(it) }
              PatternConfigurationType.EXCLUDE ->
                  properties.filterKeys { key ->
                    val containsProp = containsProp(key, nodePatternConfiguration.properties)
                    !nodePatternConfiguration.keys.contains(key) && !containsProp
                  }
              PatternConfigurationType.INCLUDE ->
                  properties.filterKeys { key ->
                    val containsProp = containsProp(key, nodePatternConfiguration.properties)
                    !nodePatternConfiguration.keys.contains(key) && containsProp
                  }
            }
        if (withProperties) {
          mapOf(
              "keys" to properties.filterKeys { nodePatternConfiguration.keys.contains(it) },
              "properties" to filteredProperties)
        } else {
          mapOf("keys" to properties.filterKeys { nodePatternConfiguration.keys.contains(it) })
        }
      } else {
        null
      }
    }
  }
}
