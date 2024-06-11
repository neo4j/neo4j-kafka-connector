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
import org.neo4j.connectors.kafka.sink.strategy.pattern.RelationshipPattern
import org.neo4j.connectors.kafka.utils.JSONUtils
import org.neo4j.connectors.kafka.utils.StreamsUtils

class RelationshipPatternIngestionStrategy(
    private val pattern: RelationshipPattern,
    mergeNodeProperties: Boolean,
    mergeRelationshipProperties: Boolean,
) : IngestionStrategy {

  private val mergeRelationshipTemplate: String =
      """
                |${StreamsUtils.UNWIND}
                |MERGE (start${getLabelsAsString(pattern.start.labels)}{${
                    getNodeMergeKeys("start.keys", pattern.start.keyProperties.map { it.from }.toSet())
                }})
                |SET start ${if (mergeNodeProperties) "+" else ""}= event.start.properties
                |SET start += event.start.keys
                |MERGE (end${getLabelsAsString(pattern.end.labels)}{${
                    getNodeMergeKeys("end.keys", pattern.end.keyProperties.map { it.from }.toSet())
                }})
                |SET end ${if (mergeNodeProperties) "+" else ""}= event.end.properties
                |SET end += event.end.keys
                |MERGE (start)-[r:${pattern.type}]->(end)
                |SET r ${if (mergeRelationshipProperties) "+" else ""}= event.properties
            """
          .trimMargin()

  private val deleteRelationshipTemplate: String =
      """
                |${StreamsUtils.UNWIND}
                |MATCH (start${getLabelsAsString(pattern.start.labels)}{${
                    getNodeMergeKeys("start.keys", pattern.start.keyProperties.map { it.from }.toSet())
                }})
                |MATCH (end${getLabelsAsString(pattern.end.labels)}{${
                    getNodeMergeKeys("end.keys", pattern.end.keyProperties.map { it.from }.toSet())
                }})
                |MATCH (start)-[r:${pattern.type}]->(end)
                |DELETE r
            """
          .trimMargin()

  override fun mergeNodeEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents> {
    return emptyList()
  }

  override fun deleteNodeEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents> {
    return emptyList()
  }

  override fun mergeRelationshipEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents> {
    val data =
        events
            .mapNotNull { if (it.value != null) JSONUtils.asMap(it.value) else null }
            .mapNotNull { props ->
              val properties = props.flatten()
              val startKeys = pattern.start.keyProperties.map { it.from }.toSet()
              val endKeys = pattern.end.keyProperties.map { it.from }.toSet()
              val containsKeys =
                  startKeys.all { properties.containsKey(it) } &&
                      endKeys.all { properties.containsKey(it) }
              if (containsKeys) {

                val filteredProperties =
                    buildMap<String, Any?> {
                      if (pattern.includeAllValueProperties) {
                        putAll(properties.filterKeys { key -> isRelationshipProperty(key) })
                      }

                      if (pattern.includeProperties.isNotEmpty()) {
                        putAll(
                            properties.filterKeys { key ->
                              val containsProp =
                                  containsProp(
                                      key,
                                      pattern.includeProperties
                                          .map { include -> include.from }
                                          .toSet())
                              isRelationshipProperty(key) && containsProp
                            })
                      }

                      if (pattern.excludeProperties.isNotEmpty()) {
                        keys
                            .filter { key ->
                              val excluded = containsProp(key, pattern.excludeProperties)
                              isRelationshipProperty(key) && excluded
                            }
                            .forEach { remove(it) }
                      }
                    }

                val startConf = pattern.start
                val endConf = pattern.end

                val start = NodePatternIngestionStrategy.toData(startConf, props)
                val end = NodePatternIngestionStrategy.toData(endConf, props)

                mapOf("start" to start, "end" to end, "properties" to filteredProperties)
              } else {
                null
              }
            }
    return if (data.isEmpty()) {
      emptyList()
    } else {
      listOf(QueryEvents(mergeRelationshipTemplate, data))
    }
  }

  private fun isRelationshipProperty(propertyName: String): Boolean {
    return (!pattern.start.keyProperties.map { it.from }.contains(propertyName) &&
        !pattern.start.includeProperties.map { it.from }.contains(propertyName) &&
        !pattern.end.keyProperties.map { it.from }.contains(propertyName) &&
        !pattern.end.includeProperties.map { it.from }.contains(propertyName))
  }

  override fun deleteRelationshipEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents> {
    val data =
        events
            .filter { it.value == null && it.key != null }
            .mapNotNull { if (it.key != null) JSONUtils.asMap(it.key) else null }
            .mapNotNull { props ->
              val properties = props.flatten()
              val startKeys = pattern.start.keyProperties.map { it.from }.toSet()
              val endKeys = pattern.end.keyProperties.map { it.from }.toSet()
              val containsKeys =
                  startKeys.all { properties.containsKey(it) } &&
                      endKeys.all { properties.containsKey(it) }
              if (containsKeys) {
                val startConf = pattern.start
                val endConf = pattern.end

                val start = NodePatternIngestionStrategy.toData(startConf, props)
                val end = NodePatternIngestionStrategy.toData(endConf, props)

                mapOf("start" to start, "end" to end)
              } else {
                null
              }
            }
    return if (data.isEmpty()) {
      emptyList()
    } else {
      listOf(QueryEvents(deleteRelationshipTemplate, data))
    }
  }
}
