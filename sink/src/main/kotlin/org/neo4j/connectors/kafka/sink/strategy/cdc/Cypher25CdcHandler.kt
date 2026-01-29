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

import java.util.TreeMap
import org.neo4j.cdc.client.model.ChangeEvent
import org.neo4j.cdc.client.model.EntityOperation
import org.neo4j.cdc.client.model.NodeEvent
import org.neo4j.cdc.client.model.RelationshipEvent
import org.neo4j.connectors.kafka.events.EntityType
import org.neo4j.connectors.kafka.sink.ChangeQuery
import org.neo4j.connectors.kafka.sink.SinkMessage
import org.neo4j.connectors.kafka.sink.SinkStrategyHandler
import org.neo4j.connectors.kafka.sink.strategy.toChangeEvent
import org.neo4j.cypherdsl.core.Cypher
import org.neo4j.cypherdsl.core.Functions
import org.neo4j.cypherdsl.core.internal.SchemaNames
import org.neo4j.cypherdsl.core.renderer.Renderer
import org.neo4j.driver.Query
import org.slf4j.Logger
import org.slf4j.LoggerFactory

fun EntityOperation.updateStrategy(entityType: EntityType): String =
    when (this) {
      EntityOperation.CREATE -> if (entityType == EntityType.node) "merge" else "create"
      EntityOperation.UPDATE -> "merge"
      EntityOperation.DELETE -> "delete"
    }

data class GroupingKey(
    val entityType: EntityType,
    val operationType: String,
    val propertyKeys: List<Set<String>>,
) : Comparable<GroupingKey> {

  constructor(
      entityType: EntityType,
      operation: EntityOperation,
      vararg propertyKeys: Set<String>,
  ) : this(
      entityType = entityType,
      operationType = operation.updateStrategy(entityType),
      propertyKeys = propertyKeys.toList(),
  )

  override fun compareTo(other: GroupingKey): Int {
    if (this.entityType != other.entityType) {
      return if (this.entityType == EntityType.node) -1 else 1
    }

    if (this.operationType != other.operationType) {
      return this.operationType.compareTo(other.operationType)
    }

    for (i in this.propertyKeys.indices) {
      if (i >= other.propertyKeys.size) {
        return 1
      }
      val cmp = this.propertyKeys[i].size.compareTo(other.propertyKeys[i].size)
      if (cmp != 0) {
        return cmp
      }
    }

    return 0
  }
}

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
        "MERGE (start:\$(e.start.matchLabels) {$startMatchProps}) MERGE (end:\$(e.end.matchLabels) {$endMatchProps}) MERGE (start)-[r:\$(e.matchType) {$matchProps}]->(end) SET r = e.setProperties"
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

abstract class Cypher25CdcHandler(
    private val maxBatchedStatements: Int,
    private val batchSize: Int,
    private val renderer: Renderer,
) : SinkStrategyHandler {
  private val logger: Logger = LoggerFactory.getLogger(javaClass)

  data class MessageToEvent(
      val message: SinkMessage,
      val changeEvent: ChangeEvent,
      val cdcData: CdcData,
  )

  override fun handle(messages: Iterable<SinkMessage>): Iterable<Iterable<ChangeQuery>> {
    val changeEvents =
        messages
            .onEach { logger.trace("received message: {}", it) }
            .map {
              MessageToEvent(
                  it,
                  it.toChangeEvent(),
                  when (val event = it.toChangeEvent().event) {
                    is NodeEvent ->
                        when (event.operation) {
                          EntityOperation.CREATE -> transformCreate(event)
                          EntityOperation.UPDATE -> transformUpdate(event)
                          EntityOperation.DELETE -> transformDelete(event)
                          else ->
                              throw IllegalArgumentException("unknown operation ${event.operation}")
                        }

                    is RelationshipEvent ->
                        when (event.operation) {
                          EntityOperation.CREATE -> transformCreate(event)
                          EntityOperation.UPDATE -> transformUpdate(event)
                          EntityOperation.DELETE -> transformDelete(event)
                          else ->
                              throw IllegalArgumentException("unknown operation ${event.operation}")
                        }

                    else ->
                        throw IllegalArgumentException("unsupported event type ${event.eventType}")
                  },
              )
            }
    return listOf(splitEventsIntoBatches(changeEvents, maxBatchedStatements)).onEach {
      logger.trace("messages: {} ", it)
    }
  }

  private fun splitEventsIntoBatches(
      events: List<MessageToEvent>,
      maxBatchedStatements: Int,
  ): List<ChangeQuery> {
    val result = mutableListOf<ChangeQuery>()

    val queries = TreeMap<GroupingKey, String>()
    val paramsList = mutableListOf<Map<String, Any>>()

    var lastIndex = 0
    events.forEachIndexed { index, event ->
      val key = event.cdcData.groupingBasedOn()
      queries.computeIfAbsent(key) { event.cdcData.buildStatement() }
      paramsList.add(event.cdcData.toParams(queries.keys.indexOf(key)))
      if (queries.size >= maxBatchedStatements || paramsList.size >= batchSize) {
        result.add(
            ChangeQuery(
                null,
                null,
                events.subList(lastIndex, index + 1).map { it.message },
                batchedStatement(queries, paramsList),
            )
        )

        // reset for next batch
        lastIndex = index + 1
        queries.clear()
        paramsList.clear()
      }
    }

    // handle final batch, if any
    if (queries.isNotEmpty() && paramsList.isNotEmpty()) {
      result.add(
          ChangeQuery(
              null,
              null,
              events.subList(lastIndex, events.size).map { it.message },
              batchedStatement(queries, paramsList),
          )
      )
    }

    return result
  }

  private fun batchedStatement(
      queries: Map<GroupingKey, String>,
      events: List<Map<String, Any>>,
  ): Query {
    val event = Cypher.name("e")

    var unwind = Cypher.unwind(Cypher.parameter("events", events)).`as`(event)
    queries.entries.forEachIndexed { index, (_, query) ->
      unwind =
          unwind.call(
              Cypher.with(event)
                  .where(event.property("q").eq(Cypher.literalOf<Int>(index)))
                  .callRawCypher("WITH e $query")
                  .returning(Functions.count(Cypher.asterisk()).`as`(Cypher.name("c$index")))
                  .build(),
              event,
          )
    }

    val stmt = unwind.returning(Cypher.literalNull()).build()
    return Query(renderer.render(stmt), stmt.parameters)
  }

  protected abstract fun transformCreate(event: NodeEvent): CdcData

  protected abstract fun transformUpdate(event: NodeEvent): CdcData

  protected abstract fun transformDelete(event: NodeEvent): CdcData

  protected abstract fun transformCreate(event: RelationshipEvent): CdcData

  protected abstract fun transformUpdate(event: RelationshipEvent): CdcData

  protected abstract fun transformDelete(event: RelationshipEvent): CdcData
}
