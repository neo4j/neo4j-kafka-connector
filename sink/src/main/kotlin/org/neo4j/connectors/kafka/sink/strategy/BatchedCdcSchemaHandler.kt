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

import org.neo4j.cdc.client.model.EntityOperation
import org.neo4j.cdc.client.model.NodeEvent
import org.neo4j.cdc.client.model.RelationshipEvent
import org.neo4j.connectors.kafka.exceptions.InvalidDataException
import org.neo4j.connectors.kafka.sink.ChangeQuery
import org.neo4j.connectors.kafka.sink.SinkMessage
import org.neo4j.connectors.kafka.sink.SinkStrategy
import org.neo4j.connectors.kafka.sink.SinkStrategyHandler
import org.neo4j.driver.Query
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Batched CDC Schema handler that processes multiple events in a single UNWIND query. Uses
 * conditional CALL subqueries to route events to appropriate operations (CREATE, UPDATE, DELETE)
 * for both nodes and relationships.
 *
 * Unlike BatchedCdcSourceIdHandler, this handler works with schema-based keys where nodes are
 * identified by their constraint key properties rather than a fixed label/property.
 *
 * Requires Neo4j 5.26+ for dynamic label support via `SET n:$(label)` syntax.
 */
class BatchedCdcSchemaHandler(val topic: String, private val batchSize: Int = 1000) :
    SinkStrategyHandler {

  private val logger: Logger = LoggerFactory.getLogger(javaClass)

  override fun strategy() = SinkStrategy.CDC_SCHEMA

  override fun handle(messages: Iterable<SinkMessage>): Iterable<Iterable<ChangeQuery>> {
    val allMessages = messages.toList()
    if (allMessages.isEmpty()) {
      return emptyList()
    }

    // TODO: Remove this temporary logging after debugging
    logger.info("BATCH DEBUG: handle() received {} messages", allMessages.size)
    logger.trace("Processing {} messages in batched CDC Schema mode", allMessages.size)

    // Convert messages to events
    val messageToEvents =
        allMessages.map { msg ->
          val changeEvent = msg.toChangeEvent()
          CdcHandler.MessageToEvent(msg, changeEvent)
        }

    // Batch events according to batch size (spanning across txId groups)
    return messageToEvents.chunked(batchSize).map { batch -> listOf(createBatchedQuery(batch)) }
  }

  private fun createBatchedQuery(batch: List<CdcHandler.MessageToEvent>): ChangeQuery {
    val serializedEvents = batch.map { serializeEvent(it) }
    val query = "CYPHER 25 " + buildBatchedQuery()

    // TODO: Remove this temporary logging after debugging
    logger.info(
        "BATCH DEBUG: batch.size={}, serializedEvents.size={}",
        batch.size,
        serializedEvents.size,
    )
    logger.trace("Created batched CDC Schema query for {} events", batch.size)

    return ChangeQuery(
        txId = null, // Batches span across txId groups
        seq = null,
        messages = batch.map { it.message },
        query = Query(query, mapOf("events" to serializedEvents)),
    )
  }

  /**
   * Serialize a CDC event to a Map for use as a parameter in the batched query. For CDC Schema, we
   * need to capture the key structure (labels and key properties) that identify each node or
   * relationship.
   */
  private fun serializeEvent(messageToEvent: CdcHandler.MessageToEvent): Map<String, Any?> {
    val event = messageToEvent.changeEvent.event

    return when (event) {
      is NodeEvent -> serializeNodeEvent(event)
      is RelationshipEvent -> serializeRelationshipEvent(event)
      else -> throw IllegalArgumentException("unsupported event type ${event.eventType}")
    }
  }

  private fun serializeNodeEvent(event: NodeEvent): Map<String, Any?> {
    val operation = getOperation(event.operation)
    val (keyLabels, keyProperties) = extractNodeKeys(event.keys)

    return mapOf(
        "entityType" to "NODE",
        "operation" to operation,
        "keyLabels" to keyLabels,
        "keyProperties" to keyProperties,
        "properties" to getNodeProperties(event),
        "additionalLabels" to getAdditionalLabels(event, keyLabels),
        "addedLabels" to getAddedLabels(event),
        "removedLabels" to getRemovedLabels(event),
    )
  }

  private fun serializeRelationshipEvent(event: RelationshipEvent): Map<String, Any?> {
    val operation = getOperation(event.operation)
    val (startKeyLabels, startKeyProperties) = extractNodeKeys(event.start.keys)
    val (endKeyLabels, endKeyProperties) = extractNodeKeys(event.end.keys)
    val relKeyProperties = extractRelationshipKeys(event.keys)
    val hasRelKeys = relKeyProperties.isNotEmpty()

    return mapOf(
        "entityType" to "RELATIONSHIP",
        "operation" to operation,
        "type" to event.type,
        "startKeyLabels" to startKeyLabels,
        "startKeyProperties" to startKeyProperties,
        "endKeyLabels" to endKeyLabels,
        "endKeyProperties" to endKeyProperties,
        "relKeyProperties" to relKeyProperties,
        "hasRelKeys" to hasRelKeys,
        "properties" to getRelProperties(event),
    )
  }

  /**
   * Extract key labels and key properties from the node keys structure. The keys are structured as:
   * Map<LabelName, List<Map<PropertyName, PropertyValue>>>
   */
  private fun extractNodeKeys(
      keys: Map<String, List<Map<String, Any>>>
  ): Pair<List<String>, Map<String, Any>> {
    val validKeys =
        keys
            .mapValues { kvp -> kvp.value.filter { it.isNotEmpty() } }
            .filterValues { it.isNotEmpty() }

    if (validKeys.isEmpty()) {
      throw InvalidDataException(
          "schema strategy requires at least one node key with valid properties"
      )
    }

    val keyLabels = validKeys.keys.toList()
    val keyProperties =
        validKeys.flatMap { it.value }.flatMap { it.entries }.associate { it.key to it.value }

    return Pair(keyLabels, keyProperties)
  }

  /** Extract key properties from relationship keys (list of property maps). */
  private fun extractRelationshipKeys(keys: List<Map<String, Any>>): Map<String, Any> {
    return keys.flatMap { it.entries }.associate { it.key to it.value }
  }

  private fun getOperation(operation: EntityOperation): String =
      when (operation) {
        EntityOperation.CREATE -> "CREATE"
        EntityOperation.UPDATE -> "UPDATE"
        EntityOperation.DELETE -> "DELETE"
      }

  private fun getNodeProperties(event: NodeEvent): Map<String, Any?> =
      when (event.operation) {
        EntityOperation.CREATE -> event.after?.properties ?: emptyMap()
        EntityOperation.UPDATE -> event.mutatedProperties()
        EntityOperation.DELETE -> emptyMap()
      }

  /** Get additional labels (labels from after state minus key labels) for CREATE operations. */
  private fun getAdditionalLabels(event: NodeEvent, keyLabels: List<String>): List<String> =
      when (event.operation) {
        EntityOperation.CREATE -> (event.after?.labels ?: emptyList()).minus(keyLabels.toSet())
        EntityOperation.UPDATE,
        EntityOperation.DELETE -> emptyList()
      }

  private fun getAddedLabels(event: NodeEvent): List<String> =
      when (event.operation) {
        EntityOperation.UPDATE -> event.addedLabels().toList()
        EntityOperation.CREATE,
        EntityOperation.DELETE -> emptyList()
      }

  private fun getRemovedLabels(event: NodeEvent): List<String> =
      when (event.operation) {
        EntityOperation.UPDATE -> event.removedLabels().toList()
        EntityOperation.CREATE,
        EntityOperation.DELETE -> emptyList()
      }

  private fun getRelProperties(event: RelationshipEvent): Map<String, Any?> =
      when (event.operation) {
        EntityOperation.CREATE -> event.after?.properties ?: emptyMap()
        EntityOperation.UPDATE -> event.mutatedProperties()
        EntityOperation.DELETE -> emptyMap()
      }

  /**
   * Build the batched UNWIND query that handles all event types and operations. Uses conditional
   * CALL subqueries with WHERE clauses to route events to the appropriate operation.
   *
   * Uses APOC procedures for MERGE operations since Cypher doesn't support parameterized property
   * keys in MERGE patterns. For DELETE operations, uses MATCH with dynamic labels and WHERE clause
   * filtering for dynamic property keys.
   *
   * Requires:
   * - Neo4j 5.26+ for dynamic label syntax (SET n:$(label))
   * - APOC library for apoc.merge.node and apoc.merge.relationship
   */
  private fun buildBatchedQuery(): String {
    return """
      |UNWIND ${'$'}events AS event
      |// Node CREATE
      |CALL (event) {
      |  WITH event
      |  WHERE event.entityType = 'NODE' AND event.operation = 'CREATE'
      |  CALL apoc.create.node(event.keyLabels + event.additionalLabels, apoc.map.merge(event.keyProperties, event.properties)) YIELD node
      |  FINISH
      |}
      |// Node UPDATE
      |CALL (event) {
      |  WITH event
      |  WHERE event.entityType = 'NODE' AND event.operation = 'UPDATE'
      |  CALL apoc.merge.node(event.keyLabels, event.keyProperties, {}, event.properties) YIELD node AS n
      |  WITH event, n
      |  UNWIND event.addedLabels AS label
      |  SET n:${'$'}(label)
      |  WITH DISTINCT event, n
      |  UNWIND event.removedLabels AS label
      |  REMOVE n:${'$'}(label)
      |  FINISH
      |}
      |// Node DELETE
      |CALL (event) {
      |  WITH event
      |  WHERE event.entityType = 'NODE' AND event.operation = 'DELETE'
      |  WITH event, apoc.map.fromPairs([lbl IN event.keyLabels | [lbl, keys(event.keyProperties)]]) AS labelPropMap,
      |       head(keys(event.keyProperties)) AS firstKey
      |  CALL apoc.search.nodeAllReduced(labelPropMap, '=', event.keyProperties[firstKey]) YIELD id AS nodeId
      |  MATCH (node) WHERE id(node) = nodeId
      |  AND all(key IN keys(event.keyProperties) WHERE node[key] = event.keyProperties[key])
      |  DELETE node
      |  FINISH
      |}
      |// Relationship CREATE
      |CALL (event) {
      |  WITH event
      |  WHERE event.entityType = 'RELATIONSHIP' AND event.operation = 'CREATE'
      |  WITH event,
      |       apoc.map.fromPairs([lbl IN event.startKeyLabels | [lbl, keys(event.startKeyProperties)]]) AS startLabelPropMap,
      |       head(keys(event.startKeyProperties)) AS startFirstKey,
      |       apoc.map.fromPairs([lbl IN event.endKeyLabels | [lbl, keys(event.endKeyProperties)]]) AS endLabelPropMap,
      |       head(keys(event.endKeyProperties)) AS endFirstKey
      |  CALL apoc.search.nodeAllReduced(startLabelPropMap, '=', event.startKeyProperties[startFirstKey]) YIELD id AS startNodeId
      |  MATCH (startNode) WHERE id(startNode) = startNodeId
      |  AND all(key IN keys(event.startKeyProperties) WHERE startNode[key] = event.startKeyProperties[key])
      |  CALL apoc.search.nodeAllReduced(endLabelPropMap, '=', event.endKeyProperties[endFirstKey]) YIELD id AS endNodeId
      |  MATCH (endNode) WHERE id(endNode) = endNodeId
      |  AND all(key IN keys(event.endKeyProperties) WHERE endNode[key] = event.endKeyProperties[key])
      |  CALL apoc.create.relationship(startNode, event.type, apoc.map.merge(event.relKeyProperties, event.properties), endNode) YIELD rel
      |  FINISH
      |}
      |// Relationship UPDATE with relationship keys
      |CALL (event) {
      |  WITH event
      |  WHERE event.entityType = 'RELATIONSHIP' AND event.operation = 'UPDATE' AND event.hasRelKeys = true
      |  WITH event,
      |       apoc.map.fromPairs([lbl IN event.startKeyLabels | [lbl, keys(event.startKeyProperties)]]) AS startLabelPropMap,
      |       head(keys(event.startKeyProperties)) AS startFirstKey,
      |       apoc.map.fromPairs([lbl IN event.endKeyLabels | [lbl, keys(event.endKeyProperties)]]) AS endLabelPropMap,
      |       head(keys(event.endKeyProperties)) AS endFirstKey
      |  CALL apoc.search.nodeAllReduced(startLabelPropMap, '=', event.startKeyProperties[startFirstKey]) YIELD id AS startNodeId
      |  MATCH (startNode) WHERE id(startNode) = startNodeId
      |  AND all(key IN keys(event.startKeyProperties) WHERE startNode[key] = event.startKeyProperties[key])
      |  CALL apoc.search.nodeAllReduced(endLabelPropMap, '=', event.endKeyProperties[endFirstKey]) YIELD id AS endNodeId
      |  MATCH (endNode) WHERE id(endNode) = endNodeId
      |  AND all(key IN keys(event.endKeyProperties) WHERE endNode[key] = event.endKeyProperties[key])
      |  MATCH (startNode)-[r:$(event.type)]->(endNode)
      |  WHERE all(key IN keys(event.relKeyProperties) WHERE r[key] = event.relKeyProperties[key])
      |  SET r += event.properties
      |  FINISH
      |}
      |// Relationship UPDATE without relationship keys
      |CALL (event) {
      |  WITH event
      |  WHERE event.entityType = 'RELATIONSHIP' AND event.operation = 'UPDATE' AND event.hasRelKeys = false
      |  WITH event,
      |       apoc.map.fromPairs([lbl IN event.startKeyLabels | [lbl, keys(event.startKeyProperties)]]) AS startLabelPropMap,
      |       head(keys(event.startKeyProperties)) AS startFirstKey,
      |       apoc.map.fromPairs([lbl IN event.endKeyLabels | [lbl, keys(event.endKeyProperties)]]) AS endLabelPropMap,
      |       head(keys(event.endKeyProperties)) AS endFirstKey
      |  CALL apoc.search.nodeAllReduced(startLabelPropMap, '=', event.startKeyProperties[startFirstKey]) YIELD id AS startNodeId
      |  MATCH (startNode) WHERE id(startNode) = startNodeId
      |  AND all(key IN keys(event.startKeyProperties) WHERE startNode[key] = event.startKeyProperties[key])
      |  CALL apoc.search.nodeAllReduced(endLabelPropMap, '=', event.endKeyProperties[endFirstKey]) YIELD id AS endNodeId
      |  MATCH (endNode) WHERE id(endNode) = endNodeId
      |  AND all(key IN keys(event.endKeyProperties) WHERE endNode[key] = event.endKeyProperties[key])
      |  MATCH (startNode)-[r:$(event.type)]->(endNode)
      |  SET r += event.properties
      |  FINISH
      |}
      |// Relationship DELETE with relationship keys
      |CALL (event) {
      |  WITH event
      |  WHERE event.entityType = 'RELATIONSHIP' AND event.operation = 'DELETE' AND event.hasRelKeys = true
      |  WITH event,
      |       apoc.map.fromPairs([lbl IN event.startKeyLabels | [lbl, keys(event.startKeyProperties)]]) AS startLabelPropMap,
      |       head(keys(event.startKeyProperties)) AS startFirstKey,
      |       apoc.map.fromPairs([lbl IN event.endKeyLabels | [lbl, keys(event.endKeyProperties)]]) AS endLabelPropMap,
      |       head(keys(event.endKeyProperties)) AS endFirstKey
      |  CALL apoc.search.nodeAllReduced(startLabelPropMap, '=', event.startKeyProperties[startFirstKey]) YIELD id AS startNodeId
      |  MATCH (startNode) WHERE id(startNode) = startNodeId
      |  AND all(key IN keys(event.startKeyProperties) WHERE startNode[key] = event.startKeyProperties[key])
      |  CALL apoc.search.nodeAllReduced(endLabelPropMap, '=', event.endKeyProperties[endFirstKey]) YIELD id AS endNodeId
      |  MATCH (endNode) WHERE id(endNode) = endNodeId
      |  AND all(key IN keys(event.endKeyProperties) WHERE endNode[key] = event.endKeyProperties[key])
      |  MATCH (startNode)-[r:$(event.type)]->(endNode)
      |  WHERE all(key IN keys(event.relKeyProperties) WHERE r[key] = event.relKeyProperties[key])
      |  DELETE r
      |  FINISH
      |}
      |// Relationship DELETE without relationship keys
      |CALL (event) {
      |  WITH event
      |  WHERE event.entityType = 'RELATIONSHIP' AND event.operation = 'DELETE' AND event.hasRelKeys = false
      |  WITH event,
      |       apoc.map.fromPairs([lbl IN event.startKeyLabels | [lbl, keys(event.startKeyProperties)]]) AS startLabelPropMap,
      |       head(keys(event.startKeyProperties)) AS startFirstKey,
      |       apoc.map.fromPairs([lbl IN event.endKeyLabels | [lbl, keys(event.endKeyProperties)]]) AS endLabelPropMap,
      |       head(keys(event.endKeyProperties)) AS endFirstKey
      |  CALL apoc.search.nodeAllReduced(startLabelPropMap, '=', event.startKeyProperties[startFirstKey]) YIELD id AS startNodeId
      |  MATCH (startNode) WHERE id(startNode) = startNodeId
      |  AND all(key IN keys(event.startKeyProperties) WHERE startNode[key] = event.startKeyProperties[key])
      |  CALL apoc.search.nodeAllReduced(endLabelPropMap, '=', event.endKeyProperties[endFirstKey]) YIELD id AS endNodeId
      |  MATCH (endNode) WHERE id(endNode) = endNodeId
      |  AND all(key IN keys(event.endKeyProperties) WHERE endNode[key] = event.endKeyProperties[key])
      |  MATCH (startNode)-[r:$(event.type)]->(endNode)
      |  DELETE r
      |  FINISH
      |}
    """
        .trimMargin()
  }
}
