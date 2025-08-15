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

import org.neo4j.connectors.kafka.data.ConstraintData
import org.neo4j.connectors.kafka.sink.ChangeQuery
import org.neo4j.connectors.kafka.sink.SinkConfiguration
import org.neo4j.connectors.kafka.sink.SinkMessage
import org.neo4j.connectors.kafka.sink.SinkStrategy
import org.neo4j.connectors.kafka.sink.strategy.pattern.Pattern
import org.neo4j.connectors.kafka.sink.strategy.pattern.PatternConstraintValidator
import org.neo4j.connectors.kafka.sink.strategy.pattern.RelationshipPattern
import org.neo4j.cypherdsl.core.Cypher
import org.neo4j.cypherdsl.core.Literal
import org.neo4j.cypherdsl.core.Node
import org.neo4j.cypherdsl.core.Relationship
import org.neo4j.cypherdsl.core.renderer.Renderer
import org.neo4j.driver.Query
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class RelationshipPatternHandler(
    val topic: String,
    pattern: RelationshipPattern,
    private val mergeNodeProperties: Boolean,
    private val mergeRelationshipProperties: Boolean,
    private val renderer: Renderer,
    private val batchSize: Int,
    bindTimestampAs: String = SinkConfiguration.DEFAULT_BIND_TIMESTAMP_ALIAS,
    bindHeaderAs: String = SinkConfiguration.DEFAULT_BIND_HEADER_ALIAS,
    bindKeyAs: String = SinkConfiguration.DEFAULT_BIND_KEY_ALIAS,
    bindValueAs: String = SinkConfiguration.DEFAULT_BIND_VALUE_ALIAS,
) :
    PatternHandler<RelationshipPattern>(
        pattern,
        bindTimestampAs = bindTimestampAs,
        bindHeaderAs = bindHeaderAs,
        bindKeyAs = bindKeyAs,
        bindValueAs = bindValueAs,
    ) {
  private val logger: Logger = LoggerFactory.getLogger(javaClass)
  internal val query: String

  constructor(
      topic: String,
      pattern: String,
      mergeNodeProperties: Boolean,
      mergeRelationshipProperties: Boolean,
      renderer: Renderer,
      batchSize: Int,
      bindTimestampAs: String = SinkConfiguration.DEFAULT_BIND_TIMESTAMP_ALIAS,
      bindHeaderAs: String = SinkConfiguration.DEFAULT_BIND_HEADER_ALIAS,
      bindKeyAs: String = SinkConfiguration.DEFAULT_BIND_KEY_ALIAS,
      bindValueAs: String = SinkConfiguration.DEFAULT_BIND_VALUE_ALIAS,
  ) : this(
      topic = topic,
      pattern =
          when (val parsed = Pattern.parse(pattern)) {
            is RelationshipPattern -> parsed
            else ->
                throw IllegalArgumentException(
                    "Invalid pattern provided for RelationshipPatternHandler: ${parsed.javaClass.name}"
                )
          },
      mergeNodeProperties = mergeNodeProperties,
      mergeRelationshipProperties = mergeRelationshipProperties,
      renderer = renderer,
      batchSize = batchSize,
      bindTimestampAs = bindTimestampAs,
      bindHeaderAs = bindHeaderAs,
      bindKeyAs = bindKeyAs,
      bindValueAs = bindValueAs,
  )

  init {
    query = buildStatement()

    logger.debug("using Cypher query '{}' for topic '{}'", query, topic)
  }

  override fun strategy() = SinkStrategy.RELATIONSHIP_PATTERN

  data class MessageToEventList(val message: SinkMessage, val eventList: List<Any>)

  override fun handle(messages: Iterable<SinkMessage>): Iterable<Iterable<ChangeQuery>> {
    return messages
        .asSequence()
        .onEach { logger.trace("received message: '{}'", it) }
        .map {
          val isTombstoneMessage = it.value == null
          val flattened = flattenMessage(it)

          val used = mutableSetOf<String>()
          val startKeys =
              extractKeys(
                  pattern.start,
                  flattened,
                  isTombstoneMessage,
                  used,
                  bindValueAs,
                  bindKeyAs,
              )
          val endKeys =
              extractKeys(pattern.end, flattened, isTombstoneMessage, used, bindValueAs, bindKeyAs)
          val keys =
              extractKeys(pattern, flattened, isTombstoneMessage, used, bindValueAs, bindKeyAs)
          val mapped =
              if (isTombstoneMessage) {
                listOf(
                    DELETE,
                    mapOf(
                        START to mapOf(KEYS to startKeys),
                        END to mapOf(KEYS to endKeys),
                        KEYS to keys,
                    ),
                )
              } else {
                val startProperties = computeProperties(pattern.start, flattened, used)
                val endProperties = computeProperties(pattern.end, flattened, used)
                listOf(
                    CREATE,
                    mapOf(
                        START to mapOf(KEYS to startKeys, PROPERTIES to startProperties),
                        END to mapOf(KEYS to endKeys, PROPERTIES to endProperties),
                        KEYS to keys,
                        PROPERTIES to computeProperties(pattern, flattened, used),
                    ),
                )
              }

          logger.trace("message '{}' mapped to: '{}'", it, mapped)

          MessageToEventList(it, mapped)
        }
        .chunked(batchSize)
        .map {
          listOf(
              ChangeQuery(
                  null,
                  null,
                  it.map { data -> data.message },
                  Query(query, mapOf(EVENTS to it.map { data -> data.eventList })),
              )
          )
        }
        .onEach { logger.trace("mapped messages: '{}'", it) }
        .toList()
  }

  override fun validate(constraints: List<ConstraintData>) {
    val warningMessages = checkConstraints(constraints)
    warningMessages.forEach { logger.warn(it) }
  }

  internal fun checkConstraints(constraints: List<ConstraintData>): List<String> {
    val warningMessages = mutableListOf<String>()

    val startNodeWarning =
        PatternConstraintValidator.checkNodeWarning(constraints, pattern.start, pattern.text)
    val relationshipWarning =
        PatternConstraintValidator.checkRelationshipWarning(constraints, pattern, pattern.text)
    val endNodeWarning =
        PatternConstraintValidator.checkNodeWarning(constraints, pattern.end, pattern.text)

    if (startNodeWarning != null) {
      warningMessages.add(startNodeWarning)
    }
    if (relationshipWarning != null) {
      warningMessages.add(relationshipWarning)
    }
    if (endNodeWarning != null) {
      warningMessages.add(endNodeWarning)
    }
    return warningMessages
  }

  private fun buildStatement(): String {
    val createOperation = Cypher.literalOf<String>(CREATE)
    val deleteOperation = Cypher.literalOf<String>(DELETE)

    val startNode =
        Cypher.node(pattern.start.labels.first(), pattern.start.labels.drop(1))
            .withProperties(
                pattern.start.keyProperties.associate {
                  it.to to NAME_EVENT.property(START, KEYS).property(it.to)
                }
            )
            .named(START)
    val endNode =
        Cypher.node(pattern.end.labels.first(), pattern.end.labels.drop(1))
            .withProperties(
                pattern.end.keyProperties.associate {
                  it.to to NAME_EVENT.property(END, KEYS).property(it.to)
                }
            )
            .named(END)
    val relationship =
        startNode
            .relationshipTo(endNode, pattern.type)
            .withProperties(
                pattern.keyProperties.associate {
                  it.to to NAME_EVENT.property(KEYS).property(it.to)
                }
            )
            .named("relationship")

    return renderer.render(
        Cypher.unwind(Cypher.parameter(EVENTS))
            .`as`(NAME_EVENT)
            .call(
                Cypher.call(buildCreateStatement(startNode, endNode, relationship, createOperation))
                    .call(buildDeleteStatement(relationship, deleteOperation))
                    .returning(NAME_CREATED, NAME_DELETED)
                    .build(),
                NAME_EVENT,
            )
            .returning(
                Cypher.raw("sum(${'$'}E)", NAME_CREATED).`as`(NAME_CREATED),
                Cypher.raw("sum(${'$'}E)", NAME_DELETED).`as`(NAME_DELETED),
            )
            .build()
    )
  }

  private fun buildDeleteStatement(relationship: Relationship, deleteOperation: Literal<String>) =
      Cypher.with(NAME_EVENT)
          .with(NAME_EVENT)
          .where(Cypher.valueAt(NAME_EVENT, 0).eq(deleteOperation))
          .with(Cypher.valueAt(NAME_EVENT, 1).`as`(NAME_EVENT))
          .match(relationship)
          .delete(relationship)
          .returning(
              Cypher.raw("count(${'$'}E)", relationship.requiredSymbolicName).`as`(NAME_DELETED)
          )
          .build()

  private fun buildCreateStatement(
      startNode: Node,
      endNode: Node,
      relationship: Relationship,
      createOperation: Literal<String>,
  ) =
      Cypher.with(NAME_EVENT)
          .with(NAME_EVENT)
          .where(Cypher.valueAt(NAME_EVENT, 0).eq(createOperation))
          .with(Cypher.valueAt(NAME_EVENT, 1).`as`(NAME_EVENT))
          .merge(startNode)
          .let {
            if (mergeNodeProperties) {
              it.mutate(startNode.asExpression(), Cypher.property(NAME_EVENT, START, PROPERTIES))
            } else {
              it.set(startNode.asExpression(), Cypher.property(NAME_EVENT, START, PROPERTIES))
                  .mutate(startNode.asExpression(), Cypher.property(NAME_EVENT, START, KEYS))
            }
          }
          .merge(endNode)
          .let {
            if (mergeNodeProperties) {
              it.mutate(endNode.asExpression(), Cypher.property(NAME_EVENT, END, PROPERTIES))
            } else {
              it.set(endNode.asExpression(), Cypher.property(NAME_EVENT, END, PROPERTIES))
                  .mutate(endNode.asExpression(), Cypher.property(NAME_EVENT, END, KEYS))
            }
          }
          .merge(relationship)
          .let {
            if (mergeRelationshipProperties) {
              it.mutate(relationship.asExpression(), Cypher.property(NAME_EVENT, PROPERTIES))
            } else {
              it.set(relationship.asExpression(), Cypher.property(NAME_EVENT, PROPERTIES))
                  .mutate(relationship.asExpression(), Cypher.property(NAME_EVENT, KEYS))
            }
          }
          .returning(
              Cypher.raw("count(${'$'}E)", relationship.requiredSymbolicName).`as`(NAME_CREATED)
          )
          .build()
}
