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
package org.neo4j.connectors.kafka.sink.strategy.pattern

import org.neo4j.connectors.kafka.data.ConstraintData
import org.neo4j.connectors.kafka.sink.SinkConfiguration
import org.neo4j.connectors.kafka.sink.SinkMessage
import org.neo4j.connectors.kafka.sink.strategy.DeleteRelationshipSinkAction
import org.neo4j.connectors.kafka.sink.strategy.LookupMode
import org.neo4j.connectors.kafka.sink.strategy.MergeRelationshipSinkAction
import org.neo4j.connectors.kafka.sink.strategy.NodeMatcher
import org.neo4j.connectors.kafka.sink.strategy.RelationshipMatcher
import org.neo4j.connectors.kafka.sink.strategy.SinkAction
import org.neo4j.connectors.kafka.sink.strategy.SinkActionNodeReference
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class RelationshipPatternEventTransformer(
    val topic: String,
    pattern: RelationshipPattern,
    private val mergeNodeProperties: Boolean,
    private val mergeRelationshipProperties: Boolean,
    bindTimestampAs: String = SinkConfiguration.DEFAULT_BIND_TIMESTAMP_ALIAS,
    bindHeaderAs: String = SinkConfiguration.DEFAULT_BIND_HEADER_ALIAS,
    bindKeyAs: String = SinkConfiguration.DEFAULT_BIND_KEY_ALIAS,
    bindValueAs: String = SinkConfiguration.DEFAULT_BIND_VALUE_ALIAS,
) :
    PatternEventTransformer<RelationshipPattern>(
        pattern,
        bindTimestampAs = bindTimestampAs,
        bindHeaderAs = bindHeaderAs,
        bindKeyAs = bindKeyAs,
        bindValueAs = bindValueAs,
    ) {
  private val logger: Logger = LoggerFactory.getLogger(javaClass)

  constructor(
      topic: String,
      pattern: String,
      mergeNodeProperties: Boolean,
      mergeRelationshipProperties: Boolean,
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
      bindTimestampAs = bindTimestampAs,
      bindHeaderAs = bindHeaderAs,
      bindKeyAs = bindKeyAs,
      bindValueAs = bindValueAs,
  )

  override fun transform(message: SinkMessage): SinkAction {
    val isTombstoneMessage = message.value == null
    val flattened = flattenMessage(message)

    val used = mutableSetOf<String>()
    val startKeys =
        extractKeys(pattern.start, flattened, isTombstoneMessage, used, bindValueAs, bindKeyAs)
    val endKeys =
        extractKeys(pattern.end, flattened, isTombstoneMessage, used, bindValueAs, bindKeyAs)
    val keys = extractKeys(pattern, flattened, isTombstoneMessage, used, bindValueAs, bindKeyAs)

    return if (isTombstoneMessage) {
      DeleteRelationshipSinkAction(
          SinkActionNodeReference(
              NodeMatcher.ByLabelsAndProperties(pattern.start.labels, startKeys),
              LookupMode.MATCH,
          ),
          SinkActionNodeReference(
              NodeMatcher.ByLabelsAndProperties(pattern.end.labels, endKeys),
              LookupMode.MATCH,
          ),
          RelationshipMatcher.ByTypeAndProperties(
              pattern.type,
              keys,
              pattern.keyProperties.isNotEmpty(),
          ),
      )
    } else {
      val startProps = computeProperties(pattern.start, flattened, used)
      val startNode =
          if (mergeNodeProperties) {
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(pattern.start.labels, startKeys),
                LookupMode.MERGE,
                null,
                computeProperties(pattern.start, flattened, used),
            )
          } else {
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(pattern.start.labels, startKeys),
                LookupMode.MERGE,
                computeProperties(pattern.start, flattened, used),
                startKeys,
            )
          }

      val endNode =
          if (mergeNodeProperties) {
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(pattern.end.labels, endKeys),
                LookupMode.MERGE,
                null,
                computeProperties(pattern.end, flattened, used),
            )
          } else {
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(pattern.end.labels, endKeys),
                LookupMode.MERGE,
                computeProperties(pattern.end, flattened, used),
                endKeys,
            )
          }

      if (mergeRelationshipProperties) {
        MergeRelationshipSinkAction(
            startNode,
            endNode,
            RelationshipMatcher.ByTypeAndProperties(
                pattern.type,
                keys,
                pattern.keyProperties.isNotEmpty(),
            ),
            mutateProperties = computeProperties(pattern, flattened, used),
        )
      } else {
        MergeRelationshipSinkAction(
            startNode,
            endNode,
            RelationshipMatcher.ByTypeAndProperties(
                pattern.type,
                keys,
                pattern.keyProperties.isNotEmpty(),
            ),
            setProperties = computeProperties(pattern, flattened, used),
            mutateProperties = keys,
        )
      }
    }
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
}
