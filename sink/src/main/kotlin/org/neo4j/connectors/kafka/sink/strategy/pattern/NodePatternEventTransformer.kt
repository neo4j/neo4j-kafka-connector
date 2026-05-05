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
import org.neo4j.connectors.kafka.sink.strategy.DeleteNodeSinkAction
import org.neo4j.connectors.kafka.sink.strategy.MergeNodeSinkAction
import org.neo4j.connectors.kafka.sink.strategy.NodeMatcher
import org.neo4j.connectors.kafka.sink.strategy.SinkAction
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class NodePatternEventTransformer(
    val topic: String,
    pattern: NodePattern,
    private val mergeProperties: Boolean,
    bindTimestampAs: String = SinkConfiguration.DEFAULT_BIND_TIMESTAMP_ALIAS,
    bindHeaderAs: String = SinkConfiguration.DEFAULT_BIND_HEADER_ALIAS,
    bindKeyAs: String = SinkConfiguration.DEFAULT_BIND_KEY_ALIAS,
    bindValueAs: String = SinkConfiguration.DEFAULT_BIND_VALUE_ALIAS,
) :
    PatternEventTransformer<NodePattern>(
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
      mergeProperties: Boolean,
      bindTimestampAs: String = SinkConfiguration.DEFAULT_BIND_TIMESTAMP_ALIAS,
      bindHeaderAs: String = SinkConfiguration.DEFAULT_BIND_HEADER_ALIAS,
      bindKeyAs: String = SinkConfiguration.DEFAULT_BIND_KEY_ALIAS,
      bindValueAs: String = SinkConfiguration.DEFAULT_BIND_VALUE_ALIAS,
  ) : this(
      topic = topic,
      pattern =
          when (val parsed = Pattern.parse(pattern)) {
            is NodePattern -> parsed
            else ->
                throw IllegalArgumentException(
                    "Invalid pattern provided for NodePatternHandler: ${parsed.javaClass.name}"
                )
          },
      mergeProperties = mergeProperties,
      bindTimestampAs = bindTimestampAs,
      bindHeaderAs = bindHeaderAs,
      bindKeyAs = bindKeyAs,
      bindValueAs = bindValueAs,
  )

  override fun transform(message: SinkMessage): SinkAction {
    val isTombstoneMessage = message.value == null
    val flattened = flattenMessage(message)

    val used = mutableSetOf<String>()
    val keys = extractKeys(pattern, flattened, isTombstoneMessage, used, bindValueAs, bindKeyAs)

    return if (isTombstoneMessage) {
      DeleteNodeSinkAction(NodeMatcher.ByLabelsAndProperties(pattern.labels, keys), true)
    } else {
      if (mergeProperties) {
        MergeNodeSinkAction(
            NodeMatcher.ByLabelsAndProperties(pattern.labels, keys),
            null,
            computeProperties(pattern, flattened, used),
            emptySet(),
            emptySet(),
        )
      } else {
        MergeNodeSinkAction(
            NodeMatcher.ByLabelsAndProperties(pattern.labels, keys),
            computeProperties(pattern, flattened, used),
            keys,
            emptySet(),
            emptySet(),
        )
      }
    }
  }

  override fun validate(constraints: List<ConstraintData>) {
    val warningMessages = checkConstraints(constraints)
    warningMessages.forEach { logger.warn(it) }
  }

  internal fun checkConstraints(constraints: List<ConstraintData>): List<String> {
    val nodeWarning =
        PatternConstraintValidator.checkNodeWarning(constraints, pattern, pattern.text)
            ?: return emptyList()
    return listOf(nodeWarning)
  }
}
