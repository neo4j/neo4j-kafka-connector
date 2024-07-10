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
import org.neo4j.connectors.kafka.data.ConstraintEntityType
import org.neo4j.connectors.kafka.data.ConstraintType
import org.neo4j.connectors.kafka.data.NODE_KEY_CONSTRAINTS
import org.neo4j.connectors.kafka.data.RELATIONSHIP_KEY_CONSTRAINTS
import org.neo4j.connectors.kafka.utils.ListUtils.equalsIgnoreOrder

object PatternConstraintValidator {
  fun checkNodeWarnings(constraints: List<ConstraintData>, pattern: NodePattern): List<String> {
    val typeConstraintMap =
        constraints
            .filter {
              it.entityType == ConstraintEntityType.NODE.value &&
                  NODE_KEY_CONSTRAINTS.contains(it.constraintType) &&
                  pattern.labels.contains(it.labelOrType)
            }
            .groupBy { "${it.constraintType}-${it.labelOrType}" }

    val keys = pattern.keyProperties.map { it.to }

    val warningMessages = mutableListOf<String>()

    for (label in pattern.labels) {
      if (checkNodeKey(label, keys, typeConstraintMap)) {
        continue
      }

      if (checkNodeUniqueness(label, keys, typeConstraintMap) &&
          checkNodePropertyExistence(label, keys, typeConstraintMap)) {
        continue
      }

      warningMessages.add(
          "Label '$label' does not have the required constraints(KEY or UNIQUENESS and EXISTENCE) on ${keys.joinToString(", ")}")
    }

    return warningMessages
  }

  fun checkRelationshipWarnings(
      constraints: List<ConstraintData>,
      pattern: RelationshipPattern
  ): String? {
    val typeConstraintMap =
        constraints
            .filter {
              it.entityType == ConstraintEntityType.RELATIONSHIP.value &&
                  RELATIONSHIP_KEY_CONSTRAINTS.contains(it.constraintType) &&
                  pattern.type == (it.labelOrType)
            }
            .groupBy { "${it.constraintType}-${it.labelOrType}" }

    val keys = pattern.keyProperties.map { it.to }
    val type = pattern.type!!

    if (checkRelationshipKey(type, keys, typeConstraintMap)) {
      return null
    }

    if (checkRelationshipUniqueness(type, keys, typeConstraintMap) &&
        checkRelationshipPropertyExistence(type, keys, typeConstraintMap)) {
      return null
    }

    return "Relationship '${type}' does not have the required constraints(KEY or UNIQUENESS and EXISTENCE) on ${keys.joinToString(", ")}"
  }

  private fun checkNodeKey(
      label: String,
      keys: List<String>,
      typeConstraintMap: Map<String, List<ConstraintData>>
  ): Boolean {
    val nodeKeyConstraints =
        typeConstraintMap["${ConstraintType.NODE_KEY.value}-${label}"] ?: return false
    for (constraint in nodeKeyConstraints) {
      if (label == constraint.labelOrType && keys equalsIgnoreOrder constraint.properties) {
        return true
      }
    }
    return false
  }

  private fun checkNodeUniqueness(
      label: String,
      keys: List<String>,
      typeConstraintMap: Map<String, List<ConstraintData>>
  ): Boolean {
    val uniquenessConstraints =
        typeConstraintMap["${ConstraintType.NODE_UNIQUENESS.value}-${label}"] ?: return false
    for (constraint in uniquenessConstraints) {
      if (keys equalsIgnoreOrder constraint.properties) {
        return true
      }
    }
    return false
  }

  private fun checkNodePropertyExistence(
      label: String,
      keys: List<String>,
      typeConstraintMap: Map<String, List<ConstraintData>>
  ): Boolean {
    val existenceConstraints =
        typeConstraintMap["${ConstraintType.NODE_EXISTENCE.value}-${label}"] ?: return false
    val properties = existenceConstraints.flatMap { it.properties }
    return properties.containsAll(keys)
  }

  private fun checkRelationshipKey(
      type: String,
      keys: List<String>,
      typeConstraintMap: Map<String, List<ConstraintData>>
  ): Boolean {
    val relationshipKeyConstraints =
        typeConstraintMap["${ConstraintType.RELATIONSHIP_KEY.value}-${type}"] ?: return false
    for (constraint in relationshipKeyConstraints) {
      if (type == constraint.labelOrType && keys equalsIgnoreOrder constraint.properties) {
        return true
      }
    }
    return false
  }

  private fun checkRelationshipUniqueness(
      type: String,
      keys: List<String>,
      typeConstraintMap: Map<String, List<ConstraintData>>
  ): Boolean {
    val uniquenessConstraints =
        typeConstraintMap["${ConstraintType.RELATIONSHIP_UNIQUENESS.value}-${type}"] ?: return false
    for (constraint in uniquenessConstraints) {
      if (keys equalsIgnoreOrder constraint.properties) {
        return true
      }
    }
    return false
  }

  private fun checkRelationshipPropertyExistence(
      type: String,
      keys: List<String>,
      typeConstraintMap: Map<String, List<ConstraintData>>
  ): Boolean {
    val existenceConstraints =
        typeConstraintMap["${ConstraintType.RELATIONSHIP_EXISTENCE.value}-${type}"] ?: return false
    val properties = existenceConstraints.flatMap { it.properties }
    return properties.containsAll(keys)
  }
}
