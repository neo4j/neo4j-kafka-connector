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

object PatternConstraintValidator {
  fun checkNodeWarnings(
      constraints: List<ConstraintData>,
      pattern: NodePattern,
      patternString: String
  ): String? {
    val typeConstraintMap =
        constraints
            .filter {
              it.entityType == ConstraintEntityType.NODE.value &&
                  NODE_KEY_CONSTRAINTS.contains(it.constraintType) &&
                  pattern.labels.contains(it.labelOrType)
            }
            .groupBy { "${it.constraintType}-${it.labelOrType}" }

    val keys = pattern.keyProperties.map { it.to }

    for (label in pattern.labels) {
      if (checkNodeKey(label, keys, typeConstraintMap)) {
        return null
      }

      if (checkNodeUniqueness(label, keys, typeConstraintMap) &&
          checkNodePropertyExistence(label, keys, typeConstraintMap)) {
        return null
      }
    }

    return buildNodeWarningMessage(pattern, patternString, typeConstraintMap, keys)
  }

  fun checkRelationshipWarnings(
      constraints: List<ConstraintData>,
      pattern: RelationshipPattern,
      patternString: String
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

    return buildRelationshipWarningMessage(pattern, patternString, typeConstraintMap, keys)
  }

  private fun checkNodeKey(
      label: String,
      keys: List<String>,
      typeConstraintMap: Map<String, List<ConstraintData>>
  ): Boolean {
    val nodeKeyConstraints =
        typeConstraintMap["${ConstraintType.NODE_KEY.value}-${label}"] ?: return false
    for (constraint in nodeKeyConstraints) {
      if (equalsIgnoreOrder(keys, constraint.properties)) {
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
      if (equalsIgnoreOrder(keys, constraint.properties)) {
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
      if (equalsIgnoreOrder(keys, constraint.properties)) {
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
      if (equalsIgnoreOrder(keys, constraint.properties)) {
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

  private fun buildNodeWarningMessage(
      pattern: NodePattern,
      patternString: String,
      typeConstraintMap: Map<String, List<ConstraintData>>,
      keys: List<String>
  ): String {
    val stringBuilder = StringBuilder()
    if (pattern.labels.size > 1) {
      stringBuilder.append(
          "None of the labels ${pattern.labels.joinToString(", ") { "'$it'" }} match the key(s) defined by the pattern $patternString.",
      )
      stringBuilder.append("\nPlease fix at least one of the following label constraints:")
    } else {
      stringBuilder.append(
          "Label '${pattern.labels.first()}' does not match the key(s) defined by the pattern $patternString.",
      )
      stringBuilder.append("\nPlease fix the label constraint:")
    }

    addLabelConstraintsText(pattern, typeConstraintMap, stringBuilder)
    stringBuilder.append("\nExpected constraints:")
    stringBuilder.append("\n\t- ${getConstraintWarningText(ConstraintType.NODE_KEY.value, keys)}")
    stringBuilder.append("\nor:")
    stringBuilder.append(
        "\n\t- ${getConstraintWarningText(ConstraintType.NODE_UNIQUENESS.value, keys)}")
    for (key in keys) {
      stringBuilder.append(
          "\n\t- ${getConstraintWarningText(ConstraintType.NODE_EXISTENCE.value, listOf(key))}")
    }
    return stringBuilder.toString()
  }

  private fun buildRelationshipWarningMessage(
      pattern: RelationshipPattern,
      patternString: String,
      typeConstraintMap: Map<String, List<ConstraintData>>,
      keys: List<String>
  ): String {
    val stringBuilder = StringBuilder()

    val type = pattern.type!!

    stringBuilder.append(
        "Relationship '$type' does not match the key(s) defined by the pattern $patternString.",
    )
    stringBuilder.append("\nPlease fix the relationship constraints:")

    val relationshipConstraints = getRelationshipConstraints(typeConstraintMap, type)

    addExistingConstraints(relationshipConstraints, type, stringBuilder)

    stringBuilder.append("\nExpected constraints:")
    stringBuilder.append(
        "\n\t- ${getConstraintWarningText(ConstraintType.RELATIONSHIP_KEY.value, keys)}")
    stringBuilder.append("\nor:")
    stringBuilder.append(
        "\n\t- ${getConstraintWarningText(ConstraintType.RELATIONSHIP_UNIQUENESS.value, keys)}")
    for (key in keys) {
      stringBuilder.append(
          "\n\t- ${getConstraintWarningText(ConstraintType.RELATIONSHIP_EXISTENCE.value, listOf(key))}")
    }
    return stringBuilder.toString()
  }

  private fun addLabelConstraintsText(
      pattern: NodePattern,
      typeConstraintMap: Map<String, List<ConstraintData>>,
      stringBuilder: StringBuilder
  ) {
    for (label in pattern.labels) {
      val labelConstraints = getLabelConstraints(typeConstraintMap, label)
      addExistingConstraints(labelConstraints, label, stringBuilder)
    }
  }

  private fun addExistingConstraints(
      constraints: MutableList<ConstraintData>,
      labelOrType: String,
      stringBuilder: StringBuilder
  ) {
    if (constraints.isNotEmpty()) {
      stringBuilder.append("\n\t'$labelOrType' has:")
      for (constraint in constraints) {
        stringBuilder.append(
            "\n\t\t- ${
              getConstraintWarningText(
                  constraint.constraintType,
                  constraint.properties,
              )
            }",
        )
      }
    } else {
      stringBuilder.append("\n\t'$labelOrType' has no key constraints")
    }
  }

  private fun getConstraintWarningText(constraintType: String, properties: List<String>) =
      "$constraintType (${properties.joinToString(", ")})"

  private fun getLabelConstraints(
      typeConstraintMap: Map<String, List<ConstraintData>>,
      label: String
  ): MutableList<ConstraintData> {
    val labelConstraints = mutableListOf<ConstraintData>()
    labelConstraints.addAll(
        typeConstraintMap["${ConstraintType.NODE_KEY.value}-${label}"] ?: emptyList(),
    )
    labelConstraints.addAll(
        typeConstraintMap["${ConstraintType.NODE_UNIQUENESS.value}-${label}"] ?: emptyList(),
    )
    labelConstraints.addAll(
        typeConstraintMap["${ConstraintType.NODE_EXISTENCE.value}-${label}"] ?: emptyList(),
    )
    return labelConstraints
  }

  private fun getRelationshipConstraints(
      typeConstraintMap: Map<String, List<ConstraintData>>,
      type: String
  ): MutableList<ConstraintData> {
    val relationshipConstraints = mutableListOf<ConstraintData>()
    relationshipConstraints.addAll(
        typeConstraintMap["${ConstraintType.RELATIONSHIP_KEY.value}-${type}"] ?: emptyList(),
    )
    relationshipConstraints.addAll(
        typeConstraintMap["${ConstraintType.RELATIONSHIP_UNIQUENESS.value}-${type}"] ?: emptyList(),
    )
    relationshipConstraints.addAll(
        typeConstraintMap["${ConstraintType.RELATIONSHIP_EXISTENCE.value}-${type}"] ?: emptyList(),
    )
    return relationshipConstraints
  }

  private fun equalsIgnoreOrder(first: List<String>, second: List<String>): Boolean {
    return first.size == second.size && first.toSet() == second.toSet()
  }
}
