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
package org.neo4j.connectors.kafka.data

import org.neo4j.driver.Driver
import org.neo4j.driver.SessionConfig

enum class ConstraintEntityType(val value: String) {
  NODE("NODE"),
  RELATIONSHIP("RELATIONSHIP"),
}

enum class ConstraintType(val value: String) {
  NODE_KEY("NODE_KEY"),
  NODE_UNIQUENESS("UNIQUENESS"),
  NODE_EXISTENCE("NODE_PROPERTY_EXISTENCE"),
  RELATIONSHIP_KEY("RELATIONSHIP_KEY"),
  RELATIONSHIP_UNIQUENESS("RELATIONSHIP_UNIQUENESS"),
  RELATIONSHIP_EXISTENCE("RELATIONSHIP_PROPERTY_EXISTENCE")
}

val NODE_CONSTRAINTS =
    listOf(
        ConstraintType.NODE_KEY.value,
        ConstraintType.NODE_UNIQUENESS.value,
        ConstraintType.NODE_EXISTENCE.value)

val RELATIONSHIP_CONSTRAINTS =
    listOf(
        ConstraintType.RELATIONSHIP_KEY.value,
        ConstraintType.RELATIONSHIP_UNIQUENESS.value,
        ConstraintType.RELATIONSHIP_EXISTENCE.value)

data class ConstraintData(
    val entityType: String,
    val constraintType: String,
    val labelOrType: String,
    val properties: List<String>
)

fun fetchConstraintData(driver: Driver, sessionConfig: SessionConfig): List<ConstraintData> {
  return try {
    driver.session(sessionConfig).use { session ->
      session
          .run("SHOW CONSTRAINTS YIELD entityType, type, labelsOrTypes, properties RETURN *")
          .list()
          .map {
            ConstraintData(
                entityType = it.get("entityType").asString(),
                constraintType = it.get("type").asString(),
                labelOrType = it.get("labelsOrTypes").asList()[0].toString(),
                properties = it.get("properties").asList().map { property -> property.toString() })
          }
    }
  } catch (e: Exception) {
    emptyList()
  }
}
