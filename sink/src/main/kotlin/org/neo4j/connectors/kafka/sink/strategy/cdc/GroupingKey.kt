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

import org.neo4j.cdc.client.model.EntityOperation
import org.neo4j.connectors.kafka.events.EntityType

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

  override fun compareTo(other: GroupingKey): Int =
      compareEntityType(other).takeIf { it != 0 }
          ?: compareOperationType(other).takeIf { it != 0 }
          ?: comparePropertyKeys(other)

  private fun compareEntityType(other: GroupingKey): Int =
      if (entityType == other.entityType) 0 else if (entityType == EntityType.node) -1 else 1

  private fun compareOperationType(other: GroupingKey): Int =
      operationType.compareTo(other.operationType)

  private fun comparePropertyKeys(other: GroupingKey): Int {
    for (i in propertyKeys.indices) {
      if (i >= other.propertyKeys.size) return 1
      val cmp = compareKeySets(propertyKeys[i], other.propertyKeys[i])
      if (cmp != 0) return cmp
    }
    return 0
  }

  private fun compareKeySets(a: Set<String>, b: Set<String>): Int {
    val sizeCmp = a.size.compareTo(b.size)
    if (sizeCmp != 0) return sizeCmp
    val aSorted = a.sorted()
    val bSorted = b.sorted()
    for (i in aSorted.indices) {
      val cmp = aSorted[i].compareTo(bSorted[i])
      if (cmp != 0) return cmp
    }
    return 0
  }
}
