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
package org.neo4j.connectors.kafka.data.converter

import java.util.function.Function
import kotlin.collections.get
import org.neo4j.driver.Value
import org.neo4j.driver.types.Node
import org.neo4j.driver.types.Relationship

abstract class Entity(val props: Map<String, Value>) {
  fun keys(): Iterable<String> = props.keys

  fun containsKey(key: String?): Boolean = props.containsKey(key)

  fun get(key: String?): Value? = props[key]

  fun size(): Int = props.size

  fun values(): Iterable<Value> = props.values

  fun <T> values(mapFunction: Function<Value, T>): Iterable<T> =
      props.values.map { mapFunction.apply(it) }

  fun asMap(): Map<String, Any> = props

  fun <T> asMap(mapFunction: Function<Value, T>): Map<String, T> =
      props.mapValues { mapFunction.apply(it.value) }
}

class TestNode(val elementId: String, val labels: List<String>, props: Map<String, Value>) :
    Entity(props), Node {

  @Deprecated("Use elementId() instead")
  override fun id(): Long {
    throw UnsupportedOperationException("Numeric id() is deprecated. Use elementId().")
  }

  override fun elementId(): String = elementId

  override fun labels(): Iterable<String> = labels

  override fun hasLabel(label: String?): Boolean = label in labels
}

class TestRelationship(
    val elementId: String,
    val startId: String,
    val endId: String,
    val type: String,
    props: Map<String, Value>,
) : Entity(props), Relationship {

  @Deprecated("Use elementId() instead")
  override fun id(): Long {
    throw UnsupportedOperationException("Numeric id() is deprecated. Use elementId().")
  }

  override fun elementId(): String = elementId

  @Deprecated("Use startNodeElementId() instead")
  override fun startNodeId(): Long {
    throw UnsupportedOperationException(
        "Numeric startNodeId() is deprecated. Use startNodeElementId()."
    )
  }

  override fun startNodeElementId(): String = startId

  @Deprecated("Use endNodeElementId() instead")
  override fun endNodeId(): Long {
    throw UnsupportedOperationException(
        "Numeric endNodeId() is deprecated. Use endNodeElementId()."
    )
  }

  override fun endNodeElementId(): String = endId

  override fun type(): String = type

  override fun hasType(relationshipType: String?): Boolean = type == relationshipType
}
