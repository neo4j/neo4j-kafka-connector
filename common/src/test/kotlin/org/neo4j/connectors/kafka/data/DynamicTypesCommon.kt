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

import java.util.function.Function
import org.neo4j.driver.Value
import org.neo4j.driver.types.Node
import org.neo4j.driver.types.Relationship

abstract class Entity(val props: Map<String, Value>) {
  fun keys(): Iterable<String> = props.keys

  fun containsKey(key: String?): Boolean = props.containsKey(key)

  fun get(key: String?): Value? = props[key]

  fun size(): Int = props.size

  fun values(): Iterable<Value> = props.values

  fun <T : Any?> values(mapFunction: Function<Value, T>): Iterable<T> =
      props.values.map { mapFunction.apply(it) }

  fun asMap(): Map<String, Any> = props

  fun <T : Any?> asMap(mapFunction: Function<Value, T>): Map<String, T> =
      props.mapValues { mapFunction.apply(it.value) }
}

class TestNode(val id: Long, val labels: List<String>, props: Map<String, Value>) :
    Entity(props), Node {

  override fun id(): Long = id

  override fun labels(): Iterable<String> = labels

  override fun hasLabel(label: String?): Boolean = label in labels
}

class TestRelationship(
    val id: Long,
    val startId: Long,
    val endId: Long,
    val type: String,
    props: Map<String, Value>,
) : Entity(props), Relationship {
  override fun id(): Long = id

  override fun startNodeId(): Long = startId

  override fun endNodeId(): Long = endId

  override fun type(): String = type

  override fun hasType(relationshipType: String?): Boolean = type == relationshipType
}
