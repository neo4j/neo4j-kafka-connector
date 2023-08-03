/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cdc.client

@SelectorMarker interface Selector

@DslMarker annotation class SelectorMarker

enum class Change {
  CREATE,
  UPDATE,
  DELETE
}

@SelectorMarker
open class EntitySelector : Selector {
  var change: Change? = null
  var changesTo: Set<String>? = null
  var includeProperties: Set<String>? = null
  var excludeProperties: Set<String>? = null
}

@SelectorMarker
class NodeSelector : EntitySelector() {
  var labels: Set<String>? = null
  var key: Map<String, Any>? = null
}

@SelectorMarker
class RelationshipNodeSelector : Selector {
  var labels: Set<String>? = null
  var key: Map<String, Any>? = null
}

@SelectorMarker
class RelationshipSelector : EntitySelector() {
  var type: String? = null
  var start: RelationshipNodeSelector? = null
  var end: RelationshipNodeSelector? = null
  var key: Map<String, Any>? = null

  fun start(init: RelationshipNodeSelector.() -> Unit): RelationshipNodeSelector {
    val node = RelationshipNodeSelector()
    node.init()
    start = node
    return node
  }

  fun end(init: RelationshipNodeSelector.() -> Unit): RelationshipNodeSelector {
    val node = RelationshipNodeSelector()
    node.init()
    end = node
    return node
  }
}

fun entity(init: EntitySelector.() -> Unit): EntitySelector {
  val entity = EntitySelector()
  entity.init()
  return entity
}

fun node(init: NodeSelector.() -> Unit): NodeSelector {
  val node = NodeSelector()
  node.init()
  return node
}

fun relationship(init: RelationshipSelector.() -> Unit): RelationshipSelector {
  val relationship = RelationshipSelector()
  relationship.init()
  return relationship
}
