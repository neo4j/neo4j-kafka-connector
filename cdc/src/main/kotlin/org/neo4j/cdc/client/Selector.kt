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
@file:Suppress("unused")

package org.neo4j.cdc.client

@SelectorMarker
interface SelectorX {
  fun asMap(): Map<String, Any>
}

@DslMarker annotation class SelectorMarker

enum class Change(val shorthand: String) {
  CREATE("c"),
  UPDATE("u"),
  DELETE("d")
}

@SelectorMarker
open class EntitySelector : SelectorX {
  var change: Change? = null
  var changesTo: Set<String>? = null
  var includeProperties: Set<String>? = null
  var excludeProperties: Set<String>? = null

  override fun asMap(): Map<String, Any> {
    return buildMap {
      put("select", "e")
      if (change != null) put("operation", change!!.shorthand)
      if (changesTo != null) put("changesTo", changesTo!!.toList())
    }
  }
}

@SelectorMarker
class NodeSelector : EntitySelector() {
  var labels: Set<String>? = null
  var key: Map<String, Any>? = null

  override fun asMap(): Map<String, Any> {
    return buildMap {
      putAll(super.asMap())

      put("select", "n")
      if (labels != null) put("labels", labels!!.toList())
      if (key != null) put("key", key!!)
    }
  }
}

@SelectorMarker
class RelationshipNodeSelector : SelectorX {
  var labels: Set<String>? = null
  var key: Map<String, Any>? = null

  override fun asMap(): Map<String, Any> {
    TODO("Not yet implemented")
  }
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
