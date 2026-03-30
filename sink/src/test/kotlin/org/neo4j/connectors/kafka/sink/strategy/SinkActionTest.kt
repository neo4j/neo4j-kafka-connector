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
package org.neo4j.connectors.kafka.sink.strategy

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.maps.shouldBeEmpty
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldEndWith
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

class SinkActionTest {

  @Nested
  inner class NodeMatcherByLabelsAndPropertiesTest {

    @Test
    fun `should create matcher with valid labels and properties`() {
      val matcher =
          NodeMatcher.ByLabelsAndProperties(
              labels = setOf("Person", "Employee"),
              properties = mapOf("id" to 1, "name" to "John"),
          )

      matcher.labels shouldBe setOf("Person", "Employee")
      matcher.properties shouldBe mapOf("id" to 1, "name" to "John")
    }

    @Test
    fun `should create matcher with single property`() {
      val matcher =
          NodeMatcher.ByLabelsAndProperties(labels = setOf("Person"), properties = mapOf("id" to 1))

      matcher.labels shouldBe setOf("Person")
      matcher.properties shouldBe mapOf("id" to 1)
    }

    @Test
    fun `should create matcher with empty labels`() {
      val matcher =
          NodeMatcher.ByLabelsAndProperties(labels = emptySet(), properties = mapOf("id" to 1))

      matcher.labels shouldBe emptySet()
      matcher.properties shouldBe mapOf("id" to 1)
    }

    @Test
    fun `should create matcher for any node`() {
      val matcher = NodeMatcher.ByLabelsAndProperties(labels = emptySet(), properties = emptyMap())

      matcher.labels shouldBe emptySet()
      matcher.properties shouldBe emptyMap()
    }
  }

  @Nested
  inner class NodeMatcherByIdTest {

    @Test
    fun `should create matcher with positive id`() {
      val matcher = NodeMatcher.ById(id = 123L)

      matcher.id shouldBe 123L
    }

    @Test
    fun `should create matcher with zero id`() {
      val matcher = NodeMatcher.ById(id = 0L)

      matcher.id shouldBe 0L
    }

    @Test
    fun `should create matcher with negative id`() {
      val matcher = NodeMatcher.ById(id = -1L)

      matcher.id shouldBe -1L
    }
  }

  @Nested
  inner class NodeMatcherByElementIdTest {

    @Test
    fun `should create matcher with valid element id`() {
      val matcher = NodeMatcher.ByElementId(elementId = "4:abc123:456")

      matcher.elementId shouldBe "4:abc123:456"
    }

    @Test
    fun `should create matcher with empty element id`() {
      val matcher = NodeMatcher.ByElementId(elementId = "")

      matcher.elementId shouldBe ""
    }
  }

  @Nested
  inner class RelationshipMatcherByTypeAndPropertiesTest {

    @Test
    fun `should create matcher with valid type and properties`() {
      val matcher =
          RelationshipMatcher.ByTypeAndProperties(
              type = "KNOWS",
              properties = mapOf("since" to 2020),
              hasKeys = true,
          )

      matcher.type shouldBe "KNOWS"
      matcher.properties shouldBe mapOf("since" to 2020)
      matcher.hasKeys shouldBe true
    }

    @Test
    fun `should create matcher with empty properties`() {
      val matcher =
          RelationshipMatcher.ByTypeAndProperties(
              type = "KNOWS",
              properties = emptyMap(),
              hasKeys = false,
          )

      matcher.type shouldBe "KNOWS"
      matcher.properties.shouldBeEmpty()
      matcher.hasKeys shouldBe false
    }

    @Test
    fun `should throw exception when type is blank`() {
      val exception =
          shouldThrow<IllegalArgumentException> {
            RelationshipMatcher.ByTypeAndProperties(
                type = "",
                properties = mapOf("key" to "value"),
                hasKeys = true,
            )
          }

      exception.message shouldBe "type can not be blank."
    }

    @Test
    fun `should throw exception when type is whitespace only`() {
      val exception =
          shouldThrow<IllegalArgumentException> {
            RelationshipMatcher.ByTypeAndProperties(
                type = "   ",
                properties = mapOf("key" to "value"),
                hasKeys = true,
            )
          }

      exception.message shouldBe "type can not be blank."
    }
  }

  @Nested
  inner class RelationshipMatcherByIdTest {

    @Test
    fun `should create matcher with valid id`() {
      val matcher = RelationshipMatcher.ById(id = 999L)

      matcher.id shouldBe 999L
    }
  }

  @Nested
  inner class RelationshipMatcherByElementIdTest {

    @Test
    fun `should create matcher with valid element id`() {
      val matcher = RelationshipMatcher.ByElementId(elementId = "5:rel123:789")

      matcher.elementId shouldBe "5:rel123:789"
    }
  }

  @Nested
  inner class CreateNodeSinkActionTest {

    @Test
    fun `should create action with labels and properties`() {
      val action =
          CreateNodeSinkAction(
              labels = setOf("Person", "Employee"),
              properties = mapOf("name" to "John", "age" to 30),
          )

      action.labels shouldBe setOf("Person", "Employee")
      action.properties shouldBe mapOf("name" to "John", "age" to 30)
    }

    @Test
    fun `should create action with empty labels`() {
      val action = CreateNodeSinkAction(labels = emptySet(), properties = mapOf("name" to "John"))

      action.labels shouldBe emptySet()
    }

    @Test
    fun `should create action with empty properties`() {
      val action = CreateNodeSinkAction(labels = setOf("Person"), properties = emptyMap())

      action.properties.shouldBeEmpty()
    }

    @Test
    fun `should create action with null property values`() {
      val action =
          CreateNodeSinkAction(
              labels = setOf("Person"),
              properties = mapOf("name" to "John", "nickname" to null),
          )

      action.properties shouldBe mapOf("name" to "John", "nickname" to null)
    }
  }

  @Nested
  inner class UpdateNodeSinkActionTest {

    @Test
    fun `should create action with all parameters`() {
      val matcher = NodeMatcher.ByLabelsAndProperties(setOf("Person"), mapOf("id" to 1))
      val action =
          UpdateNodeSinkAction(
              matcher = matcher,
              setProperties = mapOf("name" to "John"),
              addLabels = setOf("Employee"),
              removeLabels = setOf("Contractor"),
          )

      action.matcher shouldBe matcher
      action.setProperties shouldBe mapOf("name" to "John")
      action.addLabels shouldBe setOf("Employee")
      action.removeLabels shouldBe setOf("Contractor")
    }

    @Test
    fun `should create action with empty modifiers`() {
      val matcher = NodeMatcher.ById(123L)
      val action =
          UpdateNodeSinkAction(
              matcher = matcher,
              setProperties = emptyMap(),
              addLabels = emptySet(),
              removeLabels = emptySet(),
          )

      action.matcher shouldBe matcher
      action.setProperties.shouldBeEmpty()
      action.addLabels shouldBe emptySet()
      action.removeLabels shouldBe emptySet()
    }
  }

  @Nested
  inner class MergeNodeSinkActionTest {

    @Test
    fun `should create action with all parameters`() {
      val matcher = NodeMatcher.ByLabelsAndProperties(setOf("Person"), mapOf("id" to 123))
      val action =
          MergeNodeSinkAction(
              matcher = matcher,
              setProperties = mapOf("updated" to true),
              addLabels = setOf("Active"),
              removeLabels = setOf("Inactive"),
          )

      action.matcher shouldBe matcher
      action.setProperties shouldBe mapOf("updated" to true)
      action.addLabels shouldBe setOf("Active")
      action.removeLabels shouldBe setOf("Inactive")
    }

    @Test
    fun `should throw exception when matcher is ById`() {
      val exception =
          shouldThrow<IllegalArgumentException> {
            MergeNodeSinkAction(
                matcher = NodeMatcher.ById(123L),
                setProperties = mapOf("updated" to true),
                addLabels = emptySet(),
                removeLabels = emptySet(),
            )
          }

      exception.message shouldBe
          "can only use labels and properties as a matcher for merge node action."
    }

    @Test
    fun `should throw exception when matcher is ByElementId`() {
      val exception =
          shouldThrow<IllegalArgumentException> {
            MergeNodeSinkAction(
                matcher = NodeMatcher.ByElementId("4:abc:123"),
                setProperties = mapOf("updated" to true),
                addLabels = emptySet(),
                removeLabels = emptySet(),
            )
          }

      exception.message shouldBe
          "can only use labels and properties as a matcher for merge node action."
    }
  }

  @Nested
  inner class DeleteNodeSinkActionTest {

    @Test
    fun `should create action with matcher by labels and properties`() {
      val matcher = NodeMatcher.ByLabelsAndProperties(setOf("Person"), mapOf("id" to 1))
      val action = DeleteNodeSinkAction(matcher = matcher)

      action.matcher shouldBe matcher
      action.detach shouldBe false
    }

    @Test
    fun `should create action with matcher by id`() {
      val matcher = NodeMatcher.ById(456L)
      val action = DeleteNodeSinkAction(matcher = matcher)

      action.matcher shouldBe matcher
      action.detach shouldBe false
    }

    @Test
    fun `should create action with matcher by element id`() {
      val matcher = NodeMatcher.ByElementId("4:abc:789")
      val action = DeleteNodeSinkAction(matcher = matcher)

      action.matcher shouldBe matcher
      action.detach shouldBe false
    }

    @Test
    fun `should create action with detach true`() {
      val matcher = NodeMatcher.ByLabelsAndProperties(setOf("Person"), mapOf("id" to 1))
      val action = DeleteNodeSinkAction(matcher = matcher, detach = true)

      action.matcher shouldBe matcher
      action.detach shouldBe true
    }

    @Test
    fun `should create action with detach false explicitly`() {
      val matcher = NodeMatcher.ByLabelsAndProperties(setOf("Person"), mapOf("id" to 1))
      val action = DeleteNodeSinkAction(matcher = matcher, detach = false)

      action.matcher shouldBe matcher
      action.detach shouldBe false
    }
  }

  @Nested
  inner class SinkActionNodeReferenceTest {

    @Test
    fun `should create reference with MATCH lookup mode`() {
      val matcher = NodeMatcher.ByLabelsAndProperties(setOf("Person"), mapOf("id" to 1))
      val ref = SinkActionNodeReference(matcher = matcher, lookupMode = LookupMode.MATCH)

      ref.matcher shouldBe matcher
      ref.lookupMode shouldBe LookupMode.MATCH
    }

    @Test
    fun `should create reference with MATCH lookup mode when using id matcher`() {
      val matcher = NodeMatcher.ById(4)
      val ref = SinkActionNodeReference(matcher = matcher, lookupMode = LookupMode.MATCH)

      ref.matcher shouldBe matcher
      ref.lookupMode shouldBe LookupMode.MATCH
    }

    @Test
    fun `should create reference with MATCH lookup mode when using element id matcher`() {
      val matcher = NodeMatcher.ByElementId("4:abc:1")
      val ref = SinkActionNodeReference(matcher = matcher, lookupMode = LookupMode.MATCH)

      ref.matcher shouldBe matcher
      ref.lookupMode shouldBe LookupMode.MATCH
    }

    @Test
    fun `should create reference with MERGE lookup mode`() {
      val matcher = NodeMatcher.ByLabelsAndProperties(setOf("Person"), mapOf("id" to 1))
      val ref = SinkActionNodeReference(matcher = matcher, lookupMode = LookupMode.MERGE)

      ref.matcher shouldBe matcher
      ref.lookupMode shouldBe LookupMode.MERGE
    }

    @Test
    fun `should throw exception with MERGE lookup mode when using empty labels`() {
      val exception =
          shouldThrow<IllegalArgumentException> {
            SinkActionNodeReference(
                matcher = NodeMatcher.ByLabelsAndProperties(emptySet(), mapOf("id" to 5)),
                lookupMode = LookupMode.MERGE,
            )
          }

      exception.message shouldBe "match labels must not be empty."
    }

    @Test
    fun `should throw exception with MERGE lookup mode when using empty properties`() {
      val exception =
          shouldThrow<IllegalArgumentException> {
            SinkActionNodeReference(
                matcher = NodeMatcher.ByLabelsAndProperties(setOf("Person"), emptyMap()),
                lookupMode = LookupMode.MERGE,
            )
          }

      exception.message shouldBe "match properties must not be empty."
    }

    @Test
    fun `should throw exception with MERGE lookup mode when using id matcher`() {
      val exception =
          shouldThrow<IllegalArgumentException> {
            SinkActionNodeReference(matcher = NodeMatcher.ById(4), lookupMode = LookupMode.MERGE)
          }

      exception.message shouldBe
          "can only use labels and properties as a matcher for MERGE lookup mode."
    }

    @Test
    fun `should throw exception with MERGE lookup mode when using element id matcher`() {
      val exception =
          shouldThrow<IllegalArgumentException> {
            SinkActionNodeReference(
                matcher = NodeMatcher.ByElementId("4:abc:1"),
                lookupMode = LookupMode.MERGE,
            )
          }

      exception.message shouldBe
          "can only use labels and properties as a matcher for MERGE lookup mode."
    }
  }

  @Nested
  inner class CreateRelationshipSinkActionTest {

    @Test
    fun `should create action with valid parameters`() {
      val startNode =
          SinkActionNodeReference(
              NodeMatcher.ByLabelsAndProperties(setOf("Person"), mapOf("id" to 1)),
              LookupMode.MATCH,
          )
      val endNode =
          SinkActionNodeReference(
              NodeMatcher.ByLabelsAndProperties(setOf("Company"), mapOf("id" to 2)),
              LookupMode.MERGE,
          )
      val action =
          CreateRelationshipSinkAction(
              startNode = startNode,
              endNode = endNode,
              type = "WORKS_AT",
              properties = mapOf("since" to 2020),
          )

      action.startNode shouldBe startNode
      action.endNode shouldBe endNode
      action.type shouldBe "WORKS_AT"
      action.properties shouldBe mapOf("since" to 2020)
    }

    @Test
    fun `should create action with empty properties`() {
      val startNode = SinkActionNodeReference(NodeMatcher.ById(1L), LookupMode.MATCH)
      val endNode = SinkActionNodeReference(NodeMatcher.ById(2L), LookupMode.MATCH)
      val action =
          CreateRelationshipSinkAction(
              startNode = startNode,
              endNode = endNode,
              type = "KNOWS",
              properties = emptyMap(),
          )

      action.properties.shouldBeEmpty()
    }

    @Test
    fun `should throw exception when type is blank`() {
      val startNode = SinkActionNodeReference(NodeMatcher.ById(1L), LookupMode.MATCH)
      val endNode = SinkActionNodeReference(NodeMatcher.ById(2L), LookupMode.MATCH)

      val exception =
          shouldThrow<IllegalArgumentException> {
            CreateRelationshipSinkAction(
                startNode = startNode,
                endNode = endNode,
                type = "",
                properties = mapOf("key" to "value"),
            )
          }

      exception.message shouldBe "type can not be empty."
    }

    @Test
    fun `should throw exception when node references are match any`() {
      val anyNode = SinkActionNodeReference.MATCH_ANY
      val someNode = SinkActionNodeReference(NodeMatcher.ById(2L), LookupMode.MATCH)

      sequenceOf((anyNode to someNode), (someNode to anyNode), (anyNode to anyNode)).forEach {
          (start, end) ->
        val exception =
            shouldThrow<IllegalArgumentException> {
              CreateRelationshipSinkAction(
                  startNode = start,
                  endNode = end,
                  type = "RELATED_TO",
                  properties = mapOf("key" to "value"),
              )
            }

        exception.message shouldEndWith
            "node reference must specify labels and/or properties for create relationship action."
      }
    }
  }

  @Nested
  inner class UpdateRelationshipSinkActionTest {

    @Test
    fun `should create action when both nodes use MATCH lookup mode`() {
      val startNode =
          SinkActionNodeReference(
              NodeMatcher.ByLabelsAndProperties(setOf("Person"), mapOf("id" to 1)),
              LookupMode.MATCH,
          )
      val endNode =
          SinkActionNodeReference(
              NodeMatcher.ByLabelsAndProperties(setOf("Company"), mapOf("id" to 2)),
              LookupMode.MATCH,
          )
      val matcher =
          RelationshipMatcher.ByTypeAndProperties("WORKS_AT", mapOf("since" to 2020), true)
      val action =
          UpdateRelationshipSinkAction(
              startNode = startNode,
              endNode = endNode,
              matcher = matcher,
              setProperties = mapOf("updated" to true),
          )

      action.startNode shouldBe startNode
      action.endNode shouldBe endNode
      action.matcher shouldBe matcher
      action.setProperties shouldBe mapOf("updated" to true)
    }

    @Test
    fun `should create action with matcher by id`() {
      val startNode = SinkActionNodeReference(NodeMatcher.ById(1L), LookupMode.MATCH)
      val endNode = SinkActionNodeReference(NodeMatcher.ById(2L), LookupMode.MATCH)
      val matcher = RelationshipMatcher.ById(999L)
      val action =
          UpdateRelationshipSinkAction(
              startNode = startNode,
              endNode = endNode,
              matcher = matcher,
              setProperties = emptyMap(),
          )

      action.matcher shouldBe matcher
    }

    @Test
    fun `should create action with matcher by element id`() {
      val startNode = SinkActionNodeReference(NodeMatcher.ByElementId("4:abc:1"), LookupMode.MATCH)
      val endNode = SinkActionNodeReference(NodeMatcher.ByElementId("4:abc:2"), LookupMode.MATCH)
      val matcher = RelationshipMatcher.ByElementId("5:rel:123")
      val action =
          UpdateRelationshipSinkAction(
              startNode = startNode,
              endNode = endNode,
              matcher = matcher,
              setProperties = mapOf("prop" to "value"),
          )

      action.matcher shouldBe matcher
    }

    @Test
    fun `should create action with matcher by id and node references are match any`() {
      val startNode = SinkActionNodeReference.MATCH_ANY
      val endNode = SinkActionNodeReference.MATCH_ANY
      val matcher = RelationshipMatcher.ById(123)
      val action =
          UpdateRelationshipSinkAction(
              startNode = startNode,
              endNode = endNode,
              matcher = matcher,
              setProperties = mapOf("prop" to "value"),
          )

      action.matcher shouldBe matcher
    }

    @Test
    fun `should create action with matcher by element id and node references are match any`() {
      val startNode = SinkActionNodeReference.MATCH_ANY
      val endNode = SinkActionNodeReference.MATCH_ANY
      val matcher = RelationshipMatcher.ByElementId("5:rel:123")
      val action =
          UpdateRelationshipSinkAction(
              startNode = startNode,
              endNode = endNode,
              matcher = matcher,
              setProperties = mapOf("prop" to "value"),
          )

      action.matcher shouldBe matcher
    }

    @Test
    fun `should create action when relationship has keys and node references are match any`() {
      val startNode = SinkActionNodeReference.MATCH_ANY
      val endNode = SinkActionNodeReference.MATCH_ANY
      val matcher =
          RelationshipMatcher.ByTypeAndProperties("WORKS_AT", mapOf("since" to 2020), true)
      val action =
          UpdateRelationshipSinkAction(
              startNode = startNode,
              endNode = endNode,
              matcher = matcher,
              setProperties = mapOf("prop" to "value"),
          )

      action.matcher shouldBe matcher
    }

    @Test
    fun `should throw exception when relationship has no keys and node references are match any`() {
      val anyNode = SinkActionNodeReference.MATCH_ANY
      val someNode = SinkActionNodeReference(NodeMatcher.ById(2L), LookupMode.MATCH)

      sequenceOf((anyNode to someNode), (someNode to anyNode), (anyNode to anyNode)).forEach {
          (start, end) ->
        val exception =
            shouldThrow<IllegalArgumentException> {
              UpdateRelationshipSinkAction(
                  startNode = start,
                  endNode = end,
                  matcher =
                      RelationshipMatcher.ByTypeAndProperties("RELATED_TO", emptyMap(), false),
                  setProperties = mapOf("prop" to "value"),
              )
            }

        exception.message shouldEndWith
            "node matcher must contain at least one key property for keyless relationship update action."
      }
    }

    @Test
    fun `should throw exception node references use MERGE lookup mode`() {
      val mergeNode =
          SinkActionNodeReference(
              NodeMatcher.ByLabelsAndProperties(setOf("Person"), mapOf("id" to 1)),
              LookupMode.MERGE,
          )
      val matchNode =
          SinkActionNodeReference(
              NodeMatcher.ByLabelsAndProperties(setOf("Person"), mapOf("id" to 2)),
              LookupMode.MATCH,
          )

      sequenceOf((mergeNode to matchNode), (matchNode to mergeNode), (mergeNode to mergeNode))
          .forEach { (start, end) ->
            val exception =
                shouldThrow<IllegalArgumentException> {
                  UpdateRelationshipSinkAction(
                      startNode = start,
                      endNode = end,
                      matcher =
                          RelationshipMatcher.ByTypeAndProperties("RELATED_TO", emptyMap(), false),
                      setProperties = mapOf("prop" to "value"),
                  )
                }

            exception.message shouldEndWith
                "node must use MATCH lookup mode for update relationship action."
          }
    }
  }

  @Nested
  inner class MergeRelationshipSinkActionTest {

    @Test
    fun `should create action with MATCH lookup modes`() {
      val startNode = SinkActionNodeReference(NodeMatcher.ById(1L), LookupMode.MATCH)
      val endNode = SinkActionNodeReference(NodeMatcher.ById(2L), LookupMode.MATCH)
      val matcher = RelationshipMatcher.ByTypeAndProperties("KNOWS", mapOf("since" to 2020), true)
      val action =
          MergeRelationshipSinkAction(
              startNode = startNode,
              endNode = endNode,
              matcher = matcher,
              setProperties = mapOf("updated" to true),
          )

      action.startNode shouldBe startNode
      action.endNode shouldBe endNode
      action.matcher shouldBe matcher
      action.setProperties shouldBe mapOf("updated" to true)
    }

    @Test
    fun `should throw exception node references are match any`() {
      val anyNode = SinkActionNodeReference.MATCH_ANY
      val matchNode =
          SinkActionNodeReference(
              NodeMatcher.ByLabelsAndProperties(setOf("Person"), mapOf("id" to 2)),
              LookupMode.MATCH,
          )

      sequenceOf((anyNode to matchNode), (matchNode to anyNode), (anyNode to anyNode)).forEach {
          (start, end) ->
        val exception =
            shouldThrow<IllegalArgumentException> {
              MergeRelationshipSinkAction(
                  startNode = start,
                  endNode = end,
                  matcher =
                      RelationshipMatcher.ByTypeAndProperties("RELATED_TO", emptyMap(), false),
                  setProperties = mapOf("prop" to "value"),
              )
            }

        exception.message shouldEndWith
            "node matcher must contain at least one key property for relationship merge action."
      }
    }
  }

  @Nested
  inner class DeleteRelationshipSinkActionTest {

    @Test
    fun `should create action when both nodes use MATCH lookup mode`() {
      val startNode =
          SinkActionNodeReference(
              NodeMatcher.ByLabelsAndProperties(setOf("Person"), mapOf("id" to 1)),
              LookupMode.MATCH,
          )
      val endNode =
          SinkActionNodeReference(
              NodeMatcher.ByLabelsAndProperties(setOf("Company"), mapOf("id" to 2)),
              LookupMode.MATCH,
          )
      val matcher =
          RelationshipMatcher.ByTypeAndProperties("WORKS_AT", mapOf("since" to 2020), true)
      val action =
          DeleteRelationshipSinkAction(startNode = startNode, endNode = endNode, matcher = matcher)

      action.startNode shouldBe startNode
      action.endNode shouldBe endNode
      action.matcher shouldBe matcher
    }

    @Test
    fun `should create action with matcher by id`() {
      val startNode = SinkActionNodeReference(NodeMatcher.ById(1L), LookupMode.MATCH)
      val endNode = SinkActionNodeReference(NodeMatcher.ById(2L), LookupMode.MATCH)
      val matcher = RelationshipMatcher.ById(999L)
      val action =
          DeleteRelationshipSinkAction(startNode = startNode, endNode = endNode, matcher = matcher)

      action.matcher shouldBe matcher
    }

    @Test
    fun `should create action with matcher by element id`() {
      val startNode = SinkActionNodeReference(NodeMatcher.ByElementId("4:abc:1"), LookupMode.MATCH)
      val endNode = SinkActionNodeReference(NodeMatcher.ByElementId("4:abc:2"), LookupMode.MATCH)
      val matcher = RelationshipMatcher.ByElementId("5:rel:123")
      val action =
          DeleteRelationshipSinkAction(startNode = startNode, endNode = endNode, matcher = matcher)

      action.matcher shouldBe matcher
    }

    @Test
    fun `should create action with matcher by id and node references are match any`() {
      val startNode = SinkActionNodeReference.MATCH_ANY
      val endNode = SinkActionNodeReference.MATCH_ANY
      val matcher = RelationshipMatcher.ById(123)
      val action =
          DeleteRelationshipSinkAction(startNode = startNode, endNode = endNode, matcher = matcher)

      action.matcher shouldBe matcher
    }

    @Test
    fun `should create action with matcher by element id and node references are match any`() {
      val startNode = SinkActionNodeReference.MATCH_ANY
      val endNode = SinkActionNodeReference.MATCH_ANY
      val matcher = RelationshipMatcher.ByElementId("5:rel:123")
      val action =
          DeleteRelationshipSinkAction(startNode = startNode, endNode = endNode, matcher = matcher)

      action.matcher shouldBe matcher
    }

    @Test
    fun `should create action when relationship has keys and node references are match any`() {
      val startNode = SinkActionNodeReference.MATCH_ANY
      val endNode = SinkActionNodeReference.MATCH_ANY
      val matcher =
          RelationshipMatcher.ByTypeAndProperties("WORKS_AT", mapOf("since" to 2020), true)
      val action =
          DeleteRelationshipSinkAction(startNode = startNode, endNode = endNode, matcher = matcher)

      action.matcher shouldBe matcher
    }

    @Test
    fun `should throw exception when relationship has no keys and node references are match any`() {
      val anyNode = SinkActionNodeReference.MATCH_ANY
      val someNode = SinkActionNodeReference(NodeMatcher.ById(2L), LookupMode.MATCH)

      sequenceOf((anyNode to someNode), (someNode to anyNode), (anyNode to anyNode)).forEach {
          (start, end) ->
        val exception =
            shouldThrow<IllegalArgumentException> {
              DeleteRelationshipSinkAction(
                  startNode = start,
                  endNode = end,
                  matcher = RelationshipMatcher.ByTypeAndProperties("RELATED_TO", emptyMap(), false),
              )
            }

        exception.message shouldEndWith
            "node matcher must contain at least one key property for keyless relationship delete action."
      }
    }

    @Test
    fun `should throw exception node references use MERGE lookup mode`() {
      val mergeNode =
          SinkActionNodeReference(
              NodeMatcher.ByLabelsAndProperties(setOf("Person"), mapOf("id" to 1)),
              LookupMode.MERGE,
          )
      val matchNode =
          SinkActionNodeReference(
              NodeMatcher.ByLabelsAndProperties(setOf("Person"), mapOf("id" to 2)),
              LookupMode.MATCH,
          )

      sequenceOf((mergeNode to matchNode), (matchNode to mergeNode), (mergeNode to mergeNode))
          .forEach { (start, end) ->
            val exception =
                shouldThrow<IllegalArgumentException> {
                  UpdateRelationshipSinkAction(
                      startNode = start,
                      endNode = end,
                      matcher =
                          RelationshipMatcher.ByTypeAndProperties("RELATED_TO", emptyMap(), false),
                      setProperties = mapOf("prop" to "value"),
                  )
                }

            exception.message shouldEndWith
                "node must use MATCH lookup mode for update relationship action."
          }
    }
  }

  @Nested
  inner class LookupModeTest {

    @Test
    fun `should have MATCH enum value`() {
      LookupMode.MATCH.name shouldBe "MATCH"
    }

    @Test
    fun `should have MERGE enum value`() {
      LookupMode.MERGE.name shouldBe "MERGE"
    }

    @Test
    fun `should have exactly two enum values`() {
      LookupMode.entries shouldHaveSize 2
      LookupMode.entries shouldContainExactlyInAnyOrder listOf(LookupMode.MATCH, LookupMode.MERGE)
    }
  }
}
