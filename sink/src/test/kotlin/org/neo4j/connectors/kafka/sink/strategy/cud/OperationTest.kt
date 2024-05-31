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
package org.neo4j.connectors.kafka.sink.strategy.cud

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.neo4j.connectors.kafka.sink.strategy.InvalidDataException

class OperationTest {

  @Test
  fun `should construct create node event`() {
    val operation =
        Operation.from(
            mapOf(
                Keys.TYPE to Type.NODE.name,
                Keys.OPERATION to OperationType.CREATE.name,
                Keys.LABELS to listOf("LabelA", "LabelB"),
                Keys.PROPERTIES to mapOf("prop1" to 1, "prop2" to "test", "prop3" to true)))

    operation shouldBe
        CreateNode(
            setOf("LabelA", "LabelB"), mapOf("prop1" to 1, "prop2" to "test", "prop3" to true))
  }

  @Test
  fun `should construct update node event`() {
    val operation =
        Operation.from(
            mapOf(
                Keys.TYPE to Type.NODE.name,
                Keys.OPERATION to OperationType.UPDATE.name,
                Keys.LABELS to listOf("LabelA", "LabelB"),
                Keys.IDS to mapOf("id1" to 1, "id2" to "string"),
                Keys.PROPERTIES to mapOf("prop1" to 1, "prop2" to "test", "prop3" to true)))

    operation shouldBe
        UpdateNode(
            setOf("LabelA", "LabelB"),
            mapOf("id1" to 1, "id2" to "string"),
            mapOf("prop1" to 1, "prop2" to "test", "prop3" to true))
  }

  @Test
  fun `should construct merge node event`() {
    val operation =
        Operation.from(
            mapOf(
                Keys.TYPE to Type.NODE.name,
                Keys.OPERATION to OperationType.MERGE.name,
                Keys.LABELS to listOf("LabelA", "LabelB"),
                Keys.IDS to mapOf("id1" to 1, "id2" to "string"),
                Keys.PROPERTIES to mapOf("prop1" to 1, "prop2" to "test", "prop3" to true)))

    operation shouldBe
        MergeNode(
            setOf("LabelA", "LabelB"),
            mapOf("id1" to 1, "id2" to "string"),
            mapOf("prop1" to 1, "prop2" to "test", "prop3" to true))
  }

  @Test
  fun `should construct delete node event`() {
    val operation =
        Operation.from(
            mapOf(
                Keys.TYPE to Type.NODE.name,
                Keys.OPERATION to OperationType.DELETE.name,
                Keys.LABELS to listOf("LabelA", "LabelB"),
                Keys.IDS to mapOf("id1" to 1, "id2" to "string")))

    operation shouldBe
        DeleteNode(setOf("LabelA", "LabelB"), mapOf("id1" to 1, "id2" to "string"), false)
  }

  @Test
  fun `should construct delete node event with explicit detach mode`() {
    val operation =
        Operation.from(
            mapOf(
                Keys.TYPE to Type.NODE.name,
                Keys.OPERATION to OperationType.DELETE.name,
                Keys.LABELS to listOf("LabelA", "LabelB"),
                Keys.IDS to mapOf("id1" to 1, "id2" to "string"),
                Keys.DETACH to true))

    operation shouldBe
        DeleteNode(setOf("LabelA", "LabelB"), mapOf("id1" to 1, "id2" to "string"), true)
  }

  @Test
  fun `should construct create relationship event`() {
    val operation =
        Operation.from(
            mapOf(
                Keys.TYPE to Type.RELATIONSHIP.name,
                Keys.OPERATION to OperationType.CREATE.name,
                Keys.RELATION_TYPE to "RELATED_TO",
                Keys.FROM to
                    mapOf(
                        Keys.LABELS to listOf("LabelA"),
                        Keys.IDS to mapOf("id" to 1),
                    ),
                Keys.TO to
                    mapOf(
                        Keys.LABELS to listOf("LabelB"),
                        Keys.IDS to mapOf("id" to 2),
                    ),
                Keys.PROPERTIES to mapOf("prop1" to 1, "prop2" to "test", "prop3" to true)))

    operation shouldBe
        CreateRelationship(
            "RELATED_TO",
            NodeReference(setOf("LabelA"), mapOf("id" to 1), LookupMode.MATCH),
            NodeReference(setOf("LabelB"), mapOf("id" to 2), LookupMode.MATCH),
            mapOf("prop1" to 1, "prop2" to "test", "prop3" to true))
  }

  @Test
  fun `should construct create relationship event with explicit op on nodes`() {
    val operation =
        Operation.from(
            mapOf(
                Keys.TYPE to Type.RELATIONSHIP.name,
                Keys.OPERATION to OperationType.CREATE.name,
                Keys.RELATION_TYPE to "RELATED_TO",
                Keys.FROM to
                    mapOf(
                        Keys.LABELS to listOf("LabelA"),
                        Keys.IDS to mapOf("id" to 1),
                        Keys.OPERATION to "MATCH"),
                Keys.TO to
                    mapOf(
                        Keys.LABELS to listOf("LabelB"),
                        Keys.IDS to mapOf("id" to 2),
                        Keys.OPERATION to "MERGE"),
                Keys.PROPERTIES to mapOf("prop1" to 1, "prop2" to "test", "prop3" to true)))

    operation shouldBe
        CreateRelationship(
            "RELATED_TO",
            NodeReference(setOf("LabelA"), mapOf("id" to 1), LookupMode.MATCH),
            NodeReference(setOf("LabelB"), mapOf("id" to 2), LookupMode.MERGE),
            mapOf("prop1" to 1, "prop2" to "test", "prop3" to true))
  }

  @Test
  fun `should construct update relationship event`() {
    val operation =
        Operation.from(
            mapOf(
                Keys.TYPE to Type.RELATIONSHIP.name,
                Keys.OPERATION to OperationType.UPDATE.name,
                Keys.RELATION_TYPE to "RELATED_TO",
                Keys.FROM to
                    mapOf(
                        Keys.LABELS to listOf("LabelA"),
                        Keys.IDS to mapOf("id" to 1),
                        Keys.OPERATION to "MATCH"),
                Keys.TO to
                    mapOf(
                        Keys.LABELS to listOf("LabelB"),
                        Keys.IDS to mapOf("id" to 2),
                        Keys.OPERATION to "MERGE"),
                Keys.PROPERTIES to mapOf("prop1" to 1, "prop2" to "test", "prop3" to true)))

    operation shouldBe
        UpdateRelationship(
            "RELATED_TO",
            NodeReference(setOf("LabelA"), mapOf("id" to 1), LookupMode.MATCH),
            NodeReference(setOf("LabelB"), mapOf("id" to 2), LookupMode.MERGE),
            emptyMap(),
            mapOf("prop1" to 1, "prop2" to "test", "prop3" to true))
  }

  @Test
  fun `should construct update relationship event with ids`() {
    val operation =
        Operation.from(
            mapOf(
                Keys.TYPE to Type.RELATIONSHIP.name,
                Keys.OPERATION to OperationType.UPDATE.name,
                Keys.RELATION_TYPE to "RELATED_TO",
                Keys.FROM to
                    mapOf(
                        Keys.LABELS to listOf("LabelA"),
                        Keys.IDS to mapOf("id" to 1),
                        Keys.OPERATION to "MATCH"),
                Keys.TO to
                    mapOf(
                        Keys.LABELS to listOf("LabelB"),
                        Keys.IDS to mapOf("id" to 2),
                        Keys.OPERATION to "MERGE"),
                Keys.IDS to mapOf("id" to 3),
                Keys.PROPERTIES to mapOf("prop1" to 1, "prop2" to "test", "prop3" to true)))

    operation shouldBe
        UpdateRelationship(
            "RELATED_TO",
            NodeReference(setOf("LabelA"), mapOf("id" to 1), LookupMode.MATCH),
            NodeReference(setOf("LabelB"), mapOf("id" to 2), LookupMode.MERGE),
            mapOf("id" to 3),
            mapOf("prop1" to 1, "prop2" to "test", "prop3" to true))
  }

  @Test
  fun `should construct merge relationship event`() {
    val operation =
        Operation.from(
            mapOf(
                Keys.TYPE to Type.RELATIONSHIP.name,
                Keys.OPERATION to OperationType.MERGE.name,
                Keys.RELATION_TYPE to "RELATED_TO",
                Keys.FROM to
                    mapOf(
                        Keys.LABELS to listOf("LabelA"),
                        Keys.IDS to mapOf("id" to 1),
                        Keys.OPERATION to "MATCH"),
                Keys.TO to
                    mapOf(
                        Keys.LABELS to listOf("LabelB"),
                        Keys.IDS to mapOf("id" to 2),
                        Keys.OPERATION to "MERGE"),
                Keys.PROPERTIES to mapOf("prop1" to 1, "prop2" to "test", "prop3" to true)))

    operation shouldBe
        MergeRelationship(
            "RELATED_TO",
            NodeReference(setOf("LabelA"), mapOf("id" to 1), LookupMode.MATCH),
            NodeReference(setOf("LabelB"), mapOf("id" to 2), LookupMode.MERGE),
            emptyMap(),
            mapOf("prop1" to 1, "prop2" to "test", "prop3" to true))
  }

  @Test
  fun `should construct merge relationship event with ids`() {
    val operation =
        Operation.from(
            mapOf(
                Keys.TYPE to Type.RELATIONSHIP.name,
                Keys.OPERATION to OperationType.MERGE.name,
                Keys.RELATION_TYPE to "RELATED_TO",
                Keys.FROM to
                    mapOf(
                        Keys.LABELS to listOf("LabelA"),
                        Keys.IDS to mapOf("id" to 1),
                        Keys.OPERATION to "MATCH"),
                Keys.TO to
                    mapOf(
                        Keys.LABELS to listOf("LabelB"),
                        Keys.IDS to mapOf("id" to 2),
                        Keys.OPERATION to "MERGE"),
                Keys.IDS to mapOf("id" to 3),
                Keys.PROPERTIES to mapOf("prop1" to 1, "prop2" to "test", "prop3" to true)))

    operation shouldBe
        MergeRelationship(
            "RELATED_TO",
            NodeReference(setOf("LabelA"), mapOf("id" to 1), LookupMode.MATCH),
            NodeReference(setOf("LabelB"), mapOf("id" to 2), LookupMode.MERGE),
            mapOf("id" to 3),
            mapOf("prop1" to 1, "prop2" to "test", "prop3" to true))
  }

  @Test
  fun `should construct delete relationship event`() {
    val operation =
        Operation.from(
            mapOf(
                Keys.TYPE to Type.RELATIONSHIP.name,
                Keys.OPERATION to OperationType.DELETE.name,
                Keys.RELATION_TYPE to "RELATED_TO",
                Keys.FROM to
                    mapOf(
                        Keys.LABELS to listOf("LabelA"),
                        Keys.IDS to mapOf("id" to 1),
                        Keys.OPERATION to "MATCH"),
                Keys.TO to
                    mapOf(
                        Keys.LABELS to listOf("LabelB"),
                        Keys.IDS to mapOf("id" to 2),
                        Keys.OPERATION to "MERGE")))

    operation shouldBe
        DeleteRelationship(
            "RELATED_TO",
            NodeReference(setOf("LabelA"), mapOf("id" to 1), LookupMode.MATCH),
            NodeReference(setOf("LabelB"), mapOf("id" to 2), LookupMode.MERGE),
            emptyMap())
  }

  @Test
  fun `should construct delete relationship event with ids`() {
    val operation =
        Operation.from(
            mapOf(
                Keys.TYPE to Type.RELATIONSHIP.name,
                Keys.OPERATION to OperationType.DELETE.name,
                Keys.RELATION_TYPE to "RELATED_TO",
                Keys.FROM to
                    mapOf(
                        Keys.LABELS to listOf("LabelA"),
                        Keys.IDS to mapOf("id" to 1),
                        Keys.OPERATION to "MATCH"),
                Keys.TO to
                    mapOf(
                        Keys.LABELS to listOf("LabelB"),
                        Keys.IDS to mapOf("id" to 2),
                        Keys.OPERATION to "MERGE"),
                Keys.IDS to mapOf("id" to 3)))

    operation shouldBe
        DeleteRelationship(
            "RELATED_TO",
            NodeReference(setOf("LabelA"), mapOf("id" to 1), LookupMode.MATCH),
            NodeReference(setOf("LabelB"), mapOf("id" to 2), LookupMode.MERGE),
            mapOf("id" to 3))
  }

  @Test
  fun `should deserialize correct lookup mode case insensitive`() {
    val operation =
        Operation.from(
            mapOf(
                Keys.TYPE to Type.RELATIONSHIP.name,
                Keys.OPERATION to OperationType.DELETE.name,
                Keys.RELATION_TYPE to "RELATED_TO",
                Keys.FROM to
                    mapOf(
                        Keys.LABELS to listOf("LabelA"),
                        Keys.IDS to mapOf("id" to 1),
                        Keys.OPERATION to "mAtcH"),
                Keys.TO to
                    mapOf(
                        Keys.LABELS to listOf("LabelB"),
                        Keys.IDS to mapOf("id" to 2),
                        Keys.OPERATION to "mErgE"),
                Keys.IDS to mapOf("id" to 3)))

    operation shouldBe
        DeleteRelationship(
            "RELATED_TO",
            NodeReference(setOf("LabelA"), mapOf("id" to 1), LookupMode.MATCH),
            NodeReference(setOf("LabelB"), mapOf("id" to 2), LookupMode.MERGE),
            mapOf("id" to 3))
  }

  @Test
  fun `should throw when ids is specified for node creation`() {
    assertDataExceptionThrown(
        """
        {
          "type": "node",
          "op": "create",
          "labels": [],
          "ids": {
            "id": 1
          },
          "properties": {
            "prop1": 1,
            "prop2": "value"
          }
        }
        """)
  }

  @Test
  fun `should throw when properties is missing for node creation`() {
    assertDataExceptionThrown(
        """
        {
          "type": "node",
          "op": "create",
          "labels": []
        }
        """)
  }

  @Test
  fun `should throw when ids is missing for node update or merge`() {
    listOf("update", "merge").forEach {
      assertDataExceptionThrown(
          """
        {
          "type": "node",
          "op": "$it",
          "labels": [],
          "properties": {
            "prop1": 1
          }
        }
        """)
    }
  }

  @Test
  fun `should throw when properties is given for node deletion`() {
    assertDataExceptionThrown(
        """
        {
          "type": "node",
          "op": "delete",
          "labels": [],
          "ids": {
            "id": 1
          },
          "properties": {
            "prop1": 1
          }
        }
        """)
  }

  @Test
  fun `should throw when ids is specified for relationship creation`() {
    assertDataExceptionThrown(
        """
        {
          "type": "relationship",
          "op": "create",
          "rel_type": "RELATED",
          "from": {
            "labels": ["LabelA"],
            "ids": { "id": 1 } 
          },
          "to": {
            "labels": ["LabelB"],
            "ids": { "id": 2 } 
          },
          "ids": {
            "id": 3
          },
          "properties": {
            "prop1": 1,
            "prop2": "value"
          }
        }
        """)
  }

  @Test
  fun `should throw when properties is missing for relationship creation`() {
    assertDataExceptionThrown(
        """
                  {
          "type": "relationship",
          "op": "create",
          "rel_type": "RELATED",
          "from": {
            "labels": ["LabelA"],
            "ids": { "id": 1 } 
          },
          "to": {
            "labels": ["LabelB"],
            "ids": { "id": 2 } 
          }
        }
        """)
  }

  @Test
  fun `should throw when from or to ids is missing for relationship update or merge`() {
    listOf("update", "merge").forEach {
      assertDataExceptionThrown(
          """
        {
          "type": "relationship",
          "op": "$it",
          "rel_type": "RELATED",
          "from": {
            "labels": ["LabelA"],
            "ids": { "id": 1 } 
          },
          "to": {
            "labels": ["LabelB"]
          },
          "properties": {
            "prop1": 1
          }
        }
        """)
    }
  }

  @Test
  fun `should throw when properties is given for relationship deletion`() {
    assertDataExceptionThrown(
        """
        {
          "type": "relationship",
          "op": "delete",
          "rel_type": "RELATED",
          "from": {
            "labels": ["LabelA"],
            "ids": { "id": 1 } 
          },
          "to": {
            "labels": ["LabelB"],
            "ids": { "id": 2 }
          },
          "properties": {
            "prop1": 1
          }
        }
        """)
  }

  private fun assertDataExceptionThrown(json: String): InvalidDataException {
    val map =
        jacksonObjectMapper().readerForMapOf(Any::class.java).readValue<Map<String, Any>>(json)
    return assertThrows<InvalidDataException> { Operation.from(map) }
  }
}
