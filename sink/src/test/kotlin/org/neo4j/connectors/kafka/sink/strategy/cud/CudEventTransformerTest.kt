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

import io.kotest.matchers.shouldBe
import org.apache.kafka.connect.data.Schema
import org.junit.jupiter.api.Test
import org.neo4j.connectors.kafka.sink.strategy.CreateNodeSinkAction
import org.neo4j.connectors.kafka.sink.strategy.CreateRelationshipSinkAction
import org.neo4j.connectors.kafka.sink.strategy.DeleteNodeSinkAction
import org.neo4j.connectors.kafka.sink.strategy.DeleteRelationshipSinkAction
import org.neo4j.connectors.kafka.sink.strategy.HandlerTest
import org.neo4j.connectors.kafka.sink.strategy.LookupMode
import org.neo4j.connectors.kafka.sink.strategy.MergeNodeSinkAction
import org.neo4j.connectors.kafka.sink.strategy.MergeRelationshipSinkAction
import org.neo4j.connectors.kafka.sink.strategy.NodeMatcher
import org.neo4j.connectors.kafka.sink.strategy.RelationshipMatcher
import org.neo4j.connectors.kafka.sink.strategy.SinkActionNodeReference
import org.neo4j.connectors.kafka.sink.strategy.UpdateNodeSinkAction
import org.neo4j.connectors.kafka.sink.strategy.UpdateRelationshipSinkAction

class CudEventTransformerTest : HandlerTest() {

  private val transformer = CudEventTransformer()

  @Test
  fun `should create node`() {
    val sinkMessage =
        newMessage(
            Schema.STRING_SCHEMA,
            """
                {
                  "type": "node",
                  "op": "create",
                  "labels": ["Foo", "Bar"],
                  "properties": {
                    "id": 1,
                    "foo": "foo-value"
                  }
                }
                """,
        )

    transformer.transform(sinkMessage) shouldBe
        CreateNodeSinkAction(
            labels = setOf("Foo", "Bar"),
            properties = mapOf("id" to 1, "foo" to "foo-value"),
        )
  }

  @Test
  fun `should create node without labels`() {
    val sinkMessage =
        newMessage(
            Schema.STRING_SCHEMA,
            """
                {
                  "type": "node",
                  "op": "create",
                  "properties": {
                    "id": 1,
                    "foo": "foo-value"
                  }
                }
                """,
        )

    transformer.transform(sinkMessage) shouldBe
        CreateNodeSinkAction(
            labels = emptySet(),
            properties = mapOf("id" to 1, "foo" to "foo-value"),
        )
  }

  @Test
  fun `should update node`() {
    val sinkMessage =
        newMessage(
            Schema.STRING_SCHEMA,
            """
                {
                  "type": "node",
                  "op": "UPDATE",
                  "labels": ["Foo", "Bar"],
                  "ids": {
                    "id": 0
                  },
                  "properties": {
                    "id": 1,
                    "foo": "foo-value"
                  }
                }
                """,
        )

    transformer.transform(sinkMessage) shouldBe
        UpdateNodeSinkAction(
            matcher =
                NodeMatcher.ByLabelsAndProperties(
                    labels = setOf("Foo", "Bar"),
                    properties = mapOf("id" to 0),
                ),
            setProperties = mapOf("id" to 1, "foo" to "foo-value"),
            addLabels = emptySet(),
            removeLabels = emptySet(),
        )
  }

  @Test
  fun `should update node without labels`() {
    val sinkMessage =
        newMessage(
            Schema.STRING_SCHEMA,
            """
                {
                  "type": "node",
                  "op": "UPDATE",
                  "ids": {
                    "id": 0
                  },
                  "properties": {
                    "id": 1,
                    "foo": "foo-value"
                  }
                }
                """,
        )

    transformer.transform(sinkMessage) shouldBe
        UpdateNodeSinkAction(
            matcher =
                NodeMatcher.ByLabelsAndProperties(
                    labels = emptySet(),
                    properties = mapOf("id" to 0),
                ),
            setProperties = mapOf("id" to 1, "foo" to "foo-value"),
            addLabels = emptySet(),
            removeLabels = emptySet(),
        )
  }

  @Test
  fun `should merge node`() {
    val sinkMessage =
        newMessage(
            Schema.STRING_SCHEMA,
            """
                {
                  "type": "NODE",
                  "op": "merge",
                  "labels": ["Foo", "Bar"],
                  "ids": {
                    "id": 0
                  },
                  "properties": {
                    "id": 1,
                    "foo": "foo-value"
                  }
                }
                """,
        )

    transformer.transform(sinkMessage) shouldBe
        MergeNodeSinkAction(
            matcher =
                NodeMatcher.ByLabelsAndProperties(
                    labels = setOf("Foo", "Bar"),
                    properties = mapOf("id" to 0),
                ),
            setProperties = mapOf("id" to 1, "foo" to "foo-value"),
            addLabels = emptySet(),
            removeLabels = emptySet(),
        )
  }

  @Test
  fun `should merge node without labels`() {
    val sinkMessage =
        newMessage(
            Schema.STRING_SCHEMA,
            """
                {
                  "type": "NODE",
                  "op": "merge",
                  "ids": {
                    "id": 0
                  },
                  "properties": {
                    "id": 1,
                    "foo": "foo-value"
                  }
                }
                """,
        )

    transformer.transform(sinkMessage) shouldBe
        MergeNodeSinkAction(
            matcher =
                NodeMatcher.ByLabelsAndProperties(
                    labels = emptySet(),
                    properties = mapOf("id" to 0),
                ),
            setProperties = mapOf("id" to 1, "foo" to "foo-value"),
            addLabels = emptySet(),
            removeLabels = emptySet(),
        )
  }

  @Test
  fun `should delete node`() {
    val sinkMessage =
        newMessage(
            Schema.STRING_SCHEMA,
            """
                {
                  "type": "NODE",
                  "op": "delete",
                  "labels": ["Foo", "Bar"],
                  "ids": {
                    "id": 0
                  },
                  "detach": true
                }
                """,
        )

    transformer.transform(sinkMessage) shouldBe
        DeleteNodeSinkAction(
            matcher =
                NodeMatcher.ByLabelsAndProperties(
                    labels = setOf("Foo", "Bar"),
                    properties = mapOf("id" to 0),
                ),
            detach = true,
        )
  }

  @Test
  fun `should delete node without labels`() {
    val sinkMessage =
        newMessage(
            Schema.STRING_SCHEMA,
            """
                {
                  "type": "NODE",
                  "op": "delete",
                  "ids": {
                    "id": 0
                  },
                  "detach": true
                }
                """,
        )

    transformer.transform(sinkMessage) shouldBe
        DeleteNodeSinkAction(
            matcher =
                NodeMatcher.ByLabelsAndProperties(
                    labels = emptySet(),
                    properties = mapOf("id" to 0),
                ),
            detach = true,
        )
  }

  @Test
  fun `should delete node without detach`() {
    val sinkMessage =
        newMessage(
            Schema.STRING_SCHEMA,
            """
                {
                  "type": "NODE",
                  "op": "delete",
                  "labels": ["Foo", "Bar"],
                  "ids": {
                    "id": 0
                  }                
                }
                """,
        )

    transformer.transform(sinkMessage) shouldBe
        DeleteNodeSinkAction(
            matcher =
                NodeMatcher.ByLabelsAndProperties(
                    labels = setOf("Foo", "Bar"),
                    properties = mapOf("id" to 0),
                ),
            detach = false,
        )
  }

  @Test
  fun `should create relationship`() {
    val sinkMessage =
        newMessage(
            Schema.STRING_SCHEMA,
            """
                {
                  "type": "relationship",
                  "op": "create",
                  "rel_type": "RELATED_TO",
                  "from": {
                    "labels": ["Foo"],
                    "ids": {
                      "id": 0
                    }
                  },
                  "to": {
                    "labels": ["Bar"],
                    "ids": {
                      "id": 1
                    }
                  },
                  "properties": {
                    "by": "incident"
                  }
                }
                """,
        )

    transformer.transform(sinkMessage) shouldBe
        CreateRelationshipSinkAction(
            startNode =
                SinkActionNodeReference(
                    NodeMatcher.ByLabelsAndProperties(setOf("Foo"), mapOf("id" to 0)),
                    LookupMode.MATCH,
                ),
            endNode =
                SinkActionNodeReference(
                    NodeMatcher.ByLabelsAndProperties(setOf("Bar"), mapOf("id" to 1)),
                    LookupMode.MATCH,
                ),
            type = "RELATED_TO",
            properties = mapOf("by" to "incident"),
        )
  }

  @Test
  fun `should create relationship by merging nodes`() {
    val sinkMessage =
        newMessage(
            Schema.STRING_SCHEMA,
            """
                {
                  "type": "relationship",
                  "op": "create",
                  "rel_type": "RELATED_TO",
                  "from": {
                    "labels": ["Foo"],
                    "ids": {
                      "id": 0
                    },
                    "op": "merge"
                  },
                  "to": {
                    "labels": ["Bar"],
                    "ids": {
                      "id": 1
                    },
                    "op": "match"
                  },
                  "properties": {
                    "by": "incident"
                  }
                }
                """,
        )

    transformer.transform(sinkMessage) shouldBe
        CreateRelationshipSinkAction(
            startNode =
                SinkActionNodeReference(
                    NodeMatcher.ByLabelsAndProperties(setOf("Foo"), mapOf("id" to 0)),
                    LookupMode.MERGE,
                ),
            endNode =
                SinkActionNodeReference(
                    NodeMatcher.ByLabelsAndProperties(setOf("Bar"), mapOf("id" to 1)),
                    LookupMode.MATCH,
                ),
            type = "RELATED_TO",
            properties = mapOf("by" to "incident"),
        )
  }

  @Test
  fun `should update relationship`() {
    val sinkMessage =
        newMessage(
            Schema.STRING_SCHEMA,
            """
                {
                  "type": "relationship",
                  "op": "update",
                  "rel_type": "RELATED_TO",
                  "from": {
                    "labels": ["Foo"],
                    "ids": {
                      "id": 0
                    }
                  },
                  "to": {
                    "labels": ["Bar"],
                    "ids": {
                      "id": 1
                    }
                  },
                  "properties": {
                    "by": "incident"
                  }
                }
                """,
        )

    transformer.transform(sinkMessage) shouldBe
        UpdateRelationshipSinkAction(
            startNode =
                SinkActionNodeReference(
                    NodeMatcher.ByLabelsAndProperties(setOf("Foo"), mapOf("id" to 0)),
                    LookupMode.MATCH,
                ),
            endNode =
                SinkActionNodeReference(
                    NodeMatcher.ByLabelsAndProperties(setOf("Bar"), mapOf("id" to 1)),
                    LookupMode.MATCH,
                ),
            matcher =
                RelationshipMatcher.ByTypeAndProperties(
                    type = "RELATED_TO",
                    properties = emptyMap(),
                    hasKeys = false,
                ),
            setProperties = mapOf("by" to "incident"),
        )
  }

  @Test
  fun `should update relationship with ids`() {
    val sinkMessage =
        newMessage(
            Schema.STRING_SCHEMA,
            """
                {
                  "type": "relationship",
                  "op": "update",
                  "rel_type": "RELATED_TO",
                  "from": {
                    "labels": ["Foo"],
                    "ids": {
                      "id": 0
                    }
                  },
                  "to": {
                    "labels": ["Bar"],
                    "ids": {
                      "id": 1
                    }
                  },
                  "ids": {
                    "id": 5
                  },
                  "properties": {
                    "by": "incident"
                  }
                }
                """,
        )

    transformer.transform(sinkMessage) shouldBe
        UpdateRelationshipSinkAction(
            startNode =
                SinkActionNodeReference(
                    NodeMatcher.ByLabelsAndProperties(setOf("Foo"), mapOf("id" to 0)),
                    LookupMode.MATCH,
                ),
            endNode =
                SinkActionNodeReference(
                    NodeMatcher.ByLabelsAndProperties(setOf("Bar"), mapOf("id" to 1)),
                    LookupMode.MATCH,
                ),
            matcher =
                RelationshipMatcher.ByTypeAndProperties(
                    type = "RELATED_TO",
                    properties = mapOf("id" to 5),
                    hasKeys = true,
                ),
            setProperties = mapOf("by" to "incident"),
        )
  }

  @Test
  fun `should merge relationship`() {
    val sinkMessage =
        newMessage(
            Schema.STRING_SCHEMA,
            """
                {
                  "type": "relationship",
                  "op": "merge",
                  "rel_type": "RELATED_TO",
                  "from": {
                    "labels": ["Foo"],
                    "ids": {
                      "id": 0
                    }
                  },
                  "to": {
                    "labels": ["Bar"],
                    "ids": {
                      "id": 1
                    },
                    "op": "merge"
                  },
                  "properties": {
                    "by": "incident"
                  }
                }
                """,
        )

    transformer.transform(sinkMessage) shouldBe
        MergeRelationshipSinkAction(
            startNode =
                SinkActionNodeReference(
                    NodeMatcher.ByLabelsAndProperties(setOf("Foo"), mapOf("id" to 0)),
                    LookupMode.MATCH,
                ),
            endNode =
                SinkActionNodeReference(
                    NodeMatcher.ByLabelsAndProperties(setOf("Bar"), mapOf("id" to 1)),
                    LookupMode.MERGE,
                ),
            matcher =
                RelationshipMatcher.ByTypeAndProperties(
                    type = "RELATED_TO",
                    properties = emptyMap(),
                    hasKeys = false,
                ),
            setProperties = mapOf("by" to "incident"),
        )
  }

  @Test
  fun `should merge relationship with ids`() {
    val sinkMessage =
        newMessage(
            Schema.STRING_SCHEMA,
            """
                {
                  "type": "relationship",
                  "op": "MERGE",
                  "rel_type": "RELATED_TO",
                  "from": {
                    "labels": ["Foo"],
                    "ids": {
                      "id": 0
                    }
                  },
                  "to": {
                    "labels": ["Bar"],
                    "ids": {
                      "id": 1
                    },
                    "op": "merge"
                  },
                  "ids": {
                    "id": 5
                  },
                  "properties": {
                    "by": "incident"
                  }
                }
                """,
        )

    transformer.transform(sinkMessage) shouldBe
        MergeRelationshipSinkAction(
            startNode =
                SinkActionNodeReference(
                    NodeMatcher.ByLabelsAndProperties(setOf("Foo"), mapOf("id" to 0)),
                    LookupMode.MATCH,
                ),
            endNode =
                SinkActionNodeReference(
                    NodeMatcher.ByLabelsAndProperties(setOf("Bar"), mapOf("id" to 1)),
                    LookupMode.MERGE,
                ),
            matcher =
                RelationshipMatcher.ByTypeAndProperties(
                    type = "RELATED_TO",
                    properties = mapOf("id" to 5),
                    hasKeys = true,
                ),
            setProperties = mapOf("by" to "incident"),
        )
  }

  @Test
  fun `should delete relationship`() {
    val sinkMessage =
        newMessage(
            Schema.STRING_SCHEMA,
            """
                {
                  "type": "relationship",
                  "op": "delete",
                  "rel_type": "RELATED_TO",
                  "from": {
                    "labels": ["Foo"],
                    "ids": {
                      "id": 0
                    }
                  },
                  "to": {
                    "labels": ["Bar"],
                    "ids": {
                      "id": 1
                    }
                  }
                }
                """,
        )

    transformer.transform(sinkMessage) shouldBe
        DeleteRelationshipSinkAction(
            startNode =
                SinkActionNodeReference(
                    NodeMatcher.ByLabelsAndProperties(setOf("Foo"), mapOf("id" to 0)),
                    LookupMode.MATCH,
                ),
            endNode =
                SinkActionNodeReference(
                    NodeMatcher.ByLabelsAndProperties(setOf("Bar"), mapOf("id" to 1)),
                    LookupMode.MATCH,
                ),
            matcher =
                RelationshipMatcher.ByTypeAndProperties(
                    type = "RELATED_TO",
                    properties = emptyMap(),
                    hasKeys = false,
                ),
        )
  }

  @Test
  fun `should delete relationship with ids`() {
    val sinkMessage =
        newMessage(
            Schema.STRING_SCHEMA,
            """
                {
                  "type": "relationship",
                  "op": "DELETE",
                  "rel_type": "RELATED_TO",
                  "from": {
                    "labels": ["Foo"],
                    "ids": {
                      "id": 0
                    }
                  },
                  "to": {
                    "labels": ["Bar"],
                    "ids": {
                      "id": 1
                    }
                  },
                  "ids": {
                    "id": 5
                  }
                }
                """,
        )

    transformer.transform(sinkMessage) shouldBe
        DeleteRelationshipSinkAction(
            startNode =
                SinkActionNodeReference(
                    NodeMatcher.ByLabelsAndProperties(setOf("Foo"), mapOf("id" to 0)),
                    LookupMode.MATCH,
                ),
            endNode =
                SinkActionNodeReference(
                    NodeMatcher.ByLabelsAndProperties(setOf("Bar"), mapOf("id" to 1)),
                    LookupMode.MATCH,
                ),
            matcher =
                RelationshipMatcher.ByTypeAndProperties(
                    type = "RELATED_TO",
                    properties = mapOf("id" to 5),
                    hasKeys = true,
                ),
        )
  }
}
