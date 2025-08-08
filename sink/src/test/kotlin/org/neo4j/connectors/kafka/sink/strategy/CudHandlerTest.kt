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

import io.kotest.matchers.shouldBe
import org.apache.kafka.connect.data.Schema
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.neo4j.connectors.kafka.sink.ChangeQuery
import org.neo4j.connectors.kafka.utils.JSONUtils
import org.neo4j.cypherdsl.core.renderer.Renderer
import org.neo4j.cypherdsl.parser.CypherParser
import org.neo4j.driver.Query

class CudHandlerTest : HandlerTest() {

  @Test
  fun `should create node`() {
    val handler = CudHandler("my-topic", Renderer.getDefaultRenderer(), 100)

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
    handler.handle(listOf(sinkMessage)) shouldBe
        listOf(
            listOf(
                ChangeQuery(
                    null,
                    null,
                    listOf(sinkMessage),
                    Query(
                        CypherParser.parse("CREATE (n:`Foo`:`Bar` {}) SET n = ${'$'}properties")
                            .cypher,
                        mapOf("properties" to mapOf("id" to 1, "foo" to "foo-value")),
                    ),
                )
            )
        )
  }

  @Test
  fun `should create node without labels`() {
    val handler = CudHandler("my-topic", Renderer.getDefaultRenderer(), 100)

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
    handler.handle(listOf(sinkMessage)) shouldBe
        listOf(
            listOf(
                ChangeQuery(
                    null,
                    null,
                    listOf(sinkMessage),
                    Query(
                        CypherParser.parse("CREATE (n {}) SET n = ${'$'}properties").cypher,
                        mapOf("properties" to mapOf("id" to 1, "foo" to "foo-value")),
                    ),
                )
            )
        )
  }

  @Test
  fun `should update node`() {
    val handler = CudHandler("my-topic", Renderer.getDefaultRenderer(), 100)

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
    handler.handle(listOf(sinkMessage)) shouldBe
        listOf(
            listOf(
                ChangeQuery(
                    null,
                    null,
                    listOf(sinkMessage),
                    Query(
                        CypherParser.parse(
                                "MATCH (n:`Foo`:`Bar` {id: ${'$'}keys.id}) SET n += ${'$'}properties"
                            )
                            .cypher,
                        mapOf(
                            "keys" to mapOf("id" to 0),
                            "properties" to mapOf("id" to 1, "foo" to "foo-value"),
                        ),
                    ),
                )
            )
        )
  }

  @Test
  fun `should update node without labels`() {
    val handler = CudHandler("my-topic", Renderer.getDefaultRenderer(), 100)

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
    handler.handle(listOf(sinkMessage)) shouldBe
        listOf(
            listOf(
                ChangeQuery(
                    null,
                    null,
                    listOf(sinkMessage),
                    Query(
                        CypherParser.parse(
                                "MATCH (n {id: ${'$'}keys.id}) SET n += ${'$'}properties"
                            )
                            .cypher,
                        mapOf(
                            "keys" to mapOf("id" to 0),
                            "properties" to mapOf("id" to 1, "foo" to "foo-value"),
                        ),
                    ),
                )
            )
        )
  }

  @Test
  fun `should merge node`() {
    val handler = CudHandler("my-topic", Renderer.getDefaultRenderer(), 100)

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
    handler.handle(listOf(sinkMessage)) shouldBe
        listOf(
            listOf(
                ChangeQuery(
                    null,
                    null,
                    listOf(sinkMessage),
                    Query(
                        CypherParser.parse(
                                "MERGE (n:`Foo`:`Bar` {id: ${'$'}keys.id}) SET n += ${'$'}properties"
                            )
                            .cypher,
                        mapOf(
                            "keys" to mapOf("id" to 0),
                            "properties" to mapOf("id" to 1, "foo" to "foo-value"),
                        ),
                    ),
                )
            )
        )
  }

  @Test
  fun `should merge node without labels`() {
    val handler = CudHandler("my-topic", Renderer.getDefaultRenderer(), 100)

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
    handler.handle(listOf(sinkMessage)) shouldBe
        listOf(
            listOf(
                ChangeQuery(
                    null,
                    null,
                    listOf(sinkMessage),
                    Query(
                        CypherParser.parse(
                                "MERGE (n {id: ${'$'}keys.id}) SET n += ${'$'}properties"
                            )
                            .cypher,
                        mapOf(
                            "keys" to mapOf("id" to 0),
                            "properties" to mapOf("id" to 1, "foo" to "foo-value"),
                        ),
                    ),
                )
            )
        )
  }

  @Test
  fun `should delete node`() {
    val handler = CudHandler("my-topic", Renderer.getDefaultRenderer(), 100)

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
    handler.handle(listOf(sinkMessage)) shouldBe
        listOf(
            listOf(
                ChangeQuery(
                    null,
                    null,
                    listOf(sinkMessage),
                    Query(
                        CypherParser.parse(
                                "MATCH (n:`Foo`:`Bar` {id: ${'$'}keys.id}) DETACH DELETE n"
                            )
                            .cypher,
                        mapOf("keys" to mapOf("id" to 0)),
                    ),
                )
            )
        )
  }

  @Test
  fun `should delete node without labels`() {
    val handler = CudHandler("my-topic", Renderer.getDefaultRenderer(), 100)

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
    handler.handle(listOf(sinkMessage)) shouldBe
        listOf(
            listOf(
                ChangeQuery(
                    null,
                    null,
                    listOf(sinkMessage),
                    Query(
                        CypherParser.parse("MATCH (n {id: ${'$'}keys.id}) DETACH DELETE n").cypher,
                        mapOf("keys" to mapOf("id" to 0)),
                    ),
                )
            )
        )
  }

  @Test
  fun `should delete node without detach`() {
    val handler = CudHandler("my-topic", Renderer.getDefaultRenderer(), 100)

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
    handler.handle(listOf(sinkMessage)) shouldBe
        listOf(
            listOf(
                ChangeQuery(
                    null,
                    null,
                    listOf(sinkMessage),
                    Query(
                        CypherParser.parse("MATCH (n:`Foo`:`Bar` {id: ${'$'}keys.id}) DELETE n")
                            .cypher,
                        mapOf("keys" to mapOf("id" to 0)),
                    ),
                )
            )
        )
  }

  @Test
  fun `should create relationship`() {
    val handler = CudHandler("my-topic", Renderer.getDefaultRenderer(), 100)

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
    handler.handle(listOf(sinkMessage)) shouldBe
        listOf(
            listOf(
                ChangeQuery(
                    null,
                    null,
                    listOf(sinkMessage),
                    Query(
                        CypherParser.parse(
                                """
                              MATCH (start:`Foo` {id: ${'$'}start.keys.id}) WITH start
                              MATCH (end:`Bar` {id: ${'$'}end.keys.id}) WITH start, end
                              CREATE (start)-[r:`RELATED_TO`]->(end)
                              SET r = ${'$'}properties
                            """
                            )
                            .cypher,
                        mapOf(
                            "start" to mapOf("keys" to mapOf("id" to 0)),
                            "end" to mapOf("keys" to mapOf("id" to 1)),
                            "properties" to mapOf("by" to "incident"),
                        ),
                    ),
                )
            )
        )
  }

  @Test
  fun `should create relationship by merging nodes`() {
    val handler = CudHandler("my-topic", Renderer.getDefaultRenderer(), 100)

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
    handler.handle(listOf(sinkMessage)) shouldBe
        listOf(
            listOf(
                ChangeQuery(
                    null,
                    null,
                    listOf(sinkMessage),
                    Query(
                        CypherParser.parse(
                                """
                              MERGE (start:`Foo` {id: ${'$'}start.keys.id}) WITH start
                              MATCH (end:`Bar` {id: ${'$'}end.keys.id}) WITH start, end
                              CREATE (start)-[r:`RELATED_TO`]->(end)
                              SET r = ${'$'}properties
                            """
                            )
                            .cypher,
                        mapOf(
                            "start" to mapOf("keys" to mapOf("id" to 0)),
                            "end" to mapOf("keys" to mapOf("id" to 1)),
                            "properties" to mapOf("by" to "incident"),
                        ),
                    ),
                )
            )
        )
  }

  @Test
  fun `should update relationship`() {
    val handler = CudHandler("my-topic", Renderer.getDefaultRenderer(), 100)

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
                    },
                    "op": "merge"
                  },
                  "properties": {
                    "by": "incident"
                  }
                }
                """,
        )
    handler.handle(listOf(sinkMessage)) shouldBe
        listOf(
            listOf(
                ChangeQuery(
                    null,
                    null,
                    listOf(sinkMessage),
                    Query(
                        CypherParser.parse(
                                """
                              MATCH (start:`Foo` {id: ${'$'}start.keys.id}) WITH start
                              MERGE (end:`Bar` {id: ${'$'}end.keys.id}) WITH start, end
                              MATCH (start)-[r:`RELATED_TO` {}]->(end)
                              SET r += ${'$'}properties
                            """
                            )
                            .cypher,
                        mapOf(
                            "start" to mapOf("keys" to mapOf("id" to 0)),
                            "end" to mapOf("keys" to mapOf("id" to 1)),
                            "keys" to emptyMap(),
                            "properties" to mapOf("by" to "incident"),
                        ),
                    ),
                )
            )
        )
  }

  @Test
  fun `should update relationship with ids`() {
    val handler = CudHandler("my-topic", Renderer.getDefaultRenderer(), 100)

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
    handler.handle(listOf(sinkMessage)) shouldBe
        listOf(
            listOf(
                ChangeQuery(
                    null,
                    null,
                    listOf(sinkMessage),
                    Query(
                        CypherParser.parse(
                                """
                              MATCH (start:`Foo` {id: ${'$'}start.keys.id}) WITH start
                              MERGE (end:`Bar` {id: ${'$'}end.keys.id}) WITH start, end
                              MATCH (start)-[r:`RELATED_TO` {id: ${'$'}keys.id}]->(end)
                              SET r += ${'$'}properties
                            """
                            )
                            .cypher,
                        mapOf(
                            "start" to mapOf("keys" to mapOf("id" to 0)),
                            "end" to mapOf("keys" to mapOf("id" to 1)),
                            "keys" to mapOf("id" to 5),
                            "properties" to mapOf("by" to "incident"),
                        ),
                    ),
                )
            )
        )
  }

  @Test
  fun `should merge relationship`() {
    val handler = CudHandler("my-topic", Renderer.getDefaultRenderer(), 100)

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
    handler.handle(listOf(sinkMessage)) shouldBe
        listOf(
            listOf(
                ChangeQuery(
                    null,
                    null,
                    listOf(sinkMessage),
                    Query(
                        CypherParser.parse(
                                """
                              MATCH (start:`Foo` {id: ${'$'}start.keys.id}) WITH start
                              MERGE (end:`Bar` {id: ${'$'}end.keys.id}) WITH start, end
                              MERGE (start)-[r:`RELATED_TO` {}]->(end)
                              SET r += ${'$'}properties
                            """
                            )
                            .cypher,
                        mapOf(
                            "start" to mapOf("keys" to mapOf("id" to 0)),
                            "end" to mapOf("keys" to mapOf("id" to 1)),
                            "keys" to emptyMap(),
                            "properties" to mapOf("by" to "incident"),
                        ),
                    ),
                )
            )
        )
  }

  @Test
  fun `should merge relationship with ids`() {
    val handler = CudHandler("my-topic", Renderer.getDefaultRenderer(), 100)

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
    handler.handle(listOf(sinkMessage)) shouldBe
        listOf(
            listOf(
                ChangeQuery(
                    null,
                    null,
                    listOf(sinkMessage),
                    Query(
                        CypherParser.parse(
                                """
                              MATCH (start:`Foo` {id: ${'$'}start.keys.id}) WITH start
                              MERGE (end:`Bar` {id: ${'$'}end.keys.id}) WITH start, end
                              MERGE (start)-[r:`RELATED_TO` {id: ${'$'}keys.id}]->(end)
                              SET r += ${'$'}properties
                            """
                            )
                            .cypher,
                        mapOf(
                            "start" to mapOf("keys" to mapOf("id" to 0)),
                            "end" to mapOf("keys" to mapOf("id" to 1)),
                            "keys" to mapOf("id" to 5),
                            "properties" to mapOf("by" to "incident"),
                        ),
                    ),
                )
            )
        )
  }

  @Test
  fun `should delete relationship`() {
    val handler = CudHandler("my-topic", Renderer.getDefaultRenderer(), 100)

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
    handler.handle(listOf(sinkMessage)) shouldBe
        listOf(
            listOf(
                ChangeQuery(
                    null,
                    null,
                    listOf(sinkMessage),
                    Query(
                        CypherParser.parse(
                                """
                              MATCH (start:`Foo` {id: ${'$'}start.keys.id}) WITH start
                              MATCH (end:`Bar` {id: ${'$'}end.keys.id}) WITH start, end
                              MATCH (start)-[r:`RELATED_TO` {}]->(end)
                              DELETE r
                            """
                            )
                            .cypher,
                        mapOf(
                            "start" to mapOf("keys" to mapOf("id" to 0)),
                            "end" to mapOf("keys" to mapOf("id" to 1)),
                            "keys" to emptyMap(),
                        ),
                    ),
                )
            )
        )
  }

  @Test
  fun `should delete relationship with ids`() {
    val handler = CudHandler("my-topic", Renderer.getDefaultRenderer(), 100)

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
    handler.handle(listOf(sinkMessage)) shouldBe
        listOf(
            listOf(
                ChangeQuery(
                    null,
                    null,
                    listOf(sinkMessage),
                    Query(
                        CypherParser.parse(
                                """
                              MATCH (start:`Foo` {id: ${'$'}start.keys.id}) WITH start
                              MATCH (end:`Bar` {id: ${'$'}end.keys.id}) WITH start, end
                              MATCH (start)-[r:`RELATED_TO` {id: ${'$'}keys.id}]->(end)
                              DELETE r
                            """
                            )
                            .cypher,
                        mapOf(
                            "start" to mapOf("keys" to mapOf("id" to 0)),
                            "end" to mapOf("keys" to mapOf("id" to 1)),
                            "keys" to mapOf("id" to 5),
                        ),
                    ),
                )
            )
        )
  }

  @ParameterizedTest
  @ValueSource(ints = [1, 6, 25, 100])
  fun `should support mixed operations with different batch sizes`(batchSize: Int) {
    val modulo = 4
    val handler = CudHandler("my-topic", Renderer.getDefaultRenderer(), batchSize)

    val messages =
        (0..<100)
            .map {
              val id = it / modulo
              when (it % modulo) {
                0 ->
                    mapOf(
                        "type" to "NODE",
                        "op" to "CREATE",
                        "labels" to listOf("Foo"),
                        "properties" to mapOf("id" to id),
                    )
                1 ->
                    mapOf(
                        "type" to "NODE",
                        "op" to "UPDATE",
                        "labels" to listOf("Foo"),
                        "ids" to mapOf("id" to id),
                        "properties" to mapOf("id" to id, "prop1" to "a value"),
                    )
                2 ->
                    mapOf(
                        "type" to "NODE",
                        "op" to "MERGE",
                        "labels" to listOf("Foo"),
                        "ids" to mapOf("id" to id),
                        "properties" to
                            mapOf("id" to id, "prop1" to "a value", "prop2" to "b value"),
                    )
                3 ->
                    mapOf(
                        "type" to "NODE",
                        "op" to "DELETE",
                        "labels" to listOf("Foo"),
                        "ids" to mapOf("id" to id),
                    )
                else -> throw IllegalArgumentException("unexpected")
              }
            }
            .map { newMessage(Schema.STRING_SCHEMA, JSONUtils.writeValueAsString(it)) }

    val result = handler.handle(messages)

    result shouldBe
        (0..<100)
            .map {
              val id = it / modulo

              ChangeQuery(
                  null,
                  null,
                  listOf(messages[it]),
                  when (it % modulo) {
                    0 ->
                        Query(
                            "CREATE (n:`Foo` {}) SET n = ${'$'}properties",
                            mapOf("properties" to mapOf("id" to id)),
                        )
                    1 ->
                        Query(
                            "MATCH (n:`Foo` {id: ${'$'}keys.id}) SET n += ${'$'}properties",
                            mapOf(
                                "keys" to mapOf("id" to id),
                                "properties" to mapOf("id" to id, "prop1" to "a value"),
                            ),
                        )
                    2 ->
                        Query(
                            "MERGE (n:`Foo` {id: ${'$'}keys.id}) SET n += ${'$'}properties",
                            mapOf(
                                "keys" to mapOf("id" to id),
                                "properties" to
                                    mapOf("id" to id, "prop1" to "a value", "prop2" to "b value"),
                            ),
                        )
                    3 ->
                        Query(
                            "MATCH (n:`Foo` {id: ${'$'}keys.id}) DELETE n",
                            mapOf("keys" to mapOf("id" to id)),
                        )
                    else -> throw IllegalArgumentException("unexpected")
                  },
              )
            }
            .chunked(batchSize)
  }

  @ParameterizedTest
  @ValueSource(ints = [1, 6, 25, 100])
  fun `should support mixed relationship operations`(batchSize: Int) {
    val modulo = 4
    val handler = CudHandler("my-topic", Renderer.getDefaultRenderer(), batchSize)

    val messages =
        (0..<100)
            .map {
              val id = it / modulo
              when (it % modulo) {
                0 ->
                    mapOf(
                        "type" to "RELATIONSHIP",
                        "op" to "CREATE",
                        "rel_type" to "RELATED_TO",
                        "from" to mapOf("labels" to listOf("Foo"), "ids" to mapOf("id" to id)),
                        "to" to mapOf("labels" to listOf("Bar"), "ids" to mapOf("id" to id)),
                        "properties" to mapOf("id" to id),
                    )
                1 ->
                    mapOf(
                        "type" to "RELATIONSHIP",
                        "op" to "UPDATE",
                        "rel_type" to "RELATED_TO",
                        "from" to mapOf("labels" to listOf("Foo"), "ids" to mapOf("id" to id)),
                        "to" to mapOf("labels" to listOf("Bar"), "ids" to mapOf("id" to id)),
                        "properties" to mapOf("id" to id, "prop1" to "a value"),
                    )
                2 ->
                    mapOf(
                        "type" to "relationship",
                        "op" to "merge",
                        "rel_type" to "RELATED_TO",
                        "from" to mapOf("labels" to listOf("Foo"), "ids" to mapOf("id" to id)),
                        "to" to mapOf("labels" to listOf("Bar"), "ids" to mapOf("id" to id)),
                        "properties" to
                            mapOf("id" to id, "prop1" to "a value", "prop2" to "b value"),
                    )
                3 ->
                    mapOf(
                        "type" to "relationship",
                        "op" to "delete",
                        "rel_type" to "RELATED_TO",
                        "from" to mapOf("labels" to listOf("Foo"), "ids" to mapOf("id" to id)),
                        "to" to mapOf("labels" to listOf("Bar"), "ids" to mapOf("id" to id)),
                    )
                else -> throw IllegalArgumentException("unexpected")
              }
            }
            .map { newMessage(Schema.STRING_SCHEMA, JSONUtils.writeValueAsString(it)) }

    val result = handler.handle(messages)

    result shouldBe
        (0..<100)
            .map {
              val id = it / modulo

              ChangeQuery(
                  null,
                  null,
                  listOf(messages[it]),
                  when (it % modulo) {
                    0 ->
                        Query(
                            "MATCH (start:`Foo` {id: ${'$'}start.keys.id}) WITH start MATCH (end:`Bar` {id: ${'$'}end.keys.id}) WITH start, end CREATE (start)-[r:`RELATED_TO`]->(end) SET r = ${'$'}properties",
                            mapOf(
                                "start" to mapOf("keys" to mapOf("id" to id)),
                                "end" to mapOf("keys" to mapOf("id" to id)),
                                "properties" to mapOf("id" to id),
                            ),
                        )
                    1 ->
                        Query(
                            "MATCH (start:`Foo` {id: ${'$'}start.keys.id}) WITH start MATCH (end:`Bar` {id: ${'$'}end.keys.id}) WITH start, end MATCH (start)-[r:`RELATED_TO` {}]->(end) SET r += ${'$'}properties",
                            mapOf(
                                "start" to mapOf("keys" to mapOf("id" to id)),
                                "end" to mapOf("keys" to mapOf("id" to id)),
                                "keys" to emptyMap(),
                                "properties" to mapOf("id" to id, "prop1" to "a value"),
                            ),
                        )
                    2 ->
                        Query(
                            "MATCH (start:`Foo` {id: ${'$'}start.keys.id}) WITH start MATCH (end:`Bar` {id: ${'$'}end.keys.id}) WITH start, end MERGE (start)-[r:`RELATED_TO` {}]->(end) SET r += ${'$'}properties",
                            mapOf(
                                "start" to mapOf("keys" to mapOf("id" to id)),
                                "end" to mapOf("keys" to mapOf("id" to id)),
                                "keys" to emptyMap(),
                                "properties" to
                                    mapOf("id" to id, "prop1" to "a value", "prop2" to "b value"),
                            ),
                        )
                    3 ->
                        Query(
                            "MATCH (start:`Foo` {id: ${'$'}start.keys.id}) WITH start MATCH (end:`Bar` {id: ${'$'}end.keys.id}) WITH start, end MATCH (start)-[r:`RELATED_TO` {}]->(end) DELETE r",
                            mapOf(
                                "start" to mapOf("keys" to mapOf("id" to id)),
                                "end" to mapOf("keys" to mapOf("id" to id)),
                                "keys" to emptyMap(),
                            ),
                        )
                    else -> throw IllegalArgumentException("unexpected")
                  },
              )
            }
            .chunked(batchSize)
  }
}
