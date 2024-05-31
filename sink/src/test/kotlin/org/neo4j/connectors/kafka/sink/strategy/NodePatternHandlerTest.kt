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
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneOffset
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.junit.jupiter.api.Test
import org.neo4j.connectors.kafka.data.DynamicTypes
import org.neo4j.connectors.kafka.data.SimpleTypes
import org.neo4j.connectors.kafka.sink.ChangeQuery
import org.neo4j.cypherdsl.core.renderer.Renderer
import org.neo4j.cypherdsl.parser.CypherParser
import org.neo4j.driver.Query

class NodePatternHandlerTest : HandlerTest() {

  @Test
  fun `should construct correct query for simple pattern`() {
    val handler =
        NodePatternHandler(
            "my-topic",
            "(:ALabel{!id})",
            mergeProperties = true,
            renderer = Renderer.getDefaultRenderer(),
            batchSize = 1)

    handler.query shouldBe
        CypherParser.parse(
                """
                UNWIND ${'$'}events AS event 
                CALL { WITH event
                  WITH event WHERE event[0] = 'C'
                  WITH event[1] AS event
                  MERGE (n:`ALabel` {id: event.keys.id})
                  SET n += event.properties
                  RETURN count(n) AS created
                } 
                CALL { WITH event
                  WITH event WHERE event[0] = 'D'
                  WITH event[1] AS event 
                  MATCH (n:`ALabel` {id: event.keys.id})
                  DETACH DELETE n
                  RETURN count(n) AS deleted
                } 
                RETURN sum(created) AS created, sum(deleted) AS deleted                 
                  """
                    .trimIndent())
            .cypher
  }

  @Test
  fun `should construct correct query for aliased properties`() {
    val handler =
        NodePatternHandler(
            "my-topic",
            "(:ALabel{!id: aProperty})",
            mergeProperties = true,
            renderer = Renderer.getDefaultRenderer(),
            batchSize = 1)

    handler.query shouldBe
        CypherParser.parse(
                """
                UNWIND ${'$'}events AS event 
                CALL { WITH event
                  WITH event WHERE event[0] = 'C'
                  WITH event[1] AS event
                  MERGE (n:`ALabel` {id: event.keys.id})
                  SET n += event.properties
                  RETURN count(n) AS created
                } 
                CALL { WITH event
                  WITH event WHERE event[0] = 'D'
                  WITH event[1] AS event 
                  MATCH (n:`ALabel` {id: event.keys.id})
                  DETACH DELETE n
                  RETURN count(n) AS deleted
                } 
                RETURN sum(created) AS created, sum(deleted) AS deleted                 
                  """
                    .trimIndent())
            .cypher
  }

  @Test
  fun `should construct correct query for composite keys`() {
    val handler =
        NodePatternHandler(
            "my-topic",
            "(:ALabel{!idA: aProperty, !idB: bProperty})",
            mergeProperties = true,
            renderer = Renderer.getDefaultRenderer(),
            batchSize = 1)

    handler.query shouldBe
        CypherParser.parse(
                """
                UNWIND ${'$'}events AS event 
                CALL { WITH event
                  WITH event WHERE event[0] = 'C'
                  WITH event[1] AS event
                  MERGE (n:`ALabel` {idA: event.keys.idA, idB: event.keys.idB})
                  SET n += event.properties
                  RETURN count(n) AS created
                } 
                CALL { WITH event
                  WITH event WHERE event[0] = 'D'
                  WITH event[1] AS event 
                  MATCH (n:`ALabel` {idA: event.keys.idA, idB: event.keys.idB})
                  DETACH DELETE n
                  RETURN count(n) AS deleted
                } 
                RETURN sum(created) AS created, sum(deleted) AS deleted                 
                  """
                    .trimIndent())
            .cypher
  }

  @Test
  fun `should construct correct query without merging properties`() {
    val handler =
        NodePatternHandler(
            "my-topic",
            "(:ALabel{!id: aProperty})",
            mergeProperties = false,
            renderer = Renderer.getDefaultRenderer(),
            batchSize = 1)

    handler.query shouldBe
        CypherParser.parse(
                """
                UNWIND ${'$'}events AS event 
                CALL { WITH event
                  WITH event WHERE event[0] = 'C'
                  WITH event[1] AS event
                  MERGE (n:`ALabel` {id: event.keys.id})
                  SET n = event.properties
                  SET n += event.keys
                  RETURN count(n) AS created
                } 
                CALL { WITH event
                  WITH event WHERE event[0] = 'D'
                  WITH event[1] AS event 
                  MATCH (n:`ALabel` {id: event.keys.id})
                  DETACH DELETE n
                  RETURN count(n) AS deleted
                } 
                RETURN sum(created) AS created, sum(deleted) AS deleted                 
                  """
                    .trimIndent())
            .cypher
  }

  @Test
  fun `should construct correct query with multiple labels`() {
    val handler =
        NodePatternHandler(
            "my-topic",
            "(:ALabel:BLabel{!id: aProperty})",
            mergeProperties = false,
            renderer = Renderer.getDefaultRenderer(),
            batchSize = 1)

    handler.query shouldBe
        CypherParser.parse(
                """
                UNWIND ${'$'}events AS event 
                CALL { WITH event
                  WITH event WHERE event[0] = 'C'
                  WITH event[1] AS event
                  MERGE (n:`ALabel`:`BLabel` {id: event.keys.id})
                  SET n = event.properties
                  SET n += event.keys
                  RETURN count(n) AS created
                } 
                CALL { WITH event
                  WITH event WHERE event[0] = 'D'
                  WITH event[1] AS event 
                  MATCH (n:`ALabel`:`BLabel` {id: event.keys.id})
                  DETACH DELETE n
                  RETURN count(n) AS deleted
                } 
                RETURN sum(created) AS created, sum(deleted) AS deleted                 
                  """
                    .trimIndent())
            .cypher
  }

  @Test
  fun `should construct correct query with escaped names`() {
    val handler =
        NodePatternHandler(
            "my-topic",
            "(:`ALabel With Space`:BLabel{ !id: aProperty, !`another id`: bProperty, name: `another property`, `last name`: last_name})",
            mergeProperties = false,
            renderer = Renderer.getDefaultRenderer(),
            batchSize = 1)

    handler.query shouldBe
        CypherParser.parse(
                """
                UNWIND ${'$'}events AS event 
                CALL { WITH event
                  WITH event WHERE event[0] = 'C'
                  WITH event[1] AS event
                  MERGE (n:`ALabel With Space`:`BLabel` {id: event.keys.id, `another id`: event.keys.`another id`})
                  SET n = event.properties
                  SET n += event.keys
                  RETURN count(n) AS created
                } 
                CALL { WITH event
                  WITH event WHERE event[0] = 'D'
                  WITH event[1] AS event 
                  MATCH (n:`ALabel With Space`:`BLabel` {id: event.keys.id, `another id`: event.keys.`another id`})
                  DETACH DELETE n
                  RETURN count(n) AS deleted
                } 
                RETURN sum(created) AS created, sum(deleted) AS deleted                 
                  """
                    .trimIndent())
            .cypher
  }

  @Test
  fun `should include all properties`() {
    assertQueryAndParameters(
        "(:ALabel {!id})",
        value = """{"id": 1, "name": "john", "surname": "doe", "dob": "2000-01-01"}""",
        expected =
            listOf(
                listOf(
                    "C",
                    mapOf(
                        "keys" to mapOf("id" to 1),
                        "properties" to
                            mapOf<String, Any?>(
                                "name" to "john", "surname" to "doe", "dob" to "2000-01-01")))))
  }

  @Test
  fun `should remap key properties from message value fields to message key fields for tombstone messages`() {
    assertQueryAndParameters(
        "(:ALabel{!id: __value.old_id})",
        key = """{"old_id": 1}""",
        expected = listOf(listOf("D", mapOf("keys" to mapOf("id" to 1)))))
  }

  @Test
  fun `should include all properties with structs`() {
    val schema =
        SchemaBuilder.struct()
            .field("id", Schema.INT32_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .field("surname", Schema.STRING_SCHEMA)
            .field("dob", SimpleTypes.LOCALDATE_STRUCT.schema)
            .build()

    assertQueryAndParameters(
        "(:ALabel {!id})",
        valueSchema = schema,
        value =
            Struct(schema)
                .put("id", 1)
                .put("name", "john")
                .put("surname", "doe")
                .put(
                    "dob",
                    DynamicTypes.toConnectValue(
                        SimpleTypes.LOCALDATE_STRUCT.schema, LocalDate.of(2000, 1, 1))),
        expected =
            listOf(
                listOf(
                    "C",
                    mapOf(
                        "keys" to mapOf("id" to 1),
                        "properties" to
                            mapOf<String, Any?>(
                                "name" to "john",
                                "surname" to "doe",
                                "dob" to LocalDate.of(2000, 1, 1))))))
  }

  @Test
  fun `should include nested properties`() {
    assertQueryAndParameters(
        "(:ALabel {!id})",
        value =
            """{"id": 1, "name": "john", "surname": "doe", "dob": "2000-01-01", "address": {"city": "london", "country": "uk"}}""",
        expected =
            listOf(
                listOf(
                    "C",
                    mapOf(
                        "keys" to mapOf("id" to 1),
                        "properties" to
                            mapOf<String, Any?>(
                                "name" to "john",
                                "surname" to "doe",
                                "dob" to "2000-01-01",
                                "address.city" to "london",
                                "address.country" to "uk"),
                    ))))
  }

  @Test
  fun `should include properties`() {
    assertQueryAndParameters(
        "(:ALabel {!id,surname,address.country})",
        value =
            """{"id": 1, "name": "john", "surname": "doe", "dob": "2000-01-01", "address": {"city": "london", "country": "uk"}}""",
        expected =
            listOf(
                listOf(
                    "C",
                    mapOf(
                        "keys" to mapOf("id" to 1),
                        "properties" to
                            mapOf<String, Any?>("surname" to "doe", "address.country" to "uk"),
                    ))))
  }

  @Test
  fun `should include nested properties by hierarchy`() {
    assertQueryAndParameters(
        "(:ALabel {!id, address})",
        value =
            """{"id": 1, "name": "john", "surname": "doe", "dob": "2000-01-01", "address": {"city": "london", "country": "uk"}}""",
        expected =
            listOf(
                listOf(
                    "C",
                    mapOf(
                        "keys" to mapOf("id" to 1),
                        "properties" to
                            mapOf<String, Any?>(
                                "address.city" to "london", "address.country" to "uk"),
                    ))))
  }

  @Test
  fun `should exclude properties`() {
    assertQueryAndParameters(
        "(:ALabel {!id, -name, -surname})",
        value =
            """{"id": 1, "name": "john", "surname": "doe", "dob": "2000-01-01", "address": {"city": "london", "country": "uk"}}""",
        expected =
            listOf(
                listOf(
                    "C",
                    mapOf(
                        "keys" to mapOf("id" to 1),
                        "properties" to
                            mapOf<String, Any?>(
                                "dob" to "2000-01-01",
                                "address.country" to "uk",
                                "address.city" to "london"),
                    ))))
  }

  @Test
  fun `should exclude nested properties`() {
    assertQueryAndParameters(
        "(:ALabel {!id, -address.city})",
        value =
            """{"id": 1, "name": "john", "surname": "doe", "dob": "2000-01-01", "address": {"city": "london", "country": "uk"}}""",
        expected =
            listOf(
                listOf(
                    "C",
                    mapOf(
                        "keys" to mapOf("id" to 1),
                        "properties" to
                            mapOf<String, Any?>(
                                "name" to "john",
                                "surname" to "doe",
                                "dob" to "2000-01-01",
                                "address.country" to "uk"),
                    ))))
  }

  @Test
  fun `should exclude nested properties by hierarchy`() {
    assertQueryAndParameters(
        "(:ALabel {!id, -address})",
        value =
            """{"id": 1, "name": "john", "surname": "doe", "dob": "2000-01-01", "address": {"city": "london", "country": "uk"}}""",
        expected =
            listOf(
                listOf(
                    "C",
                    mapOf(
                        "keys" to mapOf("id" to 1),
                        "properties" to
                            mapOf<String, Any?>(
                                "name" to "john", "surname" to "doe", "dob" to "2000-01-01"),
                    ))))
  }

  @Test
  fun `should alias properties`() {
    assertQueryAndParameters(
        "(:ALabel {!id, first_name: name, last_name: surname})",
        value =
            """{"id": 1, "name": "john", "surname": "doe", "dob": "2000-01-01", "address": {"city": "london", "country": "uk"}}""",
        expected =
            listOf(
                listOf(
                    "C",
                    mapOf(
                        "keys" to mapOf("id" to 1),
                        "properties" to
                            mapOf<String, Any?>("first_name" to "john", "last_name" to "doe"),
                    ))))
  }

  @Test
  fun `should alias nested properties`() {
    assertQueryAndParameters(
        "(:ALabel {!id, first_name: name, last_name: surname, lives_in: address.city})",
        value =
            """{"id": 1, "name": "john", "surname": "doe", "dob": "2000-01-01", "address": {"city": "london", "country": "uk"}}""",
        expected =
            listOf(
                listOf(
                    "C",
                    mapOf(
                        "keys" to mapOf("id" to 1),
                        "properties" to
                            mapOf<String, Any?>(
                                "first_name" to "john",
                                "last_name" to "doe",
                                "lives_in" to "london"),
                    ))))
  }

  @Test
  fun `should alias nested properties by hierarchy`() {
    assertQueryAndParameters(
        "(:ALabel {!id, first_name: name, last_name: surname, home_address: address})",
        value =
            """{"id": 1, "name": "john", "surname": "doe", "dob": "2000-01-01", "address": {"city": "london", "country": "uk"}}""",
        expected =
            listOf(
                listOf(
                    "C",
                    mapOf(
                        "keys" to mapOf("id" to 1),
                        "properties" to
                            mapOf<String, Any?>(
                                "first_name" to "john",
                                "last_name" to "doe",
                                "home_address.city" to "london",
                                "home_address.country" to "uk"),
                    ))))
  }

  @Test
  fun `should include explicit properties`() {
    assertQueryAndParameters(
        "(:ALabel {!id, name: __value.name, surname: __value.surname})",
        value =
            """{"id": 1, "name": "john", "surname": "doe", "dob": "2000-01-01", "address": {"city": "london", "country": "uk"}}""",
        expected =
            listOf(
                listOf(
                    "C",
                    mapOf(
                        "keys" to mapOf("id" to 1),
                        "properties" to
                            mapOf<String, Any?>("name" to "john", "surname" to "doe")))))
  }

  @Test
  fun `should include explicit nested properties`() {
    assertQueryAndParameters(
        "(:ALabel {!id, name: __value.name, surname: __value.surname, city: __value.address.city})",
        value =
            """{"id": 1, "name": "john", "surname": "doe", "dob": "2000-01-01", "address": {"city": "london", "country": "uk"}}""",
        expected =
            listOf(
                listOf(
                    "C",
                    mapOf(
                        "keys" to mapOf("id" to 1),
                        "properties" to
                            mapOf<String, Any?>(
                                "name" to "john", "surname" to "doe", "city" to "london")))))
  }

  @Test
  fun `should include explicit nested properties by hierarchy`() {
    assertQueryAndParameters(
        "(:ALabel {!id, name: __value.name, surname: __value.surname, home_address: __value.address})",
        value =
            """{"id": 1, "name": "john", "surname": "doe", "dob": "2000-01-01", "address": {"city": "london", "country": "uk"}}""",
        expected =
            listOf(
                listOf(
                    "C",
                    mapOf(
                        "keys" to mapOf("id" to 1),
                        "properties" to
                            mapOf<String, Any?>(
                                "name" to "john",
                                "surname" to "doe",
                                "home_address.city" to "london",
                                "home_address.country" to "uk"),
                    ))))
  }

  @Test
  fun `should be able to mix implicit and explicit properties`() {
    assertQueryAndParameters(
        "(:ALabel {!id, name, last_name: surname, home_address: __value.address})",
        value =
            """{"id": 1, "name": "john", "surname": "doe", "dob": "2000-01-01", "address": {"city": "london", "country": "uk"}}""",
        expected =
            listOf(
                listOf(
                    "C",
                    mapOf(
                        "keys" to mapOf("id" to 1),
                        "properties" to
                            mapOf<String, Any?>(
                                "name" to "john",
                                "last_name" to "doe",
                                "home_address.city" to "london",
                                "home_address.country" to "uk"),
                    ))))
  }

  @Test
  fun `should be able to use other message fields`() {
    assertQueryAndParameters(
        "(:ALabel {!id: __key.id, name: __value.name, surname: __value.surname, created_at: __timestamp})",
        key = """{"id": 1}""",
        value =
            """{"name": "john", "surname": "doe", "dob": "2000-01-01", "address": {"city": "london", "country": "uk"}}""",
        expected =
            listOf(
                listOf(
                    "C",
                    mapOf(
                        "keys" to mapOf("id" to 1),
                        "properties" to
                            mapOf<String, Any?>(
                                "name" to "john",
                                "surname" to "doe",
                                "created_at" to
                                    Instant.ofEpochMilli(TIMESTAMP).atOffset(ZoneOffset.UTC)),
                    ))))
  }

  @Test
  fun `should be able to delete`() {
    assertQueryAndParameters(
        "(:ALabel {!id: __key.id, name: __value.name, surname: __value.surname, created_at: __timestamp})",
        key = """{"id": 1}""",
        expected =
            listOf(
                listOf(
                    "D",
                    mapOf(
                        "keys" to mapOf("id" to 1),
                    ))))
  }

  @Test
  fun `should be able to delete with structs`() {
    val schema = SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).build()

    assertQueryAndParameters(
        "(:ALabel {!id: __key.id, name: __value.name, surname: __value.surname, created_at: __timestamp})",
        keySchema = schema,
        key = Struct(schema).put("id", 1),
        expected =
            listOf(
                listOf(
                    "D",
                    mapOf(
                        "keys" to mapOf("id" to 1),
                    ))))
  }

  @Test
  fun `should be able to delete with implicit key`() {
    assertQueryAndParameters(
        "(:ALabel {!id, name: __value.name, surname: __value.surname, created_at: __timestamp})",
        key = """{"id": 1}""",
        expected =
            listOf(
                listOf(
                    "D",
                    mapOf(
                        "keys" to mapOf("id" to 1),
                    ))))
  }

  private fun assertQueryAndParameters(
      pattern: String,
      keySchema: Schema = Schema.STRING_SCHEMA,
      key: Any? = null,
      valueSchema: Schema = Schema.STRING_SCHEMA,
      value: Any? = null,
      expected: List<List<Any>> = emptyList()
  ) {
    val handler = NodePatternHandler("my-topic", pattern, false, Renderer.getDefaultRenderer(), 1)
    handler.handle(
        listOf(
            newMessage(valueSchema, value, keySchema = keySchema, key = key),
        ),
    ) shouldBe
        listOf(
            listOf(
                ChangeQuery(
                    null,
                    null,
                    Query(
                        CypherParser.parse(
                                """
                            UNWIND ${'$'}events AS event 
                            CALL { WITH event
                              WITH event WHERE event[0] = 'C'
                              WITH event[1] AS event
                              MERGE (n:`ALabel` {id: event.keys.id})
                              SET n = event.properties
                              SET n += event.keys
                              RETURN count(n) AS created
                            } 
                            CALL { WITH event
                              WITH event WHERE event[0] = 'D'
                              WITH event[1] AS event 
                              MATCH (n:`ALabel` {id: event.keys.id})
                              DETACH DELETE n
                              RETURN count(n) AS deleted
                            } 
                            RETURN sum(created) AS created, sum(deleted) AS deleted
                          """
                                    .trimIndent(),
                            )
                            .cypher,
                        mapOf("events" to expected),
                    ),
                ),
            ),
        )
  }
}
