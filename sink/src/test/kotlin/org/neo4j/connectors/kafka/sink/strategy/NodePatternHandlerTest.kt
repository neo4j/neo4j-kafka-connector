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
import io.kotest.matchers.throwable.shouldHaveMessage
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneOffset
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.neo4j.connectors.kafka.data.ConstraintData
import org.neo4j.connectors.kafka.data.ConstraintEntityType
import org.neo4j.connectors.kafka.data.ConstraintType
import org.neo4j.connectors.kafka.data.PropertyType
import org.neo4j.connectors.kafka.data.PropertyType.schema
import org.neo4j.connectors.kafka.data.converter.ExtendedValueConverter
import org.neo4j.connectors.kafka.exceptions.InvalidDataException
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
            batchSize = 1,
        )

    handler.query shouldBe
        CypherParser.parse(
                """
                UNWIND ${'$'}events AS event 
                CALL { WITH event 
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
                  RETURN created, deleted
                }
                RETURN sum(created) AS created, sum(deleted) AS deleted                 
                  """
                    .trimIndent()
            )
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
            batchSize = 1,
        )

    handler.query shouldBe
        CypherParser.parse(
                """
                UNWIND ${'$'}events AS event 
                CALL { WITH event 
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
                  RETURN created, deleted
                }
                RETURN sum(created) AS created, sum(deleted) AS deleted                 
                  """
                    .trimIndent()
            )
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
            batchSize = 1,
        )

    handler.query shouldBe
        CypherParser.parse(
                """
                UNWIND ${'$'}events AS event 
                CALL { WITH event 
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
                  RETURN created, deleted
                }
                RETURN sum(created) AS created, sum(deleted) AS deleted                 
                  """
                    .trimIndent()
            )
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
            batchSize = 1,
        )

    handler.query shouldBe
        CypherParser.parse(
                """
                UNWIND ${'$'}events AS event 
                CALL { WITH event 
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
                  RETURN created, deleted
                }
                RETURN sum(created) AS created, sum(deleted) AS deleted                 
                  """
                    .trimIndent()
            )
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
            batchSize = 1,
        )

    handler.query shouldBe
        CypherParser.parse(
                """
                UNWIND ${'$'}events AS event 
                CALL { WITH event 
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
                  RETURN created, deleted
                }
                RETURN sum(created) AS created, sum(deleted) AS deleted                 
                  """
                    .trimIndent()
            )
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
            batchSize = 1,
        )

    handler.query shouldBe
        CypherParser.parse(
                """
                UNWIND ${'$'}events AS event 
                CALL { WITH event 
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
                  RETURN created, deleted
                }
                RETURN sum(created) AS created, sum(deleted) AS deleted                 
                  """
                    .trimIndent()
            )
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
                                "name" to "john",
                                "surname" to "doe",
                                "dob" to "2000-01-01",
                            ),
                    ),
                )
            ),
    )
  }

  @Test
  fun `should remap key properties from message value fields to message key fields for tombstone messages`() {
    assertQueryAndParameters(
        "(:ALabel{!id: __value.old_id})",
        key = """{"old_id": 1}""",
        expected = listOf(listOf("D", mapOf("keys" to mapOf("id" to 1)))),
    )
  }

  @Test
  fun `should include all properties with structs`() {
    val schema =
        SchemaBuilder.struct()
            .field("id", Schema.INT32_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .field("surname", Schema.STRING_SCHEMA)
            .field("dob", PropertyType.schema)
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
                    ExtendedValueConverter().value(PropertyType.schema, LocalDate.of(2000, 1, 1)),
                ),
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
                                "dob" to LocalDate.of(2000, 1, 1),
                            ),
                    ),
                )
            ),
    )
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
                                "address.country" to "uk",
                            ),
                    ),
                )
            ),
    )
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
                    ),
                )
            ),
    )
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
                                "address.city" to "london",
                                "address.country" to "uk",
                            ),
                    ),
                )
            ),
    )
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
                                "address.city" to "london",
                            ),
                    ),
                )
            ),
    )
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
                                "address.country" to "uk",
                            ),
                    ),
                )
            ),
    )
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
                                "name" to "john",
                                "surname" to "doe",
                                "dob" to "2000-01-01",
                            ),
                    ),
                )
            ),
    )
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
                    ),
                )
            ),
    )
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
                                "lives_in" to "london",
                            ),
                    ),
                )
            ),
    )
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
                                "home_address.country" to "uk",
                            ),
                    ),
                )
            ),
    )
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
                        "properties" to mapOf<String, Any?>("name" to "john", "surname" to "doe"),
                    ),
                )
            ),
    )
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
                                "name" to "john",
                                "surname" to "doe",
                                "city" to "london",
                            ),
                    ),
                )
            ),
    )
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
                                "home_address.country" to "uk",
                            ),
                    ),
                )
            ),
    )
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
                                "home_address.country" to "uk",
                            ),
                    ),
                )
            ),
    )
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
                                    Instant.ofEpochMilli(TIMESTAMP).atOffset(ZoneOffset.UTC),
                            ),
                    ),
                )
            ),
    )
  }

  @Test
  fun `should be able to delete`() {
    assertQueryAndParameters(
        "(:ALabel {!id: __key.id, name: __value.name, surname: __value.surname, created_at: __timestamp})",
        key = """{"id": 1}""",
        expected = listOf(listOf("D", mapOf("keys" to mapOf("id" to 1)))),
    )
  }

  @Test
  fun `should be able to delete with structs`() {
    val schema = SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).build()

    assertQueryAndParameters(
        "(:ALabel {!id: __key.id, name: __value.name, surname: __value.surname, created_at: __timestamp})",
        keySchema = schema,
        key = Struct(schema).put("id", 1),
        expected = listOf(listOf("D", mapOf("keys" to mapOf("id" to 1)))),
    )
  }

  @Test
  fun `should be able to delete with implicit key`() {
    assertQueryAndParameters(
        "(:ALabel {!id, name: __value.name, surname: __value.surname, created_at: __timestamp})",
        key = """{"id": 1}""",
        expected = listOf(listOf("D", mapOf("keys" to mapOf("id" to 1)))),
    )
  }

  @Test
  fun `should fail when the key is not located in the message`() {
    assertThrowsHandler<InvalidDataException>(
        pattern = "(:Person{!id, !secondary_id, name, surname})",
        key = """{}""",
        value = """{"name": "John", "surname": "Doe"}""",
        message = "Key 'id' could not be located in the message.",
    )
  }

  @Test
  fun `should fail when explicit key is not located in the keys`() {
    assertThrowsHandler<InvalidDataException>(
        pattern = "(:Person{!id: __key.old_id, name, surname})",
        key = """{}""",
        value = """{"name": "John", "surname": "Doe"}""",
        message = "Key 'old_id' could not be located in the keys.",
    )
  }

  @Test
  fun `should fail when explicit key is not located in the values`() {
    assertThrowsHandler<InvalidDataException>(
        pattern = "(:Person{!id: __value.old_id, name, surname})",
        key = """{}""",
        value = """{"name": "John", "surname": "Doe"}""",
        message = "Key 'old_id' could not be located in the values.",
    )
  }

  @Test
  fun `should fail when the key is not located in the keys with composite key pattern`() {
    assertThrowsHandler<InvalidDataException>(
        pattern = "(:Person{!id, !second_id, name, surname})",
        key = """{"id": 1}""",
        value = """{"name": "John", "surname": "Doe"}""",
        message = "Key 'second_id' could not be located in the message.",
    )
  }

  @Test
  fun `checkConstraints should not return warning messages if node key constraint provided with all keys`() {
    val handler =
        NodePatternHandler(
            "my-topic",
            "(:ALabel{!id, !second_id, name})",
            mergeProperties = true,
            renderer = Renderer.getDefaultRenderer(),
            batchSize = 1,
        )

    val constraints =
        listOf(
            ConstraintData(
                entityType = ConstraintEntityType.NODE.value,
                constraintType = ConstraintType.NODE_KEY.value,
                labelOrType = "ALabel",
                properties = listOf("id", "second_id"),
            )
        )

    val warningMessages = handler.checkConstraints(constraints)

    warningMessages shouldBe emptyList()
  }

  @Test
  fun `checkConstraints should not return warning messages if node uniqueness and existence constraints provided with all keys`() {
    val handler =
        NodePatternHandler(
            "my-topic",
            "(:ALabel{!id, !second_id, name})",
            mergeProperties = true,
            renderer = Renderer.getDefaultRenderer(),
            batchSize = 1,
        )

    val constraints =
        listOf(
            ConstraintData(
                entityType = ConstraintEntityType.NODE.value,
                constraintType = ConstraintType.NODE_UNIQUENESS.value,
                labelOrType = "ALabel",
                properties = listOf("id", "second_id"),
            ),
            ConstraintData(
                entityType = ConstraintEntityType.NODE.value,
                constraintType = ConstraintType.NODE_EXISTENCE.value,
                labelOrType = "ALabel",
                properties = listOf("id"),
            ),
            ConstraintData(
                entityType = ConstraintEntityType.NODE.value,
                constraintType = ConstraintType.NODE_EXISTENCE.value,
                labelOrType = "ALabel",
                properties = listOf("second_id"),
            ),
        )

    val warningMessages = handler.checkConstraints(constraints)

    warningMessages shouldBe emptyList()
  }

  @Test
  fun `checkConstraints should return warning messages with single label pattern`() {
    val handler =
        NodePatternHandler(
            "my-topic",
            "(:ALabel{!id, !second_id, name})",
            mergeProperties = true,
            renderer = Renderer.getDefaultRenderer(),
            batchSize = 1,
        )

    val constraints = emptyList<ConstraintData>()

    val warningMessages = handler.checkConstraints(constraints)

    warningMessages shouldBe
        listOf(
            "Label 'ALabel' does not match the key(s) defined by the pattern (:ALabel {!id: id, !second_id: second_id, name: name})." +
                "\nPlease fix the label constraint:" +
                "\n\t'ALabel' has no key constraints" +
                "\nExpected constraints:" +
                "\n\t- NODE_KEY (id, second_id)" +
                "\nor:" +
                "\n\t- UNIQUENESS (id, second_id)" +
                "\n\t- NODE_PROPERTY_EXISTENCE (id)" +
                "\n\t- NODE_PROPERTY_EXISTENCE (second_id)"
        )
  }

  @Test
  fun `checkConstraints should return warning messages with empty list of constraints`() {
    val handler =
        NodePatternHandler(
            "my-topic",
            "(:ALabel:BLabel{!id, !second_id, name})",
            mergeProperties = true,
            renderer = Renderer.getDefaultRenderer(),
            batchSize = 1,
        )

    val constraints = emptyList<ConstraintData>()

    val warningMessages = handler.checkConstraints(constraints)

    warningMessages shouldBe
        listOf(
            "None of the labels 'ALabel', 'BLabel' match the key(s) defined by the pattern (:ALabel:BLabel {!id: id, !second_id: second_id, name: name})." +
                "\nPlease fix at least one of the following label constraints:" +
                "\n\t'ALabel' has no key constraints" +
                "\n\t'BLabel' has no key constraints" +
                "\nExpected constraints:" +
                "\n\t- NODE_KEY (id, second_id)" +
                "\nor:" +
                "\n\t- UNIQUENESS (id, second_id)" +
                "\n\t- NODE_PROPERTY_EXISTENCE (id)" +
                "\n\t- NODE_PROPERTY_EXISTENCE (second_id)"
        )
  }

  @Test
  fun `checkConstraints should return warning messages with existing node key constraint`() {
    val handler =
        NodePatternHandler(
            "my-topic",
            "(:ALabel:BLabel{!id, !second_id, name})",
            mergeProperties = true,
            renderer = Renderer.getDefaultRenderer(),
            batchSize = 1,
        )

    val constraints =
        listOf(
            ConstraintData(
                entityType = ConstraintEntityType.NODE.value,
                constraintType = ConstraintType.NODE_KEY.value,
                labelOrType = "ALabel",
                properties = listOf("id"),
            ),
            ConstraintData(
                entityType = ConstraintEntityType.NODE.value,
                constraintType = ConstraintType.NODE_KEY.value,
                labelOrType = "BLabel",
                properties = listOf("id"),
            ),
        )

    val warningMessages = handler.checkConstraints(constraints)

    warningMessages shouldBe
        listOf(
            "None of the labels 'ALabel', 'BLabel' match the key(s) defined by the pattern (:ALabel:BLabel {!id: id, !second_id: second_id, name: name})." +
                "\nPlease fix at least one of the following label constraints:" +
                "\n\t'ALabel' has:" +
                "\n\t\t- NODE_KEY (id)" +
                "\n\t'BLabel' has:" +
                "\n\t\t- NODE_KEY (id)" +
                "\nExpected constraints:" +
                "\n\t- NODE_KEY (id, second_id)" +
                "\nor:" +
                "\n\t- UNIQUENESS (id, second_id)" +
                "\n\t- NODE_PROPERTY_EXISTENCE (id)" +
                "\n\t- NODE_PROPERTY_EXISTENCE (second_id)"
        )
  }

  @Test
  fun `checkConstraints should return warning messages with existing uniqueness and existence constraints`() {
    val handler =
        NodePatternHandler(
            "my-topic",
            "(:ALabel:BLabel{!id, !second_id, name})",
            mergeProperties = true,
            renderer = Renderer.getDefaultRenderer(),
            batchSize = 1,
        )

    val constraints =
        listOf(
            ConstraintData(
                entityType = ConstraintEntityType.NODE.value,
                constraintType = ConstraintType.NODE_UNIQUENESS.value,
                labelOrType = "ALabel",
                properties = listOf("id"),
            ),
            ConstraintData(
                entityType = ConstraintEntityType.NODE.value,
                constraintType = ConstraintType.NODE_EXISTENCE.value,
                labelOrType = "ALabel",
                properties = listOf("id"),
            ),
            ConstraintData(
                entityType = ConstraintEntityType.NODE.value,
                constraintType = ConstraintType.NODE_EXISTENCE.value,
                labelOrType = "ALabel",
                properties = listOf("second_id"),
            ),
            ConstraintData(
                entityType = ConstraintEntityType.NODE.value,
                constraintType = ConstraintType.NODE_UNIQUENESS.value,
                labelOrType = "BLabel",
                properties = listOf("id", "second_id"),
            ),
            ConstraintData(
                entityType = ConstraintEntityType.NODE.value,
                constraintType = ConstraintType.NODE_EXISTENCE.value,
                labelOrType = "BLabel",
                properties = listOf("id"),
            ),
        )

    val warningMessages = handler.checkConstraints(constraints)

    warningMessages shouldBe
        listOf(
            "None of the labels 'ALabel', 'BLabel' match the key(s) defined by the pattern (:ALabel:BLabel {!id: id, !second_id: second_id, name: name})." +
                "\nPlease fix at least one of the following label constraints:" +
                "\n\t'ALabel' has:" +
                "\n\t\t- UNIQUENESS (id)" +
                "\n\t\t- NODE_PROPERTY_EXISTENCE (id)" +
                "\n\t\t- NODE_PROPERTY_EXISTENCE (second_id)" +
                "\n\t'BLabel' has:" +
                "\n\t\t- UNIQUENESS (id, second_id)" +
                "\n\t\t- NODE_PROPERTY_EXISTENCE (id)" +
                "\nExpected constraints:" +
                "\n\t- NODE_KEY (id, second_id)" +
                "\nor:" +
                "\n\t- UNIQUENESS (id, second_id)" +
                "\n\t- NODE_PROPERTY_EXISTENCE (id)" +
                "\n\t- NODE_PROPERTY_EXISTENCE (second_id)"
        )
  }

  private fun assertQueryAndParameters(
      pattern: String,
      keySchema: Schema = Schema.STRING_SCHEMA,
      key: Any? = null,
      valueSchema: Schema = Schema.STRING_SCHEMA,
      value: Any? = null,
      expected: List<List<Any>> = emptyList(),
  ) {
    val handler = NodePatternHandler("my-topic", pattern, false, Renderer.getDefaultRenderer(), 1)
    val sinkMessage = newMessage(valueSchema, value, keySchema = keySchema, key = key)
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
                            UNWIND ${'$'}events AS event 
                            CALL { WITH event 
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
                              RETURN created, deleted
                            }
                            RETURN sum(created) AS created, sum(deleted) AS deleted
                          """
                                    .trimIndent()
                            )
                            .cypher,
                        mapOf("events" to expected),
                    ),
                )
            )
        )
  }

  private inline fun <reified T : Throwable> assertThrowsHandler(
      pattern: String,
      keySchema: Schema = Schema.STRING_SCHEMA,
      key: Any? = null,
      valueSchema: Schema = Schema.STRING_SCHEMA,
      value: Any? = null,
      message: String? = null,
  ) {
    val handler = NodePatternHandler("my-topic", pattern, false, Renderer.getDefaultRenderer(), 1)
    val sinkMessage = newMessage(valueSchema, value, keySchema = keySchema, key = key)

    if (message != null) {
      assertThrows<T> { handler.handle(listOf(sinkMessage)) } shouldHaveMessage message
    } else {
      assertThrows<T> { handler.handle(listOf(sinkMessage)) }
    }
  }
}
