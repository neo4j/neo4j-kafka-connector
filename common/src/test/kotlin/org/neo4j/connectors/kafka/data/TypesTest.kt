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

import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.beInstanceOf
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.OffsetTime
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.stream.Stream
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Named
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.ArgumentsProvider
import org.junit.jupiter.params.provider.ArgumentsSource
import org.neo4j.cdc.client.CDCClient
import org.neo4j.connectors.kafka.data.PropertyType.BOOLEAN
import org.neo4j.connectors.kafka.data.PropertyType.DURATION
import org.neo4j.connectors.kafka.data.PropertyType.FLOAT
import org.neo4j.connectors.kafka.data.PropertyType.LOCAL_DATE
import org.neo4j.connectors.kafka.data.PropertyType.LOCAL_DATE_TIME
import org.neo4j.connectors.kafka.data.PropertyType.LOCAL_TIME
import org.neo4j.connectors.kafka.data.PropertyType.OFFSET_TIME
import org.neo4j.connectors.kafka.data.PropertyType.POINT
import org.neo4j.connectors.kafka.data.PropertyType.ZONED_DATE_TIME
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.SessionConfig
import org.neo4j.driver.Values
import org.testcontainers.containers.Neo4jContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

@Testcontainers
class TypesTest {
  companion object {
    @Container
    val neo4j: Neo4jContainer<*> =
        Neo4jContainer("neo4j:5-enterprise")
            .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
            .withoutAuthentication()

    private lateinit var driver: Driver

    @BeforeAll
    @JvmStatic
    fun setUpContainer() {
      driver = GraphDatabase.driver(neo4j.boltUrl, AuthTokens.none())
    }

    @AfterAll
    @JvmStatic
    fun tearDownContainer() {
      driver.close()
    }
  }

  @BeforeEach
  fun `start with an empty database`() {
    driver.session(SessionConfig.forDatabase("system")).use {
      it.run("CREATE OR REPLACE DATABASE neo4j WAIT").consume()
    }
  }

  @ParameterizedTest(name = "{0}")
  @ArgumentsSource(SimpleDriverValues::class)
  fun `should build schema and values out of simple driver values and convert them back`(
      input: Any?,
      expectedSchema: Schema,
      expectedValue: Any?
  ) {
    driver.session().use {
      val returned = it.run("RETURN \$value", mapOf("value" to input)).single().get(0).asObject()
      val schema = DynamicTypes.toConnectSchema(returned)
      val converted = DynamicTypes.toConnectValue(schema, returned)
      val reverted = DynamicTypes.fromConnectValue(schema, converted)

      schema shouldBe expectedSchema
      converted shouldBe expectedValue
      reverted shouldBe input
    }
  }

  object SimpleDriverValues : ArgumentsProvider {

    override fun provideArguments(p0: ExtensionContext?): Stream<out Arguments> {
      return Stream.of(
          Arguments.of(Named.of("null", null), PropertyType.schema, null),
          Arguments.of(
              Named.of("boolean", true),
              PropertyType.schema,
              Struct(PropertyType.schema).put(BOOLEAN, true)),
          Arguments.of(Named.of("long", 1), PropertyType.schema, PropertyType.toConnectValue(1L)),
          Arguments.of(
              Named.of("float", 1.0),
              PropertyType.schema,
              Struct(PropertyType.schema).put(FLOAT, 1.0)),
          Arguments.of(
              Named.of("string", "a string"),
              PropertyType.schema,
              PropertyType.toConnectValue("a string")),
          LocalDate.of(1999, 12, 31).let {
            Arguments.of(
                Named.of("local date", it),
                PropertyType.schema,
                Struct(PropertyType.schema).put(LOCAL_DATE, DateTimeFormatter.ISO_DATE.format(it)))
          },
          LocalTime.of(23, 59, 59, 5).let {
            Arguments.of(
                Named.of("local time", it),
                PropertyType.schema,
                Struct(PropertyType.schema).put(LOCAL_TIME, DateTimeFormatter.ISO_TIME.format(it)))
          },
          LocalDateTime.of(1999, 12, 31, 23, 59, 59, 5).let {
            Arguments.of(
                Named.of("local date time", it),
                PropertyType.schema,
                Struct(PropertyType.schema)
                    .put(LOCAL_DATE_TIME, DateTimeFormatter.ISO_DATE_TIME.format(it)))
          },
          OffsetTime.of(23, 59, 59, 5, ZoneOffset.ofHours(1)).let {
            Arguments.of(
                Named.of("offset time", it),
                PropertyType.schema,
                Struct(PropertyType.schema).put(OFFSET_TIME, DateTimeFormatter.ISO_TIME.format(it)))
          },
          OffsetDateTime.of(1999, 12, 31, 23, 59, 59, 5, ZoneOffset.ofHours(1)).let {
            Arguments.of(
                Named.of("offset date time", it),
                PropertyType.schema,
                Struct(PropertyType.schema)
                    .put(ZONED_DATE_TIME, DateTimeFormatter.ISO_DATE_TIME.format(it)))
          },
          ZonedDateTime.of(1999, 12, 31, 23, 59, 59, 5, ZoneId.of("Europe/Istanbul")).let {
            Arguments.of(
                Named.of("offset date time", it),
                PropertyType.schema,
                Struct(PropertyType.schema)
                    .put(ZONED_DATE_TIME, DateTimeFormatter.ISO_DATE_TIME.format(it)))
          },
          Arguments.of(
              Named.of("duration", Values.isoDuration(5, 2, 23, 5).asIsoDuration()),
              PropertyType.schema,
              Struct(PropertyType.schema)
                  .put(
                      DURATION,
                      Struct(PropertyType.durationSchema)
                          .put("months", 5L)
                          .put("days", 2L)
                          .put("seconds", 23L)
                          .put("nanoseconds", 5))),
          Arguments.of(
              Named.of("point - 2d", Values.point(7203, 2.3, 4.5).asPoint()),
              PropertyType.schema,
              Struct(PropertyType.schema)
                  .put(
                      POINT,
                      Struct(PropertyType.pointSchema)
                          .put("dimension", 2.toByte())
                          .put("srid", 7203)
                          .put("x", 2.3)
                          .put("y", 4.5)
                          .put("z", null))),
          Arguments.of(
              Named.of("point - 3d", Values.point(4979, 12.78, 56.7, 100.0).asPoint()),
              PropertyType.schema,
              Struct(PropertyType.schema)
                  .put(
                      POINT,
                      Struct(PropertyType.pointSchema)
                          .put("dimension", 3.toByte())
                          .put("srid", 4979)
                          .put("x", 12.78)
                          .put("y", 56.7)
                          .put("z", 100.0))),
          Arguments.of(
              Named.of("list - uniformly typed elements", (1L..50L).toList()),
              SchemaBuilder.array(PropertyType.schema).build(),
              (1L..50L).map { PropertyType.toConnectValue(it) }.toList()),
          Arguments.of(
              Named.of("list - non-uniformly typed elements", listOf(1, true, 2.0, "a string")),
              SchemaBuilder.array(PropertyType.schema).build(),
              listOf(
                  PropertyType.toConnectValue(1L),
                  Struct(PropertyType.schema).put(BOOLEAN, true),
                  Struct(PropertyType.schema).put(FLOAT, 2.0),
                  PropertyType.toConnectValue("a string"))),
          Arguments.of(
              Named.of("map - uniformly typed values", mapOf("a" to 1, "b" to 2, "c" to 3)),
              SchemaBuilder.map(Schema.STRING_SCHEMA, PropertyType.schema).build(),
              mapOf(
                  "a" to PropertyType.toConnectValue(1L),
                  "b" to PropertyType.toConnectValue(2L),
                  "c" to PropertyType.toConnectValue(3L))),
          Arguments.of(
              Named.of(
                  "map - non-uniformly typed values", mapOf("a" to 1, "b" to true, "c" to 3.0)),
              SchemaBuilder.map(Schema.STRING_SCHEMA, PropertyType.schema).build(),
              mapOf(
                  "a" to PropertyType.toConnectValue(1L),
                  "b" to Struct(PropertyType.schema).put(BOOLEAN, true),
                  "c" to Struct(PropertyType.schema).put(FLOAT, 3.0))))
    }
  }

  @Test
  fun `should build schema and values out of nodes and relationships`() {
    driver.session().use {
      val record =
          it.run(
                  """
                CREATE (p:Person) SET p = ${'$'}person
                CREATE (c:Company) SET c =${'$'}company
                CREATE (p)-[r:WORKS_FOR]->(c) SET r = ${'$'}works_for 
                RETURN p, c, r
              """
                      .trimIndent(),
                  mapOf(
                      "person" to
                          mapOf(
                              "name" to "john",
                              "surname" to "doe",
                              "dob" to LocalDate.of(1999, 12, 31)),
                      "company" to mapOf("name" to "acme corp", "est" to LocalDate.of(1980, 1, 1)),
                      "works_for" to
                          mapOf("contractId" to 5916, "since" to LocalDate.of(2000, 1, 5))))
              .single()

      val person = record.get("p").asNode()
      schemaAndValue(person).also { (schema, converted, reverted) ->
        schema shouldBe
            SchemaBuilder.struct()
                .field("<id>", Schema.INT64_SCHEMA)
                .field("<labels>", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                .field("name", PropertyType.schema)
                .field("surname", PropertyType.schema)
                .field("dob", PropertyType.schema)
                .build()

        converted shouldBe
            Struct(schema)
                .put("<id>", person.id())
                .put("<labels>", person.labels().toList())
                .put("name", PropertyType.toConnectValue("john"))
                .put("surname", PropertyType.toConnectValue("doe"))
                .put("dob", PropertyType.toConnectValue(LocalDate.of(1999, 12, 31)))

        reverted shouldBe
            mapOf(
                "<id>" to person.id(),
                "<labels>" to person.labels().toList(),
                "name" to "john",
                "surname" to "doe",
                "dob" to LocalDate.of(1999, 12, 31))
      }

      val company = record.get("c").asNode()
      schemaAndValue(company).also { (schema, converted, reverted) ->
        schema shouldBe
            SchemaBuilder.struct()
                .field("<id>", Schema.INT64_SCHEMA)
                .field("<labels>", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                .field("name", PropertyType.schema)
                .field("est", PropertyType.schema)
                .build()

        converted shouldBe
            Struct(schema)
                .put("<id>", company.id())
                .put("<labels>", company.labels().toList())
                .put("name", PropertyType.toConnectValue("acme corp"))
                .put("est", PropertyType.toConnectValue(LocalDate.of(1980, 1, 1)))

        reverted shouldBe
            mapOf(
                "<id>" to company.id(),
                "<labels>" to company.labels().toList(),
                "name" to "acme corp",
                "est" to LocalDate.of(1980, 1, 1))
      }

      val worksFor = record.get("r").asRelationship()
      schemaAndValue(worksFor).also { (schema, converted, reverted) ->
        schema shouldBe
            SchemaBuilder.struct()
                .field("<id>", Schema.INT64_SCHEMA)
                .field("<type>", Schema.STRING_SCHEMA)
                .field("<start.id>", Schema.INT64_SCHEMA)
                .field("<end.id>", Schema.INT64_SCHEMA)
                .field("contractId", PropertyType.schema)
                .field("since", PropertyType.schema)
                .build()

        converted shouldBe
            Struct(schema)
                .put("<id>", worksFor.id())
                .put("<type>", worksFor.type())
                .put("<start.id>", worksFor.startNodeId())
                .put("<end.id>", worksFor.endNodeId())
                .put("contractId", PropertyType.toConnectValue(5916L))
                .put("since", PropertyType.toConnectValue(LocalDate.of(2000, 1, 5)))

        reverted shouldBe
            mapOf(
                "<id>" to worksFor.id(),
                "<type>" to worksFor.type(),
                "<start.id>" to worksFor.startNodeId(),
                "<end.id>" to worksFor.endNodeId(),
                "contractId" to 5916L,
                "since" to LocalDate.of(2000, 1, 5))
      }
    }
  }

  @Test
  fun `should build schema and value for change events and convert back`() {
    // set-up cdc
    driver.session().use {
      it.run("ALTER DATABASE neo4j SET OPTION txLogEnrichment 'FULL' WAIT").consume()

      // set-up constraints
      it.run("CREATE CONSTRAINT FOR (n:Person) REQUIRE (n.name, n.surname) IS KEY").consume()
      it.run("CREATE CONSTRAINT FOR (n:Company) REQUIRE (n.name) IS KEY").consume()
      it.run("CREATE CONSTRAINT FOR ()-[r:WORKS_FOR]->() REQUIRE (r.contractId) IS KEY").consume()
    }

    val cdc = CDCClient(driver)
    val changeId = cdc.current().block()
    driver.session().use {
      it.run(
              """
                CREATE (p:Person) SET p = ${'$'}person
                CREATE (c:Company) SET c =${'$'}company
                CREATE (p)-[r:WORKS_FOR]->(c) SET r = ${'$'}works_for 
              """
                  .trimIndent(),
              mapOf(
                  "person" to
                      mapOf(
                          "name" to "john",
                          "surname" to "doe",
                          "dob" to LocalDate.of(1999, 12, 31)),
                  "company" to mapOf("name" to "acme corp", "est" to LocalDate.of(1980, 1, 1)),
                  "works_for" to mapOf("contractId" to 5916, "since" to LocalDate.of(2000, 1, 5))))
          .consume()

      val changes = cdc.query(changeId).collectList().block()

      val changeEventConverter = ChangeEventConverter()
      changes!!.take(2).forEach { change ->
        val converted = changeEventConverter.toConnectValue(change)
        val schema = converted.schema()
        val value = converted.value() as Struct

        schema.type() shouldBe Schema.Type.STRUCT
        schema.field("event").schema() should
            { eventSchema ->
              eventSchema.type() shouldBe Schema.Type.STRUCT
            }
        value.get("event") should beInstanceOf<Struct>()

        val reverted = value.toChangeEvent()
        reverted shouldBe change
      }

      changes[2].also { change ->
        val converted = changeEventConverter.toConnectValue(change)
        val schema = converted.schema()
        val value = converted.value() as Struct

        schema.type() shouldBe Schema.Type.STRUCT
        schema.field("event").schema() should
            { eventSchema ->
              eventSchema.type() shouldBe Schema.Type.STRUCT
            }
        value.get("event") should beInstanceOf<Struct>()

        val reverted = value.toChangeEvent()
        reverted shouldBe change
      }
    }
  }

  @Test
  fun `should build schema for a complex value`() {
    val returned =
        driver.session().use {
          val record =
              it.run(
                      """
                      WITH
                      {
                         id: 'ROOT_ID',
                         root: [
                             { children: [] },
                             { children: [{ name: "child" }] }
                         ],
                         arr: [null, {foo: "bar"}],
                         arr_mixed: [{foo: "bar"}, null, {foo: 1}]
                      } AS data
                      RETURN data, data.id AS id
                    """)
                  .single()

          buildMap {
            this["id"] = record["id"].asString()
            this["data"] = record["data"].asMap().toSortedMap()
          }
        }

    val expectedSchema =
        SchemaBuilder.struct()
            .field("id", PropertyType.schema)
            .field(
                "data",
                SchemaBuilder.struct()
                    .field(
                        "arr",
                        SchemaBuilder.array(
                                SchemaBuilder.map(Schema.STRING_SCHEMA, PropertyType.schema)
                                    .optional()
                                    .build())
                            .optional()
                            .build())
                    .field(
                        "arr_mixed",
                        SchemaBuilder.array(
                                SchemaBuilder.map(Schema.STRING_SCHEMA, PropertyType.schema)
                                    .optional()
                                    .build())
                            .optional()
                            .build())
                    .field("id", PropertyType.schema)
                    .field(
                        "root",
                        SchemaBuilder.array(
                                SchemaBuilder.map(
                                        Schema.STRING_SCHEMA,
                                        SchemaBuilder.array(
                                                SchemaBuilder.map(
                                                        Schema.STRING_SCHEMA, PropertyType.schema)
                                                    .optional()
                                                    .build())
                                            .optional()
                                            .build())
                                    .optional()
                                    .build())
                            .optional()
                            .build())
                    .optional()
                    .build())
            .optional()
            .build()
    val schema = DynamicTypes.toConnectSchema(returned, optional = true)
    schema shouldBe expectedSchema

    val converted = DynamicTypes.toConnectValue(schema, returned)
    converted shouldBe
        Struct(schema)
            .put("id", PropertyType.toConnectValue("ROOT_ID"))
            .put(
                "data",
                Struct(schema.field("data").schema())
                    .put("arr", listOf(null, mapOf("foo" to PropertyType.toConnectValue("bar"))))
                    .put(
                        "arr_mixed",
                        listOf(
                            mapOf("foo" to PropertyType.toConnectValue("bar")),
                            null,
                            mapOf("foo" to PropertyType.toConnectValue(1L))))
                    .put("id", PropertyType.toConnectValue("ROOT_ID"))
                    .put(
                        "root",
                        listOf(
                            mapOf("children" to listOf<Any>()),
                            mapOf(
                                "children" to
                                    listOf(
                                        mapOf("name" to PropertyType.toConnectValue("child")))))))

    val reverted = DynamicTypes.fromConnectValue(schema, converted)
    reverted shouldBe returned
  }

  private fun schemaAndValue(value: Any): Triple<Schema, Any?, Any?> {
    val schema = DynamicTypes.toConnectSchema(value)
    val converted = DynamicTypes.toConnectValue(schema, value)
    val reverted = DynamicTypes.fromConnectValue(schema, converted)
    return Triple(schema, converted, reverted)
  }
}
