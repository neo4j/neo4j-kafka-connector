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
          Arguments.of(Named.of("null", null), SimpleTypes.NULL.schema(), null),
          Arguments.of(Named.of("boolean", true), SimpleTypes.BOOLEAN.schema(), true),
          Arguments.of(Named.of("long", 1), SimpleTypes.LONG.schema(), 1L),
          Arguments.of(Named.of("float", 1.0), SimpleTypes.FLOAT.schema(), 1.0),
          Arguments.of(Named.of("string", "a string"), SimpleTypes.STRING.schema(), "a string"),
          LocalDate.of(1999, 12, 31).let {
            Arguments.of(
                Named.of("local date", it),
                SimpleTypes.LOCALDATE_STRUCT.schema(),
                Struct(SimpleTypes.LOCALDATE_STRUCT.schema).put(EPOCH_DAYS, it.toEpochDay()))
          },
          LocalTime.of(23, 59, 59, 5).let {
            Arguments.of(
                Named.of("local time", it),
                SimpleTypes.LOCALTIME_STRUCT.schema(),
                Struct(SimpleTypes.LOCALTIME_STRUCT.schema).put(NANOS_OF_DAY, it.toNanoOfDay()))
          },
          LocalDateTime.of(1999, 12, 31, 23, 59, 59, 5).let {
            Arguments.of(
                Named.of("local date time", it),
                SimpleTypes.LOCALDATETIME_STRUCT.schema(),
                Struct(SimpleTypes.LOCALDATETIME_STRUCT.schema)
                    .put(EPOCH_DAYS, it.toLocalDate().toEpochDay())
                    .put(NANOS_OF_DAY, it.toLocalTime().toNanoOfDay()))
          },
          OffsetTime.of(23, 59, 59, 5, ZoneOffset.ofHours(1)).let {
            Arguments.of(
                Named.of("offset time", it),
                SimpleTypes.OFFSETTIME_STRUCT.schema(),
                Struct(SimpleTypes.OFFSETTIME_STRUCT.schema)
                    .put(NANOS_OF_DAY, it.toLocalTime().toNanoOfDay())
                    .put(ZONE_ID, it.offset.id))
          },
          OffsetDateTime.of(1999, 12, 31, 23, 59, 59, 5, ZoneOffset.ofHours(1)).let {
            Arguments.of(
                Named.of("offset date time", it),
                SimpleTypes.ZONEDDATETIME_STRUCT.schema(),
                Struct(SimpleTypes.ZONEDDATETIME_STRUCT.schema)
                    .put(EPOCH_SECONDS, it.toEpochSecond())
                    .put(NANOS_OF_SECOND, it.nano)
                    .put(ZONE_ID, it.offset.id))
          },
          ZonedDateTime.of(1999, 12, 31, 23, 59, 59, 5, ZoneId.of("Europe/Istanbul")).let {
            Arguments.of(
                Named.of("offset date time", it),
                SimpleTypes.ZONEDDATETIME_STRUCT.schema(),
                Struct(SimpleTypes.ZONEDDATETIME_STRUCT.schema)
                    .put(EPOCH_SECONDS, it.toEpochSecond())
                    .put(NANOS_OF_SECOND, it.nano)
                    .put(ZONE_ID, it.zone.id))
          },
          Arguments.of(
              Named.of("duration", Values.isoDuration(5, 2, 23, 5).asIsoDuration()),
              SimpleTypes.DURATION.schema(),
              Struct(SimpleTypes.DURATION.schema())
                  .put("months", 5L)
                  .put("days", 2L)
                  .put("seconds", 23L)
                  .put("nanoseconds", 5)),
          Arguments.of(
              Named.of("point - 2d", Values.point(7203, 2.3, 4.5).asPoint()),
              SimpleTypes.POINT.schema(),
              Struct(SimpleTypes.POINT.schema())
                  .put("dimension", 2.toByte())
                  .put("srid", 7203)
                  .put("x", 2.3)
                  .put("y", 4.5)
                  .put("z", null)),
          Arguments.of(
              Named.of("point - 3d", Values.point(4979, 12.78, 56.7, 100.0).asPoint()),
              SimpleTypes.POINT.schema(),
              Struct(SimpleTypes.POINT.schema())
                  .put("dimension", 3.toByte())
                  .put("srid", 4979)
                  .put("x", 12.78)
                  .put("y", 56.7)
                  .put("z", 100.0)),
          Arguments.of(
              Named.of("list - uniformly typed elements", (1L..50L).toList()),
              SchemaBuilder.array(SimpleTypes.LONG.schema()).build(),
              (1L..50L).toList()),
          Arguments.of(
              Named.of("list - non-uniformly typed elements", listOf(1, true, 2.0, "a string")),
              SchemaBuilder.struct()
                  .field("e0", SimpleTypes.LONG.schema())
                  .field("e1", SimpleTypes.BOOLEAN.schema())
                  .field("e2", SimpleTypes.FLOAT.schema())
                  .field("e3", SimpleTypes.STRING.schema())
                  .build(),
              Struct(
                      SchemaBuilder.struct()
                          .field("e0", SimpleTypes.LONG.schema())
                          .field("e1", SimpleTypes.BOOLEAN.schema())
                          .field("e2", SimpleTypes.FLOAT.schema())
                          .field("e3", SimpleTypes.STRING.schema())
                          .build())
                  .put("e0", 1L)
                  .put("e1", true)
                  .put("e2", 2.0)
                  .put("e3", "a string")),
          Arguments.of(
              Named.of("map - uniformly typed values", mapOf("a" to 1, "b" to 2, "c" to 3)),
              SchemaBuilder.map(SimpleTypes.STRING.schema(), SimpleTypes.LONG.schema()).build(),
              mapOf("a" to 1, "b" to 2, "c" to 3)),
          Arguments.of(
              Named.of(
                  "map - non-uniformly typed values", mapOf("a" to 1, "b" to true, "c" to 3.0)),
              SchemaBuilder.struct()
                  .field("a", SimpleTypes.LONG.schema())
                  .field("b", SimpleTypes.BOOLEAN.schema())
                  .field("c", SimpleTypes.FLOAT.schema())
                  .build(),
              Struct(
                      SchemaBuilder.struct()
                          .field("a", SimpleTypes.LONG.schema())
                          .field("b", SimpleTypes.BOOLEAN.schema())
                          .field("c", SimpleTypes.FLOAT.schema())
                          .build())
                  .put("a", 1L)
                  .put("b", true)
                  .put("c", 3.0)))
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
                .field("<id>", SimpleTypes.LONG.schema())
                .field("<labels>", SchemaBuilder.array(SimpleTypes.STRING.schema()).build())
                .field("name", SimpleTypes.STRING.schema())
                .field("surname", SimpleTypes.STRING.schema())
                .field("dob", SimpleTypes.LOCALDATE_STRUCT.schema())
                .build()

        converted shouldBe
            Struct(schema)
                .put("<id>", person.id())
                .put("<labels>", person.labels().toList())
                .put("name", "john")
                .put("surname", "doe")
                .put(
                    "dob",
                    Struct(SimpleTypes.LOCALDATE_STRUCT.schema())
                        .put(EPOCH_DAYS, LocalDate.of(1999, 12, 31).toEpochDay()))

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
                .field("<id>", SimpleTypes.LONG.schema())
                .field("<labels>", SchemaBuilder.array(SimpleTypes.STRING.schema()).build())
                .field("name", SimpleTypes.STRING.schema())
                .field("est", SimpleTypes.LOCALDATE_STRUCT.schema())
                .build()

        converted shouldBe
            Struct(schema)
                .put("<id>", company.id())
                .put("<labels>", company.labels().toList())
                .put("name", "acme corp")
                .put(
                    "est",
                    Struct(SimpleTypes.LOCALDATE_STRUCT.schema())
                        .put(EPOCH_DAYS, LocalDate.of(1980, 1, 1).toEpochDay()))

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
                .field("<id>", SimpleTypes.LONG.schema())
                .field("<type>", SimpleTypes.STRING.schema())
                .field("<start.id>", SimpleTypes.LONG.schema())
                .field("<end.id>", SimpleTypes.LONG.schema())
                .field("contractId", SimpleTypes.LONG.schema())
                .field("since", SimpleTypes.LOCALDATE_STRUCT.schema())
                .build()

        converted shouldBe
            Struct(schema)
                .put("<id>", worksFor.id())
                .put("<type>", worksFor.type())
                .put("<start.id>", worksFor.startNodeId())
                .put("<end.id>", worksFor.endNodeId())
                .put("contractId", 5916L)
                .put(
                    "since",
                    Struct(SimpleTypes.LOCALDATE_STRUCT.schema())
                        .put(EPOCH_DAYS, LocalDate.of(2000, 1, 5).toEpochDay()))

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
            .field("id", Schema.OPTIONAL_STRING_SCHEMA)
            .field(
                "data",
                SchemaBuilder.struct()
                    .field(
                        "arr",
                        SchemaBuilder.array(
                                SchemaBuilder.map(
                                        Schema.STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA)
                                    .optional()
                                    .build())
                            .optional()
                            .build())
                    .field(
                        "arr_mixed",
                        SchemaBuilder.struct()
                            .field(
                                "e0",
                                SchemaBuilder.map(
                                        Schema.STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA)
                                    .optional()
                                    .build())
                            .field("e1", SimpleTypes.NULL.schema())
                            .field(
                                "e2",
                                SchemaBuilder.map(
                                        Schema.STRING_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA)
                                    .optional()
                                    .build())
                            .optional()
                            .build())
                    .field("id", Schema.OPTIONAL_STRING_SCHEMA)
                    .field(
                        "root",
                        SchemaBuilder.array(
                                SchemaBuilder.map(
                                        Schema.STRING_SCHEMA,
                                        SchemaBuilder.array(
                                                SchemaBuilder.map(
                                                        Schema.STRING_SCHEMA,
                                                        Schema.OPTIONAL_STRING_SCHEMA)
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
            .put("id", "ROOT_ID")
            .put(
                "data",
                Struct(schema.field("data").schema())
                    .put("arr", listOf(null, mapOf("foo" to "bar")))
                    .put(
                        "arr_mixed",
                        Struct(schema.field("data").schema().field("arr_mixed").schema())
                            .put("e0", mapOf("foo" to "bar"))
                            .put("e1", null)
                            .put("e2", mapOf("foo" to 1L)))
                    .put("id", "ROOT_ID")
                    .put(
                        "root",
                        listOf(
                            mapOf("children" to listOf<Any>()),
                            mapOf("children" to listOf(mapOf("name" to "child"))))))

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
