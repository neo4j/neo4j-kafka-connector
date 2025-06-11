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
import org.junit.jupiter.api.Assumptions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Named
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.ArgumentsProvider
import org.junit.jupiter.params.provider.ArgumentsSource
import org.junit.jupiter.params.support.ParameterDeclarations
import org.neo4j.caniuse.CanIUse.canIUse
import org.neo4j.caniuse.Dbms
import org.neo4j.caniuse.Neo4jDetector
import org.neo4j.cdc.client.CDCClient
import org.neo4j.connectors.kafka.configuration.PayloadMode
import org.neo4j.connectors.kafka.data.PropertyType.BOOLEAN
import org.neo4j.connectors.kafka.data.PropertyType.DURATION
import org.neo4j.connectors.kafka.data.PropertyType.FLOAT
import org.neo4j.connectors.kafka.data.PropertyType.LOCAL_DATE
import org.neo4j.connectors.kafka.data.PropertyType.LOCAL_DATE_TIME
import org.neo4j.connectors.kafka.data.PropertyType.LOCAL_TIME
import org.neo4j.connectors.kafka.data.PropertyType.LONG_LIST
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
import org.testcontainers.utility.DockerImageName

@Testcontainers
class TypesTest {
  companion object {
    @Container
    val neo4j: Neo4jContainer<*> =
        Neo4jContainer(neo4jImage())
            .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
            .withExposedPorts(7687)
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

    @JvmStatic
    fun neo4jImage(): DockerImageName =
        System.getenv("NEO4J_TEST_IMAGE")
            .ifBlank {
              throw IllegalArgumentException(
                  "NEO4J_TEST_IMAGE environment variable is not defined!")
            }
            .run { DockerImageName.parse(this).asCompatibleSubstituteFor("neo4j") }
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
      payloadMode: PayloadMode,
      expectedSchema: Schema,
      expectedValue: Any?
  ) {
    driver.session().use {
      val returned = it.run("RETURN \$value", mapOf("value" to input)).single().get(0).asObject()
      val schema = DynamicTypes.toConnectSchema(payloadMode, returned)
      val converted = DynamicTypes.toConnectValue(schema, returned)
      val reverted = DynamicTypes.fromConnectValue(schema, converted)

      schema shouldBe expectedSchema
      converted shouldBe expectedValue
      reverted shouldBe input
    }
  }

  object SimpleDriverValues : ArgumentsProvider {

    override fun provideArguments(
        parameters: ParameterDeclarations?,
        context: ExtensionContext?
    ): Stream<out Arguments?>? {
      return Stream.of(
          Arguments.of(
              Named.of("null-extended", null), PayloadMode.EXTENDED, PropertyType.schema, null),
          Arguments.of(
              Named.of("null-compact", null), PayloadMode.COMPACT, SimpleTypes.NULL.schema, null),
          Arguments.of(
              Named.of("boolean-extended", true),
              PayloadMode.EXTENDED,
              PropertyType.schema,
              PropertyType.getPropertyStruct(BOOLEAN, true)),
          Arguments.of(
              Named.of("boolean-compact", true),
              PayloadMode.COMPACT,
              SimpleTypes.BOOLEAN.schema,
              true),
          Arguments.of(
              Named.of("long-extended", 1),
              PayloadMode.EXTENDED,
              PropertyType.schema,
              PropertyType.toConnectValue(1L)),
          Arguments.of(
              Named.of("long-compact", 1), PayloadMode.COMPACT, SimpleTypes.LONG.schema, 1L),
          Arguments.of(
              Named.of("float-extended", 1.0),
              PayloadMode.EXTENDED,
              PropertyType.schema,
              PropertyType.getPropertyStruct(FLOAT, 1.0)),
          Arguments.of(
              Named.of("float-extended", 1.0), PayloadMode.COMPACT, SimpleTypes.FLOAT.schema, 1.0),
          Arguments.of(
              Named.of("string-extended", "a string"),
              PayloadMode.EXTENDED,
              PropertyType.schema,
              PropertyType.toConnectValue("a string")),
          Arguments.of(
              Named.of("string-compact", "a string"),
              PayloadMode.COMPACT,
              SimpleTypes.STRING.schema,
              "a string"),
          LocalDate.of(1999, 12, 31).let {
            Arguments.of(
                Named.of("local date-extended", it),
                PayloadMode.EXTENDED,
                PropertyType.schema,
                PropertyType.getPropertyStruct(LOCAL_DATE, DateTimeFormatter.ISO_DATE.format(it)))
          },
          LocalDate.of(1999, 12, 31).let {
            Arguments.of(
                Named.of("local date-compact", it),
                PayloadMode.COMPACT,
                SimpleTypes.LOCALDATE.schema,
                DateTimeFormatter.ISO_DATE.format(it))
          },
          LocalTime.of(23, 59, 59, 5).let {
            Arguments.of(
                Named.of("local time-extended", it),
                PayloadMode.EXTENDED,
                PropertyType.schema,
                PropertyType.getPropertyStruct(LOCAL_TIME, DateTimeFormatter.ISO_TIME.format(it)))
          },
          LocalTime.of(23, 59, 59, 5).let {
            Arguments.of(
                Named.of("local time-compact", it),
                PayloadMode.COMPACT,
                SimpleTypes.LOCALTIME.schema,
                DateTimeFormatter.ISO_TIME.format(it))
          },
          LocalDateTime.of(1999, 12, 31, 23, 59, 59, 5).let {
            Arguments.of(
                Named.of("local date time-extended", it),
                PayloadMode.EXTENDED,
                PropertyType.schema,
                PropertyType.getPropertyStruct(
                    LOCAL_DATE_TIME, DateTimeFormatter.ISO_DATE_TIME.format(it)))
          },
          LocalDateTime.of(1999, 12, 31, 23, 59, 59, 5).let {
            Arguments.of(
                Named.of("local date time-compact", it),
                PayloadMode.COMPACT,
                SimpleTypes.LOCALDATETIME.schema,
                DateTimeFormatter.ISO_DATE_TIME.format(it))
          },
          OffsetTime.of(23, 59, 59, 5, ZoneOffset.ofHours(1)).let {
            Arguments.of(
                Named.of("offset time-extended", it),
                PayloadMode.EXTENDED,
                PropertyType.schema,
                PropertyType.getPropertyStruct(OFFSET_TIME, DateTimeFormatter.ISO_TIME.format(it)))
          },
          OffsetTime.of(23, 59, 59, 5, ZoneOffset.ofHours(1)).let {
            Arguments.of(
                Named.of("offset time-compact", it),
                PayloadMode.COMPACT,
                SimpleTypes.OFFSETTIME.schema,
                DateTimeFormatter.ISO_TIME.format(it))
          },
          OffsetDateTime.of(1999, 12, 31, 23, 59, 59, 5, ZoneOffset.ofHours(1)).let {
            Arguments.of(
                Named.of("offset date time-extended", it),
                PayloadMode.EXTENDED,
                PropertyType.schema,
                PropertyType.getPropertyStruct(
                    ZONED_DATE_TIME, DateTimeFormatter.ISO_DATE_TIME.format(it)))
          },
          OffsetDateTime.of(1999, 12, 31, 23, 59, 59, 5, ZoneOffset.ofHours(1)).let {
            Arguments.of(
                Named.of("offset date time-compact", it),
                PayloadMode.COMPACT,
                SimpleTypes.ZONEDDATETIME.schema,
                DateTimeFormatter.ISO_DATE_TIME.format(it))
          },
          ZonedDateTime.of(1999, 12, 31, 23, 59, 59, 5, ZoneId.of("Europe/Istanbul")).let {
            Arguments.of(
                Named.of("zoned date time-extended", it),
                PayloadMode.EXTENDED,
                PropertyType.schema,
                PropertyType.getPropertyStruct(
                    ZONED_DATE_TIME, DateTimeFormatter.ISO_DATE_TIME.format(it)))
          },
          ZonedDateTime.of(1999, 12, 31, 23, 59, 59, 5, ZoneId.of("Europe/Istanbul")).let {
            Arguments.of(
                Named.of("zoned date time-compact", it),
                PayloadMode.COMPACT,
                SimpleTypes.ZONEDDATETIME.schema,
                DateTimeFormatter.ISO_DATE_TIME.format(it))
          },
          Arguments.of(
              Named.of("duration-extended", Values.isoDuration(5, 2, 23, 5).asIsoDuration()),
              PayloadMode.EXTENDED,
              PropertyType.schema,
              PropertyType.getPropertyStruct(
                  DURATION,
                  Struct(PropertyType.durationSchema)
                      .put("months", 5L)
                      .put("days", 2L)
                      .put("seconds", 23L)
                      .put("nanoseconds", 5))),
          Arguments.of(
              Named.of("duration-compact", Values.isoDuration(5, 2, 23, 5).asIsoDuration()),
              PayloadMode.COMPACT,
              SimpleTypes.DURATION.schema,
              Struct(SimpleTypes.DURATION.schema)
                  .put("months", 5L)
                  .put("days", 2L)
                  .put("seconds", 23L)
                  .put("nanoseconds", 5)),
          Arguments.of(
              Named.of("point - 2d-extended", Values.point(7203, 2.3, 4.5).asPoint()),
              PayloadMode.EXTENDED,
              PropertyType.schema,
              PropertyType.getPropertyStruct(
                  POINT,
                  Struct(PropertyType.pointSchema)
                      .put("dimension", 2.toByte())
                      .put("srid", 7203)
                      .put("x", 2.3)
                      .put("y", 4.5)
                      .put("z", null))),
          Arguments.of(
              Named.of("point - 2d-compact", Values.point(7203, 2.3, 4.5).asPoint()),
              PayloadMode.COMPACT,
              SimpleTypes.POINT.schema,
              Struct(SimpleTypes.POINT.schema)
                  .put("dimension", 2.toByte())
                  .put("srid", 7203)
                  .put("x", 2.3)
                  .put("y", 4.5)
                  .put("z", null)),
          Arguments.of(
              Named.of("point - 3d-extended", Values.point(4979, 12.78, 56.7, 100.0).asPoint()),
              PayloadMode.EXTENDED,
              PropertyType.schema,
              PropertyType.getPropertyStruct(
                  POINT,
                  Struct(PropertyType.pointSchema)
                      .put("dimension", 3.toByte())
                      .put("srid", 4979)
                      .put("x", 12.78)
                      .put("y", 56.7)
                      .put("z", 100.0))),
          Arguments.of(
              Named.of("point - 3d-compact", Values.point(4979, 12.78, 56.7, 100.0).asPoint()),
              PayloadMode.COMPACT,
              SimpleTypes.POINT.schema,
              Struct(SimpleTypes.POINT.schema)
                  .put("dimension", 3.toByte())
                  .put("srid", 4979)
                  .put("x", 12.78)
                  .put("y", 56.7)
                  .put("z", 100.0)),
          Arguments.of(
              Named.of("list - empty-extended", emptyList<Any>()),
              PayloadMode.EXTENDED,
              PropertyType.schema,
              PropertyType.getPropertyStruct(LONG_LIST, emptyList<Long>())),
          Arguments.of(
              Named.of("list - empty-compact", emptyList<Any>()),
              PayloadMode.COMPACT,
              SchemaBuilder.array(SimpleTypes.NULL.schema(true)).build(),
              emptyList<Any>()),
          Arguments.of(
              Named.of("list - long-extended", (1L..50L).toList()),
              PayloadMode.EXTENDED,
              PropertyType.schema,
              PropertyType.getPropertyStruct(LONG_LIST, (1L..50L).toList())),
          Arguments.of(
              Named.of("list - long-compact", (1L..50L).toList()),
              PayloadMode.COMPACT,
              SchemaBuilder.array(SimpleTypes.LONG.schema()).build(),
              (1L..50L).toList()),
          Arguments.of(
              Named.of(
                  "list - non-uniformly typed elements-extended", listOf(1, true, 2.0, "a string")),
              PayloadMode.EXTENDED,
              SchemaBuilder.array(PropertyType.schema).build(),
              listOf(
                  PropertyType.toConnectValue(1L),
                  PropertyType.getPropertyStruct(BOOLEAN, true),
                  PropertyType.getPropertyStruct(FLOAT, 2.0),
                  PropertyType.toConnectValue("a string"))),
          Arguments.of(
              Named.of(
                  "list - non-uniformly typed elements-compact", listOf(1, true, 2.0, "a string")),
              PayloadMode.COMPACT,
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
              Named.of(
                  "map - uniformly typed values-extended", mapOf("a" to 1, "b" to 2, "c" to 3)),
              PayloadMode.EXTENDED,
              SchemaBuilder.map(Schema.STRING_SCHEMA, PropertyType.schema).build(),
              mapOf(
                  "a" to PropertyType.toConnectValue(1L),
                  "b" to PropertyType.toConnectValue(2L),
                  "c" to PropertyType.toConnectValue(3L))),
          Arguments.of(
              Named.of("map - uniformly typed values-compact", mapOf("a" to 1, "b" to 2, "c" to 3)),
              PayloadMode.COMPACT,
              SchemaBuilder.map(SimpleTypes.STRING.schema(), SimpleTypes.LONG.schema()).build(),
              mapOf("a" to 1L, "b" to 2L, "c" to 3L)),
          Arguments.of(
              Named.of(
                  "map - non-uniformly typed values-extended",
                  mapOf("a" to 1, "b" to true, "c" to 3.0)),
              PayloadMode.EXTENDED,
              SchemaBuilder.map(Schema.STRING_SCHEMA, PropertyType.schema).build(),
              mapOf(
                  "a" to PropertyType.toConnectValue(1L),
                  "b" to PropertyType.getPropertyStruct(BOOLEAN, true),
                  "c" to PropertyType.getPropertyStruct(FLOAT, 3.0))),
          Arguments.of(
              Named.of(
                  "map - non-uniformly typed values-compact",
                  mapOf("a" to 1, "b" to true, "c" to 3.0)),
              PayloadMode.COMPACT,
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
  fun `should build schema and values out of nodes and relationships with extended payload`() {
    val payloadMode = PayloadMode.EXTENDED

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
      schemaAndValue(payloadMode, person).also { (schema, converted, reverted) ->
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
      schemaAndValue(payloadMode, company).also { (schema, converted, reverted) ->
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
      schemaAndValue(payloadMode, worksFor).also { (schema, converted, reverted) ->
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
  fun `should build schema and values out of nodes and relationships with compact payload`() {
    val payloadMode = PayloadMode.COMPACT
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
      schemaAndValue(payloadMode, person).also { (schema, converted, reverted) ->
        schema shouldBe
            SchemaBuilder.struct()
                .field("<id>", SimpleTypes.LONG.schema())
                .field("<labels>", SchemaBuilder.array(SimpleTypes.STRING.schema()).build())
                .field("name", SimpleTypes.STRING.schema())
                .field("surname", SimpleTypes.STRING.schema())
                .field("dob", SimpleTypes.LOCALDATE.schema())
                .build()

        converted shouldBe
            Struct(schema)
                .put("<id>", person.id())
                .put("<labels>", person.labels().toList())
                .put("name", "john")
                .put("surname", "doe")
                .put("dob", DateTimeFormatter.ISO_DATE.format(LocalDate.of(1999, 12, 31)))

        reverted shouldBe
            mapOf(
                "<id>" to person.id(),
                "<labels>" to person.labels().toList(),
                "name" to "john",
                "surname" to "doe",
                "dob" to LocalDate.of(1999, 12, 31))
      }

      val company = record.get("c").asNode()
      schemaAndValue(payloadMode, company).also { (schema, converted, reverted) ->
        schema shouldBe
            SchemaBuilder.struct()
                .field("<id>", SimpleTypes.LONG.schema())
                .field("<labels>", SchemaBuilder.array(SimpleTypes.STRING.schema()).build())
                .field("name", SimpleTypes.STRING.schema())
                .field("est", SimpleTypes.LOCALDATE.schema())
                .build()

        converted shouldBe
            Struct(schema)
                .put("<id>", company.id())
                .put("<labels>", company.labels().toList())
                .put("name", "acme corp")
                .put("est", DateTimeFormatter.ISO_DATE.format(LocalDate.of(1980, 1, 1)))

        reverted shouldBe
            mapOf(
                "<id>" to company.id(),
                "<labels>" to company.labels().toList(),
                "name" to "acme corp",
                "est" to LocalDate.of(1980, 1, 1))
      }

      val worksFor = record.get("r").asRelationship()
      schemaAndValue(payloadMode, worksFor).also { (schema, converted, reverted) ->
        schema shouldBe
            SchemaBuilder.struct()
                .field("<id>", SimpleTypes.LONG.schema())
                .field("<type>", SimpleTypes.STRING.schema())
                .field("<start.id>", SimpleTypes.LONG.schema())
                .field("<end.id>", SimpleTypes.LONG.schema())
                .field("contractId", SimpleTypes.LONG.schema())
                .field("since", SimpleTypes.LOCALDATE.schema())
                .build()

        converted shouldBe
            Struct(schema)
                .put("<id>", worksFor.id())
                .put("<type>", worksFor.type())
                .put("<start.id>", worksFor.startNodeId())
                .put("<end.id>", worksFor.endNodeId())
                .put("contractId", 5916L)
                .put("since", DateTimeFormatter.ISO_DATE.format(LocalDate.of(2000, 1, 5)))

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
  fun `should build schema and value for change events and convert back with extended payload`() {
    Assumptions.assumeTrue(
        canIUse(Dbms.changeDataCapture()).withNeo4j(Neo4jDetector.detect(driver)))

    val payloadMode = PayloadMode.EXTENDED
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

      val changeEventConverter = ChangeEventConverter(payloadMode = payloadMode)
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
  fun `should build schema and value for change events and convert back with compact payload`() {
    Assumptions.assumeTrue(
        canIUse(Dbms.changeDataCapture()).withNeo4j(Neo4jDetector.detect(driver)))

    val payloadMode = PayloadMode.COMPACT
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

      val changeEventConverter = ChangeEventConverter(payloadMode = payloadMode)
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
  fun `should build schema for a complex value with extended payload`() {
    val payloadMode = PayloadMode.EXTENDED
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
    val schema = DynamicTypes.toConnectSchema(payloadMode, returned, optional = true)
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

  @Test
  fun `should build schema for a complex value`() {
    val payloadMode = PayloadMode.COMPACT
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
    val schema = DynamicTypes.toConnectSchema(payloadMode, returned, optional = true)
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

  private fun schemaAndValue(payloadMode: PayloadMode, value: Any): Triple<Schema, Any?, Any?> {
    val schema = DynamicTypes.toConnectSchema(payloadMode, value)
    val converted = DynamicTypes.toConnectValue(schema, value)
    val reverted = DynamicTypes.fromConnectValue(schema, converted)
    return Triple(schema, converted, reverted)
  }
}
