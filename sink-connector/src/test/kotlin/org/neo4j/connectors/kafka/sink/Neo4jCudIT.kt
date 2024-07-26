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
package org.neo4j.connectors.kafka.sink

import com.fasterxml.jackson.databind.ObjectMapper
import io.kotest.assertions.nondeterministic.eventually
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import java.time.LocalDate
import kotlin.time.Duration.Companion.seconds
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.junit.jupiter.api.Test
import org.neo4j.connectors.kafka.data.DynamicTypes
import org.neo4j.connectors.kafka.data.PropertyType
import org.neo4j.connectors.kafka.data.PropertyType.schema
import org.neo4j.connectors.kafka.testing.TestSupport.runTest
import org.neo4j.connectors.kafka.testing.format.KafkaConverter
import org.neo4j.connectors.kafka.testing.format.KeyValueConverter
import org.neo4j.connectors.kafka.testing.kafka.ConvertingKafkaProducer
import org.neo4j.connectors.kafka.testing.kafka.KafkaMessage
import org.neo4j.connectors.kafka.testing.sink.CudStrategy
import org.neo4j.connectors.kafka.testing.sink.Neo4jSink
import org.neo4j.connectors.kafka.testing.sink.TopicProducer
import org.neo4j.connectors.kafka.utils.JSONUtils
import org.neo4j.driver.Session
import org.neo4j.driver.Values

abstract class Neo4jCudIT {

  companion object {
    const val TOPIC = "test"
    const val TOPIC_1 = "test-1"
    const val TOPIC_2 = "test-2"
    const val TOPIC_3 = "test-3"
  }

  @Neo4jSink(cud = [CudStrategy(TOPIC)])
  @Test
  fun `should create node from json string`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session.run("CREATE CONSTRAINT FOR (n:Foo) REQUIRE n.id IS KEY").consume()
    session.run("CREATE CONSTRAINT FOR (n:Bar) REQUIRE n.id IS KEY").consume()

    producer.publish(
        valueSchema = Schema.STRING_SCHEMA,
        value =
            """{
                  "type": "node",
                  "op": "create",
                  "labels": ["Foo", "Bar"],
                  "properties": {
                    "id": 1,
                    "foo": "foo-value"
                  }
                }""")

    eventually(30.seconds) { session.run("MATCH (n) RETURN n", emptyMap()).single() }
        .get("n")
        .asNode() should
        {
          it.labels() shouldBe listOf("Foo", "Bar")
          it.asMap() shouldBe mapOf("id" to 1L, "foo" to "foo-value")
        }
  }

  @Neo4jSink(cud = [CudStrategy(TOPIC)])
  @Test
  fun `should create node from byte array`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session.run("CREATE CONSTRAINT FOR (n:Foo) REQUIRE n.id IS KEY").consume()
    session.run("CREATE CONSTRAINT FOR (n:Bar) REQUIRE n.id IS KEY").consume()

    producer.publish(
        valueSchema = Schema.BYTES_SCHEMA,
        value =
            ObjectMapper()
                .writeValueAsBytes(
                    mapOf(
                        "type" to "node",
                        "op" to "create",
                        "labels" to listOf("Foo", "Bar"),
                        "properties" to mapOf("id" to 1L, "foo" to "foo-value"))))

    eventually(30.seconds) { session.run("MATCH (n) RETURN n", emptyMap()).single() }
        .get("n")
        .asNode() should
        {
          it.labels() shouldBe listOf("Foo", "Bar")
          it.asMap() shouldBe mapOf("id" to 1L, "foo" to "foo-value")
        }
  }

  @Neo4jSink(cud = [CudStrategy(TOPIC)])
  @Test
  fun `should create node from struct`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session.run("CREATE CONSTRAINT FOR (n:Foo) REQUIRE n.id IS KEY").consume()
    session.run("CREATE CONSTRAINT FOR (n:Bar) REQUIRE n.id IS KEY").consume()

    val propertiesSchema =
        SchemaBuilder.struct()
            .field("id", Schema.INT64_SCHEMA)
            .field("foo", Schema.STRING_SCHEMA)
            .field("dob", PropertyType.schema)
            .field("place", PropertyType.schema)
            .build()
    val createNodeSchema =
        SchemaBuilder.struct()
            .field("type", Schema.STRING_SCHEMA)
            .field("op", Schema.STRING_SCHEMA)
            .field("labels", SchemaBuilder.array(Schema.STRING_SCHEMA))
            .field("properties", propertiesSchema)
            .build()

    producer.publish(
        valueSchema = createNodeSchema,
        value =
            Struct(createNodeSchema)
                .put("type", "node")
                .put("op", "create")
                .put("labels", listOf("Foo", "Bar"))
                .put(
                    "properties",
                    Struct(propertiesSchema)
                        .put("id", 1L)
                        .put("foo", "foo-value")
                        .put(
                            "dob",
                            DynamicTypes.toConnectValue(
                                PropertyType.schema, LocalDate.of(1995, 1, 1)))
                        .put(
                            "place",
                            DynamicTypes.toConnectValue(
                                PropertyType.schema, Values.point(7203, 1.0, 2.5).asPoint()))),
    )

    eventually(30.seconds) { session.run("MATCH (n) RETURN n", emptyMap()).single() }
        .get("n")
        .asNode() should
        {
          it.labels() shouldBe listOf("Foo", "Bar")
          it.asMap() shouldBe
              mapOf(
                  "id" to 1L,
                  "foo" to "foo-value",
                  "dob" to LocalDate.of(1995, 1, 1),
                  "place" to Values.point(7203, 1.0, 2.5).asPoint())
        }
  }

  @Neo4jSink(cud = [CudStrategy(TOPIC)])
  @Test
  fun `should update node from struct`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session.run("CREATE CONSTRAINT FOR (n:Foo) REQUIRE n.id IS KEY").consume()
    session.run("CREATE CONSTRAINT FOR (n:Bar) REQUIRE n.id IS KEY").consume()

    session
        .run(
            "CREATE (n:Foo:Bar) SET n = ${'$'}props",
            mapOf("props" to mapOf("id" to 1L, "foo" to "foo-value")))
        .consume()

    val idsSchema = SchemaBuilder.struct().field("id", Schema.INT64_SCHEMA).build()
    val propertiesSchema =
        SchemaBuilder.struct()
            .field("foo", Schema.STRING_SCHEMA)
            .field("dob", PropertyType.schema)
            .field("place", PropertyType.schema)
            .build()
    val updateNodeSchema =
        SchemaBuilder.struct()
            .field("type", Schema.STRING_SCHEMA)
            .field("op", Schema.STRING_SCHEMA)
            .field("labels", SchemaBuilder.array(Schema.STRING_SCHEMA))
            .field("ids", idsSchema)
            .field("properties", propertiesSchema)
            .build()

    producer.publish(
        valueSchema = updateNodeSchema,
        value =
            Struct(updateNodeSchema)
                .put("type", "node")
                .put("op", "update")
                .put("labels", listOf("Foo", "Bar"))
                .put("ids", Struct(idsSchema).put("id", 1L))
                .put(
                    "properties",
                    Struct(propertiesSchema)
                        .put("foo", "foo-value-updated")
                        .put(
                            "dob",
                            DynamicTypes.toConnectValue(
                                PropertyType.schema, LocalDate.of(1995, 1, 1)))
                        .put(
                            "place",
                            DynamicTypes.toConnectValue(
                                PropertyType.schema, Values.point(7203, 1.0, 2.5).asPoint()))),
    )

    eventually(30.seconds) {
      session.run("MATCH (n) RETURN n", emptyMap()).single().get("n").asNode() should
          {
            it.labels() shouldBe listOf("Foo", "Bar")
            it.asMap() shouldBe
                mapOf(
                    "id" to 1L,
                    "foo" to "foo-value-updated",
                    "dob" to LocalDate.of(1995, 1, 1),
                    "place" to Values.point(7203, 1.0, 2.5).asPoint())
          }
    }
  }

  @Neo4jSink(cud = [CudStrategy(TOPIC)])
  @Test
  fun `should merge node from struct`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session.run("CREATE CONSTRAINT FOR (n:Foo) REQUIRE n.id IS KEY").consume()
    session.run("CREATE CONSTRAINT FOR (n:Bar) REQUIRE n.id IS KEY").consume()

    session
        .run(
            "CREATE (n:Foo:Bar) SET n = ${'$'}props",
            mapOf("props" to mapOf("id" to 0L, "foo" to "foo-value")))
        .consume()

    val idsSchema = SchemaBuilder.struct().field("id", Schema.INT64_SCHEMA).build()
    val propertiesSchema =
        SchemaBuilder.struct()
            .field("id", Schema.INT64_SCHEMA)
            .field("foo_new", Schema.STRING_SCHEMA)
            .field("dob", PropertyType.schema)
            .field("place", PropertyType.schema)
            .build()
    val mergeNodeSchema =
        SchemaBuilder.struct()
            .field("type", Schema.STRING_SCHEMA)
            .field("op", Schema.STRING_SCHEMA)
            .field("labels", SchemaBuilder.array(Schema.STRING_SCHEMA))
            .field("ids", idsSchema)
            .field("properties", propertiesSchema)
            .build()

    producer.publish(
        valueSchema = mergeNodeSchema,
        value =
            Struct(mergeNodeSchema)
                .put("type", "node")
                .put("op", "merge")
                .put("labels", listOf("Foo", "Bar"))
                .put("ids", Struct(idsSchema).put("id", 0L))
                .put(
                    "properties",
                    Struct(propertiesSchema)
                        .put("id", 1L)
                        .put("foo_new", "foo-new-value-merged")
                        .put(
                            "dob",
                            DynamicTypes.toConnectValue(
                                PropertyType.schema, LocalDate.of(1995, 1, 1)))
                        .put(
                            "place",
                            DynamicTypes.toConnectValue(
                                PropertyType.schema, Values.point(7203, 1.0, 2.5).asPoint()))),
    )

    eventually(30.seconds) {
      session.run("MATCH (n) RETURN n", emptyMap()).single().get("n").asNode() should
          {
            it.labels() shouldBe listOf("Foo", "Bar")
            it.asMap() shouldBe
                mapOf(
                    "id" to 1L,
                    "foo" to "foo-value",
                    "foo_new" to "foo-new-value-merged",
                    "dob" to LocalDate.of(1995, 1, 1),
                    "place" to Values.point(7203, 1.0, 2.5).asPoint())
          }
    }
  }

  @Neo4jSink(cud = [CudStrategy(TOPIC)])
  @Test
  fun `should delete node from struct`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session.run("CREATE CONSTRAINT FOR (n:Foo) REQUIRE n.id IS KEY").consume()
    session.run("CREATE CONSTRAINT FOR (n:Bar) REQUIRE n.id IS KEY").consume()

    session
        .run(
            "CREATE (n:Foo:Bar) SET n = ${'$'}props",
            mapOf("props" to mapOf("id" to 0L, "foo" to "foo-value")))
        .consume()

    val idsSchema = SchemaBuilder.struct().field("id", Schema.INT64_SCHEMA).build()
    val deleteNodeSchema =
        SchemaBuilder.struct()
            .field("type", Schema.STRING_SCHEMA)
            .field("op", Schema.STRING_SCHEMA)
            .field("labels", SchemaBuilder.array(Schema.STRING_SCHEMA))
            .field("ids", idsSchema)
            .build()

    producer.publish(
        valueSchema = deleteNodeSchema,
        value =
            Struct(deleteNodeSchema)
                .put("type", "node")
                .put("op", "delete")
                .put("labels", listOf("Foo", "Bar"))
                .put("ids", Struct(idsSchema).put("id", 0L)))

    eventually(30.seconds) {
      session
          .run("MATCH (n) RETURN count(n) AS count", emptyMap())
          .single()
          .get("count")
          .asLong() shouldBe 0
    }
  }

  @Neo4jSink(cud = [CudStrategy(TOPIC)])
  @Test
  fun `should detach delete node`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session.run("CREATE CONSTRAINT FOR (n:Foo) REQUIRE n.id IS KEY").consume()
    session.run(
        """
      CREATE (f:Foo) SET f = ${'$'}foo
      CREATE (b:Bar) SET b = ${'$'}bar
      CREATE (f)-[r:RELATED_TO]->(b) SET r = ${'$'}r
    """
            .trimIndent(),
        mapOf(
            "foo" to mapOf("id" to 1L, "foo" to "foo-value"),
            "bar" to mapOf("id" to 1L, "bar" to "bar-value"),
            "r" to mapOf("by" to "incident")))

    producer.publish(
        valueSchema = Schema.STRING_SCHEMA,
        value =
            """{
                  "type": "NODE",
                  "op": "delete",
                  "labels": ["Foo"],
                  "ids": {
                    "id": 1
                  },
                  "detach": true
                }""")

    eventually(30.seconds) {
      session
          .run("MATCH (:Foo)-[r:RELATED_TO]->(:Bar) RETURN count(r) AS count", emptyMap())
          .single()
          .get("count")
          .asLong() shouldBe 0

      session.run("MATCH (n) RETURN n", emptyMap()).single().get("n").asNode() should
          {
            it.labels() shouldBe listOf("Bar")
            it.asMap() shouldBe mapOf("id" to 1L, "bar" to "bar-value")
          }
    }
  }

  @Neo4jSink(cud = [CudStrategy(TOPIC)])
  @Test
  fun `should create and update node`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session.run("CREATE CONSTRAINT FOR (n:Foo) REQUIRE n.id IS KEY").consume()

    producer.publish(
        KafkaMessage(
            valueSchema = Schema.STRING_SCHEMA,
            value =
                """{
                  "type": "node",
                  "op": "create",
                  "labels": ["Foo"],
                  "properties": {
                    "id": 0,
                    "foo": "foo-value"
                  }
                }"""),
        KafkaMessage(
            valueSchema = Schema.STRING_SCHEMA,
            value =
                """{
                  "type": "node",
                  "op": "UPDATE",
                  "labels": ["Foo"],
                  "ids": {
                    "id": 0
                  },
                  "properties": {
                    "id": 1,
                    "foo": "foo-value-updated"
                  }
                }"""))

    eventually(30.seconds) {
      session.run("MATCH (n) RETURN n", emptyMap()).single().get("n").asNode() should
          {
            it.labels() shouldBe listOf("Foo")
            it.asMap() shouldBe mapOf("id" to 1L, "foo" to "foo-value-updated")
          }
    }
  }

  @Neo4jSink(cud = [CudStrategy(TOPIC)])
  @Test
  fun `should not create node with update operation`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session.run("CREATE CONSTRAINT FOR (n:Foo) REQUIRE n.id IS KEY").consume()

    producer.publish(
        KafkaMessage(
            valueSchema = Schema.STRING_SCHEMA,
            value =
                """{
                  "type": "node",
                  "op": "UPDATE",
                  "labels": ["Foo"],
                  "ids": {
                    "id": 1
                  },
                  "properties": {
                    "id": 1,
                    "foo": "foo-value-updated"
                  }
                }"""),
        KafkaMessage(
            valueSchema = Schema.STRING_SCHEMA,
            value =
                """{
                  "type": "node",
                  "op": "create",
                  "labels": ["Foo"],
                  "properties": {
                    "id": 2,
                    "foo": "foo-value"
                  }
                }"""))

    eventually(30.seconds) {
      session.run("MATCH (n) RETURN n").single().get("n").asNode() should
          {
            it.labels() shouldBe listOf("Foo")
            it.asMap() shouldBe mapOf("id" to 2L, "foo" to "foo-value")
          }
    }
  }

  @Neo4jSink(cud = [CudStrategy(TOPIC_1), CudStrategy(TOPIC_2)])
  @Test
  fun `should create and delete node from different topics`(
      @TopicProducer(TOPIC_1) producer1: ConvertingKafkaProducer,
      @TopicProducer(TOPIC_2) producer2: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session.run("CREATE CONSTRAINT FOR (n:Foo) REQUIRE n.id IS KEY").consume()

    producer1.publish(
        valueSchema = Schema.STRING_SCHEMA,
        value =
            """{
                  "type": "node",
                  "op": "create",
                  "labels": ["Foo"],
                  "properties": {
                    "id": 1,
                    "foo": "foo-value"
                  }
                }""")

    eventually(30.seconds) {
      session.run("MATCH (n) RETURN n", emptyMap()).single().get("n").asNode() should
          {
            it.labels() shouldBe listOf("Foo")
            it.asMap() shouldBe mapOf("id" to 1L, "foo" to "foo-value")
          }
    }

    producer2.publish(
        valueSchema = Schema.STRING_SCHEMA,
        value =
            """{
                  "type": "NODE",
                  "op": "delete",
                  "labels": ["Foo"],
                  "ids": {
                    "id": 1
                  }
                }""")

    eventually(30.seconds) {
      session
          .run("MATCH (n) RETURN count(n) AS count", emptyMap())
          .single()
          .get("count")
          .asLong() shouldBe 0
    }
  }

  @Neo4jSink(cud = [CudStrategy(TOPIC)])
  @Test
  fun `should create, delete and recreate node`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session.run("CREATE CONSTRAINT FOR (n:Foo) REQUIRE n.id IS KEY").consume()
    session.run("CREATE CONSTRAINT FOR (n:Bar) REQUIRE n.id IS KEY").consume()

    producer.publish(
        KafkaMessage(
            valueSchema = Schema.STRING_SCHEMA,
            value =
                """{
                  "type": "node",
                  "op": "create",
                  "labels": ["Foo"],
                  "properties": {
                    "id": 0,
                    "foo": "foo-value"
                  }
                }"""),
        KafkaMessage(
            valueSchema = Schema.STRING_SCHEMA,
            value =
                """{
                  "type": "NODE",
                  "op": "delete",
                  "labels": ["Foo"],
                  "ids": {
                    "id": 0
                  },
                  "detach": true
                }"""),
        KafkaMessage(
            valueSchema = Schema.STRING_SCHEMA,
            value =
                """{
                  "type": "node",
                  "op": "create",
                  "labels": ["Foo", "Bar"],
                  "properties": {
                    "id": 1,
                    "foo": "foo-value-new"
                  }
                }"""))

    eventually(30.seconds) {
      session.run("MATCH (n) RETURN n", emptyMap()).single().get("n").asNode() should
          {
            it.labels() shouldBe listOf("Foo", "Bar")
            it.asMap() shouldBe mapOf("id" to 1L, "foo" to "foo-value-new")
          }
    }
  }

  @Neo4jSink(cud = [CudStrategy(TOPIC)])
  @Test
  fun `should create 1000 nodes`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session.run("CREATE CONSTRAINT FOR (n:Foo) REQUIRE n.id IS KEY").consume()

    producer.publish(
        *(1..1000)
            .map {
              KafkaMessage(
                  valueSchema = Schema.STRING_SCHEMA,
                  value =
                      """{
                  "type": "node",
                  "op": "create",
                  "labels": ["Foo"],
                  "properties": {
                    "id": ${it},
                    "foo": "foo-value-${it}"
                  }
                }""")
            }
            .toTypedArray())

    eventually(30.seconds) {
      session
          .run("MATCH (n:Foo) RETURN count(n) AS count", emptyMap())
          .single()
          .get("count")
          .asLong() shouldBe 1000
    }
  }

  @Neo4jSink(cud = [CudStrategy(TOPIC)])
  @Test
  fun `should create relationship`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session.run("CREATE CONSTRAINT FOR (n:Foo) REQUIRE n.id IS KEY").consume()
    session.run("CREATE CONSTRAINT FOR (n:Bar) REQUIRE n.id IS KEY").consume()

    session
        .run(
            """
                CREATE (f:Foo) SET f = ${'$'}foo
                CREATE (b:Bar) SET b = ${'$'}bar
              """
                .trimIndent(),
            mapOf(
                "foo" to mapOf("id" to 1L, "foo" to "foo-value"),
                "bar" to mapOf("id" to 1L, "bar" to "bar-value")))
        .consume()

    producer.publish(
        valueSchema = Schema.STRING_SCHEMA,
        value =
            """
                {
                  "type": "relationship",
                  "op": "create",
                  "rel_type": "RELATED_TO",
                  "from": {
                    "labels": ["Foo"],
                    "ids": {
                      "id": 1
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
                """)

    eventually(30.seconds) {
      session
          .run(
              "MATCH (:Foo {id: ${'$'}fooId})-[r]->(:Bar {id: ${'$'}barId}) RETURN r",
              mapOf("fooId" to 1L, "barId" to 1L))
          .single()
          .get("r")
          .asRelationship() should
          {
            it.type() shouldBe "RELATED_TO"
            it.asMap() shouldBe mapOf("by" to "incident")
          }
    }
  }

  @Neo4jSink(cud = [CudStrategy(TOPIC)])
  @Test
  fun `should create relationship by merging nodes`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session.run("CREATE CONSTRAINT FOR (n:Foo) REQUIRE n.id IS KEY").consume()
    session.run("CREATE CONSTRAINT FOR (n:Bar) REQUIRE n.id IS KEY").consume()

    producer.publish(
        valueSchema = Schema.STRING_SCHEMA,
        value =
            """
                {
                  "type": "relationship",
                  "op": "create",
                  "rel_type": "RELATED_TO",
                  "from": {
                    "labels": ["Foo"],
                    "ids": {
                      "id": 1
                    },
                    "op": "merge"
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
                """)

    eventually(30.seconds) {
      val result =
          session
              .run(
                  "MATCH (f:Foo {id: ${'$'}fooId})-[r:RELATED_TO]->(b:Bar {id: ${'$'}barId}) RETURN f,b,r",
                  mapOf("fooId" to 1L, "barId" to 1L))
              .single()

      result.get("f").asNode() should
          {
            it.labels() shouldBe listOf("Foo")
            it.asMap() shouldBe mapOf("id" to 1L)
          }

      result.get("r").asRelationship() should
          {
            it.type() shouldBe "RELATED_TO"
            it.asMap() shouldBe mapOf("by" to "incident")
          }

      result.get("b").asNode() should
          {
            it.labels() shouldBe listOf("Bar")
            it.asMap() shouldBe mapOf("id" to 1L)
          }
    }
  }

  @Neo4jSink(cud = [CudStrategy(TOPIC)])
  @Test
  fun `should create relationship by merging one of the nodes`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session.run("CREATE CONSTRAINT FOR (n:Foo) REQUIRE n.id IS KEY").consume()
    session.run("CREATE CONSTRAINT FOR (n:Bar) REQUIRE n.id IS KEY").consume()

    session.run("CREATE (b:Bar) SET b = ${'$'}bar", mapOf("bar" to mapOf("id" to 1L))).consume()

    producer.publish(
        valueSchema = Schema.STRING_SCHEMA,
        value =
            """
                {
                  "type": "relationship",
                  "op": "create",
                  "rel_type": "RELATED_TO",
                  "from": {
                    "labels": ["Foo"],
                    "ids": {
                      "id": 1
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
                """)

    eventually(30.seconds) {
      val result =
          session
              .run(
                  "MATCH (f:Foo {id: ${'$'}fooId})-[r:RELATED_TO]->(b:Bar {id: ${'$'}barId}) RETURN f,b,r",
                  mapOf("fooId" to 1L, "barId" to 1L))
              .single()

      result.get("f").asNode() should
          {
            it.labels() shouldBe listOf("Foo")
            it.asMap() shouldBe mapOf("id" to 1L)
          }

      result.get("r").asRelationship() should
          {
            it.type() shouldBe "RELATED_TO"
            it.asMap() shouldBe mapOf("by" to "incident")
          }

      result.get("b").asNode() should
          {
            it.labels() shouldBe listOf("Bar")
            it.asMap() shouldBe mapOf("id" to 1L)
          }
    }
  }

  @Neo4jSink(cud = [CudStrategy(TOPIC)])
  @Test
  fun `should update relationship`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session.run("CREATE CONSTRAINT FOR (n:Foo) REQUIRE n.id IS KEY").consume()
    session.run("CREATE CONSTRAINT FOR (n:Bar) REQUIRE n.id IS KEY").consume()

    session
        .run(
            """
                CREATE (f:Foo) SET f = ${'$'}foo
                CREATE (b:Bar) SET b = ${'$'}bar
                CREATE (f)-[r:RELATED_TO]->(b) SET r = ${'$'}r
              """
                .trimIndent(),
            mapOf(
                "foo" to mapOf("id" to 1L, "foo" to "foo-value"),
                "bar" to mapOf("id" to 1L, "bar" to "bar-value"),
                "r" to mapOf("by" to "incident")))
        .consume()

    producer.publish(
        valueSchema = Schema.STRING_SCHEMA,
        value =
            """
                {
                  "type": "relationship",
                  "op": "update",
                  "rel_type": "RELATED_TO",
                  "from": {
                    "labels": ["Foo"],
                    "ids": {
                      "id": 1
                    }
                  },
                  "to": {
                    "labels": ["Bar"],
                    "ids": {
                      "id": 1
                    }
                  },
                  "properties": {
                    "by": "incident-updated"
                  }
                }
                """)

    eventually(30.seconds) {
      session
          .run(
              "MATCH (:Foo {id: ${'$'}fooId})-[r]->(:Bar {id: ${'$'}barId}) RETURN r",
              mapOf("fooId" to 1L, "barId" to 1L))
          .single()
          .get("r")
          .asRelationship() should
          {
            it.type() shouldBe "RELATED_TO"
            it.asMap() shouldBe mapOf("by" to "incident-updated")
          }
    }
  }

  @Neo4jSink(cud = [CudStrategy(TOPIC)])
  @Test
  fun `should update relationship with ids`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session.run("CREATE CONSTRAINT FOR (n:Foo) REQUIRE n.id IS KEY").consume()
    session.run("CREATE CONSTRAINT FOR (n:Bar) REQUIRE n.id IS KEY").consume()
    session
        .run("CREATE CONSTRAINT FOR ()-[r:RELATED_TO]-() REQUIRE r.id IS RELATIONSHIP KEY")
        .consume()

    session
        .run(
            """
                CREATE (f:Foo) SET f = ${'$'}foo
                CREATE (b:Bar) SET b = ${'$'}bar
                CREATE (f)-[r:RELATED_TO]->(b) SET r = ${'$'}r
              """
                .trimIndent(),
            mapOf(
                "foo" to mapOf("id" to 1L, "foo" to "foo-value"),
                "bar" to mapOf("id" to 1L, "bar" to "bar-value"),
                "r" to mapOf("id" to 2L, "by" to "incident")))
        .consume()

    producer.publish(
        valueSchema = Schema.STRING_SCHEMA,
        value =
            """
                {
                  "type": "relationship",
                  "op": "update",
                  "rel_type": "RELATED_TO",
                  "from": {
                    "labels": ["Foo"],
                    "ids": {
                      "id": 1
                    }
                  },
                  "to": {
                    "labels": ["Bar"],
                    "ids": {
                      "id": 1
                    }
                  },
                  "ids": {
                    "id": 2
                  },
                  "properties": {
                    "by": "incident-updated"
                  }
                }
                """)

    eventually(30.seconds) {
      session
          .run(
              "MATCH (:Foo {id: ${'$'}fooId})-[r]->(:Bar {id: ${'$'}barId}) RETURN r",
              mapOf("fooId" to 1L, "barId" to 1L))
          .single()
          .get("r")
          .asRelationship() should
          {
            it.type() shouldBe "RELATED_TO"
            it.asMap() shouldBe mapOf("id" to 2L, "by" to "incident-updated")
          }
    }
  }

  @Neo4jSink(cud = [CudStrategy(TOPIC)])
  @Test
  fun `should merge relationship`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session.run("CREATE CONSTRAINT FOR (n:Foo) REQUIRE n.id IS KEY").consume()
    session.run("CREATE CONSTRAINT FOR (n:Bar) REQUIRE n.id IS KEY").consume()

    session
        .run(
            """
                CREATE (f:Foo) SET f = ${'$'}foo
                CREATE (b:Bar) SET b = ${'$'}bar
                CREATE (f)-[r:RELATED_TO]->(b) SET r = ${'$'}r
              """
                .trimIndent(),
            mapOf(
                "foo" to mapOf("id" to 1L, "foo" to "foo-value"),
                "bar" to mapOf("id" to 1L, "bar" to "bar-value"),
                "r" to mapOf("by" to "incident")))
        .consume()

    producer.publish(
        valueSchema = Schema.STRING_SCHEMA,
        value =
            """
                {
                  "type": "relationship",
                  "op": "merge",
                  "rel_type": "RELATED_TO",
                  "from": {
                    "labels": ["Foo"],
                    "ids": {
                      "id": 1
                    }
                  },
                  "to": {
                    "labels": ["Bar"],
                    "ids": {
                      "id": 1
                    }
                  },
                  "properties": {
                    "by": "incident-updated",
                    "by-new": "incident-merged"
                  }
                }
                """)

    eventually(30.seconds) {
      session
          .run(
              "MATCH (:Foo {id: ${'$'}fooId})-[r]->(:Bar {id: ${'$'}barId}) RETURN r",
              mapOf("fooId" to 1L, "barId" to 1L))
          .single()
          .get("r")
          .asRelationship() should
          {
            it.type() shouldBe "RELATED_TO"
            it.asMap() shouldBe mapOf("by" to "incident-updated", "by-new" to "incident-merged")
          }
    }
  }

  @Neo4jSink(cud = [CudStrategy(TOPIC)])
  @Test
  fun `should merge relationship with merging nodes`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session.run("CREATE CONSTRAINT FOR (n:Foo) REQUIRE n.id IS KEY").consume()
    session.run("CREATE CONSTRAINT FOR (n:Bar) REQUIRE n.id IS KEY").consume()

    producer.publish(
        valueSchema = Schema.STRING_SCHEMA,
        value =
            """
                {
                  "type": "relationship",
                  "op": "merge",
                  "rel_type": "RELATED_TO",
                  "from": {
                    "labels": ["Foo"],
                    "ids": {
                      "id": 1
                    },
                    "op": "merge"
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
                """)

    eventually(30.seconds) {
      val result =
          session
              .run(
                  "MATCH (f:Foo {id: ${'$'}fooId})-[r]->(b:Bar {id: ${'$'}barId}) RETURN f,b,r",
                  mapOf("fooId" to 1L, "barId" to 1L))
              .single()

      result.get("f").asNode() should
          {
            it.labels() shouldBe listOf("Foo")
            it.asMap() shouldBe mapOf("id" to 1L)
          }

      result.get("r").asRelationship() should
          {
            it.type() shouldBe "RELATED_TO"
            it.asMap() shouldBe mapOf("by" to "incident")
          }

      result.get("b").asNode() should
          {
            it.labels() shouldBe listOf("Bar")
            it.asMap() shouldBe mapOf("id" to 1L)
          }
    }
  }

  @Neo4jSink(cud = [CudStrategy(TOPIC)])
  @Test
  fun `should merge relationship with merging one of the nodes`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session.run("CREATE CONSTRAINT FOR (n:Foo) REQUIRE n.id IS KEY").consume()
    session.run("CREATE CONSTRAINT FOR (n:Bar) REQUIRE n.id IS KEY").consume()

    session.run("CREATE (f:Foo) SET f = ${'$'}foo ", mapOf("foo" to mapOf("id" to 1L))).consume()

    producer.publish(
        valueSchema = Schema.STRING_SCHEMA,
        value =
            """
                {
                  "type": "relationship",
                  "op": "merge",
                  "rel_type": "RELATED_TO",
                  "from": {
                    "labels": ["Foo"],
                    "ids": {
                      "id": 1
                    },
                    "op": "match"
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
                """)

    eventually(30.seconds) {
      val result =
          session
              .run(
                  "MATCH (f:Foo {id: ${'$'}fooId})-[r]->(b:Bar {id: ${'$'}barId}) RETURN f,b,r",
                  mapOf("fooId" to 1L, "barId" to 1L))
              .single()

      result.get("f").asNode() should
          {
            it.labels() shouldBe listOf("Foo")
            it.asMap() shouldBe mapOf("id" to 1L)
          }

      result.get("r").asRelationship() should
          {
            it.type() shouldBe "RELATED_TO"
            it.asMap() shouldBe mapOf("by" to "incident")
          }

      result.get("b").asNode() should
          {
            it.labels() shouldBe listOf("Bar")
            it.asMap() shouldBe mapOf("id" to 1L)
          }
    }
  }

  @Neo4jSink(cud = [CudStrategy(TOPIC)])
  @Test
  fun `should merge relationship with ids`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session.run("CREATE CONSTRAINT FOR (n:Foo) REQUIRE n.id IS KEY").consume()
    session.run("CREATE CONSTRAINT FOR (n:Bar) REQUIRE n.id IS KEY").consume()
    session
        .run("CREATE CONSTRAINT FOR ()-[r:RELATED_TO]-() REQUIRE r.id IS RELATIONSHIP KEY")
        .consume()

    session
        .run(
            """
                CREATE (f:Foo) SET f = ${'$'}foo
                CREATE (b:Bar) SET b = ${'$'}bar
                CREATE (f)-[r:RELATED_TO]->(b) SET r = ${'$'}r
              """
                .trimIndent(),
            mapOf(
                "foo" to mapOf("id" to 1L, "foo" to "foo-value"),
                "bar" to mapOf("id" to 1L, "bar" to "bar-value"),
                "r" to mapOf("id" to 2L, "by" to "incident")))
        .consume()

    producer.publish(
        valueSchema = Schema.STRING_SCHEMA,
        value =
            """
                {
                  "type": "relationship",
                  "op": "merge",
                  "rel_type": "RELATED_TO",
                  "from": {
                    "labels": ["Foo"],
                    "ids": {
                      "id": 1
                    }
                  },
                  "to": {
                    "labels": ["Bar"],
                    "ids": {
                      "id": 1
                    }
                  },
                  "ids": {
                    "id": 2
                  },
                  "properties": {
                    "by": "incident-updated",
                    "by-new": "incident-merged"
                  }
                }
                """)

    eventually(30.seconds) {
      session
          .run(
              "MATCH (:Foo {id: ${'$'}fooId})-[r]->(:Bar {id: ${'$'}barId}) RETURN r",
              mapOf("fooId" to 1L, "barId" to 1L))
          .single()
          .get("r")
          .asRelationship() should
          {
            it.type() shouldBe "RELATED_TO"
            it.asMap() shouldBe
                mapOf("id" to 2L, "by" to "incident-updated", "by-new" to "incident-merged")
          }
    }
  }

  @Neo4jSink(cud = [CudStrategy(TOPIC)])
  @Test
  fun `should delete relationship`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session.run("CREATE CONSTRAINT FOR (n:Foo) REQUIRE n.id IS KEY").consume()
    session.run("CREATE CONSTRAINT FOR (n:Bar) REQUIRE n.id IS KEY").consume()

    session
        .run(
            """
                CREATE (f:Foo) SET f = ${'$'}foo
                CREATE (b:Bar) SET b = ${'$'}bar
                CREATE (f)-[r:RELATED_TO]->(b) SET r = ${'$'}r
              """
                .trimIndent(),
            mapOf(
                "foo" to mapOf("id" to 1L, "foo" to "foo-value"),
                "bar" to mapOf("id" to 1L, "bar" to "bar-value"),
                "r" to mapOf("by" to "incident")))
        .consume()

    producer.publish(
        valueSchema = Schema.STRING_SCHEMA,
        value =
            """
                {
                  "type": "relationship",
                  "op": "delete",
                  "rel_type": "RELATED_TO",
                  "from": {
                    "labels": ["Foo"],
                    "ids": {
                      "id": 1
                    }
                  },
                  "to": {
                    "labels": ["Bar"],
                    "ids": {
                      "id": 1
                    }
                  }
                }
                """)

    eventually(30.seconds) {
      session
          .run(
              "MATCH (:Foo {id: ${'$'}fooId})-[r:RELATED_TO]->(:Bar {id: ${'$'}barId}) RETURN count(r) as count",
              mapOf("fooId" to 1L, "barId" to 1L))
          .single()
          .get("count")
          .asLong() shouldBe 0
    }
  }

  @Neo4jSink(cud = [CudStrategy(TOPIC)])
  @Test
  fun `should delete relationship with ids`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session.run("CREATE CONSTRAINT FOR (n:Foo) REQUIRE n.id IS KEY").consume()
    session.run("CREATE CONSTRAINT FOR (n:Bar) REQUIRE n.id IS KEY").consume()
    session
        .run("CREATE CONSTRAINT FOR ()-[r:RELATED_TO]-() REQUIRE r.id IS RELATIONSHIP KEY")
        .consume()

    session
        .run(
            """
                CREATE (f:Foo) SET f = ${'$'}foo
                CREATE (b:Bar) SET b = ${'$'}bar
                CREATE (f)-[r:RELATED_TO]->(b) SET r = ${'$'}r
              """
                .trimIndent(),
            mapOf(
                "foo" to mapOf("id" to 1L, "foo" to "foo-value"),
                "bar" to mapOf("id" to 1L, "bar" to "bar-value"),
                "r" to mapOf("id" to 2L, "by" to "incident")))
        .consume()

    producer.publish(
        valueSchema = Schema.STRING_SCHEMA,
        value =
            """
                {
                  "type": "relationship",
                  "op": "delete",
                  "rel_type": "RELATED_TO",
                  "from": {
                    "labels": ["Foo"],
                    "ids": {
                      "id": 1
                    }
                  },
                  "to": {
                    "labels": ["Bar"],
                    "ids": {
                      "id": 1
                    }
                  },
                  "ids": {
                    "id": 2
                  }
                }
                """)

    eventually(30.seconds) {
      session
          .run(
              "MATCH (:Foo {id: ${'$'}fooId})-[r:RELATED_TO {id: ${'$'}rId}]->(:Bar {id: ${'$'}barId}) RETURN count(r) as count",
              mapOf("fooId" to 1L, "barId" to 1L, "rId" to 2L))
          .single()
          .get("count")
          .asLong() shouldBe 0
    }
  }

  @Neo4jSink(cud = [CudStrategy(TOPIC_1), CudStrategy(TOPIC_2), CudStrategy(TOPIC_3)])
  @Test
  fun `should merge nodes and relationship from different topics`(
      @TopicProducer(TOPIC_1) producer1: ConvertingKafkaProducer,
      @TopicProducer(TOPIC_2) producer2: ConvertingKafkaProducer,
      @TopicProducer(TOPIC_3) producer3: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session.run("CREATE CONSTRAINT FOR (n:Foo) REQUIRE n.id IS KEY").consume()
    session.run("CREATE CONSTRAINT FOR (n:Bar) REQUIRE n.id IS KEY").consume()

    producer1.publish(
        valueSchema = Schema.STRING_SCHEMA,
        value =
            """{
                  "type": "node",
                  "op": "merge",
                  "labels": ["Foo"],
                  "ids": {
                    "id": 1
                  },
                  "properties": {
                    "foo": "foo-value"
                  }
                }""")

    producer2.publish(
        valueSchema = Schema.STRING_SCHEMA,
        value =
            """{
                    "type": "node",
                    "op": "merge",
                    "labels": ["Bar"],
                    "ids": {
                      "id": 1
                    },
                    "properties": {
                      "bar": "bar-value"
                    }
                  }""")

    producer3.publish(
        valueSchema = Schema.STRING_SCHEMA,
        value =
            """
                    {
                      "type": "relationship",
                      "op": "create",
                      "rel_type": "RELATED_TO",
                      "from": {
                        "labels": ["Foo"],
                        "ids": {
                          "id": 1
                        },
                        "op": "merge"
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
                    """)

    eventually(60.seconds) {
      val result =
          session.run("MATCH (f:Foo)-[r:RELATED_TO]->(b:Bar) RETURN f,b,r", emptyMap()).single()

      result.get("f").asNode() should
          {
            it.labels() shouldBe listOf("Foo")
            it.asMap() shouldBe mapOf("id" to 1L, "foo" to "foo-value")
          }

      result.get("r").asRelationship() should
          {
            it.type() shouldBe "RELATED_TO"
            it.asMap() shouldBe mapOf("by" to "incident")
          }

      result.get("b").asNode() should
          {
            it.labels() shouldBe listOf("Bar")
            it.asMap() shouldBe mapOf("id" to 1L, "bar" to "bar-value")
          }
    }
  }

  @Neo4jSink(cud = [CudStrategy(TOPIC)])
  @Test
  fun `should handle mixed events`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session
  ) = runTest {
    session.run("CREATE CONSTRAINT FOR (n:Foo) REQUIRE n.id IS KEY").consume()
    session.run("CREATE CONSTRAINT FOR (n:Bar) REQUIRE n.id IS KEY").consume()

    val kafkaMessages = mutableListOf<KafkaMessage>()
    for (i in 0 ..< 100) {
      kafkaMessages.add(
          KafkaMessage(
              valueSchema = Schema.STRING_SCHEMA,
              value =
                  JSONUtils.writeValueAsString(
                      mapOf(
                          "type" to "node",
                          "op" to "create",
                          "labels" to listOf("Foo"),
                          "properties" to mapOf("id" to i, "foo" to "${i}-foo-value")))))

      kafkaMessages.add(
          KafkaMessage(
              valueSchema = Schema.STRING_SCHEMA,
              value =
                  JSONUtils.writeValueAsString(
                      mapOf(
                          "type" to "node",
                          "op" to "create",
                          "labels" to listOf("Bar"),
                          "properties" to mapOf("id" to i, "bar" to "${i}-bar-value")))))

      kafkaMessages.add(
          KafkaMessage(
              valueSchema = Schema.STRING_SCHEMA,
              value =
                  JSONUtils.writeValueAsString(
                      mapOf(
                          "type" to "relationship",
                          "op" to "create",
                          "rel_type" to "RELATED_TO",
                          "from" to mapOf("labels" to listOf("Foo"), "ids" to mapOf("id" to i)),
                          "to" to mapOf("labels" to listOf("Bar"), "ids" to mapOf("id" to i)),
                          "properties" to mapOf("id" to i, "by" to "${i}-incident")))))
    }

    val modulo = 4
    kafkaMessages.addAll(
        (0 ..< 100)
            .map {
              when (it % modulo) {
                0 ->
                    mapOf(
                        "type" to "node",
                        "op" to "update",
                        "labels" to listOf("Foo"),
                        "ids" to mapOf("id" to it),
                        "properties" to mapOf("id" to it, "foo" to "$it-foo-value-updated"))
                1 ->
                    mapOf(
                        "type" to "node",
                        "op" to "merge",
                        "labels" to listOf("Bar"),
                        "ids" to mapOf("id" to it),
                        "properties" to
                            mapOf(
                                "id" to it,
                                "bar" to "$it-bar-value-updated",
                                "bar-new" to "$it-new-bar-value-merged"))
                2 ->
                    mapOf(
                        "type" to "relationship",
                        "op" to "update",
                        "rel_type" to "RELATED_TO",
                        "from" to mapOf("labels" to listOf("Foo"), "ids" to mapOf("id" to it)),
                        "to" to mapOf("labels" to listOf("Bar"), "ids" to mapOf("id" to it)),
                        "properties" to mapOf("id" to it, "by" to "$it-incident-updated"))
                3 ->
                    mapOf(
                        "type" to "relationship",
                        "op" to "merge",
                        "rel_type" to "RELATED_TO",
                        "from" to mapOf("labels" to listOf("Foo"), "ids" to mapOf("id" to it)),
                        "to" to mapOf("labels" to listOf("Bar"), "ids" to mapOf("id" to it)),
                        "properties" to
                            mapOf(
                                "id" to it,
                                "by" to "$it-incident-updated",
                                "by-new" to "$it-new-incident-merged"))
                else -> throw IllegalArgumentException("unexpected")
              }
            }
            .map { eventMap ->
              KafkaMessage(
                  valueSchema = Schema.STRING_SCHEMA,
                  value = JSONUtils.writeValueAsString(eventMap))
            })

    producer.publish(*kafkaMessages.toTypedArray())

    eventually(30.seconds) {
      session
          .run("MATCH (f:Foo)-[r:RELATED_TO]->(b:Bar) RETURN f,b,r", emptyMap())
          .list()
          .forEachIndexed { index, record ->
            when (index % modulo) {
              0 -> {
                record.get("f").asNode() should
                    {
                      it.labels() shouldBe listOf("Foo")
                      it.asMap() shouldBe mapOf("id" to index, "foo" to "$index-foo-value-updated")
                    }
                record.get("b").asNode() should
                    {
                      it.labels() shouldBe listOf("Bar")
                      it.asMap() shouldBe mapOf("id" to index, "bar" to "$index-bar-value")
                    }
                record.get("r").asRelationship() should
                    {
                      it.type() shouldBe "RELATED_TO"
                      it.asMap() shouldBe mapOf("id" to index, "by" to "$index-incident")
                    }
              }
              1 -> {
                record.get("f").asNode() should
                    {
                      it.labels() shouldBe listOf("Foo")
                      it.asMap() shouldBe mapOf("id" to index, "foo" to "$index-foo-value")
                    }
                record.get("b").asNode() should
                    {
                      it.labels() shouldBe listOf("Bar")
                      it.asMap() shouldBe
                          mapOf(
                              "id" to index,
                              "bar" to "$index-bar-value-updated",
                              "bar-new" to "$index-new-bar-value-merged")
                    }
                record.get("r").asRelationship() should
                    {
                      it.type() shouldBe "RELATED_TO"
                      it.asMap() shouldBe mapOf("id" to index, "by" to "$index-incident")
                    }
              }
              2 -> {
                record.get("f").asNode() should
                    {
                      it.labels() shouldBe listOf("Foo")
                      it.asMap() shouldBe mapOf("id" to index, "foo" to "$index-foo-value")
                    }
                record.get("b").asNode() should
                    {
                      it.labels() shouldBe listOf("Bar")
                      it.asMap() shouldBe mapOf("id" to index, "bar" to "$index-bar-value")
                    }
                record.get("r").asRelationship() should
                    {
                      it.type() shouldBe "RELATED_TO"
                      it.asMap() shouldBe mapOf("id" to index, "by" to "$index-incident-updated")
                    }
              }
              3 -> {
                record.get("f").asNode() should
                    {
                      it.labels() shouldBe listOf("Foo")
                      it.asMap() shouldBe mapOf("id" to index, "foo" to "$index-foo-value")
                    }
                record.get("b").asNode() should
                    {
                      it.labels() shouldBe listOf("Bar")
                      it.asMap() shouldBe mapOf("id" to index, "bar" to "$index-bar-value")
                    }
                record.get("r").asRelationship() should
                    {
                      it.type() shouldBe "RELATED_TO"
                      it.asMap() shouldBe
                          mapOf(
                              "id" to index,
                              "by" to "$index-incident-updated",
                              "by-new" to "$index-new-incident-merged")
                    }
              }
            }
          }
    }
  }

  @KeyValueConverter(key = KafkaConverter.AVRO, value = KafkaConverter.AVRO)
  class Neo4jCudAvroIT : Neo4jCudIT()

  @KeyValueConverter(key = KafkaConverter.JSON_SCHEMA, value = KafkaConverter.JSON_SCHEMA)
  class Neo4jCudJsonIT : Neo4jCudIT()

  @KeyValueConverter(key = KafkaConverter.PROTOBUF, value = KafkaConverter.PROTOBUF)
  class Neo4jCudProtobufIT : Neo4jCudIT()
}
