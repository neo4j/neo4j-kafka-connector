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
package org.neo4j.connectors.kafka.source

import java.time.Duration
import java.time.LocalDate
import org.junit.jupiter.api.Test
import org.neo4j.caniuse.Neo4j
import org.neo4j.cdc.client.model.ChangeEvent
import org.neo4j.cdc.client.model.EntityOperation
import org.neo4j.cdc.client.model.EntityOperation.CREATE
import org.neo4j.cdc.client.model.EntityOperation.UPDATE
import org.neo4j.cdc.client.model.EventType.NODE
import org.neo4j.connectors.kafka.configuration.PayloadMode
import org.neo4j.connectors.kafka.testing.assertions.ChangeEventAssert.Companion.assertThat
import org.neo4j.connectors.kafka.testing.assertions.TopicVerifier
import org.neo4j.connectors.kafka.testing.createNodeKeyConstraint
import org.neo4j.connectors.kafka.testing.format.KafkaConverter.AVRO
import org.neo4j.connectors.kafka.testing.format.KafkaConverter.JSON_EMBEDDED
import org.neo4j.connectors.kafka.testing.format.KafkaConverter.JSON_SCHEMA
import org.neo4j.connectors.kafka.testing.format.KafkaConverter.PROTOBUF
import org.neo4j.connectors.kafka.testing.format.KeyValueConverter
import org.neo4j.connectors.kafka.testing.kafka.ConvertingKafkaConsumer
import org.neo4j.connectors.kafka.testing.source.CdcMetadata
import org.neo4j.connectors.kafka.testing.source.CdcSource
import org.neo4j.connectors.kafka.testing.source.CdcSourceParam
import org.neo4j.connectors.kafka.testing.source.CdcSourceTopic
import org.neo4j.connectors.kafka.testing.source.Neo4jSource
import org.neo4j.connectors.kafka.testing.source.SourceStrategy.CDC
import org.neo4j.connectors.kafka.testing.source.TopicConsumer
import org.neo4j.driver.Session
import org.neo4j.driver.TransactionConfig

abstract class Neo4jCdcSourceNodesIT {

  @Neo4jSource(
      startFrom = "EARLIEST",
      strategy = CDC,
      cdc =
          CdcSource(
              topics =
                  arrayOf(
                      CdcSourceTopic(
                          topic = "neo4j-cdc-topic",
                          patterns =
                              arrayOf(CdcSourceParam("(:TestSource{name,+surname,-age})"))))))
  @Test
  fun `should publish changes caught by patterns`(
      @TopicConsumer(topic = "neo4j-cdc-topic", offset = "earliest")
      consumer: ConvertingKafkaConsumer,
      session: Session
  ) {
    session
        .run("CREATE (:TestSource {name: 'Jane', surname: 'Doe', age: 42})", emptyMap())
        .consume()
    session.run("MATCH (ts:TestSource {name: 'Jane'}) SET ts.surname = 'Smith'").consume()
    session.run("MATCH (ts:TestSource {name: 'Jane'}) DELETE ts").consume()

    TopicVerifier.create<ChangeEvent, ChangeEvent>(consumer)
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(NODE)
              .hasOperation(CREATE)
              .labelledAs("TestSource")
              .hasNoBeforeState()
              .hasAfterStateProperties(mapOf("name" to "Jane", "surname" to "Doe"))
        }
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(NODE)
              .hasOperation(UPDATE)
              .labelledAs("TestSource")
              .hasBeforeStateProperties(mapOf("name" to "Jane", "surname" to "Doe"))
              .hasAfterStateProperties(mapOf("name" to "Jane", "surname" to "Smith"))
        }
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(NODE)
              .hasOperation(EntityOperation.DELETE)
              .labelledAs("TestSource")
              .hasBeforeStateProperties(mapOf("name" to "Jane", "surname" to "Smith"))
              .hasNoAfterState()
        }
        .verifyWithin(Duration.ofSeconds(30))
  }

  @Neo4jSource(
      startFrom = "EARLIEST",
      strategy = CDC,
      cdc =
          CdcSource(
              topics =
                  arrayOf(
                      CdcSourceTopic(
                          topic = "neo4j-cdc-topic-prop-remove-add",
                          patterns = arrayOf(CdcSourceParam("(:TestSource)"))))))
  @Test
  fun `should publish property removal and additions`(
      @TopicConsumer(topic = "neo4j-cdc-topic-prop-remove-add", offset = "earliest")
      consumer: ConvertingKafkaConsumer,
      session: Session
  ) {
    session
        .run("CREATE (:TestSource {name: 'Jane', surname: 'Doe', age: 42})", emptyMap())
        .consume()
    session
        .run("MATCH (ts:TestSource {name: 'Jane'}) SET ts.surname = 'Smith', ts.age = NULL")
        .consume()
    session.run("MATCH (ts:TestSource {name: 'Jane'}) SET ts.age = 50").consume()
    session.run("MATCH (ts:TestSource {name: 'Jane'}) DELETE ts").consume()

    TopicVerifier.create<ChangeEvent, ChangeEvent>(consumer)
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(NODE)
              .hasOperation(CREATE)
              .labelledAs("TestSource")
              .hasNoBeforeState()
              .hasAfterStateProperties(mapOf("name" to "Jane", "surname" to "Doe", "age" to 42L))
        }
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(NODE)
              .hasOperation(UPDATE)
              .labelledAs("TestSource")
              .hasBeforeStateProperties(mapOf("name" to "Jane", "surname" to "Doe", "age" to 42L))
              .hasAfterStateProperties(mapOf("name" to "Jane", "surname" to "Smith"))
        }
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(NODE)
              .hasOperation(UPDATE)
              .labelledAs("TestSource")
              .hasBeforeStateProperties(mapOf("name" to "Jane", "surname" to "Smith"))
              .hasAfterStateProperties(
                  mapOf(
                      "name" to "Jane",
                      "surname" to "Smith",
                      "age" to 50L,
                  ))
        }
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(NODE)
              .hasOperation(EntityOperation.DELETE)
              .labelledAs("TestSource")
              .hasBeforeStateProperties(
                  mapOf(
                      "name" to "Jane",
                      "surname" to "Smith",
                      "age" to 50L,
                  ))
              .hasNoAfterState()
        }
        .verifyWithin(Duration.ofSeconds(30))
  }

  @Neo4jSource(
      startFrom = "EARLIEST",
      strategy = CDC,
      cdc =
          CdcSource(
              patternsIndexed = true,
              topics =
                  arrayOf(
                      CdcSourceTopic(
                          topic = "neo4j-cdc-update-topic",
                          patterns =
                              arrayOf(
                                  CdcSourceParam(
                                      value = "(:TestSource{name,+surname,-age,email})")),
                          operations = arrayOf(CdcSourceParam(value = "UPDATE")),
                          changesTo = arrayOf(CdcSourceParam(value = "surname,email"))))))
  @Test
  fun `should publish only specified field changes on update`(
      @TopicConsumer(topic = "neo4j-cdc-update-topic", offset = "earliest")
      consumer: ConvertingKafkaConsumer,
      session: Session
  ) {
    session
        .run(
            "CREATE (:TestSource {name: 'Jane', surname: 'Doe', age: 42, email: 'janedoe@example.com'})",
            mapOf())
        .consume()
    session.run("MATCH (ts:TestSource {name: 'Jane'}) SET ts.surname = 'Smith'").consume()
    session
        .run(
            "MATCH (ts:TestSource {name: 'Jane'}) SET ts.surname = 'Cook', ts.email = 'janecook@example.com'")
        .consume()
    session
        .run(
            "MATCH (ts:TestSource {name: 'Jane'}) SET ts.surname = 'Johansson', ts.email = 'janejoh@example.com'")
        .consume()

    TopicVerifier.create<ChangeEvent, ChangeEvent>(consumer)
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(NODE)
              .hasOperation(UPDATE)
              .labelledAs("TestSource")
              .hasBeforeStateProperties(
                  mapOf("name" to "Jane", "surname" to "Smith", "email" to "janedoe@example.com"))
              .hasAfterStateProperties(
                  mapOf("name" to "Jane", "surname" to "Cook", "email" to "janecook@example.com"))
        }
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(NODE)
              .hasOperation(UPDATE)
              .labelledAs("TestSource")
              .hasBeforeStateProperties(
                  mapOf("name" to "Jane", "surname" to "Cook", "email" to "janecook@example.com"))
              .hasAfterStateProperties(
                  mapOf(
                      "name" to "Jane", "surname" to "Johansson", "email" to "janejoh@example.com"))
        }
        .verifyWithin(Duration.ofSeconds(30))
  }

  @Neo4jSource(
      startFrom = "EARLIEST",
      strategy = CDC,
      cdc =
          CdcSource(
              patternsIndexed = true,
              topics =
                  arrayOf(
                      CdcSourceTopic(
                          topic = "cdc-creates",
                          patterns = arrayOf(CdcSourceParam("(:TestSource)")),
                          operations = arrayOf(CdcSourceParam("CREATE"))),
                      CdcSourceTopic(
                          topic = "cdc-updates",
                          patterns = arrayOf(CdcSourceParam("(:TestSource)")),
                          operations = arrayOf(CdcSourceParam("UPDATE"))),
                      CdcSourceTopic(
                          topic = "cdc-deletes",
                          patterns = arrayOf(CdcSourceParam("(:TestSource)")),
                          operations = arrayOf(CdcSourceParam("DELETE"))))))
  @Test
  fun `should publish each operation to a separate topic`(
      @TopicConsumer(topic = "cdc-creates", offset = "earliest")
      createsConsumer: ConvertingKafkaConsumer,
      @TopicConsumer(topic = "cdc-updates", offset = "earliest")
      updatesConsumer: ConvertingKafkaConsumer,
      @TopicConsumer(topic = "cdc-deletes", offset = "earliest")
      deletesConsumer: ConvertingKafkaConsumer,
      session: Session
  ) {
    session.run("CREATE (:TestSource {name: 'Jane', surname: 'Doe', age: 42})", mapOf()).consume()
    session.run("MATCH (ts:TestSource {name: 'Jane'}) SET ts.surname = 'Smith'").consume()
    session.run("MATCH (ts:TestSource {name: 'Jane'}) DELETE ts").consume()

    TopicVerifier.create<ChangeEvent, ChangeEvent>(createsConsumer)
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(NODE)
              .hasOperation(CREATE)
              .labelledAs("TestSource")
              .hasNoBeforeState()
              .hasAfterStateProperties(mapOf("name" to "Jane", "surname" to "Doe", "age" to 42L))
        }
        .verifyWithin(Duration.ofSeconds(30))
    TopicVerifier.create<ChangeEvent, ChangeEvent>(updatesConsumer)
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(NODE)
              .hasOperation(UPDATE)
              .labelledAs("TestSource")
              .hasBeforeStateProperties(mapOf("name" to "Jane", "surname" to "Doe", "age" to 42L))
              .hasAfterStateProperties(
                  mapOf(
                      "name" to "Jane",
                      "surname" to "Smith",
                      "age" to 42L,
                  ))
        }
        .verifyWithin(Duration.ofSeconds(30))
    TopicVerifier.create<ChangeEvent, ChangeEvent>(deletesConsumer)
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(NODE)
              .hasOperation(EntityOperation.DELETE)
              .labelledAs("TestSource")
              .hasBeforeStateProperties(
                  mapOf(
                      "name" to "Jane",
                      "surname" to "Smith",
                      "age" to 42L,
                  ))
              .hasNoAfterState()
        }
        .verifyWithin(Duration.ofSeconds(30))
  }

  @Neo4jSource(
      startFrom = "EARLIEST",
      strategy = CDC,
      cdc =
          CdcSource(
              patternsIndexed = true,
              topics =
                  arrayOf(
                      CdcSourceTopic(
                          topic = "cdc", patterns = arrayOf(CdcSourceParam("(:TestSource)"))))))
  @Test
  fun `should publish each operation to a single topic`(
      @TopicConsumer(topic = "cdc", offset = "earliest") consumer: ConvertingKafkaConsumer,
      session: Session
  ) {
    session.run("CREATE (:TestSource {name: 'Jane', surname: 'Doe', age: 42})", mapOf()).consume()
    session.run("MATCH (ts:TestSource {name: 'Jane'}) SET ts.surname = 'Smith'").consume()
    session.run("MATCH (ts:TestSource {name: 'Jane'}) DELETE ts").consume()

    TopicVerifier.create<ChangeEvent, ChangeEvent>(consumer)
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(NODE)
              .hasOperation(CREATE)
              .labelledAs("TestSource")
              .hasNoBeforeState()
              .hasAfterStateProperties(mapOf("name" to "Jane", "surname" to "Doe", "age" to 42L))
        }
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(NODE)
              .hasOperation(UPDATE)
              .labelledAs("TestSource")
              .hasBeforeStateProperties(mapOf("name" to "Jane", "surname" to "Doe", "age" to 42L))
              .hasAfterStateProperties(
                  mapOf(
                      "name" to "Jane",
                      "surname" to "Smith",
                      "age" to 42L,
                  ))
        }
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(NODE)
              .hasOperation(EntityOperation.DELETE)
              .labelledAs("TestSource")
              .hasBeforeStateProperties(
                  mapOf(
                      "name" to "Jane",
                      "surname" to "Smith",
                      "age" to 42L,
                  ))
              .hasNoAfterState()
        }
        .verifyWithin(Duration.ofSeconds(30))
  }

  @Neo4jSource(
      startFrom = "EARLIEST",
      strategy = CDC,
      cdc =
          CdcSource(
              patternsIndexed = true,
              topics =
                  arrayOf(
                      CdcSourceTopic(
                          topic = "neo4j-cdc-metadata",
                          patterns = arrayOf(CdcSourceParam("(:TestSource)")),
                          metadata =
                              arrayOf(CdcMetadata(key = "txMetadata.testLabel", value = "B"))))))
  @Test
  fun `should publish changes marked with specific transaction metadata attribute`(
      @TopicConsumer(topic = "neo4j-cdc-metadata", offset = "earliest")
      consumer: ConvertingKafkaConsumer,
      session: Session
  ) {
    val transaction1 =
        session.beginTransaction(
            TransactionConfig.builder().withMetadata(mapOf("testLabel" to "A")).build())
    transaction1.run("CREATE (:TestSource {name: 'John'})", mapOf()).consume()
    transaction1.commit()

    val transaction2 =
        session.beginTransaction(
            TransactionConfig.builder().withMetadata(mapOf("testLabel" to "B")).build())
    transaction2.run("CREATE (:TestSource {name: 'Alice'})", mapOf()).consume()
    transaction2.commit()

    TopicVerifier.create<ChangeEvent, ChangeEvent>(consumer)
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(NODE)
              .hasOperation(CREATE)
              .labelledAs("TestSource")
              .hasNoBeforeState()
              .hasAfterStateProperties(mapOf("name" to "Alice"))
              .hasTxMetadata(mapOf("testLabel" to "B"))
        }
        .verifyWithin(Duration.ofSeconds(30))
  }

  @Neo4jSource(
      startFrom = "EARLIEST",
      strategy = CDC,
      cdc =
          CdcSource(
              topics =
                  arrayOf(
                      CdcSourceTopic(
                          topic = "neo4j-cdc-keys",
                          patterns = arrayOf(CdcSourceParam("(:TestSource)"))))))
  @Test
  fun `should publish changes containing node keys`(
      @TopicConsumer(topic = "neo4j-cdc-keys", offset = "earliest")
      consumer: ConvertingKafkaConsumer,
      session: Session,
      neo4j: Neo4j
  ) {
    session.createNodeKeyConstraint(neo4j, "testSourceId", "TestSource", "id")
    session.createNodeKeyConstraint(neo4j, "testSourceName", "TestSource", "name")
    session.createNodeKeyConstraint(neo4j, "employedId", "Employee", "employeeId")
    session.run("CALL db.awaitIndexes()").consume()

    session
        .run("CREATE (:TestSource:Employee {id: 1, name: 'John', employeeId: 456})", mapOf())
        .consume()

    TopicVerifier.create<ChangeEvent, ChangeEvent>(consumer)
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(NODE)
              .hasOperation(CREATE)
              .hasLabels(setOf("TestSource", "Employee"))
              .hasNoBeforeState()
              .hasAfterStateProperties(mapOf("id" to 1L, "name" to "John", "employeeId" to 456L))
              .hasNodeKeys(
                  mapOf(
                      "TestSource" to listOf(mapOf("id" to 1L), mapOf("name" to "John")),
                      "Employee" to listOf(mapOf("employeeId" to 456L))))
        }
        .verifyWithin(Duration.ofSeconds(30))
  }

  @Neo4jSource(
      startFrom = "EARLIEST",
      strategy = CDC,
      cdc =
          CdcSource(
              topics =
                  arrayOf(
                      CdcSourceTopic(
                          topic = "cdc", patterns = arrayOf(CdcSourceParam("(:TestSource)"))))))
  @Test
  fun `should publish changes with arrays`(
      @TopicConsumer(topic = "cdc", offset = "earliest") consumer: ConvertingKafkaConsumer,
      session: Session
  ) {
    session
        .run(
            "CREATE (n:TestSource) SET n = ${'$'}props",
            mapOf(
                "props" to
                    mapOf(
                        "prop1" to arrayOf(1, 2, 3, 4),
                        "prop2" to arrayOf(LocalDate.of(1999, 1, 1), LocalDate.of(2000, 1, 1)),
                        "prop3" to listOf("a", "b", "c"),
                        "prop4" to arrayOf<Boolean>(),
                        "prop5" to listOf<Double>())))
        .consume()

    TopicVerifier.create<ChangeEvent, ChangeEvent>(consumer)
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(NODE)
              .hasOperation(CREATE)
              .labelledAs("TestSource")
              .hasNoBeforeState()
              .hasAfterStateProperties(
                  mapOf(
                      "prop1" to listOf(1L, 2L, 3L, 4L),
                      "prop2" to listOf(LocalDate.of(1999, 1, 1), LocalDate.of(2000, 1, 1)),
                      "prop3" to listOf("a", "b", "c"),
                      "prop4" to emptyList<Boolean>(),
                      "prop5" to emptyList<Double>()))
        }
        .verifyWithin(Duration.ofSeconds(30))
  }

  @Neo4jSource(
      startFrom = "EARLIEST",
      strategy = CDC,
      cdc =
          CdcSource(
              topics =
                  arrayOf(
                      CdcSourceTopic(
                          topic = "cdc", patterns = arrayOf(CdcSourceParam("(:TestSource)"))))))
  @Test
  fun `should publish with multiple keys on the same property`(
      @TopicConsumer(topic = "cdc", offset = "earliest") consumer: ConvertingKafkaConsumer,
      session: Session,
      neo4j: Neo4j
  ) {
    session.createNodeKeyConstraint(neo4j, "test_key1", "TestSource", "prop1", "prop2")
    session.createNodeKeyConstraint(neo4j, "test_key2", "TestSource", "prop1")
    session.run("CALL db.awaitIndexes()").consume()
    session
        .run(
            "CREATE (n:TestSource) SET n = ${'$'}props",
            mapOf("props" to mapOf("prop1" to "value1", "prop2" to "value2")))
        .consume()

    TopicVerifier.create<ChangeEvent, ChangeEvent>(consumer)
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(NODE)
              .hasOperation(CREATE)
              .hasNodeKeys(
                  mapOf(
                      "TestSource" to
                          listOf(
                              mapOf("prop1" to "value1", "prop2" to "value2"),
                              mapOf("prop1" to "value1"))))
              .labelledAs("TestSource")
              .hasNoBeforeState()
              .hasAfterStateProperties(mapOf("prop1" to "value1", "prop2" to "value2"))
        }
        .verifyWithin(Duration.ofSeconds(30))
  }
}

@KeyValueConverter(key = AVRO, value = AVRO, payloadMode = PayloadMode.EXTENDED)
class Neo4jCdcSourceNodesAvroExtendedIT : Neo4jCdcSourceNodesIT()

@KeyValueConverter(key = AVRO, value = AVRO, payloadMode = PayloadMode.COMPACT)
class Neo4jCdcSourceNodesAvroCompactIT : Neo4jCdcSourceNodesIT()

@KeyValueConverter(key = JSON_SCHEMA, value = JSON_SCHEMA, payloadMode = PayloadMode.EXTENDED)
class Neo4jCdcSourceNodesJsonSchemaExtendedIT : Neo4jCdcSourceNodesIT()

@KeyValueConverter(key = JSON_SCHEMA, value = JSON_SCHEMA, payloadMode = PayloadMode.COMPACT)
class Neo4jCdcSourceNodesJsonSchemaCompactIT : Neo4jCdcSourceNodesIT()

@KeyValueConverter(key = JSON_EMBEDDED, value = JSON_EMBEDDED, payloadMode = PayloadMode.EXTENDED)
class Neo4jCdcSourceNodesJsonEmbeddedExtendedIT : Neo4jCdcSourceNodesIT()

@KeyValueConverter(key = JSON_EMBEDDED, value = JSON_EMBEDDED, payloadMode = PayloadMode.COMPACT)
class Neo4jCdcSourceNodesJsonEmbeddedCompactIT : Neo4jCdcSourceNodesIT()

@KeyValueConverter(key = PROTOBUF, value = PROTOBUF, payloadMode = PayloadMode.EXTENDED)
class Neo4jCdcSourceNodesProtobufExtendedIT : Neo4jCdcSourceNodesIT()
