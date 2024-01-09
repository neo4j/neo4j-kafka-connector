/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import org.neo4j.connectors.kafka.testing.assertions.AvroCdcRecordAssert.Companion.assertThat
import org.neo4j.connectors.kafka.testing.assertions.EventType
import org.neo4j.connectors.kafka.testing.assertions.Operation
import org.neo4j.connectors.kafka.testing.assertions.TopicVerifier
import org.neo4j.connectors.kafka.testing.source.CdcMetadata
import org.neo4j.connectors.kafka.testing.source.CdcSource
import org.neo4j.connectors.kafka.testing.source.CdcSourceParam
import org.neo4j.connectors.kafka.testing.source.CdcSourceTopic
import org.neo4j.connectors.kafka.testing.source.Neo4jSource
import org.neo4j.connectors.kafka.testing.source.SourceStrategy.CDC
import org.neo4j.connectors.kafka.testing.source.TopicConsumer
import org.neo4j.driver.Session
import org.neo4j.driver.TransactionConfig

class Neo4jCdcSourceNodesIT {

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
                              arrayOf(
                                  CdcSourceParam("(:TestSource{name,+surname,+execId,-age})"))))))
  @Test
  fun `should read changes caught by patterns`(
      testInfo: TestInfo,
      @TopicConsumer(
          topic = "neo4j-cdc-topic",
          offset = "earliest",
          keyDeserializer = StringDeserializer::class)
      consumer: KafkaConsumer<String, GenericRecord>,
      session: Session
  ) {
    val executionId = testInfo.displayName + System.currentTimeMillis()
    session
        .run(
            "CREATE (:TestSource {name: 'Jane', surname: 'Doe', age: 42, execId: \$execId})",
            mapOf("execId" to executionId))
        .consume()
    session.run("MATCH (ts:TestSource {name: 'Jane'}) SET ts.surname = 'Smith'").consume()
    session.run("MATCH (ts:TestSource {name: 'Jane'}) DELETE ts").consume()

    TopicVerifier.create(consumer)
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(EventType.NODE)
              .hasOperation(Operation.CREATE)
              .labelledAs("TestSource")
              .hasNoBeforeState()
              .hasAfterStateProperties(
                  mapOf("name" to "Jane", "surname" to "Doe", "execId" to executionId))
        }
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(EventType.NODE)
              .hasOperation(Operation.UPDATE)
              .labelledAs("TestSource")
              .hasBeforeStateProperties(
                  mapOf("name" to "Jane", "surname" to "Doe", "execId" to executionId))
              .hasAfterStateProperties(
                  mapOf("name" to "Jane", "surname" to "Smith", "execId" to executionId))
        }
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(EventType.NODE)
              .hasOperation(Operation.DELETE)
              .labelledAs("TestSource")
              .hasBeforeStateProperties(
                  mapOf("name" to "Jane", "surname" to "Smith", "execId" to executionId))
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
  fun `should read property removal and additions`(
      testInfo: TestInfo,
      @TopicConsumer(
          topic = "neo4j-cdc-topic-prop-remove-add",
          offset = "earliest",
          keyDeserializer = StringDeserializer::class)
      consumer: KafkaConsumer<String, GenericRecord>,
      session: Session
  ) {
    val executionId = testInfo.displayName + System.currentTimeMillis()
    session
        .run(
            "CREATE (:TestSource {name: 'Jane', surname: 'Doe', age: 42, execId: \$execId})",
            mapOf("execId" to executionId))
        .consume()
    session
        .run("MATCH (ts:TestSource {name: 'Jane'}) SET ts.surname = 'Smith', ts.age = NULL")
        .consume()
    session.run("MATCH (ts:TestSource {name: 'Jane'}) SET ts.age = 50").consume()
    session.run("MATCH (ts:TestSource {name: 'Jane'}) DELETE ts").consume()

    TopicVerifier.create(consumer)
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(EventType.NODE)
              .hasOperation(Operation.CREATE)
              .labelledAs("TestSource")
              .hasNoBeforeState()
              .hasAfterStateProperties(
                  mapOf(
                      "name" to "Jane", "surname" to "Doe", "age" to 42L, "execId" to executionId))
        }
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(EventType.NODE)
              .hasOperation(Operation.UPDATE)
              .labelledAs("TestSource")
              .hasBeforeStateProperties(
                  mapOf(
                      "name" to "Jane", "surname" to "Doe", "age" to 42L, "execId" to executionId))
              .hasAfterStateProperties(
                  mapOf("name" to "Jane", "surname" to "Smith", "execId" to executionId))
        }
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(EventType.NODE)
              .hasOperation(Operation.UPDATE)
              .labelledAs("TestSource")
              .hasBeforeStateProperties(
                  mapOf("name" to "Jane", "surname" to "Smith", "execId" to executionId))
              .hasAfterStateProperties(
                  mapOf(
                      "name" to "Jane",
                      "surname" to "Smith",
                      "age" to 50L,
                      "execId" to executionId))
        }
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(EventType.NODE)
              .hasOperation(Operation.DELETE)
              .labelledAs("TestSource")
              .hasBeforeStateProperties(
                  mapOf(
                      "name" to "Jane",
                      "surname" to "Smith",
                      "age" to 50L,
                      "execId" to executionId))
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
                                      value = "(:TestSource{name,+surname,+execId,-age,email})")),
                          operations = arrayOf(CdcSourceParam(value = "UPDATE")),
                          changesTo = arrayOf(CdcSourceParam(value = "surname,email"))))))
  @Test
  fun `should read only specified field changes on update`(
      testInfo: TestInfo,
      @TopicConsumer(
          topic = "neo4j-cdc-update-topic",
          offset = "earliest",
          keyDeserializer = StringDeserializer::class)
      consumer: KafkaConsumer<String, GenericRecord>,
      session: Session
  ) {
    val executionId = testInfo.displayName + System.currentTimeMillis()
    session
        .run(
            "CREATE (:TestSource {name: 'Jane', surname: 'Doe', age: 42, email: 'janedoe@example.com', execId: \$execId})",
            mapOf("execId" to executionId))
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

    TopicVerifier.create(consumer)
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(EventType.NODE)
              .hasOperation(Operation.UPDATE)
              .labelledAs("TestSource")
              .hasBeforeStateProperties(
                  mapOf(
                      "name" to "Jane",
                      "surname" to "Smith",
                      "execId" to executionId,
                      "email" to "janedoe@example.com"))
              .hasAfterStateProperties(
                  mapOf(
                      "name" to "Jane",
                      "surname" to "Cook",
                      "execId" to executionId,
                      "email" to "janecook@example.com"))
        }
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(EventType.NODE)
              .hasOperation(Operation.UPDATE)
              .labelledAs("TestSource")
              .hasBeforeStateProperties(
                  mapOf(
                      "name" to "Jane",
                      "surname" to "Cook",
                      "execId" to executionId,
                      "email" to "janecook@example.com"))
              .hasAfterStateProperties(
                  mapOf(
                      "name" to "Jane",
                      "surname" to "Johansson",
                      "execId" to executionId,
                      "email" to "janejoh@example.com"))
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
                          topic = "neo4j-cdc-create-inc",
                          patterns = arrayOf(CdcSourceParam("(:TestSource)"))))))
  @Test
  fun `should read changes with different properties using the default topic compatibility mode`(
      testInfo: TestInfo,
      @TopicConsumer(
          topic = "neo4j-cdc-create-inc",
          offset = "earliest",
          keyDeserializer = StringDeserializer::class)
      consumer: KafkaConsumer<String, GenericRecord>,
      session: Session
  ) {
    val executionId = testInfo.displayName + System.currentTimeMillis()
    session
        .run(
            "CREATE (:TestSource {name: 'John', execId: \$execId})", mapOf("execId" to executionId))
        .consume()
    session
        .run(
            "CREATE (:TestSource {title: 'Neo4j', execId: \$execId})",
            mapOf("execId" to executionId))
        .consume()

    TopicVerifier.create(consumer)
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(EventType.NODE)
              .hasOperation(Operation.CREATE)
              .labelledAs("TestSource")
              .hasNoBeforeState()
              .hasAfterStateProperties(mapOf("name" to "John", "execId" to executionId))
        }
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(EventType.NODE)
              .hasOperation(Operation.CREATE)
              .labelledAs("TestSource")
              .hasNoBeforeState()
              .hasAfterStateProperties(mapOf("title" to "Neo4j", "execId" to executionId))
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
  fun `should read each operation to a separate topic`(
      testInfo: TestInfo,
      @TopicConsumer(
          topic = "cdc-creates", offset = "earliest", keyDeserializer = StringDeserializer::class)
      createsConsumer: KafkaConsumer<String, GenericRecord>,
      @TopicConsumer(
          topic = "cdc-updates", offset = "earliest", keyDeserializer = StringDeserializer::class)
      updatesConsumer: KafkaConsumer<String, GenericRecord>,
      @TopicConsumer(
          topic = "cdc-deletes", offset = "earliest", keyDeserializer = StringDeserializer::class)
      deletesConsumer: KafkaConsumer<String, GenericRecord>,
      session: Session
  ) {
    val executionId = testInfo.displayName + System.currentTimeMillis()
    session
        .run(
            "CREATE (:TestSource {name: 'Jane', surname: 'Doe', age: 42, execId: \$execId})",
            mapOf("execId" to executionId))
        .consume()
    session.run("MATCH (ts:TestSource {name: 'Jane'}) SET ts.surname = 'Smith'").consume()
    session.run("MATCH (ts:TestSource {name: 'Jane'}) DELETE ts").consume()

    TopicVerifier.create(createsConsumer)
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(EventType.NODE)
              .hasOperation(Operation.CREATE)
              .labelledAs("TestSource")
              .hasNoBeforeState()
              .hasAfterStateProperties(
                  mapOf(
                      "name" to "Jane", "surname" to "Doe", "age" to 42L, "execId" to executionId))
        }
        .verifyWithin(Duration.ofSeconds(30))
    TopicVerifier.create(updatesConsumer)
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(EventType.NODE)
              .hasOperation(Operation.UPDATE)
              .labelledAs("TestSource")
              .hasBeforeStateProperties(
                  mapOf(
                      "name" to "Jane", "surname" to "Doe", "age" to 42L, "execId" to executionId))
              .hasAfterStateProperties(
                  mapOf(
                      "name" to "Jane",
                      "surname" to "Smith",
                      "age" to 42L,
                      "execId" to executionId))
        }
        .verifyWithin(Duration.ofSeconds(30))
    TopicVerifier.create(deletesConsumer)
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(EventType.NODE)
              .hasOperation(Operation.DELETE)
              .labelledAs("TestSource")
              .hasBeforeStateProperties(
                  mapOf(
                      "name" to "Jane",
                      "surname" to "Smith",
                      "age" to 42L,
                      "execId" to executionId))
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
  fun `should read changes marked with specific transaction metadata attribute`(
      testInfo: TestInfo,
      @TopicConsumer(
          topic = "neo4j-cdc-metadata",
          offset = "earliest",
          keyDeserializer = StringDeserializer::class)
      consumer: KafkaConsumer<String, GenericRecord>,
      session: Session
  ) {
    val executionId = testInfo.displayName + System.currentTimeMillis()
    val transaction1 =
        session.beginTransaction(
            TransactionConfig.builder().withMetadata(mapOf("testLabel" to "A")).build())
    transaction1
        .run(
            "CREATE (:TestSource {name: 'John', execId: \$execId})", mapOf("execId" to executionId))
        .consume()
    transaction1.commit()

    val transaction2 =
        session.beginTransaction(
            TransactionConfig.builder().withMetadata(mapOf("testLabel" to "B")).build())
    transaction2
        .run(
            "CREATE (:TestSource {name: 'Alice', execId: \$execId})",
            mapOf("execId" to executionId))
        .consume()
    transaction2.commit()

    TopicVerifier.create(consumer)
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(EventType.NODE)
              .hasOperation(Operation.CREATE)
              .labelledAs("TestSource")
              .hasNoBeforeState()
              .hasAfterStateProperties(mapOf("name" to "Alice", "execId" to executionId))
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
  fun `should read changes containing node keys`(
      testInfo: TestInfo,
      @TopicConsumer(
          topic = "neo4j-cdc-keys",
          offset = "earliest",
          keyDeserializer = StringDeserializer::class)
      consumer: KafkaConsumer<String, GenericRecord>,
      session: Session
  ) {
    val executionId = testInfo.displayName + System.currentTimeMillis()
    session
        .run("CREATE CONSTRAINT testSourceId FOR (n:TestSource) REQUIRE n.id IS NODE KEY")
        .consume()
    session
        .run("CREATE CONSTRAINT testSourceName FOR (n:TestSource) REQUIRE n.name IS NODE KEY")
        .consume()
    session
        .run("CREATE CONSTRAINT employedId FOR (n:Employee) REQUIRE n.employeeId IS NODE KEY")
        .consume()

    session
        .run(
            "CREATE (:TestSource:Employee {id: 1, name: 'John', employeeId: 456, execId: \$execId})",
            mapOf("execId" to executionId))
        .consume()

    TopicVerifier.create(consumer)
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(EventType.NODE)
              .hasOperation(Operation.CREATE)
              .hasLabels(setOf("TestSource", "Employee"))
              .hasNoBeforeState()
              .hasAfterStateProperties(
                  mapOf(
                      "id" to 1L, "name" to "John", "employeeId" to 456L, "execId" to executionId))
              .hasNodeKeys(
                  mapOf(
                      "TestSource" to listOf(mapOf("id" to 1L), mapOf("name" to "John")),
                      "Employee" to listOf(mapOf("employeeId" to 456L))))
        }
        .verifyWithin(Duration.ofSeconds(30))
  }
}
