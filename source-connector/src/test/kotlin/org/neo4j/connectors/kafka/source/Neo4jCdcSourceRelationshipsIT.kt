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
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import org.neo4j.cdc.client.model.ChangeEvent
import org.neo4j.cdc.client.model.EntityOperation
import org.neo4j.cdc.client.model.EventType
import org.neo4j.connectors.kafka.testing.assertions.ChangeEventAssert
import org.neo4j.connectors.kafka.testing.assertions.TopicVerifier2
import org.neo4j.connectors.kafka.testing.format.KafkaConverter.AVRO
import org.neo4j.connectors.kafka.testing.format.KafkaConverter.JSON_SCHEMA
import org.neo4j.connectors.kafka.testing.format.KafkaConverter.PROTOBUF
import org.neo4j.connectors.kafka.testing.format.KeyValueConverter
import org.neo4j.connectors.kafka.testing.kafka.GenericKafkaConsumer
import org.neo4j.connectors.kafka.testing.source.CdcMetadata
import org.neo4j.connectors.kafka.testing.source.CdcSource
import org.neo4j.connectors.kafka.testing.source.CdcSourceParam
import org.neo4j.connectors.kafka.testing.source.CdcSourceTopic
import org.neo4j.connectors.kafka.testing.source.Neo4jSource
import org.neo4j.connectors.kafka.testing.source.SourceStrategy.CDC
import org.neo4j.connectors.kafka.testing.source.TopicConsumer
import org.neo4j.driver.Session
import org.neo4j.driver.TransactionConfig

abstract class Neo4jCdcSourceRelationshipsIT {

  @Neo4jSource(
      startFrom = "EARLIEST",
      strategy = CDC,
      cdc =
          CdcSource(
              topics =
                  arrayOf(
                      CdcSourceTopic(
                          topic = "neo4j-cdc-rels",
                          patterns =
                              arrayOf(
                                  CdcSourceParam(
                                      "(:TestSource)-[:RELIES_TO {execId,weight,-rate}]->(:TestSource)"))))))
  @Test
  fun `should read changes caught by patterns`(
      testInfo: TestInfo,
      @TopicConsumer(topic = "neo4j-cdc-rels", offset = "earliest") consumer: GenericKafkaConsumer,
      session: Session
  ) {
    val executionId = testInfo.displayName + System.currentTimeMillis()
    val params = mapOf("execId" to executionId)
    session
        .run(
            """CREATE (s:TestSource {name: 'Bob', execId: ${'$'}execId})
            |CREATE (t:TestSource {name: 'Alice', execId: ${'$'}execId})
            |CREATE (s)-[:RELIES_TO {weight: 1, rate: 42, execId: ${'$'}execId}]->(t)
    """
                .trimMargin(),
            params)
        .consume()
    session
        .run(
            "MATCH (:TestSource)-[r:RELIES_TO {execId: \$execId}]-(:TestSource) SET r.weight = 2",
            params)
        .consume()
    session
        .run("MATCH (:TestSource)-[r:RELIES_TO {execId: \$execId}]-(:TestSource) DELETE r", params)
        .consume()

    TopicVerifier2.create(consumer, ChangeEvent::class.java)
        .assertMessageValue { value ->
          ChangeEventAssert.assertThat(value)
              .hasEventType(EventType.RELATIONSHIP)
              .hasOperation(EntityOperation.CREATE)
              .hasType("RELIES_TO")
              .startLabelledAs("TestSource")
              .endLabelledAs("TestSource")
              .hasNoBeforeState()
              .hasAfterStateProperties(mapOf("weight" to 1L, "execId" to executionId))
        }
        .assertMessageValue { value ->
          ChangeEventAssert.assertThat(value)
              .hasEventType(EventType.RELATIONSHIP)
              .hasOperation(EntityOperation.UPDATE)
              .hasType("RELIES_TO")
              .startLabelledAs("TestSource")
              .endLabelledAs("TestSource")
              .hasBeforeStateProperties(mapOf("weight" to 1L, "execId" to executionId))
              .hasAfterStateProperties(mapOf("weight" to 2L, "execId" to executionId))
        }
        .assertMessageValue { value ->
          ChangeEventAssert.assertThat(value)
              .hasEventType(EventType.RELATIONSHIP)
              .hasOperation(EntityOperation.DELETE)
              .hasType("RELIES_TO")
              .startLabelledAs("TestSource")
              .endLabelledAs("TestSource")
              .hasBeforeStateProperties(mapOf("weight" to 2L, "execId" to executionId))
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
                          topic = "neo4j-cdc-rels-prop-remove-add",
                          patterns = arrayOf(CdcSourceParam("()-[:RELIES_TO {}]->()"))))))
  @Test
  open fun `should read property removal and additions`(
      testInfo: TestInfo,
      @TopicConsumer(topic = "neo4j-cdc-rels-prop-remove-add", offset = "earliest")
      consumer: GenericKafkaConsumer,
      session: Session
  ) {
    val executionId = testInfo.displayName + System.currentTimeMillis()
    val params = mapOf("execId" to executionId)
    session
        .run(
            """CREATE (s:TestSource {name: 'Bob', execId: ${'$'}execId})
            |CREATE (t:TestSource {name: 'Alice', execId: ${'$'}execId})
            |CREATE (s)-[:RELIES_TO {weight: 1, rate: 42, execId: ${'$'}execId}]->(t)
    """
                .trimMargin(),
            params)
        .consume()
    session
        .run(
            "MATCH (:TestSource)-[r:RELIES_TO {execId: \$execId}]-(:TestSource) SET r.weight = 2, r.rate = NULL",
            params)
        .consume()
    session
        .run(
            "MATCH (:TestSource)-[r:RELIES_TO {execId: \$execId}]-(:TestSource) SET r.rate = 50",
            params)
        .consume()
    session
        .run("MATCH (:TestSource)-[r:RELIES_TO {execId: \$execId}]-(:TestSource) DELETE r", params)
        .consume()

    TopicVerifier2.create(consumer, ChangeEvent::class.java)
        .assertMessageValue { value ->
          ChangeEventAssert.assertThat(value)
              .hasEventType(EventType.RELATIONSHIP)
              .hasOperation(EntityOperation.CREATE)
              .hasType("RELIES_TO")
              .startLabelledAs("TestSource")
              .endLabelledAs("TestSource")
              .hasNoBeforeState()
              .hasAfterStateProperties(
                  mapOf("weight" to 1L, "rate" to 42L, "execId" to executionId))
        }
        .assertMessageValue { value ->
          ChangeEventAssert.assertThat(value)
              .hasEventType(EventType.RELATIONSHIP)
              .hasOperation(EntityOperation.UPDATE)
              .hasType("RELIES_TO")
              .startLabelledAs("TestSource")
              .endLabelledAs("TestSource")
              .hasBeforeStateProperties(
                  mapOf("weight" to 1L, "rate" to 42L, "execId" to executionId))
              .hasAfterStateProperties(mapOf("weight" to 2L, "execId" to executionId))
        }
        .assertMessageValue { value ->
          ChangeEventAssert.assertThat(value)
              .hasEventType(EventType.RELATIONSHIP)
              .hasOperation(EntityOperation.UPDATE)
              .hasType("RELIES_TO")
              .startLabelledAs("TestSource")
              .endLabelledAs("TestSource")
              .hasBeforeStateProperties(mapOf("weight" to 2L, "execId" to executionId))
              .hasAfterStateProperties(
                  mapOf("weight" to 2L, "rate" to 50L, "execId" to executionId))
        }
        .assertMessageValue { value ->
          ChangeEventAssert.assertThat(value)
              .hasEventType(EventType.RELATIONSHIP)
              .hasOperation(EntityOperation.DELETE)
              .hasType("RELIES_TO")
              .startLabelledAs("TestSource")
              .endLabelledAs("TestSource")
              .hasBeforeStateProperties(
                  mapOf("weight" to 2L, "rate" to 50L, "execId" to executionId))
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
                          topic = "neo4j-cdc-update-rel",
                          patterns =
                              arrayOf(CdcSourceParam(value = "(:A)-[:R {a,b,c,execId,-d}]->(:B)")),
                          operations = arrayOf(CdcSourceParam(value = "UPDATE")),
                          changesTo = arrayOf(CdcSourceParam(value = "a,c"))))))
  @Test
  fun `should read only specified field changes on update`(
      testInfo: TestInfo,
      @TopicConsumer(topic = "neo4j-cdc-update-rel", offset = "earliest")
      consumer: GenericKafkaConsumer,
      session: Session
  ) {
    val executionId = testInfo.displayName + System.currentTimeMillis()
    val params = mapOf("execId" to executionId)
    session
        .run(
            "CREATE (:A)-[:R {a: 'foo', b: 'bar', c: 'abra', d: 'cadabra', execId: \$execId}]->(:B)",
            params)
        .consume()
    session.run("MATCH (:A)-[r:R {a: 'foo'}]->(:B) SET r.a = 'mini', r.b = 'midi'").consume()
    session.run("MATCH (:A)-[r:R {a: 'mini'}]->(:B) SET r.a = 'eni', r.c = 'beni'").consume()
    session.run("MATCH (:A)-[r:R {a: 'eni'}]->(:B) SET r.a = 'obi', r.c = 'bobi'").consume()

    TopicVerifier2.create(consumer, ChangeEvent::class.java)
        .assertMessageValue { value ->
          ChangeEventAssert.assertThat(value)
              .hasEventType(EventType.RELATIONSHIP)
              .hasOperation(EntityOperation.UPDATE)
              .hasType("R")
              .startLabelledAs("A")
              .endLabelledAs("B")
              .hasBeforeStateProperties(
                  mapOf("a" to "mini", "b" to "midi", "c" to "abra", "execId" to executionId))
              .hasAfterStateProperties(
                  mapOf("a" to "eni", "b" to "midi", "c" to "beni", "execId" to executionId))
        }
        .assertMessageValue { value ->
          ChangeEventAssert.assertThat(value)
              .hasEventType(EventType.RELATIONSHIP)
              .hasOperation(EntityOperation.UPDATE)
              .hasType("R")
              .startLabelledAs("A")
              .endLabelledAs("B")
              .hasBeforeStateProperties(
                  mapOf("a" to "eni", "b" to "midi", "c" to "beni", "execId" to executionId))
              .hasAfterStateProperties(
                  mapOf("a" to "obi", "b" to "midi", "c" to "bobi", "execId" to executionId))
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
                          topic = "neo4j-cdc-create-inc-rel",
                          patterns =
                              arrayOf(CdcSourceParam("(:Person)-[:IS_EMPLOYEE]->(:Company)"))))))
  @Test
  open fun `should read changes with different properties using the default topic compatibility mode`(
      testInfo: TestInfo,
      @TopicConsumer(topic = "neo4j-cdc-create-inc-rel", offset = "earliest")
      consumer: GenericKafkaConsumer,
      session: Session
  ) {
    val executionId = testInfo.displayName + System.currentTimeMillis()
    val params = mapOf("execId" to executionId)
    session
        .run("CREATE (:Person)-[:IS_EMPLOYEE {role: 'SWE', execId: \$execId}]->(:Company)", params)
        .consume()
    session
        .run(
            "CREATE (:Person)-[:IS_EMPLOYEE {tribe: 'engineering', execId: \$execId}]->(:Company)",
            params)
        .consume()

    TopicVerifier2.create(consumer, ChangeEvent::class.java)
        .assertMessageValue { value ->
          ChangeEventAssert.assertThat(value)
              .hasEventType(EventType.RELATIONSHIP)
              .hasOperation(EntityOperation.CREATE)
              .hasType("IS_EMPLOYEE")
              .startLabelledAs("Person")
              .endLabelledAs("Company")
              .hasNoBeforeState()
              .hasAfterStateProperties(mapOf("role" to "SWE", "execId" to executionId))
        }
        .assertMessageValue { value ->
          ChangeEventAssert.assertThat(value)
              .hasEventType(EventType.RELATIONSHIP)
              .hasOperation(EntityOperation.CREATE)
              .hasType("IS_EMPLOYEE")
              .startLabelledAs("Person")
              .endLabelledAs("Company")
              .hasNoBeforeState()
              .hasAfterStateProperties(mapOf("tribe" to "engineering", "execId" to executionId))
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
                          topic = "cdc-creates-rel",
                          patterns = arrayOf(CdcSourceParam("(:Person)-[:EMPLOYED]->(:Company)")),
                          operations = arrayOf(CdcSourceParam("CREATE"))),
                      CdcSourceTopic(
                          topic = "cdc-updates-rel",
                          patterns = arrayOf(CdcSourceParam("(:Person)-[:EMPLOYED]->(:Company)")),
                          operations = arrayOf(CdcSourceParam("UPDATE"))),
                      CdcSourceTopic(
                          topic = "cdc-deletes-rel",
                          patterns = arrayOf(CdcSourceParam("(:Person)-[:EMPLOYED]->(:Company)")),
                          operations = arrayOf(CdcSourceParam("DELETE"))))))
  @Test
  fun `should read each operation to a separate topic`(
      testInfo: TestInfo,
      @TopicConsumer(topic = "cdc-creates-rel", offset = "earliest")
      createsConsumer: GenericKafkaConsumer,
      @TopicConsumer(topic = "cdc-updates-rel", offset = "earliest")
      updatesConsumer: GenericKafkaConsumer,
      @TopicConsumer(topic = "cdc-deletes-rel", offset = "earliest")
      deletesConsumer: GenericKafkaConsumer,
      session: Session
  ) {
    val executionId = testInfo.displayName + System.currentTimeMillis()
    val params = mapOf("execId" to executionId)
    session
        .run(
            "CREATE (:Person)-[:EMPLOYED {execId: \$execId, role: 'SWE'}]->(:Company)",
            mapOf("execId" to executionId))
        .consume()
    session
        .run(
            "MATCH (:Person)-[r:EMPLOYED {execId: \$execId}]->(:Company) SET r.role = 'EM'", params)
        .consume()
    session
        .run("MATCH (:Person)-[r:EMPLOYED {execId: \$execId}]->(:Company) DELETE r", params)
        .consume()

    TopicVerifier2.create(createsConsumer, ChangeEvent::class.java)
        .assertMessageValue { value ->
          ChangeEventAssert.assertThat(value)
              .hasEventType(EventType.RELATIONSHIP)
              .hasOperation(EntityOperation.CREATE)
              .hasType("EMPLOYED")
              .startLabelledAs("Person")
              .endLabelledAs("Company")
              .hasNoBeforeState()
              .hasAfterStateProperties(mapOf("role" to "SWE", "execId" to executionId))
        }
        .verifyWithin(Duration.ofSeconds(30))
    TopicVerifier2.create(updatesConsumer, ChangeEvent::class.java)
        .assertMessageValue { value ->
          ChangeEventAssert.assertThat(value)
              .hasEventType(EventType.RELATIONSHIP)
              .hasOperation(EntityOperation.UPDATE)
              .hasType("EMPLOYED")
              .startLabelledAs("Person")
              .endLabelledAs("Company")
              .hasBeforeStateProperties(mapOf("role" to "SWE", "execId" to executionId))
              .hasAfterStateProperties(mapOf("role" to "EM", "execId" to executionId))
        }
        .verifyWithin(Duration.ofSeconds(30))
    TopicVerifier2.create(deletesConsumer, ChangeEvent::class.java)
        .assertMessageValue { value ->
          ChangeEventAssert.assertThat(value)
              .hasEventType(EventType.RELATIONSHIP)
              .hasOperation(EntityOperation.DELETE)
              .hasType("EMPLOYED")
              .startLabelledAs("Person")
              .endLabelledAs("Company")
              .hasBeforeStateProperties(mapOf("role" to "EM", "execId" to executionId))
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
                          topic = "neo4j-cdc-metadata-rel",
                          patterns = arrayOf(CdcSourceParam("(:Person)-[:EMPLOYED]->(:Company)")),
                          metadata =
                              arrayOf(CdcMetadata(key = "txMetadata.testLabel", value = "B"))))))
  @Test
  fun `should read changes marked with specific transaction metadata attribute`(
      testInfo: TestInfo,
      @TopicConsumer(topic = "neo4j-cdc-metadata-rel", offset = "earliest")
      consumer: GenericKafkaConsumer,
      session: Session
  ) {
    val executionId = testInfo.displayName + System.currentTimeMillis()
    val params = mapOf("execId" to executionId)
    val transaction1 =
        session.beginTransaction(
            TransactionConfig.builder().withMetadata(mapOf("testLabel" to "A")).build())
    transaction1
        .run("CREATE (:Person)-[:EMPLOYED {execId: \$execId, role: 'SWE'}]->(:Company)", params)
        .consume()
    transaction1.commit()

    val transaction2 =
        session.beginTransaction(
            TransactionConfig.builder().withMetadata(mapOf("testLabel" to "B")).build())
    transaction2
        .run("CREATE (:Person)-[:EMPLOYED {execId: \$execId, role: 'EM'}]->(:Company)", params)
        .consume()
    transaction2.commit()

    TopicVerifier2.create(consumer, ChangeEvent::class.java)
        .assertMessageValue { value ->
          ChangeEventAssert.assertThat(value)
              .hasEventType(EventType.RELATIONSHIP)
              .hasOperation(EntityOperation.CREATE)
              .hasType("EMPLOYED")
              .startLabelledAs("Person")
              .endLabelledAs("Company")
              .hasNoBeforeState()
              .hasAfterStateProperties(mapOf("role" to "EM", "execId" to executionId))
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
                          topic = "neo4j-cdc-keys-rel",
                          patterns =
                              arrayOf(CdcSourceParam("(:Person)-[:EMPLOYED]->(:Company)"))))))
  @Test
  fun `should read changes containing relationship keys`(
      testInfo: TestInfo,
      @TopicConsumer(topic = "neo4j-cdc-keys-rel", offset = "earliest")
      consumer: GenericKafkaConsumer,
      session: Session
  ) {
    val executionId = testInfo.displayName + System.currentTimeMillis()
    session
        .run(
            "CREATE CONSTRAINT employedId FOR ()-[r:EMPLOYED]->() REQUIRE r.id IS RELATIONSHIP KEY")
        .consume()
    session
        .run(
            "CREATE CONSTRAINT employedRole FOR ()-[r:EMPLOYED]->() REQUIRE r.role IS RELATIONSHIP KEY")
        .consume()

    session
        .run(
            "CREATE (:Person)-[:EMPLOYED {execId: \$execId, id: 1, role: 'SWE'}]->(:Company)",
            mapOf("execId" to executionId))
        .consume()

    TopicVerifier2.create(consumer, ChangeEvent::class.java)
        .assertMessageValue { value ->
          ChangeEventAssert.assertThat(value)
              .hasEventType(EventType.RELATIONSHIP)
              .hasOperation(EntityOperation.CREATE)
              .hasType("EMPLOYED")
              .startLabelledAs("Person")
              .endLabelledAs("Company")
              .hasNoBeforeState()
              .hasAfterStateProperties(mapOf("id" to 1L, "role" to "SWE", "execId" to executionId))
              .hasRelationshipKeys(listOf(mapOf("id" to 1L), mapOf("role" to "SWE")))
        }
        .verifyWithin(Duration.ofSeconds(30))
  }
}

@KeyValueConverter(key = AVRO, value = AVRO)
class Neo4jCdcSourceRelationshipsAvroIT : Neo4jCdcSourceRelationshipsIT()

@KeyValueConverter(key = JSON_SCHEMA, value = JSON_SCHEMA)
class Neo4jCdcSourceRelationshipsJsonIT : Neo4jCdcSourceRelationshipsIT() {

  @Disabled // TODO
  override fun `should read changes with different properties using the default topic compatibility mode`(
      testInfo: TestInfo,
      @TopicConsumer(topic = "neo4j-cdc-create-inc-rel", offset = "earliest")
      consumer: GenericKafkaConsumer,
      session: Session
  ) {
    super
        .`should read changes with different properties using the default topic compatibility mode`(
            testInfo, consumer, session)
  }
}

@KeyValueConverter(key = PROTOBUF, value = PROTOBUF)
class Neo4jCdcSourceRelationshipsProtobufIT : Neo4jCdcSourceRelationshipsIT() {
  @Disabled // TODO
  override fun `should read property removal and additions`(
      testInfo: TestInfo,
      @TopicConsumer(topic = "neo4j-cdc-rels-prop-remove-add", offset = "earliest")
      consumer: GenericKafkaConsumer,
      session: Session
  ) {
    super.`should read property removal and additions`(testInfo, consumer, session)
  }
}
