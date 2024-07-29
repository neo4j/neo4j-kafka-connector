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
import org.junit.jupiter.api.Test
import org.neo4j.cdc.client.model.ChangeEvent
import org.neo4j.cdc.client.model.EntityOperation
import org.neo4j.cdc.client.model.EntityOperation.DELETE
import org.neo4j.cdc.client.model.EventType.RELATIONSHIP
import org.neo4j.connectors.kafka.testing.assertions.ChangeEventAssert.Companion.assertThat
import org.neo4j.connectors.kafka.testing.assertions.TopicVerifier
import org.neo4j.connectors.kafka.testing.format.KafkaConverter.AVRO
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
                                      "(:TestSource)-[:RELIES_TO {weight,-rate}]->(:TestSource)"))))))
  @Test
  fun `should read changes caught by patterns`(
      @TopicConsumer(topic = "neo4j-cdc-rels", offset = "earliest")
      consumer: ConvertingKafkaConsumer,
      session: Session
  ) {
    session
        .run(
            """CREATE (s:TestSource {name: 'Bob'})
            |CREATE (t:TestSource {name: 'Alice'})
            |CREATE (s)-[:RELIES_TO {weight: 1, rate: 42}]->(t)
    """
                .trimMargin())
        .consume()
    session.run("MATCH (:TestSource)-[r:RELIES_TO]-(:TestSource) SET r.weight = 2").consume()
    session.run("MATCH (:TestSource)-[r:RELIES_TO]-(:TestSource) DELETE r").consume()

    TopicVerifier.create<ChangeEvent, ChangeEvent>(consumer)
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(RELATIONSHIP)
              .hasOperation(EntityOperation.CREATE)
              .hasType("RELIES_TO")
              .startLabelledAs("TestSource")
              .endLabelledAs("TestSource")
              .hasNoBeforeState()
              .hasAfterStateProperties(mapOf("weight" to 1L))
        }
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(RELATIONSHIP)
              .hasOperation(EntityOperation.UPDATE)
              .hasType("RELIES_TO")
              .startLabelledAs("TestSource")
              .endLabelledAs("TestSource")
              .hasBeforeStateProperties(mapOf("weight" to 1L))
              .hasAfterStateProperties(mapOf("weight" to 2L))
        }
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(RELATIONSHIP)
              .hasOperation(DELETE)
              .hasType("RELIES_TO")
              .startLabelledAs("TestSource")
              .endLabelledAs("TestSource")
              .hasBeforeStateProperties(mapOf("weight" to 2L))
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
  fun `should read property removal and additions`(
      @TopicConsumer(topic = "neo4j-cdc-rels-prop-remove-add", offset = "earliest")
      consumer: ConvertingKafkaConsumer,
      session: Session
  ) {
    session
        .run(
            """CREATE (s:TestSource {name: 'Bob'})
            |CREATE (t:TestSource {name: 'Alice'})
            |CREATE (s)-[:RELIES_TO {weight: 1, rate: 42}]->(t)
    """
                .trimMargin())
        .consume()
    session
        .run("MATCH (:TestSource)-[r:RELIES_TO]-(:TestSource) SET r.weight = 2, r.rate = NULL")
        .consume()
    session.run("MATCH (:TestSource)-[r:RELIES_TO]-(:TestSource) SET r.rate = 50").consume()
    session.run("MATCH (:TestSource)-[r:RELIES_TO]-(:TestSource) DELETE r").consume()

    TopicVerifier.create<ChangeEvent, ChangeEvent>(consumer)
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(RELATIONSHIP)
              .hasOperation(EntityOperation.CREATE)
              .hasType("RELIES_TO")
              .startLabelledAs("TestSource")
              .endLabelledAs("TestSource")
              .hasNoBeforeState()
              .hasAfterStateProperties(mapOf("weight" to 1L, "rate" to 42L))
        }
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(RELATIONSHIP)
              .hasOperation(EntityOperation.UPDATE)
              .hasType("RELIES_TO")
              .startLabelledAs("TestSource")
              .endLabelledAs("TestSource")
              .hasBeforeStateProperties(mapOf("weight" to 1L, "rate" to 42L))
              .hasAfterStateProperties(mapOf("weight" to 2L))
        }
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(RELATIONSHIP)
              .hasOperation(EntityOperation.UPDATE)
              .hasType("RELIES_TO")
              .startLabelledAs("TestSource")
              .endLabelledAs("TestSource")
              .hasBeforeStateProperties(mapOf("weight" to 2L))
              .hasAfterStateProperties(mapOf("weight" to 2L, "rate" to 50L))
        }
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(RELATIONSHIP)
              .hasOperation(DELETE)
              .hasType("RELIES_TO")
              .startLabelledAs("TestSource")
              .endLabelledAs("TestSource")
              .hasBeforeStateProperties(mapOf("weight" to 2L, "rate" to 50L))
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
                          patterns = arrayOf(CdcSourceParam(value = "(:A)-[:R {a,b,c,-d}]->(:B)")),
                          operations = arrayOf(CdcSourceParam(value = "UPDATE")),
                          changesTo = arrayOf(CdcSourceParam(value = "a,c"))))))
  @Test
  fun `should read only specified field changes on update`(
      @TopicConsumer(topic = "neo4j-cdc-update-rel", offset = "earliest")
      consumer: ConvertingKafkaConsumer,
      session: Session
  ) {
    session.run("CREATE (:A)-[:R {a: 'foo', b: 'bar', c: 'abra', d: 'cadabra'}]->(:B)").consume()
    session.run("MATCH (:A)-[r:R {a: 'foo'}]->(:B) SET r.a = 'mini', r.b = 'midi'").consume()
    session.run("MATCH (:A)-[r:R {a: 'mini'}]->(:B) SET r.a = 'eni', r.c = 'beni'").consume()
    session.run("MATCH (:A)-[r:R {a: 'eni'}]->(:B) SET r.a = 'obi', r.c = 'bobi'").consume()

    TopicVerifier.create<ChangeEvent, ChangeEvent>(consumer)
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(RELATIONSHIP)
              .hasOperation(EntityOperation.UPDATE)
              .hasType("R")
              .startLabelledAs("A")
              .endLabelledAs("B")
              .hasBeforeStateProperties(mapOf("a" to "mini", "b" to "midi", "c" to "abra"))
              .hasAfterStateProperties(mapOf("a" to "eni", "b" to "midi", "c" to "beni"))
        }
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(RELATIONSHIP)
              .hasOperation(EntityOperation.UPDATE)
              .hasType("R")
              .startLabelledAs("A")
              .endLabelledAs("B")
              .hasBeforeStateProperties(mapOf("a" to "eni", "b" to "midi", "c" to "beni"))
              .hasAfterStateProperties(mapOf("a" to "obi", "b" to "midi", "c" to "bobi"))
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
      @TopicConsumer(topic = "neo4j-cdc-create-inc-rel", offset = "earliest")
      consumer: ConvertingKafkaConsumer,
      session: Session
  ) {
    session.run("CREATE (:Person)-[:IS_EMPLOYEE {role: 'SWE'}]->(:Company)").consume()
    session.run("CREATE (:Person)-[:IS_EMPLOYEE {tribe: 'engineering'}]->(:Company)").consume()

    TopicVerifier.create<ChangeEvent, ChangeEvent>(consumer)
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(RELATIONSHIP)
              .hasOperation(EntityOperation.CREATE)
              .hasType("IS_EMPLOYEE")
              .startLabelledAs("Person")
              .endLabelledAs("Company")
              .hasNoBeforeState()
              .hasAfterStateProperties(mapOf("role" to "SWE"))
        }
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(RELATIONSHIP)
              .hasOperation(EntityOperation.CREATE)
              .hasType("IS_EMPLOYEE")
              .startLabelledAs("Person")
              .endLabelledAs("Company")
              .hasNoBeforeState()
              .hasAfterStateProperties(mapOf("tribe" to "engineering"))
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
      @TopicConsumer(topic = "cdc-creates-rel", offset = "earliest")
      createsConsumer: ConvertingKafkaConsumer,
      @TopicConsumer(topic = "cdc-updates-rel", offset = "earliest")
      updatesConsumer: ConvertingKafkaConsumer,
      @TopicConsumer(topic = "cdc-deletes-rel", offset = "earliest")
      deletesConsumer: ConvertingKafkaConsumer,
      session: Session
  ) {
    session.run("CREATE (:Person)-[:EMPLOYED {role: 'SWE'}]->(:Company)").consume()
    session.run("MATCH (:Person)-[r:EMPLOYED]->(:Company) SET r.role = 'EM'").consume()
    session.run("MATCH (:Person)-[r:EMPLOYED]->(:Company) DELETE r").consume()

    TopicVerifier.create<ChangeEvent, ChangeEvent>(createsConsumer)
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(RELATIONSHIP)
              .hasOperation(EntityOperation.CREATE)
              .hasType("EMPLOYED")
              .startLabelledAs("Person")
              .endLabelledAs("Company")
              .hasNoBeforeState()
              .hasAfterStateProperties(mapOf("role" to "SWE"))
        }
        .verifyWithin(Duration.ofSeconds(30))
    TopicVerifier.create<ChangeEvent, ChangeEvent>(updatesConsumer)
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(RELATIONSHIP)
              .hasOperation(EntityOperation.UPDATE)
              .hasType("EMPLOYED")
              .startLabelledAs("Person")
              .endLabelledAs("Company")
              .hasBeforeStateProperties(mapOf("role" to "SWE"))
              .hasAfterStateProperties(mapOf("role" to "EM"))
        }
        .verifyWithin(Duration.ofSeconds(30))
    TopicVerifier.create<ChangeEvent, ChangeEvent>(deletesConsumer)
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(RELATIONSHIP)
              .hasOperation(DELETE)
              .hasType("EMPLOYED")
              .startLabelledAs("Person")
              .endLabelledAs("Company")
              .hasBeforeStateProperties(mapOf("role" to "EM"))
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
      @TopicConsumer(topic = "neo4j-cdc-metadata-rel", offset = "earliest")
      consumer: ConvertingKafkaConsumer,
      session: Session
  ) {
    val transaction1 =
        session.beginTransaction(
            TransactionConfig.builder().withMetadata(mapOf("testLabel" to "A")).build())
    transaction1.run("CREATE (:Person)-[:EMPLOYED {role: 'SWE'}]->(:Company)").consume()
    transaction1.commit()

    val transaction2 =
        session.beginTransaction(
            TransactionConfig.builder().withMetadata(mapOf("testLabel" to "B")).build())
    transaction2.run("CREATE (:Person)-[:EMPLOYED {role: 'EM'}]->(:Company)").consume()
    transaction2.commit()

    TopicVerifier.create<ChangeEvent, ChangeEvent>(consumer)
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(RELATIONSHIP)
              .hasOperation(EntityOperation.CREATE)
              .hasType("EMPLOYED")
              .startLabelledAs("Person")
              .endLabelledAs("Company")
              .hasNoBeforeState()
              .hasAfterStateProperties(mapOf("role" to "EM"))
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
      @TopicConsumer(topic = "neo4j-cdc-keys-rel", offset = "earliest")
      consumer: ConvertingKafkaConsumer,
      session: Session
  ) {
    session
        .run(
            "CREATE CONSTRAINT employedId FOR ()-[r:EMPLOYED]->() REQUIRE r.id IS RELATIONSHIP KEY")
        .consume()
    session
        .run(
            "CREATE CONSTRAINT employedRole FOR ()-[r:EMPLOYED]->() REQUIRE r.role IS RELATIONSHIP KEY")
        .consume()

    session.run("CREATE (:Person)-[:EMPLOYED {id: 1, role: 'SWE'}]->(:Company)").consume()

    TopicVerifier.create<ChangeEvent, ChangeEvent>(consumer)
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(RELATIONSHIP)
              .hasOperation(EntityOperation.CREATE)
              .hasType("EMPLOYED")
              .startLabelledAs("Person")
              .endLabelledAs("Company")
              .hasNoBeforeState()
              .hasAfterStateProperties(mapOf("id" to 1L, "role" to "SWE"))
              .hasRelationshipKeys(listOf(mapOf("id" to 1L), mapOf("role" to "SWE")))
        }
        .verifyWithin(Duration.ofSeconds(30))
  }
}

@KeyValueConverter(key = AVRO, value = AVRO)
class Neo4jCdcSourceRelationshipsAvroIT : Neo4jCdcSourceRelationshipsIT()

@KeyValueConverter(key = JSON_SCHEMA, value = JSON_SCHEMA)
class Neo4jCdcSourceRelationshipsJsonIT : Neo4jCdcSourceRelationshipsIT()

@KeyValueConverter(key = PROTOBUF, value = PROTOBUF)
class Neo4jCdcSourceRelationshipsProtobufIT : Neo4jCdcSourceRelationshipsIT()
