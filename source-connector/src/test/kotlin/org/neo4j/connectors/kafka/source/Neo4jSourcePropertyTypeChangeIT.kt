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
import java.time.LocalDateTime
import org.junit.jupiter.api.Test
import org.neo4j.cdc.client.model.ChangeEvent
import org.neo4j.cdc.client.model.EntityOperation.CREATE
import org.neo4j.cdc.client.model.EntityOperation.UPDATE
import org.neo4j.cdc.client.model.EventType.NODE
import org.neo4j.cdc.client.model.EventType.RELATIONSHIP
import org.neo4j.connectors.kafka.configuration.PayloadMode
import org.neo4j.connectors.kafka.testing.assertions.ChangeEventAssert.Companion.assertThat
import org.neo4j.connectors.kafka.testing.assertions.TopicVerifier
import org.neo4j.connectors.kafka.testing.format.KafkaConverter.AVRO
import org.neo4j.connectors.kafka.testing.format.KafkaConverter.JSON_EMBEDDED
import org.neo4j.connectors.kafka.testing.format.KafkaConverter.JSON_SCHEMA
import org.neo4j.connectors.kafka.testing.format.KafkaConverter.PROTOBUF
import org.neo4j.connectors.kafka.testing.format.KeyValueConverter
import org.neo4j.connectors.kafka.testing.kafka.ConvertingKafkaConsumer
import org.neo4j.connectors.kafka.testing.source.CdcSource
import org.neo4j.connectors.kafka.testing.source.CdcSourceParam
import org.neo4j.connectors.kafka.testing.source.CdcSourceTopic
import org.neo4j.connectors.kafka.testing.source.Neo4jSource
import org.neo4j.connectors.kafka.testing.source.SourceStrategy.CDC
import org.neo4j.connectors.kafka.testing.source.TopicConsumer
import org.neo4j.driver.Session

abstract class Neo4jSourceOnlyExtendedPayloadIT {

  @Neo4jSource(
      startFrom = "EARLIEST",
      strategy = CDC,
      cdc =
          CdcSource(
              patternsIndexed = true,
              topics =
                  arrayOf(
                      CdcSourceTopic(
                          topic = "cdc",
                          patterns = arrayOf(CdcSourceParam("(:TestSource)")),
                      )
                  ),
          ),
  )
  @Test
  fun `should publish changes with property type changes for nodes`(
      @TopicConsumer(topic = "cdc", offset = "earliest") consumer: ConvertingKafkaConsumer,
      session: Session,
  ) {
    session
        .run(
            "CREATE (n:TestSource) SET n = ${'$'}props",
            mapOf("props" to mapOf("name" to "Jane", "surname" to "Doe", "age" to 42)),
        )
        .consume()
    session
        .run(
            "MATCH (ts:TestSource {name: 'Jane'}) SET ts += ${'$'}props",
            mapOf(
                "props" to
                    mapOf(
                        "surname" to "Smith",
                        "age" to "42",
                        "dob" to LocalDateTime.of(1982, 1, 1, 0, 0, 0, 0),
                    )
            ),
        )
        .consume()
    session
        .run(
            "MATCH (ts:TestSource {name: 'Jane'}) SET ts += ${'$'}props",
            mapOf("props" to mapOf("age" to 42, "dob" to LocalDate.of(1982, 1, 1))),
        )
        .consume()

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
                      "age" to "42",
                      "dob" to LocalDateTime.of(1982, 1, 1, 0, 0, 0, 0),
                  )
              )
        }
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(NODE)
              .hasOperation(UPDATE)
              .labelledAs("TestSource")
              .hasBeforeStateProperties(
                  mapOf(
                      "name" to "Jane",
                      "surname" to "Smith",
                      "age" to "42",
                      "dob" to LocalDateTime.of(1982, 1, 1, 0, 0, 0, 0),
                  )
              )
              .hasAfterStateProperties(
                  mapOf(
                      "name" to "Jane",
                      "surname" to "Smith",
                      "age" to 42L,
                      "dob" to LocalDate.of(1982, 1, 1),
                  )
              )
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
                          patterns = arrayOf(CdcSourceParam("(:TestSource)")),
                      )
                  )
          ),
  )
  @Test
  fun `should publish changes with different properties using the default topic compatibility mode for nodes`(
      @TopicConsumer(topic = "neo4j-cdc-create-inc", offset = "earliest")
      consumer: ConvertingKafkaConsumer,
      session: Session,
  ) {
    session.run("CREATE (:TestSource {name: 'John'})", mapOf()).consume()
    session.run("CREATE (:TestSource {title: 'Neo4j'})", mapOf()).consume()

    TopicVerifier.create<ChangeEvent, ChangeEvent>(consumer)
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(NODE)
              .hasOperation(CREATE)
              .labelledAs("TestSource")
              .hasNoBeforeState()
              .hasAfterStateProperties(mapOf("name" to "John"))
        }
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(NODE)
              .hasOperation(CREATE)
              .labelledAs("TestSource")
              .hasNoBeforeState()
              .hasAfterStateProperties(mapOf("title" to "Neo4j"))
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
                          topic = "cdc",
                          patterns = arrayOf(CdcSourceParam("(:Person)-[:EMPLOYED]->(:Company)")),
                      )
                  ),
          ),
  )
  @Test
  fun `should publish changes with property type changes for relationships`(
      @TopicConsumer(topic = "cdc", offset = "earliest") consumer: ConvertingKafkaConsumer,
      session: Session,
  ) {
    session
        .run(
            "CREATE (:Person)-[r:EMPLOYED]->(:Company) SET r = ${'$'}props",
            mapOf("props" to mapOf("role" to "SWE")),
        )
        .consume()
    session
        .run(
            "MATCH (:Person)-[r:EMPLOYED]->(:Company) SET r += ${'$'}props",
            mapOf(
                "props" to
                    mapOf("role" to "EM", "since" to LocalDateTime.of(1999, 1, 1, 0, 0, 0, 0))
            ),
        )
        .consume()
    session
        .run(
            "MATCH (:Person)-[r:EMPLOYED]->(:Company) SET r += ${'$'}props",
            mapOf(
                "props" to mapOf("role" to listOf("EM", "SWE"), "since" to LocalDate.of(1999, 1, 1))
            ),
        )
        .consume()

    TopicVerifier.create<ChangeEvent, ChangeEvent>(consumer)
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(RELATIONSHIP)
              .hasOperation(CREATE)
              .hasType("EMPLOYED")
              .startLabelledAs("Person")
              .endLabelledAs("Company")
              .hasNoBeforeState()
              .hasAfterStateProperties(mapOf("role" to "SWE"))
        }
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(RELATIONSHIP)
              .hasOperation(UPDATE)
              .hasType("EMPLOYED")
              .startLabelledAs("Person")
              .endLabelledAs("Company")
              .hasBeforeStateProperties(mapOf("role" to "SWE"))
              .hasAfterStateProperties(
                  mapOf("role" to "EM", "since" to LocalDateTime.of(1999, 1, 1, 0, 0, 0, 0))
              )
        }
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(RELATIONSHIP)
              .hasOperation(UPDATE)
              .hasType("EMPLOYED")
              .startLabelledAs("Person")
              .endLabelledAs("Company")
              .hasBeforeStateProperties(
                  mapOf("role" to "EM", "since" to LocalDateTime.of(1999, 1, 1, 0, 0, 0, 0))
              )
              .hasAfterStateProperties(
                  mapOf("role" to listOf("EM", "SWE"), "since" to LocalDate.of(1999, 1, 1))
              )
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
                          patterns = arrayOf(CdcSourceParam("(:Person)-[:IS_EMPLOYEE]->(:Company)")),
                      )
                  )
          ),
  )
  @Test
  open fun `should publish changes with different properties using the default topic compatibility mode for relationships`(
      @TopicConsumer(topic = "neo4j-cdc-create-inc-rel", offset = "earliest")
      consumer: ConvertingKafkaConsumer,
      session: Session,
  ) {
    session.run("CREATE (:Person)-[:IS_EMPLOYEE {role: 'SWE'}]->(:Company)").consume()
    session.run("CREATE (:Person)-[:IS_EMPLOYEE {tribe: 'engineering'}]->(:Company)").consume()

    TopicVerifier.create<ChangeEvent, ChangeEvent>(consumer)
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(RELATIONSHIP)
              .hasOperation(CREATE)
              .hasType("IS_EMPLOYEE")
              .startLabelledAs("Person")
              .endLabelledAs("Company")
              .hasNoBeforeState()
              .hasAfterStateProperties(mapOf("role" to "SWE"))
        }
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(RELATIONSHIP)
              .hasOperation(CREATE)
              .hasType("IS_EMPLOYEE")
              .startLabelledAs("Person")
              .endLabelledAs("Company")
              .hasNoBeforeState()
              .hasAfterStateProperties(mapOf("tribe" to "engineering"))
        }
        .verifyWithin(Duration.ofSeconds(30))
  }
}

@KeyValueConverter(key = AVRO, value = AVRO, payloadMode = PayloadMode.EXTENDED)
class Neo4jSourceOnlyExtendedPayloadAvroIT : Neo4jSourceOnlyExtendedPayloadIT()

@KeyValueConverter(key = JSON_SCHEMA, value = JSON_SCHEMA, payloadMode = PayloadMode.EXTENDED)
class Neo4jSourceOnlyExtendedPayloadJsonSchemaIT : Neo4jSourceOnlyExtendedPayloadIT()

@KeyValueConverter(key = JSON_EMBEDDED, value = JSON_EMBEDDED, payloadMode = PayloadMode.EXTENDED)
class Neo4jSourceOnlyExtendedPayloadJsonEmbeddedIT : Neo4jSourceOnlyExtendedPayloadIT()

@KeyValueConverter(key = PROTOBUF, value = PROTOBUF, payloadMode = PayloadMode.EXTENDED)
class Neo4jSourceOnlyExtendedPayloadProtobufIT : Neo4jSourceOnlyExtendedPayloadIT()
