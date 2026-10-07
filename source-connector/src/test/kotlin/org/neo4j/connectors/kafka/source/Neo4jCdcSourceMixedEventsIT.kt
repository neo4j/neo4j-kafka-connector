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

// node and relationship events share one schema, so they can be published to the same topic (and
// the same schema registry subject) and read back
abstract class Neo4jCdcSourceMixedEventsIT {

  @Neo4jSource(
      startFrom = "EARLIEST",
      strategy = CDC,
      cdc =
          CdcSource(
              topics =
                  arrayOf(
                      CdcSourceTopic(
                          topic = "neo4j-cdc-mixed",
                          patterns =
                              arrayOf(
                                  CdcSourceParam("(:Person)"),
                                  CdcSourceParam("(:Person)-[:KNOWS]->(:Person)"),
                              ),
                      )
                  )
          ),
  )
  @Test
  fun `should publish node and relationship events to the same topic`(
      @TopicConsumer(topic = "neo4j-cdc-mixed", offset = "earliest")
      consumer: ConvertingKafkaConsumer,
      session: Session,
  ) {
    session.run("CREATE (:Person {name: 'Alice'})").consume()
    session.run("CREATE (:Person {name: 'Bob'})").consume()
    session
        .run(
            """
            |MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
            |CREATE (a)-[:KNOWS {since: 2020}]->(b)"""
                .trimMargin()
        )
        .consume()
    session.run("MATCH (:Person)-[r:KNOWS]->(:Person) SET r.since = 2021").consume()
    session.run("MATCH (p:Person {name: 'Alice'}) SET p.name = 'Alicia'").consume()

    TopicVerifier.create<ChangeEvent, ChangeEvent>(consumer)
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(NODE)
              .hasOperation(CREATE)
              .labelledAs("Person")
              .hasNoBeforeState()
              .hasAfterStateProperties(mapOf("name" to "Alice"))
        }
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(NODE)
              .hasOperation(CREATE)
              .labelledAs("Person")
              .hasNoBeforeState()
              .hasAfterStateProperties(mapOf("name" to "Bob"))
        }
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(RELATIONSHIP)
              .hasOperation(CREATE)
              .hasType("KNOWS")
              .startLabelledAs("Person")
              .endLabelledAs("Person")
              .hasNoBeforeState()
              .hasAfterStateProperties(mapOf("since" to 2020L))
        }
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(RELATIONSHIP)
              .hasOperation(UPDATE)
              .hasType("KNOWS")
              .startLabelledAs("Person")
              .endLabelledAs("Person")
              .hasBeforeStateProperties(mapOf("since" to 2020L))
              .hasAfterStateProperties(mapOf("since" to 2021L))
        }
        .assertMessageValue { value ->
          assertThat(value)
              .hasEventType(NODE)
              .hasOperation(UPDATE)
              .labelledAs("Person")
              .hasBeforeStateProperties(mapOf("name" to "Alice"))
              .hasAfterStateProperties(mapOf("name" to "Alicia"))
        }
        .verifyWithin(Duration.ofSeconds(30))
  }
}

@KeyValueConverter(key = AVRO, value = AVRO, payloadMode = PayloadMode.EXTENDED)
class Neo4jCdcSourceMixedEventsAvroExtendedIT : Neo4jCdcSourceMixedEventsIT()

@KeyValueConverter(key = AVRO, value = AVRO, payloadMode = PayloadMode.COMPACT)
class Neo4jCdcSourceMixedEventsAvroCompactIT : Neo4jCdcSourceMixedEventsIT()

@KeyValueConverter(key = JSON_SCHEMA, value = JSON_SCHEMA, payloadMode = PayloadMode.EXTENDED)
class Neo4jCdcSourceMixedEventsJsonSchemaExtendedIT : Neo4jCdcSourceMixedEventsIT()

@KeyValueConverter(key = JSON_EMBEDDED, value = JSON_EMBEDDED, payloadMode = PayloadMode.EXTENDED)
class Neo4jCdcSourceMixedEventsJsonEmbeddedExtendedIT : Neo4jCdcSourceMixedEventsIT()

@KeyValueConverter(key = JSON_EMBEDDED, value = JSON_EMBEDDED, payloadMode = PayloadMode.COMPACT)
class Neo4jCdcSourceMixedEventsJsonEmbeddedCompactIT : Neo4jCdcSourceMixedEventsIT()

@KeyValueConverter(key = PROTOBUF, value = PROTOBUF, payloadMode = PayloadMode.EXTENDED)
class Neo4jCdcSourceMixedEventsProtobufExtendedIT : Neo4jCdcSourceMixedEventsIT()
