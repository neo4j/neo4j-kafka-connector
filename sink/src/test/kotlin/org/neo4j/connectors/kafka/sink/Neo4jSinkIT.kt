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
package org.neo4j.connectors.kafka.sink

import io.confluent.connect.avro.AvroData
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import org.neo4j.connectors.kafka.testing.sink.Neo4jSink
import org.neo4j.connectors.kafka.testing.sink.TopicProducer
import org.neo4j.driver.Session

class Neo4jSinkIT {

  companion object {
    const val TOPIC = "persons"
  }

  @Neo4jSink(
      topics = [TOPIC],
      queries =
          [
              "MERGE (p:Person {name: event.name, surname: event.surname, executionId: event.executionId})"])
  @Test
  fun `writes messages to Neo4j`(
      @TopicProducer producer: KafkaProducer<String, GenericRecord>,
      session: Session,
      testInfo: TestInfo
  ) {
    val executionId = testInfo.displayName + System.currentTimeMillis()
    val avroRecord =
        GenericData.Record(
            AvroData(20)
                .fromConnectSchema(
                    SchemaBuilder.struct()
                        .field("name", Schema.STRING_SCHEMA)
                        .field("surname", Schema.STRING_SCHEMA)
                        .field("executionId", Schema.STRING_SCHEMA)
                        .build()))
    avroRecord.put("name", "Jane")
    avroRecord.put("surname", "Doe")
    avroRecord.put("executionId", executionId)
    val record = ProducerRecord<String, GenericRecord>(TOPIC, avroRecord)

    producer.send(record)

    await().atMost(30.seconds.toJavaDuration()).until {
      session
          .run(
              "MATCH (p:Person {name: \$name, surname: \$surname, executionId: \$executionId}) RETURN count(p) = 1 AS result",
              mapOf("name" to "Jane", "surname" to "Doe", "executionId" to executionId))
          .single()["result"]
          .asBoolean()
    }
  }
}
