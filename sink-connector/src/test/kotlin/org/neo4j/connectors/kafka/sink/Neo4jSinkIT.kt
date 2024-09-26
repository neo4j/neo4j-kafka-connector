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

import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import org.neo4j.connectors.kafka.testing.format.KafkaConverter.AVRO
import org.neo4j.connectors.kafka.testing.format.KafkaConverter.JSON_EMBEDDED
import org.neo4j.connectors.kafka.testing.format.KafkaConverter.JSON_SCHEMA
import org.neo4j.connectors.kafka.testing.format.KafkaConverter.PROTOBUF
import org.neo4j.connectors.kafka.testing.format.KeyValueConverter
import org.neo4j.connectors.kafka.testing.kafka.ConvertingKafkaProducer
import org.neo4j.connectors.kafka.testing.sink.CypherStrategy
import org.neo4j.connectors.kafka.testing.sink.Neo4jSink
import org.neo4j.connectors.kafka.testing.sink.TopicProducer
import org.neo4j.driver.Session

abstract class Neo4jSinkIT {
  companion object {
    private const val TOPIC = "persons"
  }

  @Neo4jSink(
      cypher =
          [
              CypherStrategy(
                  TOPIC,
                  "MERGE (p:Person {name: event.name, surname: event.surname})",
              ),
          ],
  )
  @Test
  fun `writes messages to Neo4j via sink connector`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
  ) {
    val value = mapOf("name" to "Jane", "surname" to "Doe")
    val schema =
        SchemaBuilder.struct()
            .field("name", Schema.STRING_SCHEMA)
            .field("surname", Schema.STRING_SCHEMA)
            .build()
    val struct = Struct(schema)
    schema.fields().forEach { struct.put(it, value[it.name()]) }

    producer.publish(value = struct, valueSchema = schema)

    await().atMost(30.seconds.toJavaDuration()).until {
      session
          .run(
              "MATCH (p:Person {name: \$name, surname: \$surname}) RETURN count(p) = 1 AS result",
              mapOf("name" to "Jane", "surname" to "Doe"),
          )
          .single()["result"]
          .asBoolean()
    }
  }
}

@KeyValueConverter(key = AVRO, value = AVRO) class Neo4jSinkAvroIT : Neo4jSinkIT()

@KeyValueConverter(key = JSON_SCHEMA, value = JSON_SCHEMA)
class Neo4jSinkJsonSchemaIT : Neo4jSinkIT()

@KeyValueConverter(key = JSON_EMBEDDED, value = JSON_EMBEDDED)
class Neo4jSinkJsonEmbeddedIT : Neo4jSinkIT()

@KeyValueConverter(key = PROTOBUF, value = PROTOBUF) class Neo4jSinkProtobufIT : Neo4jSinkIT()
