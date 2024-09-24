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

import io.kotest.matchers.shouldBe
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import org.neo4j.connectors.kafka.testing.format.KafkaConverter.JSON_RAW
import org.neo4j.connectors.kafka.testing.format.KeyValueConverter
import org.neo4j.connectors.kafka.testing.kafka.ConvertingKafkaProducer
import org.neo4j.connectors.kafka.testing.sink.CypherStrategy
import org.neo4j.connectors.kafka.testing.sink.Neo4jSink
import org.neo4j.connectors.kafka.testing.sink.TopicProducer
import org.neo4j.driver.Session

@KeyValueConverter(key = JSON_RAW, value = JSON_RAW)
class Neo4jSinkRawJsonIT {
  companion object {
    private const val TOPIC = "persons"
  }

  @Neo4jSink(
      cypher =
          [
              CypherStrategy(
                  TOPIC,
                  "WITH __value AS person MERGE (p:Person {name: person.name, surname: person.surname})",
              ),
          ],
  )
  @Test
  fun `should support json map`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
  ) {
    producer.publish(
        value = mapOf("name" to "Jane", "surname" to "Doe"),
        valueSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build(),
    )

    await().atMost(30.seconds.toJavaDuration()).untilAsserted {
      session
          .run(
              "MATCH (p:Person {name: ${'$'}name, surname: ${'$'}surname}) RETURN count(p) as result",
              mapOf("name" to "Jane", "surname" to "Doe"),
          )
          .single()["result"]
          .asLong() shouldBe 1L
    }
  }

  @Neo4jSink(
      cypher =
          [
              CypherStrategy(
                  TOPIC,
                  "WITH __value AS persons UNWIND persons AS person MERGE (p:Person {name: person.name, surname: person.surname})",
              ),
          ],
  )
  @Test
  fun `should support json list`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
  ) {
    producer.publish(
        value =
            listOf(
                mapOf("name" to "Jane", "surname" to "Doe"),
                mapOf("name" to "John", "surname" to "Doe"),
            ),
        valueSchema =
            SchemaBuilder.array(
                    SchemaBuilder.map(
                            Schema.STRING_SCHEMA,
                            Schema.STRING_SCHEMA,
                        )
                        .build(),
                )
                .build(),
    )

    await().atMost(30.seconds.toJavaDuration()).untilAsserted {
      session
          .run(
              "MATCH (p:Person) WHERE [p.name, p.surname] IN ${'$'}names RETURN count(p) as result",
              mapOf("names" to listOf(listOf("Jane", "Doe"), listOf("John", "Doe"))),
          )
          .single()["result"]
          .asLong() shouldBe 2L
    }
  }

  @Neo4jSink(
      cypher =
          [
              CypherStrategy(
                  TOPIC,
                  "WITH __value AS name MERGE (p:Person {name: name})",
              ),
          ],
  )
  @Test
  fun `should support raw string value`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
  ) {
    producer.publish(
        value = "John",
        valueSchema = Schema.STRING_SCHEMA,
    )

    await().atMost(30.seconds.toJavaDuration()).untilAsserted {
      session
          .run(
              "MATCH (p:Person {name: ${'$'}name}) RETURN count(p) as result",
              mapOf("name" to "John"),
          )
          .single()["result"]
          .asLong() shouldBe 1L
    }
  }

  @Neo4jSink(
      cypher =
          [
              CypherStrategy(
                  TOPIC,
                  "WITH __value AS age MERGE (p:Person {age: age})",
              ),
          ],
  )
  @Test
  fun `should support raw numeric value`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
  ) {
    producer.publish(
        value = 25L,
        valueSchema = Schema.INT64_SCHEMA,
    )

    await().atMost(30.seconds.toJavaDuration()).untilAsserted {
      session
          .run(
              "MATCH (p:Person {age: ${'$'}age}) RETURN count(p) as result",
              mapOf("age" to 25L),
          )
          .single()["result"]
          .asLong() shouldBe 1L
    }
  }

  @Neo4jSink(
      cypher =
          [
              CypherStrategy(
                  TOPIC,
                  "WITH __value AS status MERGE (p:Person {single: status})",
              ),
          ],
  )
  @Test
  fun `should support raw boolean value`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
  ) {
    producer.publish(
        value = true,
        valueSchema = Schema.BOOLEAN_SCHEMA,
    )

    await().atMost(30.seconds.toJavaDuration()).untilAsserted {
      session
          .run(
              "MATCH (p:Person {single: ${'$'}single}) RETURN count(p) as result",
              mapOf("single" to true),
          )
          .single()["result"]
          .asLong() shouldBe 1L
    }
  }
}
