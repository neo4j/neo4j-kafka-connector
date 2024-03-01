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

package org.neo4j.connectors.kafka.testing.kafka

import java.net.URI
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.Header
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Values
import org.apache.kafka.connect.storage.SimpleHeaderConverter
import org.neo4j.cdc.client.model.ChangeEvent
import org.neo4j.connectors.kafka.data.ChangeEventExtensions.toConnectValue
import org.neo4j.connectors.kafka.data.Headers
import org.neo4j.connectors.kafka.events.StreamsTransactionEvent
import org.neo4j.connectors.kafka.testing.SchemaRegistrySupport
import org.neo4j.connectors.kafka.testing.format.KafkaConverter
import org.neo4j.connectors.kafka.testing.sink.SchemaCompatibilityMode
import org.neo4j.connectors.kafka.utils.JSONUtils

data class ConvertingKafkaProducer(
    val schemaRegistryURI: URI,
    val keyCompatibilityMode: SchemaCompatibilityMode,
    val keyConverter: KafkaConverter,
    val valueCompatibilityMode: SchemaCompatibilityMode,
    val valueConverter: KafkaConverter,
    val kafkaProducer: KafkaProducer<Any, Any>,
    val topic: String
) {

  fun publish(
      keySchema: Schema? = null,
      key: Any? = null,
      valueSchema: Schema,
      value: Any,
      timestamp: Long? = null,
      headers: Map<String, Any> = emptyMap()
  ) {
    val serialisedKey =
        when (key) {
          null -> null
          else -> keyConverter.testShimSerializer.serialize(key, keySchema!!)
        }
    val serialisedValue = valueConverter.testShimSerializer.serialize(value, valueSchema)
    val converter = SimpleHeaderConverter()
    val recordHeaders =
        headers.map { e ->
          object : Header {
            override fun key(): String = e.key

            override fun value(): ByteArray {
              return converter.fromConnectHeader("", e.key, Values.inferSchema(e.value), e.value)
            }
          }
        }

    ensureSchemaCompatibility(topic)

    val record: ProducerRecord<Any, Any> =
        ProducerRecord(topic, null, timestamp, serialisedKey, serialisedValue, recordHeaders)
    kafkaProducer.send(record).get()
  }

  fun publish(event: ChangeEvent) {
    val connectValue = event.toConnectValue()

    publish(
        keySchema = connectValue.schema(),
        key = connectValue.value(),
        valueSchema = connectValue.schema(),
        value = connectValue.value(),
        timestamp = event.metadata.txCommitTime.toInstant().toEpochMilli(),
        headers = Headers.from(event).associate { it.key() to it.value() })
  }

  fun publish(event: StreamsTransactionEvent) {
    publish(valueSchema = Schema.STRING_SCHEMA, value = JSONUtils.writeValueAsString(event))
  }

  private fun ensureSchemaCompatibility(topic: String) {
    SchemaRegistrySupport.setCompatibilityMode(
        schemaRegistryURI, "$topic-key", keyCompatibilityMode)
    SchemaRegistrySupport.setCompatibilityMode(
        schemaRegistryURI, "$topic-value", valueCompatibilityMode)
  }
}
