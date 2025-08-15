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
package org.neo4j.connectors.kafka.testing.kafka

import java.net.URI
import java.time.Instant
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.Header
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Values
import org.apache.kafka.connect.storage.SimpleHeaderConverter
import org.neo4j.cdc.client.model.ChangeEvent
import org.neo4j.connectors.kafka.configuration.PayloadMode
import org.neo4j.connectors.kafka.data.ChangeEventConverter
import org.neo4j.connectors.kafka.data.Headers
import org.neo4j.connectors.kafka.events.StreamsTransactionEvent
import org.neo4j.connectors.kafka.testing.SchemaRegistrySupport
import org.neo4j.connectors.kafka.testing.format.KafkaConverter
import org.neo4j.connectors.kafka.testing.sink.SchemaCompatibilityMode
import org.neo4j.connectors.kafka.utils.JSONUtils

data class KafkaMessage(
    val keySchema: Schema? = null,
    val key: Any? = null,
    val valueSchema: Schema? = null,
    val value: Any? = null,
    val timestamp: Instant? = null,
    val headers: Map<String, Any> = emptyMap(),
)

data class ConvertingKafkaProducer(
    val schemaRegistryURI: URI,
    val keyCompatibilityMode: SchemaCompatibilityMode,
    val keyConverter: KafkaConverter,
    val valueCompatibilityMode: SchemaCompatibilityMode,
    val valueConverter: KafkaConverter,
    val kafkaProducer: KafkaProducer<Any, Any>,
    val topic: String,
) {

  init {
    ensureSchemaCompatibility(topic)
  }

  private val changeEventConverter = ChangeEventConverter(PayloadMode.EXTENDED)

  fun publish(vararg kafkaMessages: KafkaMessage) {
    kafkaProducer.beginTransaction()
    try {
      kafkaMessages.forEach {
        val serializedKey =
            when (it.key) {
              null -> null
              else ->
                  keyConverter.testShimSerializer.serialize(
                      it.key,
                      it.keySchema ?: throw IllegalArgumentException("null key schema"),
                      true,
                  )
            }
        val serializedValue =
            when (it.value) {
              null -> null
              else ->
                  valueConverter.testShimSerializer.serialize(
                      it.value,
                      it.valueSchema ?: throw IllegalArgumentException("null value schema"),
                      false,
                  )
            }
        val converter = SimpleHeaderConverter()
        val recordHeaders =
            it.headers.map { e ->
              object : Header {
                override fun key(): String = e.key

                override fun value(): ByteArray {
                  return converter.fromConnectHeader(
                      "",
                      e.key,
                      Values.inferSchema(e.value),
                      e.value,
                  )
                }
              }
            }

        val record: ProducerRecord<Any, Any> =
            ProducerRecord(
                topic,
                null,
                it.timestamp?.toEpochMilli(),
                serializedKey,
                serializedValue,
                recordHeaders,
            )
        kafkaProducer.send(record).get()
      }
      kafkaProducer.commitTransaction()
    } catch (e: Exception) {
      kafkaProducer.abortTransaction()
      throw e
    }
  }

  fun publish(
      keySchema: Schema? = null,
      key: Any? = null,
      valueSchema: Schema? = null,
      value: Any? = null,
      timestamp: Instant? = null,
      headers: Map<String, Any> = emptyMap(),
  ) {
    publish(KafkaMessage(keySchema, key, valueSchema, value, timestamp, headers))
  }

  fun publish(event: ChangeEvent) {
    val connectValue = changeEventConverter.toConnectValue(event)

    publish(
        keySchema = connectValue.schema(),
        key = connectValue.value(),
        valueSchema = connectValue.schema(),
        value = connectValue.value(),
        timestamp = event.metadata.txCommitTime.toInstant(),
        headers = Headers.from(event).associate { it.key() to it.value() },
    )
  }

  fun publish(vararg events: ChangeEvent) {
    val kafkaMessages = mutableListOf<KafkaMessage>()

    events.forEach {
      val connectValue = changeEventConverter.toConnectValue(it)
      kafkaMessages.add(
          KafkaMessage(
              keySchema = connectValue.schema(),
              key = connectValue.value(),
              valueSchema = connectValue.schema(),
              value = connectValue.value(),
              timestamp = it.metadata.txCommitTime.toInstant(),
              headers = Headers.from(it).associate { header -> header.key() to header.value() },
          )
      )
    }

    publish(*kafkaMessages.toTypedArray())
  }

  fun publish(event: StreamsTransactionEvent) {
    publish(valueSchema = Schema.STRING_SCHEMA, value = JSONUtils.writeValueAsString(event))
  }

  private fun ensureSchemaCompatibility(topic: String) {
    SchemaRegistrySupport.setCompatibilityMode(
        schemaRegistryURI,
        "$topic-key",
        keyCompatibilityMode,
    )
    SchemaRegistrySupport.setCompatibilityMode(
        schemaRegistryURI,
        "$topic-value",
        valueCompatibilityMode,
    )
  }
}
