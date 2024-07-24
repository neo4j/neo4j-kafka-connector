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
package org.neo4j.connectors.kafka.testing.assertions

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import java.time.Duration
import java.util.function.Predicate
import kotlin.math.min
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.storage.Converter
import org.awaitility.Awaitility
import org.awaitility.core.ConditionTimeoutException
import org.neo4j.cdc.client.model.ChangeEvent
import org.neo4j.connectors.kafka.data.toChangeEvent
import org.neo4j.connectors.kafka.testing.kafka.ConvertingKafkaConsumer
import org.neo4j.connectors.kafka.testing.kafka.GenericRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class TopicVerifier<K, V>(
    private val consumer: ConvertingKafkaConsumer,
    private val keyConverter: Converter,
    private val valueConverter: Converter,
    private val keyAssertionClass: Class<K>,
    private val valueAssertionClass: Class<V>
) {

  private val log: Logger = LoggerFactory.getLogger(this::class.java)

  private var messagePredicates = mutableListOf<Predicate<ConsumerRecord<ByteArray, ByteArray>>>()

  fun assertMessageKey(schemaTopic: String? = null, assertion: (K?) -> Unit): TopicVerifier<K, V> {
    return assertMessage(schemaTopic) { assertion(it.key) }
  }

  fun assertMessageValue(schemaTopic: String? = null, assertion: (V) -> Unit): TopicVerifier<K, V> {
    return assertMessage(schemaTopic) { assertion(it.value) }
  }

  @Suppress("UNCHECKED_CAST")
  fun assertMessage(
      schemaTopic: String? = null,
      assertion: (GenericRecord<K, V>) -> Unit
  ): TopicVerifier<K, V> {
    messagePredicates.add { record ->
      try {
        val genericRecord =
            GenericRecord(
                raw = record,
                key =
                    convert(
                        keyConverter,
                        keyAssertionClass,
                        schemaTopic ?: record.topic(),
                        record.key())
                        as K,
                value =
                    convert(
                        valueConverter,
                        valueAssertionClass,
                        schemaTopic ?: record.topic(),
                        record.value())
                        as V)

        assertion(genericRecord)
        true
      } catch (e: java.lang.AssertionError) {
        log.debug("Assertion has failed", e)
        false
      }
    }
    return this
  }

  fun verifyWithin(timeout: Duration) {
    val predicates = messagePredicates.toList()
    if (predicates.isEmpty()) {
      throw AssertionError("expected at least 1 expected message predicate but got none")
    }
    val receivedMessages = RingBuffer<ConsumerRecord<ByteArray, ByteArray>>(predicates.size)
    try {
      Awaitility.await().atMost(timeout).until {
        consumer.kafkaConsumer.poll(Duration.ofMillis(500)).forEach { receivedMessages.add(it) }
        val messages = receivedMessages.toList()
        messages.size == predicates.size &&
            predicates.foldIndexed(true) { i, prev, predicate ->
              prev && predicate.test(messages[i])
            }
      }
    } catch (e: ConditionTimeoutException) {
      throw AssertionError(
          "Timeout of ${timeout.toMillis()}s reached: could not verify all ${predicates.size} predicate(s) on received messages")
    }
  }

  @Suppress("UNCHECKED_CAST")
  private fun <V> convert(
      converter: Converter,
      assertionClass: Class<V>,
      topic: String,
      value: ByteArray?
  ): Any? {
    return when (val sourceValue = converter.toConnectData(topic, value).value()) {
      is Struct ->
          when (assertionClass) {
            ChangeEvent::class.java -> sourceValue.toChangeEvent()
            Map::class.java -> structToMap(sourceValue)
            else -> sourceValue as V
          }
      is String ->
          if (sourceValue == "null") null
          else sourceValue // ByteArray deserializer is deserializing incoming null values from json
      // schema to corresponding byte array of "null" string
      else -> sourceValue
    }
  }

  private fun structToMap(struct: Struct): Map<String, Any?> {
    val map = mutableMapOf<String, Any?>()
    struct
        .schema()
        .fields()
        .filter { struct.get(it) != null }
        .forEach { field ->
          when (field.schema().type()) {
            Schema.Type.STRUCT -> map[field.name()] = structToMap(struct.getStruct(field.name()))
            Schema.Type.ARRAY -> map[field.name()] = convertList(struct.getArray<Any>(field.name()))
            else -> map[field.name()] = struct.get(field)
          }
        }
    return map
  }

  private fun convertList(list: Iterable<*>): List<Any?> {
    return list.map {
      when (it) {
        is Struct -> structToMap(it)
        is Iterable<*> -> convertList(it)
        else -> it
      }
    }
  }

  companion object {
    inline fun <reified K, reified V> create(
        consumer: ConvertingKafkaConsumer,
    ): TopicVerifier<K, V> {
      val keyConverter = consumer.keyConverter.converterProvider()
      keyConverter.configure(
          mapOf(
              AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG to
                  consumer.schemaRegistryUrlProvider()),
          true)

      val valueConverter = consumer.valueConverter.converterProvider()
      valueConverter.configure(
          mapOf(
              AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG to
                  consumer.schemaRegistryUrlProvider()),
          false)

      return TopicVerifier(
          consumer = consumer,
          keyConverter = keyConverter,
          valueConverter = valueConverter,
          keyAssertionClass = K::class.java,
          valueAssertionClass = V::class.java)
    }

    fun createForMap(consumer: ConvertingKafkaConsumer) =
        create<Map<String, Any>, Map<String, Any>>(consumer)
  }
}

class RingBuffer<E>(capacity: Int) {
  private var index: Int
  private var size: Int
  private val data: Array<Any?> = Array(capacity) { null }

  init {
    this.index = 0
    this.size = 0
  }

  fun add(element: E) {
    synchronized(data) {
      this.data[this.index] = element
      this.size = min(this.size + 1, this.data.size)
      this.index = ((this.index + 1) % this.data.size)
    }
  }

  @Suppress("UNCHECKED_CAST")
  fun toList(): List<E> {
    synchronized(data) {
      if (this.size == 0) {
        return emptyList()
      }
      val start = if (this.size < this.data.size) 0 else this.index
      val indices = (start..<this.size) + (0..<start)
      return indices.map { i -> data[i] as E }.toList()
    }
  }
}
