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
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.storage.Converter
import org.awaitility.Awaitility
import org.awaitility.core.ConditionTimeoutException
import org.neo4j.cdc.client.model.ChangeEvent
import org.neo4j.connectors.kafka.data.DynamicTypes
import org.neo4j.connectors.kafka.data.toChangeEvent
import org.neo4j.connectors.kafka.testing.format.KafkaConverter
import org.neo4j.connectors.kafka.testing.kafka.ConvertingKafkaConsumer
import org.neo4j.connectors.kafka.testing.kafka.GenericRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class TopicVerifier<K, V>(
    private val consumer: ConvertingKafkaConsumer,
    private val keyConverter: Converter,
    private val valueConverter: Converter,
    private val keyAssertionClass: Class<K>,
    private val valueAssertionClass: Class<V>,
) {

  private val log: Logger = LoggerFactory.getLogger(this::class.java)

  private var inAnyOrder: Boolean = false
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
                        record.key(),
                    )
                        as K,
                value =
                    convert(
                        valueConverter,
                        valueAssertionClass,
                        schemaTopic ?: record.topic(),
                        record.value(),
                    )
                        as V,
            )

        log.trace("testing message {} against assertion", genericRecord)
        assertion(genericRecord)
        log.trace("assertion successful")
        true
      } catch (e: java.lang.AssertionError) {
        log.debug("assertion failed", e)
        false
      }
    }
    return this
  }

  fun inAnyOrder(): TopicVerifier<K, V> {
    this.inAnyOrder = true
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
        val messages = receivedMessages.toList().toMutableList()

        log.trace("received {} messages", messages.size)
        if (messages.size != predicates.size) {
          log.trace("did not receive enough messages to test {} assertions", predicates.size)
          return@until false
        }

        if (inAnyOrder) {
          log.trace("running assertions in any order")
          var queue = predicates.toList()
          while (true) {
            val predicate = queue.firstOrNull()
            if (predicate == null) {
              break
            }

            log.trace("testing assertion {}", predicates.size - queue.size)
            val matched = messages.find { predicate.test(it) }
            if (matched == null) {
              return@until false
            }

            messages.remove(matched)
            queue = queue.drop(1)
          }

          true
        } else {
          log.trace("running assertions in strict order")
          predicates.foldIndexed(true) { i, prev, predicate ->
            log.trace("testing assertion {}", i)
            prev && predicate.test(messages[i])
          }
        }
      }
    } catch (e: ConditionTimeoutException) {
      throw AssertionError(
          "Timeout of ${timeout.toMillis()}s reached: could not verify all ${predicates.size} predicate(s) on received messages",
      )
    }
  }

  @Suppress("UNCHECKED_CAST")
  private fun <V> convert(
      converter: Converter,
      assertionClass: Class<V>,
      topic: String,
      value: ByteArray?,
  ): Any? {
    return when (val sourceValue = converter.toConnectData(topic, value).value()) {
      is Struct ->
          when (assertionClass) {
            ChangeEvent::class.java -> sourceValue.toChangeEvent()
            Map::class.java ->
                DynamicTypes.fromConnectValue(sourceValue.schema(), sourceValue, true)
            else -> sourceValue as V
          }
      else -> sourceValue
    }
  }

  companion object {
    inline fun <reified K, reified V> create(
        consumer: ConvertingKafkaConsumer,
    ): TopicVerifier<K, V> {
      val keyConverter = consumer.keyConverter.converterProvider()
      when (consumer.keyConverter) {
        KafkaConverter.JSON_EMBEDDED ->
            keyConverter.configure(mapOf("schemas.enable" to true), true)
        KafkaConverter.JSON_RAW -> keyConverter.configure(mapOf("schemas.enable" to false), true)
        else ->
            keyConverter.configure(
                mapOf(
                    AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG to
                        consumer.schemaRegistryUrlProvider(),
                ),
                true,
            )
      }

      val valueConverter = consumer.valueConverter.converterProvider()
      when (consumer.valueConverter) {
        KafkaConverter.JSON_EMBEDDED ->
            valueConverter.configure(mapOf("schemas.enable" to true), false)
        KafkaConverter.JSON_RAW -> valueConverter.configure(mapOf("schemas.enable" to false), false)
        else ->
            valueConverter.configure(
                mapOf(
                    AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG to
                        consumer.schemaRegistryUrlProvider(),
                ),
                false,
            )
      }

      return TopicVerifier(
          consumer = consumer,
          keyConverter = keyConverter,
          valueConverter = valueConverter,
          keyAssertionClass = K::class.java,
          valueAssertionClass = V::class.java,
      )
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
