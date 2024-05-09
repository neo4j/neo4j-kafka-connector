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

import java.time.Duration
import java.util.function.Predicate
import kotlin.math.min
import org.awaitility.Awaitility
import org.awaitility.core.ConditionTimeoutException
import org.neo4j.connectors.kafka.testing.kafka.ConvertingKafkaConsumer
import org.neo4j.connectors.kafka.testing.kafka.GenericRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class TopicVerifier<K, V>(
    private val consumer: ConvertingKafkaConsumer,
    private val keyAssertionClass: Class<K>,
    private val valueAssertionClass: Class<V>
) {

  private val log: Logger = LoggerFactory.getLogger(this::class.java)

  private var messagePredicates = mutableListOf<Predicate<GenericRecord<K, V>>>()

  fun assertMessageKey(assertion: (K?) -> Unit): TopicVerifier<K, V> {
    return assertMessage { assertion(it.key) }
  }

  fun assertMessageValue(assertion: (V) -> Unit): TopicVerifier<K, V> {
    return assertMessage { assertion(it.value) }
  }

  fun assertMessage(assertion: (GenericRecord<K, V>) -> Unit): TopicVerifier<K, V> {
    messagePredicates.add { record ->
      try {
        assertion(record)
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
    val receivedMessages = RingBuffer<GenericRecord<K, V>>(predicates.size)
    try {
      Awaitility.await().atMost(timeout).until {
        consumer.kafkaConsumer
            .poll(Duration.ofMillis(500))
            .map {
              val value: V =
                  consumer.valueConverter.testShimDeserializer.deserialize(
                      it.value(), valueAssertionClass)!!
              GenericRecord(
                  raw = it,
                  key =
                      consumer.keyConverter.testShimDeserializer.deserialize(
                          it.key(), keyAssertionClass),
                  value = value)
            }
            .forEach { receivedMessages.add(it) }
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

  companion object {
    inline fun <reified K, reified V> create(
        consumer: ConvertingKafkaConsumer
    ): TopicVerifier<K, V> {
      return TopicVerifier(consumer, K::class.java, V::class.java)
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
      val indices = (start ..< this.size) + (0 ..< start)
      return indices.map { i -> data[i] as E }.toList()
    }
  }
}
