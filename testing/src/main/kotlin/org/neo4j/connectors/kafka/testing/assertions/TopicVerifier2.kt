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

package org.neo4j.connectors.kafka.testing.assertions

import java.time.Duration
import java.util.function.Predicate
import org.awaitility.Awaitility
import org.awaitility.core.ConditionTimeoutException
import org.neo4j.connectors.kafka.testing.kafka.GenericKafkaConsumer
import org.neo4j.connectors.kafka.testing.kafka.GenericRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class TopicVerifier2<K, V>(
    private val consumer: GenericKafkaConsumer,
    private val keyAssertionClass: Class<K>,
    private val valueAssertionClass: Class<V>
) {

  private val log: Logger = LoggerFactory.getLogger(this::class.java)

  private var messagePredicates = mutableListOf<Predicate<GenericRecord<K, V>>>()

  fun assertMessageValue(assertion: (V) -> Unit): TopicVerifier2<K, V> {
    messagePredicates.add { record ->
      try {
        assertion(record.value!!)
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
              GenericRecord(
                  key = consumer.keyConverter.mapper.map(it.key(), keyAssertionClass),
                  value = consumer.valueConverter.mapper.map(it.value(), valueAssertionClass))
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
    fun <K, V> create(
        consumer: GenericKafkaConsumer,
        keyAssertionClass: Class<K>,
        valueAssertionClass: Class<V>
    ): TopicVerifier2<K, V> {
      return TopicVerifier2(consumer, keyAssertionClass, valueAssertionClass)
    }
  }
}
