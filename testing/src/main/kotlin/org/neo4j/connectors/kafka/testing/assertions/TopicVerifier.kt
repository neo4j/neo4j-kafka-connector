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
import kotlin.math.min
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.awaitility.Awaitility.await
import org.awaitility.core.ConditionTimeoutException
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class TopicVerifier<K, V>(private val consumer: KafkaConsumer<K, V>) {

  private val log: Logger = LoggerFactory.getLogger(this::class.java)

  private var messageValuePredicates = mutableListOf<Predicate<V>>()

  fun expectMessageValueMatching(predicate: Predicate<V>): TopicVerifier<K, V> {
    messageValuePredicates.add(predicate)
    return this
  }

  fun assertMessageValue(assertion: (V) -> Unit): TopicVerifier<K, V> {
    return expectMessageValueMatching { value ->
      try {
        assertion(value)
        true
      } catch (e: java.lang.AssertionError) {
        log.debug("Assertion has failed", e)
        false
      }
    }
  }

  /**
   * Verifies that the provided predicates pass for each of the corresponding actual messages. This
   * assertion only keeps the last _n_ received messages, where _n_ corresponds to the number of
   * predicates. The first predicate applies to the first message, the second predicate to the
   * second message, etc. The assertion will fail if:
   * - any of the predicate fails
   * - the number of predicates exceeds the number of actual messages The verification is retried
   *   until it succeeds or the provided (or default) timeout is reached.
   */
  fun verifyWithin(timeout: Duration): Unit {
    val predicates = messageValuePredicates.toList()
    if (predicates.isEmpty()) {
      throw AssertionError("expected at least 1 expected message predicate but got none")
    }
    val receivedMessages = RingBuffer<V>(predicates.size)
    try {
      await().atMost(timeout).until {
        consumer.poll(Duration.ofMillis(500)).forEach { receivedMessages.add(it.value()) }
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
    fun <K, V> create(consumer: KafkaConsumer<K, V>): TopicVerifier<K, V> {
      return TopicVerifier(consumer)
    }
  }
}

class RingBuffer<E>(capacity: Int) {
  private var index: Int
  private var size: Int
  private val data: Array<Any?>

  init {
    this.data = Array(capacity) { null }
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
