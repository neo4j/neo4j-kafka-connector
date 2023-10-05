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
package streams.kafka.connect.source.testing

import java.time.Duration
import kotlin.math.min
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.awaitility.Awaitility.await

class ConsumerAssertions<K, V>(private val consumer: KafkaConsumer<K, V>) {

  private var timeout = Duration.ofSeconds(60)

  fun awaitingAtMost(duration: Duration): ConsumerAssertions<K, V> {
    this.timeout = duration
    return this
  }

  fun <T> hasReceivedValues(mapper: (V) -> T, vararg messages: T): ConsumerAssertions<K, V> {
    if (messages.isEmpty()) {
      throw AssertionError("expected at least 1 expected received message but got none")
    }
    val records = RingBuffer<T>(messages.size)
    await().atMost(timeout).until {
      // TODO: handle small timeout values
      consumer.poll(timeout.dividedBy(5)).forEach { records.add(mapper(it.value())) }
      messages.toList() == records.toList()
    }
    return this
  }

  companion object {
    fun <K, V> assertThat(consumer: KafkaConsumer<K, V>): ConsumerAssertions<K, V> {
      return ConsumerAssertions(consumer)
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
