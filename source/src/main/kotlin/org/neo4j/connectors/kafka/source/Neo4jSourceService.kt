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
package org.neo4j.connectors.kafka.source

import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.storage.OffsetStorageReader
import org.neo4j.driver.Record
import org.neo4j.driver.Values
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class Neo4jSourceService(
    private val config: SourceConfiguration,
    offsetStorageReader: OffsetStorageReader
) : AutoCloseable {

  private val log: Logger = LoggerFactory.getLogger(Neo4jSourceService::class.java)

  private val queue: BlockingQueue<SourceRecord> = LinkedBlockingQueue()
  private val error: AtomicReference<Throwable> = AtomicReference(null)

  private val sourcePartition = config.partition

  private val isClose = AtomicBoolean()

  private val lastCheck: AtomicLong by lazy {
    val offset = offsetStorageReader.offset(sourcePartition) ?: emptyMap()
    // if the user wants to recover from LAST_COMMITTED
    val startValue =
        if (config.streamFrom == StreamingFrom.LAST_COMMITTED &&
            offset["value"] != null &&
            offset["property"] == config.queryStreamingProperty) {
          log.info(
              "Resuming offset $offset, the ${SourceConfiguration.STREAM_FROM} value is ignored")
          offset["value"] as Long
        } else {
          if (config.streamFrom == StreamingFrom.LAST_COMMITTED) {
            log.info(
                "You provided ${SourceConfiguration.STREAM_FROM}: ${config.streamFrom} but no offset has been found, we'll start to consume from NOW")
          } else {
            log.info(
                "No offset to resume, we'll use the provided value of ${SourceConfiguration.STREAM_FROM}: ${config.streamFrom}")
          }
          config.streamFrom.value()
        }
    AtomicLong(startValue)
  }

  private val pollInterval = config.queryPollingInterval.inWholeMilliseconds
  private val isStreamingPropertyDefined = config.queryStreamingProperty.isNotBlank()
  private val streamingProperty = config.queryStreamingProperty.ifBlank { "undefined" }

  @DelicateCoroutinesApi
  private val job: Job =
      GlobalScope.launch(Dispatchers.IO) {
        var lastCheckHadResult = false
        while (isActive) {
          try {
            // if the user doesn't set the streaming property we fall back to an
            // internal mechanism
            if (!isStreamingPropertyDefined) {
              // we update the lastCheck property only if the last loop round
              // returned results otherwise we stick to the old value
              if (lastCheckHadResult) {
                lastCheck.set(System.currentTimeMillis() - pollInterval)
              }
            }
            config
                .session()
                .readTransaction(
                    { tx ->
                      val result = tx.run(config.query, mapOf("lastCheck" to lastCheck.get()))
                      lastCheckHadResult = result.hasNext()
                      result.forEach { record ->
                        try {
                          val sourceRecord = toSourceRecord(record)
                          queue.put(sourceRecord)
                        } catch (e: Exception) {
                          setError(e)
                        }
                      }
                    },
                    config.txConfig())
            delay(pollInterval)
          } catch (e: Exception) {
            setError(e)
          }
        }
      }

  private fun toSourceRecord(record: Record): SourceRecord {
    val thisValue = computeLastTimestamp(record)
    return SourceRecordBuilder()
        .withRecord(record)
        .withTopic(config.topic)
        .withSourcePartition(sourcePartition)
        .withStreamingProperty(streamingProperty)
        .withEnforceSchema(config.enforceSchema)
        .withTimestamp(thisValue)
        .build()
  }

  private fun computeLastTimestamp(record: Record) =
      try {
        if (isStreamingPropertyDefined) {
          val value = record.get(config.queryStreamingProperty, Values.value(-1L)).asLong()
          lastCheck.getAndUpdate { oldValue ->
            if (oldValue >= value) {
              oldValue
            } else {
              value
            }
          }
          value
        } else {
          lastCheck.get()
        }
      } catch (e: Throwable) {
        lastCheck.get()
      }

  private fun checkError() {
    val fatalError = error.getAndSet(null)
    if (fatalError != null) {
      throw ConnectException(fatalError)
    }
  }

  fun poll(): List<SourceRecord>? {
    if (isClose.get()) {
      return null
    }
    checkError()
    // Block until at least one item is available or until the
    // courtesy timeout expires, giving the framework a chance
    // to pause the connector.
    val firstEvent = queue.poll(1, TimeUnit.SECONDS)
    if (firstEvent == null) {
      log.debug("Poll returns 0 results")
      return null // Looks weird, but caller expects it.
    }

    val events = mutableListOf<SourceRecord>()
    return try {
      events.add(firstEvent)
      queue.drainTo(events, config.queryBatchSize - 1)
      log.info("Poll returns {} result(s)", events.size)
      events
    } catch (e: Exception) {
      setError(e)
      null
    }
  }

  private fun setError(e: Exception) {
    if (e !is CancellationException) {
      if (error.compareAndSet(null, e)) {
        log.error("Error:", e)
      }
    }
  }

  @DelicateCoroutinesApi
  override fun close() {
    isClose.set(true)
    runBlocking { job.cancelAndJoin() }
    config.close()
    log.info("Neo4j Source Service closed successfully")
  }
}
