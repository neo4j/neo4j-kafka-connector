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

class Neo4jQueryService(
    private val config: SourceConfiguration,
    private val offsetStorageReader: OffsetStorageReader
) : AutoCloseable {

  private val log: Logger = LoggerFactory.getLogger(Neo4jQueryService::class.java)

  private val queue: BlockingQueue<SourceRecord> = LinkedBlockingQueue()
  private val error: AtomicReference<Throwable> = AtomicReference(null)

  private val sourcePartition = config.partition

  private val isClose = AtomicBoolean()

  private val currentOffset: AtomicLong = AtomicLong(resumeFrom())

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
              // TODO: Not sure what this does exactly
              if (lastCheckHadResult) {
                currentOffset.set(System.currentTimeMillis() - pollInterval)
              }
            }
            config
                .session()
                .readTransaction(
                    { tx ->
                      val result = tx.run(config.query, mapOf("lastCheck" to currentOffset.get()))
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
          currentOffset.getAndUpdate { oldValue ->
            if (oldValue >= value) {
              oldValue
            } else {
              value
            }
          }
          value
        } else {
          currentOffset.get()
        }
      } catch (e: Throwable) {
        // TODO: should we not log an error here?
        currentOffset.get()
      }

  fun poll(): List<SourceRecord>? {
    if (isClose.get()) {
      return null
    }

    return errorOr {
      // Block until at least one item is available or until the
      // courtesy timeout expires, giving the framework a chance
      // to pause the connector.
      val firstEvent = queue.poll(1, TimeUnit.SECONDS)
      if (firstEvent == null) {
        log.debug("Poll returns 0 results")
        return@errorOr null // Looks weird, but caller expects it.
      }

      val events = mutableListOf<SourceRecord>()
      return@errorOr try {
        events.add(firstEvent)
        queue.drainTo(events, config.batchSize - 1)
        log.info("Poll returns {} result(s)", events.size)
        events
      } catch (e: Exception) {
        setError(e)
        null
      }
    }
  }

  private fun <T> errorOr(action: () -> T): T {
    // throw any exception that has been stored until now
    checkError()

    val result = action()
    if (result == null) {
      // if we don't get any data from action, check the stored error again
      checkError()
    }

    return result
  }

  private fun checkError() {
    val stored = error.getAndSet(null)
    if (stored != null) {
      throw ConnectException(stored)
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

  private fun resumeFrom(): Long {
    val offset = offsetStorageReader.offset(config.partition) ?: emptyMap()

    if (!config.ignoreStoredOffset &&
        offset["value"] is Long &&
        offset["property"] == config.queryStreamingProperty) {
      log.debug("previously stored offset is {}", offset["value"])
      return offset["value"] as Long
    }

    val value =
        when (config.startFrom) {
          StartFrom.EARLIEST -> (-1)
          StartFrom.NOW -> System.currentTimeMillis()
          StartFrom.USER_PROVIDED -> config.startFromCustom.toLong()
        }

    log.debug(
        "{} is set as {} ({} = {}), offset to resume from is {}",
        SourceConfiguration.START_FROM,
        config.startFrom,
        SourceConfiguration.IGNORE_STORED_OFFSET,
        config.ignoreStoredOffset,
        value)
    return value
  }
}
