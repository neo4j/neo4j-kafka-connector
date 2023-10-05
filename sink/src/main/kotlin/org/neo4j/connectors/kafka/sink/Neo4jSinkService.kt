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
package org.neo4j.connectors.kafka.sink

import kotlin.streams.toList
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import org.apache.kafka.connect.errors.ConnectException
import org.neo4j.connectors.kafka.extensions.errors
import org.neo4j.connectors.kafka.service.StreamsSinkEntity
import org.neo4j.connectors.kafka.service.StreamsSinkService
import org.neo4j.connectors.kafka.utils.retryForException
import org.neo4j.driver.Bookmark
import org.neo4j.driver.TransactionConfig
import org.neo4j.driver.exceptions.ClientException
import org.neo4j.driver.exceptions.TransientException
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class Neo4jSinkService(private val config: SinkConfiguration) :
    StreamsSinkService(Neo4jStrategyStorage(config)) {

  private val log: Logger = LoggerFactory.getLogger(Neo4jSinkService::class.java)

  private val transactionConfig: TransactionConfig = config.txConfig()

  private val bookmarks = mutableListOf<Bookmark>()

  fun close() {
    config.close()
  }

  override fun write(query: String, events: Collection<Any>) {
    val data = mapOf<String, Any>("events" to events)
    config.session(*bookmarks.toTypedArray()).use { session ->
      try {
        runBlocking {
          retryForException(
              exceptions = arrayOf(ClientException::class.java, TransientException::class.java),
              retries = config.maxRetryAttempts,
              delayTime =
                  0) { // we use the delayTime = 0, because we delegate the retryBackoff to the
                // Neo4j
                // Java Driver
                session.writeTransaction(
                    {
                      val result = it.run(query, data)
                      if (log.isDebugEnabled) {
                        val summary = result.consume()
                        log.debug("Successfully executed query: `$query`. Summary: $summary")
                      }
                    },
                    transactionConfig)
              }
        }
      } catch (e: Exception) {
        bookmarks += session.lastBookmark()
        if (log.isDebugEnabled) {
          val subList = events.stream().limit(5.coerceAtMost(events.size).toLong()).toList()
          log.debug(
              "Exception `${e.message}` while executing query: `$query`, with data: `$subList` total-records ${events.size}")
        }
        throw e
      }
    }
  }

  fun writeData(data: Map<String, List<List<StreamsSinkEntity>>>) {
    val errors = if (config.parallelBatches) writeDataAsync(data) else writeDataSync(data)
    if (errors.isNotEmpty()) {
      throw ConnectException(
          errors
              .map { it.message }
              .toSet()
              .joinToString("\n", "Errors executing ${data.values.map { it.size }.sum()} jobs:\n"))
    }
  }

  @ExperimentalCoroutinesApi
  @ObsoleteCoroutinesApi
  private fun writeDataAsync(data: Map<String, List<List<StreamsSinkEntity>>>) = runBlocking {
    val jobs =
        data.flatMap { (topic, records) ->
          records.map { async(Dispatchers.IO) { writeForTopic(topic, it) } }
        }

    // timeout starts in writeTransaction()
    jobs.awaitAll()
    jobs.mapNotNull { it.errors() }
  }

  private fun writeDataSync(data: Map<String, List<List<StreamsSinkEntity>>>) =
      data.flatMap { (topic, records) ->
        records.mapNotNull {
          try {
            writeForTopic(topic, it)
            null
          } catch (e: Exception) {
            e
          }
        }
      }
}
