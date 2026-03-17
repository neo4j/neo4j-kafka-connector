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
package org.neo4j.connectors.kafka.metrics

import java.io.Closeable
import java.util.concurrent.atomic.AtomicLong
import kotlin.time.Duration
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.neo4j.driver.Driver
import org.neo4j.driver.TransactionConfig
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class DbTransactionMetricsData(
    metrics: Metrics,
    tags: LinkedHashMap<String, String> = linkedMapOf(),
    refreshInterval: Duration,
    neo4jDriver: Driver,
    transactionConfig: TransactionConfig,
    databaseName: String,
    dispatcher: CoroutineDispatcher = Dispatchers.Default,
) : Closeable {

  private val lastTransactionId = AtomicLong(0)
  private val scope = CoroutineScope(dispatcher + Job())

  init {
    metrics.addGauge("last_db_tx_id", "The last committed transaction id in the database", tags) {
      lastTransactionId.get()
    }

    scope.launch {
      while (isActive) {
        try {
          val explicitDatabaseName = databaseName.ifBlank { "neo4j" }
          val txId: Long =
              neo4jDriver.session().use { session ->
                session.writeTransaction(
                    { tx ->
                      tx.run(
                              "SHOW DATABASE ${"$"}dbName YIELD lastCommittedTxn RETURN lastCommittedTxn as txId",
                              mapOf("dbName" to explicitDatabaseName),
                          )
                          .list()
                          .maxOfOrNull { it.get("txId").asLong() } ?: 0
                    },
                    transactionConfig,
                )
              }
          lastTransactionId.set(txId)
        } catch (e: Throwable) {
          log.warn("Unexpected error occurred while fetching last committed transaction id", e)
        }

        delay(refreshInterval)
      }
    }
  }

  override fun close() {
    scope.cancel()
  }

  companion object {
    private val log: Logger = LoggerFactory.getLogger(DbTransactionMetricsData::class.java)
  }
}
