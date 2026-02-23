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

import java.util.concurrent.atomic.AtomicLong
import kotlin.time.Duration
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.neo4j.driver.Driver
import org.neo4j.driver.SessionConfig
import org.neo4j.driver.TransactionConfig

class DbTransactionMetricsData(
    metrics: Metrics,
    tags: LinkedHashMap<String, String> = linkedMapOf(),
    refreshTimeout: Duration,
    neo4jDriver: Driver,
    sessionConfig: SessionConfig,
    transactionConfig: TransactionConfig,
) {

  private val lastTransactionId = AtomicLong(0)

  private val scope = CoroutineScope(Dispatchers.Default + Job())

  init {
    metrics.addGauge(
        "last_db_tx_id",
        "The transaction commit timestamp of the last processed CDC message",
        tags,
    ) {
      lastTransactionId.get()
    }

    scope.launch {
      val databaseName = sessionConfig.database().orElse("neo4j")
      while (isActive) {
        val txId =
            neo4jDriver.session(sessionConfig).use { session ->
              session
                  .run(
                      "SHOW DATABASE $databaseName YIELD lastCommittedTxn RETURN lastCommittedTxn as txId",
                      transactionConfig,
                  )
                  .single()
                  .get("txId")
                  .asLong()
            }
        lastTransactionId.set(txId)

        delay(refreshTimeout)
      }
    }
  }

  fun stop() {
    scope.cancel()
  }
}
