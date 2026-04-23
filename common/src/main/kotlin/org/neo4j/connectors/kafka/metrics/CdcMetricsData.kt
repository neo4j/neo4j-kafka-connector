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
import kotlin.time.Clock
import org.neo4j.cdc.client.model.ChangeEvent
import org.neo4j.connectors.kafka.configuration.ConnectorType

class CdcMetricsData(
    metrics: Metrics,
    connectorType: ConnectorType,
    tags: LinkedHashMap<String, String> = linkedMapOf(),
) {

  private val lastTxCommitTs: AtomicLong = AtomicLong(0L)
  private val lastTxStartTs: AtomicLong = AtomicLong(0L)
  private val lastTxId: AtomicLong = AtomicLong(0L)

  init {
    metrics.addGauge(
        "last_cdc_tx_commit_timestamp",
        "The transaction commit timestamp of the last ${connectorType.descriptionActionVerb()} CDC message",
        tags,
    ) {
      lastTxCommitTs.get()
    }
    metrics.addGauge(
        "last_cdc_tx_commit_time_delta",
        "The time (in seconds) since the last committed ${connectorType.descriptionActionVerb()} CDC message",
        tags,
    ) {
      if (lastTxCommitTs.get() == 0L) 0L // no tx to compare to
      else Clock.System.now().epochSeconds - lastTxCommitTs.get()
    }
    metrics.addGauge(
        "last_cdc_tx_start_timestamp",
        "The transaction start timestamp of the last ${connectorType.descriptionActionVerb()} CDC message",
        tags,
    ) {
      lastTxStartTs.get()
    }
    metrics.addGauge(
        "last_cdc_tx_id",
        "The transaction id of the last ${connectorType.descriptionActionVerb()} CDC message",
        tags,
    ) {
      lastTxId.get()
    }
  }

  fun update(event: ChangeEvent) {
    event.metadata?.let {
      lastTxCommitTs.set(it.txCommitTime.toEpochSecond())
      lastTxStartTs.set(it.txStartTime.toEpochSecond())
    }
    lastTxId.set(event.txId)
  }

  companion object {
    private fun ConnectorType.descriptionActionVerb(): String =
        when (this) {
          ConnectorType.SOURCE -> "polled"
          ConnectorType.SINK -> "pushed"
        }
  }
}
