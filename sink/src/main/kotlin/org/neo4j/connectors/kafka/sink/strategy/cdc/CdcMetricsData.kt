package org.neo4j.connectors.kafka.sink.strategy.cdc

import org.neo4j.connectors.kafka.metrics.Metrics
import org.neo4j.cdc.client.model.Metadata as CdcMetadata

class CdcMetricsData(
  metrics: Metrics,
  tags: LinkedHashMap<String, String> = linkedMapOf(),
) {

  private var lastTxCommitTs: Long? = null
  private var lastTxStartTs: Long? = null

  init {
    metrics.addGauge(
        "last_cdc_tx_commit_timestamp",
        "The transaction commit timestamp of the last written CDC message",
        tags,
    ) {
      lastTxCommitTs
    }
    metrics.addGauge(
        "last_cdc_tx_start_timestamp",
        "The transaction start timestamp of the last written CDC message",
        tags,
    ) {
      lastTxStartTs
    }
  }

  fun applyMetadata(metadata: CdcMetadata) {
    lastTxCommitTs = metadata.txCommitTime.toEpochSecond()
    lastTxStartTs = metadata.txStartTime.toEpochSecond()
  }

}
