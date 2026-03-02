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
package org.neo4j.connectors.kafka.sink.strategy.cdc

import org.neo4j.connectors.kafka.configuration.ConnectorType
import org.neo4j.connectors.kafka.metrics.CdcMetricsData
import org.neo4j.connectors.kafka.metrics.Metrics
import org.neo4j.connectors.kafka.sink.ChangeQuery
import org.neo4j.connectors.kafka.sink.SinkStrategy
import org.neo4j.connectors.kafka.sink.strategy.SinkBatchStrategy
import org.neo4j.connectors.kafka.sink.strategy.SinkEventTransformer
import org.neo4j.connectors.kafka.sink.strategy.SinkHandler

class CdcSinkHandler(
    strategy: SinkStrategy,
    batchStrategy: SinkBatchStrategy,
    eventTransformer: SinkEventTransformer,
    metrics: Metrics,
) : SinkHandler(strategy, batchStrategy, eventTransformer) {
  private val metricsData = CdcMetricsData(metrics, ConnectorType.SINK)

  override fun postProcessLastMessageBatch(group: Iterable<ChangeQuery>) {
    group.lastOrNull()?.messages?.lastOrNull()?.toChangeEvent()?.let { metricsData.update(it) }
  }
}
