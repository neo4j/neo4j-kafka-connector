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

import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.metrics.Gauge
import org.apache.kafka.common.metrics.MetricConfig
import org.apache.kafka.common.metrics.PluginMetrics

class KafkaMetrics(private val pluginMetrics: PluginMetrics) : Metrics {

  private val lock = ReentrantLock()
  private val registeredMetrics: MutableSet<MetricName> = mutableSetOf()

  override fun <T : Number> addGauge(
      name: String,
      description: String,
      tags: LinkedHashMap<String, String>,
      valueProvider: () -> T?,
  ) {
    lock.withLock {
      val metricName = pluginMetrics.metricName(name, description, tags)
      registeredMetrics.add(metricName)
      pluginMetrics.addMetric(
          metricName,
          object : Gauge<T> {
            override fun value(config: MetricConfig?, now: Long): T? {
              return valueProvider()
            }
          },
      )
    }
  }

  override fun close() {
    lock.withLock { registeredMetrics.forEach(pluginMetrics::removeMetric) }
  }
}
