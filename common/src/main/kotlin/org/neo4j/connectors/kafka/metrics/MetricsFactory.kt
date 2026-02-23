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

import org.apache.kafka.connect.sink.SinkTaskContext
import org.apache.kafka.connect.source.SourceTaskContext
import org.neo4j.connectors.kafka.configuration.Neo4jConfiguration
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class MetricsFactory {

  fun createMetrics(context: SinkTaskContext, config: Neo4jConfiguration): Metrics {
    return createKafkaMetrics(context) ?: createJmxMetrics(config)
  }

  fun createMetrics(context: SourceTaskContext, config: Neo4jConfiguration): Metrics {
    return createKafkaMetrics(context) ?: createJmxMetrics(config)
  }

  private fun createKafkaMetrics(context: SourceTaskContext): KafkaMetrics? {
    return try {
      val metrics = KafkaMetrics(context.pluginMetrics())
      log.info("Plugin metrics support detected")
      metrics
    } catch (_: NoSuchMethodError) {
      null
    } catch (_: NoClassDefFoundError) {
      null
    }
  }

  private fun createKafkaMetrics(context: SinkTaskContext): KafkaMetrics? {
    return try {
      val metrics = KafkaMetrics(context.pluginMetrics())
      log.info("Plugin metrics support detected")
      metrics
    } catch (_: NoSuchMethodError) {
      null
    } catch (_: NoClassDefFoundError) {
      null
    }
  }

  private fun createJmxMetrics(config: Neo4jConfiguration): JmxMetrics {
    log.error("No plugin metrics support detected. Using JMX only metrics")
    return JmxMetrics(config)
  }

  companion object {
    private val log: Logger = LoggerFactory.getLogger(MetricsFactory::class.java)
  }
}

interface Metrics {
  fun <T : Number> addGauge(
      name: String,
      description: String,
      tags: LinkedHashMap<String, String>,
      valueProvider: () -> T?,
  )
}
