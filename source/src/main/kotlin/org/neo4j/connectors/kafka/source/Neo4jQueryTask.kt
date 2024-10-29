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

import java.util.concurrent.atomic.AtomicLong
import kotlin.time.TimeSource
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.runBlocking
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.source.SourceTask
import org.neo4j.connectors.kafka.configuration.helpers.VersionUtil
import org.neo4j.connectors.kafka.data.DynamicTypes
import org.neo4j.connectors.kafka.exceptions.InvalidDataException
import org.neo4j.driver.Record
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux

class Neo4jQueryTask : SourceTask() {
  private val log: Logger = LoggerFactory.getLogger(Neo4jQueryTask::class.java)

  private lateinit var settings: Map<String, String>
  private lateinit var config: SourceConfiguration
  private lateinit var offset: AtomicLong

  override fun version(): String = VersionUtil.version(this.javaClass as Class<*>)

  override fun start(props: Map<String, String>?) {
    log.info("starting")

    settings = props!!
    config = SourceConfiguration(settings)

    offset = AtomicLong(resumeFrom(config))
    log.info("resuming from offset: ${offset.get()}")
  }

  override fun stop() {
    log.info("stopping")
    config.close()
  }

  override fun poll(): MutableList<SourceRecord> {
    log.debug("polling")
    val list = mutableListOf<SourceRecord>()

    runBlocking {
      val timeSource = TimeSource.Monotonic
      val start = timeSource.markNow()
      val limit = start + config.queryPollingDuration

      while (limit.hasNotPassedNow()) {
        Flux.from(
                config.driver
                    .rxSession(config.sessionConfig())
                    .readTransaction(
                        { it.run(config.query, mapOf("lastCheck" to offset.get())).records() },
                        config.txConfig()))
            .take(config.batchSize.toLong())
            .asFlow()
            .map { build(it) }
            .toList(list)
        if (list.isNotEmpty()) {
          break
        }

        delay(config.queryPollingInterval)
      }

      if (list.isNotEmpty()) {
        offset.set(list.last().sourceOffset()["value"] as Long)
      }
    }

    log.debug("poll resulted in {} messages", list.size)
    return list
  }

  private fun build(record: Record): SourceRecord {
    val recordAsMap = record.asMap()
    val schema =
        DynamicTypes.toConnectSchema(
            config.payloadMode, recordAsMap, optional = true, forceMapsAsStruct = true)
    val value = DynamicTypes.toConnectValue(schema, recordAsMap)

    return SourceRecord(
        config.partition,
        mapOf(
            "property" to config.queryStreamingProperty,
            "value" to
                (record.get(config.queryStreamingProperty).asObject() as? Long
                    ?: throw InvalidDataException(
                        "Returned record does not contain a valid field ${config.queryStreamingProperty} (record.get returned '${record.get(config.queryStreamingProperty)}', expected a long value)."))),
        config.topic,
        null,
        schema,
        value,
        schema,
        value)
  }

  private fun resumeFrom(config: SourceConfiguration): Long {
    val offset = context.offsetStorageReader().offset(config.partition) ?: emptyMap()
    if (!config.ignoreStoredOffset &&
        offset["value"] is Long &&
        offset["property"] == config.queryStreamingProperty) {
      log.debug("previously stored offset is {}", offset["value"])
      return offset["value"] as Long
    }

    val value =
        when (config.startFrom) {
          StartFrom.EARLIEST -> -1
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
