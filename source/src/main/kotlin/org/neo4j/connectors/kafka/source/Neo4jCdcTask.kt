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

import java.util.concurrent.atomic.AtomicReference
import kotlin.time.TimeSource
import kotlin.time.toJavaDuration
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.runBlocking
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.source.SourceTask
import org.neo4j.cdc.client.CDCClient
import org.neo4j.cdc.client.CDCService
import org.neo4j.cdc.client.model.ChangeEvent
import org.neo4j.cdc.client.model.ChangeIdentifier
import org.neo4j.connectors.kafka.configuration.helpers.VersionUtil
import org.neo4j.connectors.kafka.data.ChangeEventConverter
import org.neo4j.connectors.kafka.data.Headers
import org.neo4j.driver.SessionConfig
import org.neo4j.driver.TransactionConfig
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class Neo4jCdcTask : SourceTask() {
  private val log: Logger = LoggerFactory.getLogger(Neo4jCdcTask::class.java)

  private lateinit var settings: Map<String, String>
  private lateinit var config: SourceConfiguration
  private lateinit var sessionConfig: SessionConfig
  private lateinit var transactionConfig: TransactionConfig
  private lateinit var cdc: CDCService
  private lateinit var offset: AtomicReference<String>
  private lateinit var changeEventConverter: ChangeEventConverter

  override fun version(): String = VersionUtil.version(this.javaClass as Class<*>)

  override fun start(props: Map<String, String>?) {
    log.info("starting")

    settings = props!!
    config = SourceConfiguration(settings)
    val configBuilder = SessionConfig.builder()
    if (config.database.isNotBlank()) {
      configBuilder.withDatabase(config.database)
    }
    sessionConfig = configBuilder.build()
    transactionConfig = config.txConfig()

    cdc =
        CDCClient(
            config.driver,
            { sessionConfig },
            { transactionConfig },
            config.cdcPollingInterval.toJavaDuration(),
            *config.cdcSelectors.toTypedArray())
    log.debug("constructed cdc client")

    offset = AtomicReference(resumeFrom(config, cdc))
    log.info("resuming from offset: ${offset.get()}")

    changeEventConverter = ChangeEventConverter(config.payloadMode)
  }

  override fun stop() {
    log.info("stopping")
    config.close()
  }

  @OptIn(ExperimentalCoroutinesApi::class)
  override fun poll(): MutableList<SourceRecord> {
    log.info("polling from offset: ${offset.get()}")
    val list = mutableListOf<SourceRecord>()

    runBlocking {
      val timeSource = TimeSource.Monotonic
      val start = timeSource.markNow()
      val limit = start + config.cdcPollingDuration

      while (limit.hasNotPassedNow()) {
        cdc.query(ChangeIdentifier(offset.get()))
            .take(config.batchSize.toLong(), true)
            .asFlow()
            .flatMapConcat { build(it) }
            .toList(list)
        if (list.isNotEmpty()) {
          break
        }

        delay(config.cdcPollingInterval)
      }

      if (list.isNotEmpty()) {
        offset.set(list.last().sourceOffset()["value"] as String)
      }
    }

    log.info("poll resulted in {} messages", list.size)
    return list
  }

  private fun build(changeEvent: ChangeEvent): Flow<SourceRecord> {
    val result = mutableListOf<SourceRecord>()

    config.cdcSelectorsToTopics.forEach {
      if (it.key.matches(changeEvent)) {
        result.addAll(
            it.value.map { topic ->
              val transformed = it.key.applyProperties(changeEvent)
              val transformedValue = changeEventConverter.toConnectValue(transformed)
              val keyStrategy =
                  config.cdcTopicsToKeyStrategy.getOrDefault(topic, Neo4jCdcKeyStrategy.WHOLE_VALUE)
              val valueStrategy =
                  config.cdcTopicsToValueStrategy.getOrDefault(
                      topic, Neo4jCdcValueStrategy.CHANGE_EVENT)
              SourceRecord(
                  config.partition,
                  mapOf("value" to changeEvent.id.id),
                  topic,
                  null,
                  keyStrategy.schema(transformedValue),
                  keyStrategy.value(transformedValue),
                  valueStrategy.schema(transformedValue),
                  valueStrategy.value(transformedValue),
                  changeEvent.metadata.txCommitTime.toInstant().toEpochMilli(),
                  Headers.from(changeEvent))
            })
      }
    }

    return result.asFlow()
  }

  private fun resumeFrom(config: SourceConfiguration, cdc: CDCService): String {
    val offset = context.offsetStorageReader().offset(config.partition) ?: emptyMap()
    if (!config.ignoreStoredOffset && offset["value"] is String) {
      log.debug("previously stored offset is {}", offset["value"])
      return offset["value"] as String
    }

    val value =
        when (config.startFrom) {
          StartFrom.EARLIEST -> cdc.earliest().block()?.id!!
          StartFrom.NOW -> cdc.current().block()?.id!!
          StartFrom.USER_PROVIDED -> config.startFromCustom
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
