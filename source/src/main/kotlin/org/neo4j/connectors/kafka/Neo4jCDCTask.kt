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
package org.neo4j.connectors.kafka

import com.github.jcustenborder.kafka.connect.utils.VersionUtil
import java.time.Duration
import java.util.concurrent.atomic.AtomicReference
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.runBlocking
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.source.SourceTask
import org.neo4j.cdc.client.CDCClient
import org.neo4j.cdc.client.CDCService
import org.neo4j.cdc.client.model.ChangeEvent
import org.neo4j.cdc.client.model.ChangeIdentifier
import org.neo4j.cdc.client.model.NodeEvent
import org.neo4j.cdc.client.model.RelationshipEvent
import org.neo4j.cdc.client.pattern.Pattern
import org.neo4j.driver.Driver
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import streams.kafka.connect.source.Neo4jSourceConnectorConfig
import streams.kafka.connect.source.StreamingFrom

class Neo4jCDCTask : SourceTask() {
  private val log: Logger = LoggerFactory.getLogger(Neo4jCDCTask::class.java)

  private lateinit var settings: Map<String, String>
  private lateinit var config: Neo4jSourceConnectorConfig
  private lateinit var cdc: CDCService
  private lateinit var offset: AtomicReference<String>
  private lateinit var driver: Driver

  override fun version(): String = VersionUtil.version(this.javaClass as Class<*>)

  override fun start(props: MutableMap<String, String>?) {
    settings = props!!
    config = Neo4jSourceConnectorConfig(settings)
    driver = config.createDriver()
    cdc =
        CDCClient(
            driver,
            Duration.ofMillis(500),
            *config.cdcPatterns
                .flatMap { Pattern.parse(it) }
                .flatMap { it.toSelector() }
                .toTypedArray())
    log.info("constructed cdc client")

    offset = AtomicReference(resumeFrom(config, cdc))
    log.info("offset received: ${offset.get()}")
  }

  // TODO: form correct message structure
  private fun build(changeEvent: ChangeEvent): SourceRecord {
    return when (val event = changeEvent.event) {
      is NodeEvent ->
          SourceRecord(
              config.sourcePartition(),
              mapOf("value" to changeEvent.id.id),
              config.topic,
              Schema.STRING_SCHEMA,
              event.elementId,
              Schema.STRING_SCHEMA,
              changeEvent.toString())
      is RelationshipEvent ->
          SourceRecord(
              config.sourcePartition(),
              mapOf("value" to changeEvent.id.id),
              config.topic,
              Schema.STRING_SCHEMA,
              event.elementId,
              Schema.STRING_SCHEMA,
              changeEvent.toString())
      else -> throw IllegalArgumentException("unknown event type: ${event.eventType}")
    }
  }

  override fun stop() {
    driver.close()
  }

  override fun poll(): MutableList<SourceRecord> {
    log.info("polling")
    val list = mutableListOf<SourceRecord>()

    runBlocking {
      cdc.query(ChangeIdentifier(offset.get()))
          .take(config.batchSize.toLong(), true)
          .takeUntilOther(Mono.delay(Duration.ofMillis(config.pollInterval.toLong())))
          .asFlow()
          .map { build(it) }
          .toList(list)

      if (list.isNotEmpty()) {
        offset.set(list.last().sourceOffset()["value"] as String)
      }
    }

    log.info("poll resulted in ${list.size} messages")
    return list
  }

  private fun resumeFrom(config: Neo4jSourceConnectorConfig, cdc: CDCService): String {
    val offset = context.offsetStorageReader().offset(config.sourcePartition()) ?: emptyMap()
    if (offset["value"] != null && offset["value"] is String) {
      return offset["value"] as String
    }

    return when (config.streamingFrom) {
      StreamingFrom.ALL -> cdc.earliest().block()?.id!!
      StreamingFrom.NOW,
      StreamingFrom.LAST_COMMITTED -> cdc.current().block()?.id!!
    }
  }
}

fun <E> ReceiveChannel<E>.receiveAvailable(list: MutableList<E>, maxItems: Int) {
  var next = tryReceive()
  var count = 0
  while (next.isSuccess && count < maxItems) {
    count++
    list.add(next.getOrThrow())
    next = tryReceive()
  }
}
