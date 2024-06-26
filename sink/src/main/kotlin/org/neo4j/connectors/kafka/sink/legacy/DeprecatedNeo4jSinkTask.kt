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
package org.neo4j.connectors.kafka.sink.legacy

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.ObsoleteCoroutinesApi
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask
import org.neo4j.connectors.kafka.configuration.helpers.VersionUtil
import org.neo4j.connectors.kafka.service.errors.ErrorData
import org.neo4j.connectors.kafka.service.errors.ErrorService
import org.neo4j.connectors.kafka.service.errors.KafkaErrorService
import org.neo4j.connectors.kafka.sink.SinkConfiguration
import org.neo4j.connectors.kafka.sink.legacy.strategy.asProperties
import org.neo4j.connectors.kafka.utils.StreamsUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@Suppress("DEPRECATION")
@Deprecated("only used by the legacy connector")
class DeprecatedNeo4jSinkTask : SinkTask() {
  private val log: Logger = LoggerFactory.getLogger(DeprecatedNeo4jSinkTask::class.java)
  private lateinit var config: SinkConfiguration
  private lateinit var neo4jSinkService: DeprecatedNeo4jSinkService
  private lateinit var errorService: ErrorService

  override fun version(): String {
    return VersionUtil.version(this.javaClass as Class<*>)
  }

  override fun start(map: Map<String, String>) {
    this.config = SinkConfiguration(map)
    this.neo4jSinkService = DeprecatedNeo4jSinkService(this.config)
    this.errorService =
        KafkaErrorService(
            this.config.kafkaBrokerProperties.asProperties(),
            ErrorService.ErrorConfig.from(map.asProperties()),
            log::error)
  }

  @OptIn(ExperimentalCoroutinesApi::class, ObsoleteCoroutinesApi::class)
  override fun put(collection: Collection<SinkRecord>) {
    if (collection.isEmpty()) {
      return
    }
    try {
      val data = EventBuilder().withBatchSize(config.batchSize).withSinkRecords(collection).build()

      neo4jSinkService.writeData(data)
    } catch (e: Exception) {
      errorService.report(
          collection.map {
            ErrorData(
                it.topic(),
                it.timestamp(),
                it.key(),
                it.value(),
                it.kafkaPartition(),
                it.kafkaOffset(),
                this::class.java,
                this.config.database,
                e)
          })
    }
  }

  override fun stop() {
    log.info("Stop() - Neo4j Sink Service")
    StreamsUtils.ignoreExceptions(
        { neo4jSinkService.close() }, UninitializedPropertyAccessException::class.java)
  }
}
