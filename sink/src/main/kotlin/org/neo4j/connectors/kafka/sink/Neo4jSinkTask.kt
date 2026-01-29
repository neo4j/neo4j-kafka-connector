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
package org.neo4j.connectors.kafka.sink

import kotlin.time.measureTime
import kotlin.time.measureTimedValue
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask
import org.neo4j.caniuse.Neo4jDetector
import org.neo4j.connectors.kafka.configuration.helpers.VersionUtil
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class Neo4jSinkTask : SinkTask() {
  private val log: Logger = LoggerFactory.getLogger(Neo4jSinkTask::class.java)

  private lateinit var settings: Map<String, String>
  private lateinit var config: SinkConfiguration

  override fun version(): String = VersionUtil.version(Neo4jSinkTask::class.java)

  override fun start(props: Map<String, String>?) {
    log.info("starting")

    settings = props!!
    config = SinkConfiguration(settings)
  }

  override fun stop() {
    config.close()
  }

  override fun put(records: Collection<SinkRecord>?) {
    log.info("received {} records", records?.size ?: 0)
    val duration = measureTime {
      records
          ?.map { SinkMessage(it) }
          ?.groupBy { it.topic }
          ?.mapKeys { config.topicHandlers.getValue(it.key) }
          ?.forEach { (handler, messages) -> processMessages(handler, messages) }
    }
    log.info("processed {} records in {} ms", records?.size ?: 0, duration.inWholeMilliseconds)
  }

  private fun processMessages(handler: SinkStrategyHandler, messages: List<SinkMessage>) {
    val handled = mutableSetOf<SinkMessage>()
    try {
      log.debug("handing {} messages to handler {}", messages.size, handler.strategy())
      val (txGroups, handlerDuration) = measureTimedValue { handler.handle(messages) }
      log.debug(
          "handler {} produced {} transaction groups in {} ms",
          handler.strategy(),
          txGroups.count(),
          handlerDuration.inWholeMilliseconds,
      )

      log.debug("writing to neo4j")
      val writeDuration = measureTime {
        txGroups.forEachIndexed { index, group ->
          log.trace("processing queries for group {}", index)
          config.driver.session(config.sessionConfig()).use { session ->
            log.trace("before write transaction for group {}", index)
            session.writeTransaction(
                { tx -> group.forEach { tx.run(it.query).consume() } },
                config.txConfig(),
            )
            log.trace("after write transaction for group {}", index)
          }

          handled.addAll(group.flatMap { it.messages })
        }
      }
      log.debug(
          "wrote {} transaction groups to neo4j in {} ms",
          txGroups.count(),
          writeDuration.inWholeMilliseconds,
      )
    } catch (e: Throwable) {
      log.warn("failed to process messages, trying to identify offending message", e)

      val unhandled = messages.minus(handled)

      if (unhandled.size > 1) {
        unhandled.forEach { m -> processMessages(handler, listOf(m)) }
      } else {
        unhandled.forEach { m ->
          val reporter = context.errantRecordReporter()
          if (reporter != null) {
            reporter.report(m.record, e).get()
          } else {
            throw e
          }
        }
      }
    }
  }
}
