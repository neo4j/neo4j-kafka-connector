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
package org.neo4j.connectors.kafka.sink

import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask
import org.neo4j.connectors.kafka.configuration.helpers.VersionUtil
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class Neo4jSinkTask : SinkTask() {
  private val log: Logger = LoggerFactory.getLogger(Neo4jSinkTask::class.java)

  private lateinit var settings: Map<String, String>
  private lateinit var config: SinkConfiguration
  private lateinit var topicHandlers: Map<String, SinkStrategyHandler>

  override fun version(): String = VersionUtil.version(Neo4jSinkTask::class.java)

  override fun start(props: Map<String, String>?) {
    log.info("starting")

    settings = props!!
    config = SinkConfiguration(settings)
    topicHandlers = config.topicHandlers
  }

  override fun stop() {
    config.close()
  }

  override fun put(records: Collection<SinkRecord>?) {
    records
        ?.map { SinkMessage(it) }
        ?.groupBy { it.topic }
        ?.mapKeys { topicHandlers.getValue(it.key) }
        ?.forEach { (handler, messages) ->
          val txGroups = handler.handle(messages)

          txGroups.forEach { group ->
            config.session().use { session ->
              session.writeTransaction(
                  { tx -> group.forEach { tx.run(it.query).consume() } }, config.txConfig())
            }
          }
        }
  }
}
