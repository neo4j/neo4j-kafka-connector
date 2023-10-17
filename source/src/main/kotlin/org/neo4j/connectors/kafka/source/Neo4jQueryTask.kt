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
package org.neo4j.connectors.kafka.source

import kotlinx.coroutines.DelicateCoroutinesApi
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.source.SourceTask
import org.neo4j.connectors.kafka.configuration.helpers.VersionUtil
import org.neo4j.connectors.kafka.utils.StreamsUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class Neo4jQueryTask : SourceTask() {
  private lateinit var props: Map<String, String>
  private lateinit var config: SourceConfiguration
  private lateinit var neo4JQueryService: Neo4jQueryService

  private val log: Logger = LoggerFactory.getLogger(Neo4jQueryTask::class.java)

  override fun version(): String = VersionUtil.version(this.javaClass)

  override fun start(props: MutableMap<String, String>?) {
    this.props = props!!
    config = SourceConfiguration(this.props)
    neo4JQueryService = Neo4jQueryService(config, context.offsetStorageReader())
  }

  @DelicateCoroutinesApi
  override fun stop() {
    log.info("Stop() - Closing Neo4j Source Service.")
    StreamsUtils.ignoreExceptions(
        { neo4JQueryService.close() }, UninitializedPropertyAccessException::class.java)
  }

  override fun poll(): List<SourceRecord>? = neo4JQueryService.poll()
}
