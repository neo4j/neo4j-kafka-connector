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

import org.apache.kafka.common.config.Config
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.source.SourceConnector
import org.neo4j.connectors.kafka.configuration.helpers.VersionUtil
import org.neo4j.connectors.kafka.source.legacy.Neo4jQueryTask

class Neo4jConnector : SourceConnector() {
  private lateinit var props: Map<String, String>
  private lateinit var config: SourceConfiguration

  override fun version(): String = VersionUtil.version(Neo4jConnector::class.java)

  override fun start(props: MutableMap<String, String>?) {
    val originalProps = props!!.toMap()
    val config = SourceConfiguration(originalProps)
    config.validate()
    this.props = originalProps
    this.config = config
  }

  override fun taskClass(): Class<out Task> =
      when (config.strategy) {
        SourceType.CDC -> Neo4jCdcTask::class.java
        SourceType.QUERY -> Neo4jQueryTask::class.java
      }

  override fun taskConfigs(maxTasks: Int): List<Map<String, String>> = listOf(props)

  override fun stop() {}

  override fun config(): ConfigDef = SourceConfiguration.config()

  override fun validate(connectorConfigs: MutableMap<String, String>?): Config {
    val originals = connectorConfigs ?: emptyMap()
    val result = super.validate(originals)

    SourceConfiguration.validate(result, originals)

    return result
  }
}
