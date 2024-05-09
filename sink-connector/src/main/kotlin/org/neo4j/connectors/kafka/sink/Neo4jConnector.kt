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

import org.apache.kafka.common.config.Config
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.sink.SinkConnector
import org.neo4j.connectors.kafka.utils.PropertiesUtil

class Neo4jConnector : SinkConnector() {
  private lateinit var props: Map<String, String>

  override fun version(): String = PropertiesUtil.getVersion()

  override fun start(props: MutableMap<String, String>?) {
    this.props = props!!.toMap()
  }

  override fun taskClass(): Class<out Task> = Neo4jSinkTask::class.java

  override fun taskConfigs(maxTasks: Int): List<Map<String, String>> = List(maxTasks) { props }

  override fun stop() {}

  override fun config(): ConfigDef = SinkConfiguration.config()

  override fun validate(connectorConfigs: MutableMap<String, String>?): Config {
    val originals = connectorConfigs ?: emptyMap()
    val result = super.validate(originals)

    SinkConfiguration.validate(result)

    return result
  }
}
