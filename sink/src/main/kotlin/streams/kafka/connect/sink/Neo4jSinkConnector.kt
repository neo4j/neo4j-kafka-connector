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
package streams.kafka.connect.sink

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.sink.SinkConnector
import org.neo4j.connectors.kafka.sink.DeprecatedNeo4jSinkConfiguration
import org.neo4j.connectors.kafka.sink.Neo4jSinkTask
import org.neo4j.connectors.kafka.sink.SinkConfiguration
import org.neo4j.connectors.kafka.utils.PropertiesUtil

@Deprecated("Use org.neo4j.connectors.kafka.sink.Neo4jConnector instead")
class Neo4jSinkConnector : SinkConnector() {
  private lateinit var settings: Map<String, String>
  private lateinit var config: SinkConfiguration

  override fun taskConfigs(maxTasks: Int): List<Map<String, String>> =
      List(maxTasks) { config.originalsStrings() }

  override fun start(props: MutableMap<String, String>?) {
    settings = props!!
    config = SinkConfiguration(SinkConfiguration.migrateSettings(settings))
  }

  override fun stop() {}

  override fun version(): String {
    return PropertiesUtil.getVersion()
  }

  override fun taskClass(): Class<out Task> {
    return Neo4jSinkTask::class.java
  }

  override fun config(): ConfigDef {
    return DeprecatedNeo4jSinkConfiguration.config()
  }
}
