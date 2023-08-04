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

import com.github.jcustenborder.kafka.connect.utils.config.*
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.sink.SinkConnector
import streams.kafka.connect.utils.PropertiesUtil

@Title("Neo4j Sink Connector")
@Description(
    "The Neo4j Sink connector reads data from Kafka and and writes the data to Neo4j using a Cypher Template")
@DocumentationTip(
    "If you need to control the size of transaction that is submitted to Neo4j you try adjusting the ``consumer.max.poll.records`` setting in the worker.properties for Kafka Connect.")
@DocumentationNote(
    "For each topic you can provide a Cypher Template by using the following syntax ``neo4j.topic.cypher.<topic_name>=<cypher_query>``")
class Neo4jSinkConnector : SinkConnector() {
  private lateinit var settings: Map<String, String>
  private lateinit var config: Neo4jSinkConnectorConfig

  override fun taskConfigs(maxTasks: Int): MutableList<MutableMap<String, String>> {
    return TaskConfigs.multiple(settings, maxTasks)
  }

  override fun start(props: MutableMap<String, String>?) {
    settings = props!!
    config = Neo4jSinkConnectorConfig(settings)
  }

  override fun stop() {}

  override fun version(): String {
    return PropertiesUtil.getVersion()
  }

  override fun taskClass(): Class<out Task> {
    return Neo4jSinkTask::class.java
  }

  override fun config(): ConfigDef {
    return Neo4jSinkConnectorConfig.config()
  }
}
