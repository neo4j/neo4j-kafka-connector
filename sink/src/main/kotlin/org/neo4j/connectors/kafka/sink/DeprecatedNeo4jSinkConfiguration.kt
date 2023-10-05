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

import com.github.jcustenborder.kafka.connect.utils.config.ConfigKeyBuilder
import org.apache.kafka.common.config.ConfigDef
import org.neo4j.connectors.kafka.configuration.ConfigGroup
import org.neo4j.connectors.kafka.configuration.ConnectorType
import org.neo4j.connectors.kafka.configuration.DeprecatedNeo4jConfiguration
import org.neo4j.connectors.kafka.service.sink.strategy.SourceIdIngestionStrategyConfig
import org.neo4j.connectors.kafka.utils.PropertiesUtil

@Deprecated("use org.neo4j.connectors.kafka.sink.SinkConfiguration")
class DeprecatedNeo4jSinkConfiguration(originals: Map<*, *>) :
    DeprecatedNeo4jConfiguration(config(), originals, ConnectorType.SINK) {

  companion object {

    @Deprecated("deprecated in favour of ${SinkConfiguration.BATCH_PARALLELIZE}")
    const val BATCH_PARALLELIZE = "neo4j.batch.parallelize"

    const val TOPIC_CYPHER_PREFIX = "neo4j.topic.cypher."
    const val TOPIC_CDC_SOURCE_ID = "neo4j.topic.cdc.sourceId"
    const val TOPIC_CDC_SOURCE_ID_LABEL_NAME = "neo4j.topic.cdc.sourceId.labelName"
    const val TOPIC_CDC_SOURCE_ID_ID_NAME = "neo4j.topic.cdc.sourceId.idName"
    const val TOPIC_PATTERN_NODE_PREFIX = "neo4j.topic.pattern.node."
    const val TOPIC_PATTERN_RELATIONSHIP_PREFIX = "neo4j.topic.pattern.relationship."
    @Deprecated(
        "deprecated in favour of ${SinkConfiguration.DEFAULT_TOPIC_PATTERN_MERGE_NODE_PROPERTIES}")
    const val TOPIC_PATTERN_MERGE_NODE_PROPERTIES_ENABLED =
        "neo4j.topic.pattern.merge.node.properties.enabled"
    @Deprecated(
        "deprecated in favour of ${SinkConfiguration.DEFAULT_TOPIC_PATTERN_MERGE_RELATIONSHIP_PROPERTIES}")
    const val TOPIC_PATTERN_MERGE_RELATIONSHIP_PROPERTIES_ENABLED =
        "neo4j.topic.pattern.merge.relationship.properties.enabled"
    const val TOPIC_CDC_SCHEMA = "neo4j.topic.cdc.schema"
    const val TOPIC_CUD = "neo4j.topic.cud"

    const val DEFAULT_BATCH_PARALLELIZE = true
    const val DEFAULT_TOPIC_PATTERN_MERGE_NODE_PROPERTIES_ENABLED = false
    const val DEFAULT_TOPIC_PATTERN_MERGE_RELATIONSHIP_PROPERTIES_ENABLED = false

    fun config(): ConfigDef =
        DeprecatedNeo4jConfiguration.config()
            .define(
                ConfigKeyBuilder.of(TOPIC_CDC_SOURCE_ID, ConfigDef.Type.STRING)
                    .documentation(PropertiesUtil.getProperty(TOPIC_CDC_SOURCE_ID))
                    .importance(ConfigDef.Importance.HIGH)
                    .defaultValue("")
                    .group(ConfigGroup.TOPIC_CYPHER_MAPPING)
                    .build())
            .define(
                ConfigKeyBuilder.of(TOPIC_CDC_SOURCE_ID_LABEL_NAME, ConfigDef.Type.STRING)
                    .documentation(PropertiesUtil.getProperty(TOPIC_CDC_SOURCE_ID_LABEL_NAME))
                    .importance(ConfigDef.Importance.HIGH)
                    .defaultValue(SourceIdIngestionStrategyConfig.DEFAULT.labelName)
                    .group(ConfigGroup.TOPIC_CYPHER_MAPPING)
                    .build())
            .define(
                ConfigKeyBuilder.of(TOPIC_CDC_SOURCE_ID_ID_NAME, ConfigDef.Type.STRING)
                    .documentation(PropertiesUtil.getProperty(TOPIC_CDC_SOURCE_ID_ID_NAME))
                    .importance(ConfigDef.Importance.HIGH)
                    .defaultValue(SourceIdIngestionStrategyConfig.DEFAULT.idName)
                    .group(ConfigGroup.TOPIC_CYPHER_MAPPING)
                    .build())
            .define(
                ConfigKeyBuilder.of(TOPIC_CDC_SCHEMA, ConfigDef.Type.STRING)
                    .documentation(PropertiesUtil.getProperty(TOPIC_CDC_SCHEMA))
                    .importance(ConfigDef.Importance.HIGH)
                    .defaultValue("")
                    .group(ConfigGroup.TOPIC_CYPHER_MAPPING)
                    .build())
            .define(
                ConfigKeyBuilder.of(BATCH_PARALLELIZE, ConfigDef.Type.BOOLEAN)
                    .documentation(PropertiesUtil.getProperty(BATCH_PARALLELIZE))
                    .importance(ConfigDef.Importance.MEDIUM)
                    .defaultValue(DEFAULT_BATCH_PARALLELIZE)
                    .group(ConfigGroup.BATCH)
                    .build())
            .define(
                ConfigKeyBuilder.of(TOPIC_CUD, ConfigDef.Type.STRING)
                    .documentation(PropertiesUtil.getProperty(TOPIC_CUD))
                    .importance(ConfigDef.Importance.HIGH)
                    .defaultValue("")
                    .group(ConfigGroup.TOPIC_CYPHER_MAPPING)
                    .build())
            .define(
                ConfigKeyBuilder.of(
                        TOPIC_PATTERN_MERGE_NODE_PROPERTIES_ENABLED, ConfigDef.Type.BOOLEAN)
                    .documentation(
                        PropertiesUtil.getProperty(TOPIC_PATTERN_MERGE_NODE_PROPERTIES_ENABLED))
                    .importance(ConfigDef.Importance.MEDIUM)
                    .defaultValue(DEFAULT_TOPIC_PATTERN_MERGE_NODE_PROPERTIES_ENABLED)
                    .group(ConfigGroup.TOPIC_CYPHER_MAPPING)
                    .build())
            .define(
                ConfigKeyBuilder.of(
                        TOPIC_PATTERN_MERGE_RELATIONSHIP_PROPERTIES_ENABLED, ConfigDef.Type.BOOLEAN)
                    .documentation(
                        PropertiesUtil.getProperty(
                            TOPIC_PATTERN_MERGE_RELATIONSHIP_PROPERTIES_ENABLED))
                    .importance(ConfigDef.Importance.MEDIUM)
                    .defaultValue(DEFAULT_TOPIC_PATTERN_MERGE_RELATIONSHIP_PROPERTIES_ENABLED)
                    .group(ConfigGroup.TOPIC_CYPHER_MAPPING)
                    .build())
  }
}
