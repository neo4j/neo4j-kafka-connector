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

import org.apache.kafka.common.config.ConfigDef
import org.neo4j.connectors.kafka.configuration.ConnectorType
import org.neo4j.connectors.kafka.configuration.DeprecatedNeo4jConfiguration
import org.neo4j.connectors.kafka.configuration.helpers.ConfigKeyBuilder
import org.neo4j.connectors.kafka.configuration.helpers.Recommenders
import org.neo4j.connectors.kafka.configuration.helpers.Validators

@Deprecated("use org.neo4j.connectors.kafka.source.SourceConfiguration")
class DeprecatedNeo4jSourceConfiguration(originals: Map<*, *>) :
    DeprecatedNeo4jConfiguration(config(), originals, ConnectorType.SOURCE) {

  enum class StreamingFrom {
    ALL,
    NOW,
    LAST_COMMITTED
  }

  companion object {
    const val TOPIC = "topic"
    @Deprecated("deprecated in favour of ${SourceConfiguration.START_FROM}")
    const val STREAMING_FROM = "neo4j.streaming.from"
    @Deprecated("deprecated in favour of ${SourceConfiguration.ENFORCE_SCHEMA}")
    const val ENFORCE_SCHEMA = "neo4j.enforce.schema"
    @Deprecated("deprecated in favour of ${SourceConfiguration.QUERY_STREAMING_PROPERTY}")
    const val STREAMING_PROPERTY = "neo4j.streaming.property"
    @Deprecated("deprecated in favour of ${SourceConfiguration.QUERY_POLL_INTERVAL}")
    const val STREAMING_POLL_INTERVAL = "neo4j.streaming.poll.interval.msecs"
    @Deprecated("deprecated in favour of ${SourceConfiguration.STRATEGY}")
    const val SOURCE_TYPE = "neo4j.source.type"
    @Deprecated("deprecated in favour of ${SourceConfiguration.QUERY}")
    const val SOURCE_TYPE_QUERY = "neo4j.source.query"

    fun config(): ConfigDef =
        DeprecatedNeo4jConfiguration.config()
            .define(
                ConfigKeyBuilder.of(ENFORCE_SCHEMA, ConfigDef.Type.BOOLEAN) {
                  importance = ConfigDef.Importance.HIGH
                  defaultValue = false
                  validator = ConfigDef.NonNullValidator()
                })
            .define(
                ConfigKeyBuilder.of(STREAMING_POLL_INTERVAL, ConfigDef.Type.INT) {
                  importance = ConfigDef.Importance.HIGH
                  defaultValue = 10000
                  validator = ConfigDef.Range.atLeast(1)
                })
            .define(
                ConfigKeyBuilder.of(STREAMING_PROPERTY, ConfigDef.Type.STRING) {
                  importance = ConfigDef.Importance.HIGH
                  defaultValue = ""
                })
            .define(
                ConfigKeyBuilder.of(TOPIC, ConfigDef.Type.STRING) {
                  importance = ConfigDef.Importance.HIGH
                  validator = ConfigDef.NonEmptyString()
                })
            .define(
                ConfigKeyBuilder.of(STREAMING_FROM, ConfigDef.Type.STRING) {
                  importance = ConfigDef.Importance.HIGH
                  defaultValue = StreamingFrom.NOW.toString()
                  validator = Validators.enum(StreamingFrom::class.java)
                  recommender = Recommenders.enum(StreamingFrom::class.java)
                })
            .define(
                ConfigKeyBuilder.of(SOURCE_TYPE, ConfigDef.Type.STRING) {
                  importance = ConfigDef.Importance.HIGH
                  defaultValue = SourceType.QUERY.toString()
                  validator = Validators.enum(SourceType::class.java, SourceType.CDC)
                  recommender = Recommenders.enum(SourceType::class.java, SourceType.CDC)
                })
            .define(
                ConfigKeyBuilder.of(SOURCE_TYPE_QUERY, ConfigDef.Type.STRING) {
                  importance = ConfigDef.Importance.HIGH
                  defaultValue = ""
                })
  }
}
