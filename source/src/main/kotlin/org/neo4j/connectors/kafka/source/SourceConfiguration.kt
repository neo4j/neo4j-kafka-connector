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

import com.github.jcustenborder.kafka.connect.utils.config.ConfigKeyBuilder
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Range
import org.neo4j.connectors.kafka.configuration.ConnectorType
import org.neo4j.connectors.kafka.configuration.DeprecatedNeo4jConfiguration
import org.neo4j.connectors.kafka.configuration.Neo4jConfiguration
import org.neo4j.connectors.kafka.configuration.helpers.Recommenders
import org.neo4j.connectors.kafka.configuration.helpers.SIMPLE_DURATION_PATTERN
import org.neo4j.connectors.kafka.configuration.helpers.Validators
import org.neo4j.connectors.kafka.configuration.helpers.parseSimpleString
import org.neo4j.connectors.kafka.configuration.helpers.toSimpleString
import org.neo4j.driver.TransactionConfig
import streams.kafka.connect.source.DeprecatedNeo4jSourceConfiguration
import streams.kafka.connect.source.SourceType
import streams.kafka.connect.source.StreamingFrom
import streams.kafka.connect.utils.PropertiesUtil

class SourceConfiguration(originals: Map<*, *>) :
    Neo4jConfiguration(config(), originals, ConnectorType.SOURCE) {

  val streamFrom
    get(): StreamingFrom = StreamingFrom.valueOf(getString(STREAM_FROM))

  val enforceSchema
    get(): Boolean = getBoolean(ENFORCE_SCHEMA)

  val strategy
    get(): SourceType = SourceType.valueOf(getString(STRATEGY))

  val query
    get(): String = getString(QUERY)

  val queryStreamingProperty
    get(): String = getString(QUERY_STREAMING_PROPERTY)

  val queryPollingInterval
    get(): Duration = Duration.parseSimpleString(getString(QUERY_POLL_INTERVAL))

  val queryBatchSize
    get(): Int = getInt(QUERY_BATCH_SIZE)

  val queryTimeout
    get(): Duration = Duration.parseSimpleString(getString(QUERY_TIMEOUT))

  val topic
    get(): String = getString(TOPIC)

  val partition
    get(): Map<String, Any> {
      return when (strategy) {
        SourceType.QUERY ->
            mapOf(
                "database" to this.database, "type" to "query", "query" to query, "partition" to 1)
      }
    }

  override fun txConfig(): TransactionConfig {
    val original = super.txConfig()
    val new = TransactionConfig.builder()

    if (queryTimeout.isPositive()) {
      new.withTimeout(queryTimeout.toJavaDuration())
    }

    new.withMetadata(buildMap { original.metadata().forEach { (k, v) -> this[k] = v as Any } })

    return new.build()
  }

  companion object {
    const val STREAM_FROM = "neo4j.stream-from"
    const val STRATEGY = "neo4j.source-strategy"
    const val QUERY = "neo4j.query"
    const val QUERY_STREAMING_PROPERTY = "neo4j.query.streaming-property"
    const val QUERY_POLL_INTERVAL = "neo4j.query.poll-interval"
    const val QUERY_BATCH_SIZE = "neo4j.query.batch-size"
    const val QUERY_TIMEOUT = "neo4j.query.timeout"
    const val TOPIC = "topic"
    const val ENFORCE_SCHEMA = "neo4j.enforce-schema"

    private val DEFAULT_POLL_INTERVAL = 10.seconds
    private const val DEFAULT_QUERY_BATCH_SIZE = 1000
    private val DEFAULT_QUERY_TIMEOUT = 0.seconds

    fun migrateSettings(oldSettings: Map<String, Any>): Map<String, String> {
      val migrated = Neo4jConfiguration.migrateSettings(oldSettings, true).toMutableMap()

      oldSettings.forEach {
        when (it.key) {
          DeprecatedNeo4jSourceConfiguration.STREAMING_FROM ->
              migrated[STREAM_FROM] = it.value.toString()
          DeprecatedNeo4jSourceConfiguration.SOURCE_TYPE -> migrated[STRATEGY] = it.value.toString()
          DeprecatedNeo4jSourceConfiguration.SOURCE_TYPE_QUERY ->
              migrated[QUERY] = it.value.toString()
          DeprecatedNeo4jSourceConfiguration.STREAMING_PROPERTY ->
              migrated[QUERY_STREAMING_PROPERTY] = it.value.toString()
          DeprecatedNeo4jSourceConfiguration.STREAMING_POLL_INTERVAL ->
              migrated[QUERY_POLL_INTERVAL] = "${it.value}ms"
          DeprecatedNeo4jSourceConfiguration.ENFORCE_SCHEMA ->
              migrated[ENFORCE_SCHEMA] = it.value.toString()
          DeprecatedNeo4jSourceConfiguration.TOPIC -> migrated[TOPIC] = it.value.toString()
          DeprecatedNeo4jConfiguration.BATCH_SIZE ->
              migrated[QUERY_BATCH_SIZE] = it.value.toString()
          DeprecatedNeo4jConfiguration.BATCH_TIMEOUT_MSECS ->
              migrated[QUERY_TIMEOUT] = "${it.value}ms"
          else ->
              if (!migrated.containsKey(it.key)) {
                migrated[it.key] = it.value.toString()
              }
        }
      }

      return migrated
    }

    fun validate(config: org.apache.kafka.common.config.Config) {
      Neo4jConfiguration.validate(config)
    }

    fun config(): ConfigDef =
        Neo4jConfiguration.config()
            .define(
                ConfigKeyBuilder.of(STREAM_FROM, ConfigDef.Type.STRING)
                    .documentation(PropertiesUtil.getProperty(STREAM_FROM))
                    .importance(ConfigDef.Importance.HIGH)
                    .defaultValue(StreamingFrom.NOW.toString())
                    .validator(Validators.enum(StreamingFrom::class.java))
                    .recommender(Recommenders.enum(StreamingFrom::class.java))
                    .build())
            .define(
                ConfigKeyBuilder.of(STRATEGY, ConfigDef.Type.STRING)
                    .documentation(PropertiesUtil.getProperty(STRATEGY))
                    .importance(ConfigDef.Importance.HIGH)
                    .defaultValue(SourceType.QUERY.name)
                    .validator(Validators.enum(SourceType::class.java))
                    .recommender(Recommenders.enum(SourceType::class.java))
                    .build())
            .define(
                ConfigKeyBuilder.of(QUERY, ConfigDef.Type.STRING)
                    .documentation(PropertiesUtil.getProperty(QUERY))
                    .importance(ConfigDef.Importance.HIGH)
                    .dependents(STRATEGY)
                    .recommender(Recommenders.visibleIf(STRATEGY, SourceType.QUERY.name))
                    .build())
            .define(
                ConfigKeyBuilder.of(QUERY_STREAMING_PROPERTY, ConfigDef.Type.STRING)
                    .documentation(PropertiesUtil.getProperty(QUERY_STREAMING_PROPERTY))
                    .importance(ConfigDef.Importance.HIGH)
                    .dependents(STRATEGY)
                    .recommender(Recommenders.visibleIf(STRATEGY, SourceType.QUERY.name))
                    .defaultValue("")
                    .build())
            .define(
                ConfigKeyBuilder.of(QUERY_POLL_INTERVAL, ConfigDef.Type.STRING)
                    .documentation(PropertiesUtil.getProperty(QUERY_POLL_INTERVAL))
                    .importance(ConfigDef.Importance.HIGH)
                    .dependents(STRATEGY)
                    .recommender(Recommenders.visibleIf(STRATEGY, SourceType.QUERY.name))
                    .validator(Validators.pattern(SIMPLE_DURATION_PATTERN))
                    .defaultValue(DEFAULT_POLL_INTERVAL.toSimpleString())
                    .build())
            .define(
                ConfigKeyBuilder.of(QUERY_BATCH_SIZE, ConfigDef.Type.INT)
                    .documentation(PropertiesUtil.getProperty(QUERY_BATCH_SIZE))
                    .importance(ConfigDef.Importance.HIGH)
                    .dependents(STRATEGY)
                    .recommender(Recommenders.visibleIf(STRATEGY, SourceType.QUERY.name))
                    .validator(Range.atLeast(1))
                    .defaultValue(DEFAULT_QUERY_BATCH_SIZE)
                    .build())
            .define(
                ConfigKeyBuilder.of(QUERY_TIMEOUT, ConfigDef.Type.STRING)
                    .documentation(PropertiesUtil.getProperty(QUERY_TIMEOUT))
                    .importance(ConfigDef.Importance.HIGH)
                    .dependents(STRATEGY)
                    .recommender(Recommenders.visibleIf(STRATEGY, SourceType.QUERY.name))
                    .validator(Validators.pattern(SIMPLE_DURATION_PATTERN))
                    .defaultValue(DEFAULT_QUERY_TIMEOUT.toSimpleString())
                    .build())
            .define(
                ConfigKeyBuilder.of(ENFORCE_SCHEMA, ConfigDef.Type.BOOLEAN)
                    .documentation(PropertiesUtil.getProperty(ENFORCE_SCHEMA))
                    .importance(ConfigDef.Importance.HIGH)
                    .defaultValue(false)
                    .validator(ConfigDef.NonNullValidator())
                    .build())
            .define(
                ConfigKeyBuilder.of(TOPIC, ConfigDef.Type.STRING)
                    .documentation(PropertiesUtil.getProperty(TOPIC))
                    .importance(ConfigDef.Importance.HIGH)
                    .validator(ConfigDef.NonEmptyString())
                    .build())
  }
}
