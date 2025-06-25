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

import java.util.function.Predicate
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration
import org.apache.kafka.common.config.Config
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Range
import org.apache.kafka.common.config.ConfigException
import org.neo4j.cdc.client.model.EntityOperation
import org.neo4j.cdc.client.pattern.Pattern
import org.neo4j.cdc.client.pattern.PatternException
import org.neo4j.cdc.client.selector.EntitySelector
import org.neo4j.cdc.client.selector.NodeSelector
import org.neo4j.cdc.client.selector.RelationshipSelector
import org.neo4j.cdc.client.selector.Selector
import org.neo4j.connectors.kafka.configuration.ConnectorType
import org.neo4j.connectors.kafka.configuration.Groups
import org.neo4j.connectors.kafka.configuration.Neo4jConfiguration
import org.neo4j.connectors.kafka.configuration.PayloadMode
import org.neo4j.connectors.kafka.configuration.helpers.ConfigKeyBuilder
import org.neo4j.connectors.kafka.configuration.helpers.Recommenders
import org.neo4j.connectors.kafka.configuration.helpers.SIMPLE_DURATION_PATTERN
import org.neo4j.connectors.kafka.configuration.helpers.Validators
import org.neo4j.connectors.kafka.configuration.helpers.Validators.validateNonEmptyIfVisible
import org.neo4j.connectors.kafka.configuration.helpers.parseSimpleString
import org.neo4j.connectors.kafka.configuration.helpers.toSimpleString
import org.neo4j.driver.TransactionConfig

enum class SourceType(val description: String) {
  QUERY("query"),
  CDC("cdc")
}

enum class StartFrom {
  EARLIEST,
  NOW,
  USER_PROVIDED
}

class SourceConfiguration(originals: Map<*, *>) :
    Neo4jConfiguration(config(), originals, ConnectorType.SOURCE) {

  val forceMapsAsStruct: Boolean
    get(): Boolean = getBoolean(QUERY_FORCE_MAPS_AS_STRUCT)

  val startFrom
    get(): StartFrom = StartFrom.valueOf(getString(START_FROM))

  val startFromCustom
    get(): String = getString(START_FROM_VALUE)

  val ignoreStoredOffset
    get(): Boolean = getString(IGNORE_STORED_OFFSET).toBoolean()

  val strategy
    get(): SourceType = SourceType.valueOf(getString(STRATEGY))

  val payloadMode
    get(): PayloadMode = PayloadMode.valueOf(getString(PAYLOAD_MODE))

  val query
    get(): String = getString(QUERY)

  val queryStreamingProperty
    get(): String = getString(QUERY_STREAMING_PROPERTY)

  val queryPollingInterval
    get(): Duration = Duration.parseSimpleString(getString(QUERY_POLL_INTERVAL))

  val queryPollingDuration
    get(): Duration = Duration.parseSimpleString(getString(QUERY_POLL_DURATION))

  val batchSize
    get(): Int = getInt(BATCH_SIZE)

  val queryTimeout
    get(): Duration = Duration.parseSimpleString(getString(QUERY_TIMEOUT))

  val topic
    get(): String = getString(QUERY_TOPIC)

  val partition
    get(): Map<String, Any> {
      return when (strategy) {
        SourceType.QUERY ->
            mapOf(
                "database" to this.database, "type" to "query", "query" to query, "partition" to 1)
        SourceType.CDC -> mapOf("database" to this.database, "type" to "cdc", "partition" to 1)
      }
    }

  val cdcPollingInterval
    get(): Duration = Duration.parseSimpleString(getString(CDC_POLL_INTERVAL))

  val cdcPollingDuration
    get(): Duration = Duration.parseSimpleString(getString(CDC_POLL_DURATION))

  val cdcSelectorsToTopics: Map<Selector, List<String>> by lazy {
    when (strategy) {
      SourceType.CDC -> {
        val configMap = mutableMapOf<String, MutableList<Pattern>>()
        val nonPositionalConfigMode = mutableMapOf<String, Boolean>()
        val patternTxMetadataMap = mutableMapOf<Pattern, MutableMap<String, Any>>()

        originals()
            .entries
            .filter { CDC_PATTERNS_REGEX.matches(it.key) }
            .map { CdcPatternConfigItem(it, CDC_PATTERNS_REGEX) }
            .flatMap { Pattern.parse(it.value as String?).map { key -> key to it.topic } }
            .forEach {
              if (!configMap.containsKey(it.second)) {
                configMap[it.second] = mutableListOf()
              }

              nonPositionalConfigMode[it.second] = true

              val list = configMap.getValue(it.second)
              list.add(it.first)
            }

        originals()
            .entries
            .filter { CDC_PATTERN_ARRAY_REGEX.matches(it.key) }
            .map { CdcPatternConfigItem(it, CDC_PATTERN_ARRAY_REGEX) }
            .sorted()
            .forEach { mapPositionalPattern(it, nonPositionalConfigMode, configMap) }

        originals()
            .entries
            .filter { CDC_PATTERN_ARRAY_OPERATION_REGEX.matches(it.key) }
            .map { CdcPatternConfigItem(it, CDC_PATTERN_ARRAY_OPERATION_REGEX) }
            .forEach { mapOperation(it, nonPositionalConfigMode, configMap) }

        originals()
            .entries
            .filter { CDC_PATTERN_ARRAY_CHANGES_TO_REGEX.matches(it.key) }
            .map { CdcPatternConfigItem(it, CDC_PATTERN_ARRAY_CHANGES_TO_REGEX) }
            .forEach { mapChangesTo(it, nonPositionalConfigMode, configMap) }

        originals()
            .entries
            .filter { CDC_PATTERN_ARRAY_METADATA_REGEX.matches(it.key) }
            .map { CdcPatternConfigItem(it, CDC_PATTERN_ARRAY_METADATA_REGEX) }
            .forEach { mapMetadata(it, nonPositionalConfigMode, configMap, patternTxMetadataMap) }

        pivotMapCdcSelectorMap(configMap)
      }
      else -> emptyMap()
    }
  }

  val cdcTopicsToKeyStrategy: Map<String, Neo4jCdcKeyStrategy> by lazy {
    when (strategy) {
      SourceType.CDC -> {
        originals()
            .entries
            .filter { CDC_KEY_STRATEGY_REGEX.matches(it.key) }
            .map { CdcPatternConfigItem(it, CDC_KEY_STRATEGY_REGEX) }
            .associate { mapKeyStrategy(it) }
      }
      else -> emptyMap()
    }
  }

  val cdcTopicsToValueStrategy: Map<String, Neo4jCdcValueStrategy> by lazy {
    when (strategy) {
      SourceType.CDC -> {
        originals()
            .entries
            .filter { CDC_VALUE_STRATEGY_REGEX.matches(it.key) }
            .map { CdcPatternConfigItem(it, CDC_VALUE_STRATEGY_REGEX) }
            .associate { mapValueStrategy(it) }
      }
      else -> emptyMap()
    }
  }

  private fun mapPositionalPattern(
      configEntry: CdcPatternConfigItem,
      nonPositionalConfigMode: MutableMap<String, Boolean>,
      configMap: MutableMap<String, MutableList<Pattern>>
  ) {
    val topicName = configEntry.topic
    if (nonPositionalConfigMode.getOrDefault(topicName, false)) {
      throw ConfigException(
          "It's not allowed to mix positional and non-positional configuration for the same topic.",
      )
    }
    val patterns = Pattern.parse(configEntry.value as String?)
    if (patterns.size > 1) {
      throw ConfigException(
          "Too many patterns. Only one pattern allowed for positional pattern configuration.",
      )
    }

    val index = configEntry.index!!
    val pattern = patterns[0]
    if (!configMap.containsKey(topicName)) {
      configMap[topicName] = mutableListOf()
    }

    val list = configMap.getValue(topicName)
    if (index > list.size) {
      throw ConfigException(
          "Index $index out of bounds. Please ensure that you started the definition with a 0-based index.",
      )
    }
    list.add(pattern)
  }

  private fun mapOperation(
      configEntry: CdcPatternConfigItem,
      nonPositionalConfigMode: MutableMap<String, Boolean>,
      configMap: MutableMap<String, MutableList<Pattern>>
  ) {
    val (index, patterns) = retrieveIndexAndPattern(configEntry, nonPositionalConfigMode, configMap)
    val operation =
        when (val value = (configEntry.value as String).lowercase()) {
          "create" -> EntityOperation.CREATE
          "update" -> EntityOperation.UPDATE
          "delete" -> EntityOperation.DELETE
          else -> {
            throw ConfigException(
                "Cannot parse $value as an operation. Allowed operations are create, delete or update.")
          }
        }
    val pattern = patterns[index]
    pattern.withOperation(operation)
  }

  private fun mapChangesTo(
      configEntry: CdcPatternConfigItem,
      nonPositionalConfigMode: MutableMap<String, Boolean>,
      configMap: MutableMap<String, MutableList<Pattern>>
  ) {
    val (index, patterns) = retrieveIndexAndPattern(configEntry, nonPositionalConfigMode, configMap)
    val value = configEntry.value as String
    val changesTo = value.splitToSequence(",").map { term -> term.trim() }.toSet()
    val pattern = patterns[index]
    pattern.withChangesTo(changesTo)
  }

  private fun mapMetadata(
      configEntry: CdcPatternConfigItem,
      nonPositionalConfigMode: MutableMap<String, Boolean>,
      configMap: MutableMap<String, MutableList<Pattern>>,
      patternTxMetadataMap: MutableMap<Pattern, MutableMap<String, Any>>
  ) {
    val (index, patterns) = retrieveIndexAndPattern(configEntry, nonPositionalConfigMode, configMap)
    val metadataKey = configEntry.metadata!!
    val value = configEntry.value
    val pattern = patterns[index]
    if (metadataKey.startsWith("$METADATA_KEY_TX_METADATA.")) {
      val txMetadataKey = metadataKey.removePrefix("$METADATA_KEY_TX_METADATA.")

      val txMetadata =
          patternTxMetadataMap.getOrPut(pattern) { mutableMapOf(txMetadataKey to value) }
      txMetadata[txMetadataKey] = value

      pattern.withTxMetadata(txMetadata)
      patternTxMetadataMap[pattern] = txMetadata
    } else if (metadataKey == METADATA_KEY_EXECUTING_USER) {
      pattern.withExecutingUser(value as String)
    } else if (metadataKey == METADATA_KEY_AUTHENTICATED_USER) {
      pattern.withAuthenticatedUser(value as String)
    } else {
      throw ConfigException(
          "Unexpected metadata key: '$metadataKey' found in configuration property '${configEntry.key}'. " +
              "Valid keys are '$METADATA_KEY_AUTHENTICATED_USER', '$METADATA_KEY_EXECUTING_USER', " +
              "or keys starting with '$METADATA_KEY_TX_METADATA.*'.")
    }
  }

  private fun mapKeyStrategy(
      configEntry: CdcPatternConfigItem,
  ): Pair<String, Neo4jCdcKeyStrategy> {
    val topicName = configEntry.topic
    val value = configEntry.value
    return topicName to Neo4jCdcKeyStrategy.valueOf(value as String)
  }

  private fun mapValueStrategy(
      configEntry: CdcPatternConfigItem,
  ): Pair<String, Neo4jCdcValueStrategy> {
    val topicName = configEntry.topic
    val value = configEntry.value
    return topicName to Neo4jCdcValueStrategy.valueOf(value as String)
  }

  private fun retrieveIndexAndPattern(
      configEntry: CdcPatternConfigItem,
      nonPositionalConfigMode: MutableMap<String, Boolean>,
      configMap: MutableMap<String, MutableList<Pattern>>
  ): Pair<Int, MutableList<Pattern>> {
    val topicName = configEntry.topic
    if (nonPositionalConfigMode.getOrDefault(topicName, false)) {
      throw ConfigException(
          "It's not allowed to mix positional and non-positional configuration for the same topic.",
      )
    }
    val index = configEntry.index!!
    if (!configMap.containsKey(topicName)) {
      throw ConfigException(
          "Cannot assign config value because pattern is not defined for index $index.",
      )
    }
    val patterns = configMap.getValue(topicName)
    if (index > patterns.size - 1) {
      throw ConfigException(
          "Index $index out of bounds. Please ensure that you started the definition with a 0-based index.",
      )
    }
    return Pair(index, patterns)
  }

  private fun pivotMapCdcSelectorMap(
      patternMap: Map<String, List<Pattern>>
  ): Map<Selector, List<String>> {
    val selectorBasedMap = mutableMapOf<Selector, MutableList<String>>()
    patternMap.entries.forEach {
      for (pattern in it.value) {
        for (selector in pattern.toSelector()) {
          if (!selectorBasedMap.containsKey(selector)) {
            selectorBasedMap[selector] = mutableListOf()
          }

          val topics = selectorBasedMap.getValue(selector)
          topics.add(it.key)
          topics.sort()
        }
      }
    }

    return selectorBasedMap
  }

  val cdcSelectors: Set<Selector> by lazy {
    cdcSelectorsToTopics.keys
        .map {
          when (it) {
            is NodeSelector ->
                NodeSelector.builder()
                    .withOperation(it.operation)
                    .withChangesTo(it.changesTo)
                    .withLabels(it.labels)
                    .withKey(it.key)
                    .withTxMetadata(it.txMetadata)
                    .withExecutingUser(it.executingUser)
                    .withAuthenticatedUser(it.authenticatedUser)
                    .build()
            is RelationshipSelector ->
                RelationshipSelector.builder()
                    .withOperation(it.operation)
                    .withChangesTo(it.changesTo)
                    .withType(it.type)
                    .withStart(it.start)
                    .withEnd(it.end)
                    .withKey(it.key)
                    .withTxMetadata(it.txMetadata)
                    .withExecutingUser(it.executingUser)
                    .withAuthenticatedUser(it.authenticatedUser)
                    .build()
            is EntitySelector ->
                EntitySelector.builder()
                    .withOperation(it.operation)
                    .withChangesTo(it.changesTo)
                    .withTxMetadata(it.txMetadata)
                    .withExecutingUser(it.executingUser)
                    .withAuthenticatedUser(it.authenticatedUser)
                    .build()
            else -> throw IllegalStateException("unexpected pattern type ${it.javaClass.name}")
          }
        }
        .toSet()
  }

  override fun txConfig(): TransactionConfig {
    val original = super.txConfig()
    val new = TransactionConfig.builder()

    if (queryTimeout.isPositive()) {
      new.withTimeout(queryTimeout.toJavaDuration())
    }

    new.withMetadata(buildMap { original.metadata().forEach { (k, v) -> this[k] = v.asObject() } })

    return new.build()
  }

  override fun userAgentComment(): String = strategy.description

  fun validate() {
    val def = config()
    val originals = originalsStrings()
    val values = def.validate(originals)
    val config = Config(values)

    validate(config, originals)

    val errors =
        config
            .configValues()
            .filter { v -> v.errorMessages().isNotEmpty() }
            .flatMap { v -> v.errorMessages() }
    if (errors.isNotEmpty()) {
      throw ConfigException(errors.joinToString())
    }
  }

  data class CdcPatternConfigItem(val entry: Map.Entry<String, Any>, private val pattern: Regex) :
      Comparable<CdcPatternConfigItem> {
    private val match = pattern.matchEntire(entry.key)!!

    val key = entry.key
    val value = entry.value
    val topic = match.groups[GROUP_NAME_TOPIC]!!.value

    val index
      get(): Int? = match.groups[GROUP_NAME_INDEX]?.value?.toInt()

    val metadata
      get(): String? = match.groups[GROUP_NAME_METADATA]?.value

    override fun compareTo(other: CdcPatternConfigItem): Int {
      return when (val result = this.topic.compareTo(other.topic)) {
        0 -> (this.index ?: -1).compareTo(other.index ?: -1)
        else -> result
      }
    }
  }

  companion object {
    const val START_FROM = "neo4j.start-from"
    const val START_FROM_VALUE = "neo4j.start-from.value"
    const val IGNORE_STORED_OFFSET = "neo4j.ignore-stored-offset"
    const val STRATEGY = "neo4j.source-strategy"
    const val BATCH_SIZE = "neo4j.batch-size"
    const val QUERY = "neo4j.query"
    const val QUERY_STREAMING_PROPERTY = "neo4j.query.streaming-property"
    const val QUERY_POLL_INTERVAL = "neo4j.query.poll-interval"
    const val QUERY_POLL_DURATION = "neo4j.query.poll-duration"
    const val QUERY_TIMEOUT = "neo4j.query.timeout"
    const val QUERY_TOPIC = "neo4j.query.topic"
    const val QUERY_FORCE_MAPS_AS_STRUCT = "neo4j.query.force-maps-as-struct"
    const val CDC_POLL_INTERVAL = "neo4j.cdc.poll-interval"
    const val CDC_POLL_DURATION = "neo4j.cdc.poll-duration"
    const val PAYLOAD_MODE = "neo4j.payload-mode"
    private const val GROUP_NAME_TOPIC = "topic"
    private const val GROUP_NAME_INDEX = "index"
    private const val GROUP_NAME_METADATA = "metadata"
    private val CDC_PATTERNS_REGEX =
        Regex("^neo4j\\.cdc\\.topic\\.(?<$GROUP_NAME_TOPIC>[a-zA-Z0-9._-]+)(\\.patterns)$")
    private val CDC_KEY_STRATEGY_REGEX =
        Regex(
            "^neo4j\\.cdc\\.topic\\.(?<$GROUP_NAME_TOPIC>[a-zA-Z0-9._-]+)(\\.key-strategy)$",
        )
    private val CDC_VALUE_STRATEGY_REGEX =
        Regex(
            "^neo4j\\.cdc\\.topic\\.(?<$GROUP_NAME_TOPIC>[a-zA-Z0-9._-]+)(\\.value-strategy)$",
        )
    private val CDC_PATTERN_ARRAY_REGEX =
        Regex(
            "^neo4j\\.cdc\\.topic\\.(?<$GROUP_NAME_TOPIC>[a-zA-Z0-9._-]+)(\\.patterns)\\.(?<$GROUP_NAME_INDEX>[0-9]+)(\\.pattern)$")
    private val CDC_PATTERN_ARRAY_OPERATION_REGEX =
        Regex(
            "^neo4j\\.cdc\\.topic\\.(?<$GROUP_NAME_TOPIC>[a-zA-Z0-9._-]+)(\\.patterns)\\.(?<$GROUP_NAME_INDEX>[0-9]+)(\\.operation)$")
    private val CDC_PATTERN_ARRAY_CHANGES_TO_REGEX =
        Regex(
            "^neo4j\\.cdc\\.topic\\.(?<$GROUP_NAME_TOPIC>[a-zA-Z0-9._-]+)(\\.patterns)\\.(?<$GROUP_NAME_INDEX>[0-9]+)(\\.changesTo)$")
    private val CDC_PATTERN_ARRAY_METADATA_REGEX =
        Regex(
            "^neo4j\\.cdc\\.topic\\.(?<$GROUP_NAME_TOPIC>[a-zA-Z0-9._-]+)(\\.patterns)\\.(?<$GROUP_NAME_INDEX>[0-9]+)(\\.metadata)\\.(?<$GROUP_NAME_METADATA>[a-zA-Z0-9._-]+)$")

    private val DEFAULT_QUERY_POLL_INTERVAL = 1.seconds
    private val DEFAULT_QUERY_POLL_DURATION = 5.seconds
    private const val DEFAULT_BATCH_SIZE = 1000
    private val DEFAULT_QUERY_TIMEOUT = 0.seconds
    private const val DEFAULT_QUERY_FORCE_MAPS_AS_STRUCT = true

    private val DEFAULT_CDC_POLL_INTERVAL = 1.seconds
    private val DEFAULT_CDC_POLL_DURATION = 5.seconds
    private const val DEFAULT_STREAMING_PROPERTY = "timestamp"

    private const val METADATA_KEY_AUTHENTICATED_USER = "authenticatedUser"
    private const val METADATA_KEY_EXECUTING_USER = "executingUser"
    private const val METADATA_KEY_TX_METADATA = "txMetadata"

    fun validate(config: Config, originals: Map<String, String>) {
      validate(config)

      // START_FROM user defined validation
      config.validateNonEmptyIfVisible(START_FROM_VALUE)

      // COMMON fields
      config.validateNonEmptyIfVisible(BATCH_SIZE)

      // QUERY strategy validation
      config.validateNonEmptyIfVisible(QUERY_TOPIC)
      config.validateNonEmptyIfVisible(QUERY)
      config.validateNonEmptyIfVisible(QUERY_TIMEOUT)
      config.validateNonEmptyIfVisible(QUERY_POLL_INTERVAL)

      // CDC validation
      config.validateNonEmptyIfVisible(CDC_POLL_INTERVAL)

      val configList = config.configValues().toList()
      val strategy = configList.find { it.name() == STRATEGY }
      if (strategy?.value() == SourceType.CDC.name) {
        val cdcTopics =
            originals.entries.filter { CDC_PATTERNS_REGEX.matches(it.key) } +
                originals.entries.filter { CDC_PATTERN_ARRAY_REGEX.matches(it.key) } +
                originals.entries.filter { CDC_PATTERN_ARRAY_OPERATION_REGEX.matches(it.key) } +
                originals.entries.filter { CDC_PATTERN_ARRAY_CHANGES_TO_REGEX.matches(it.key) } +
                originals.entries.filter { CDC_PATTERN_ARRAY_METADATA_REGEX.matches(it.key) } +
                originals.entries.filter { CDC_KEY_STRATEGY_REGEX.matches(it.key) } +
                originals.entries.filter { CDC_VALUE_STRATEGY_REGEX.matches(it.key) }
        if (cdcTopics.isEmpty()) {
          strategy.addErrorMessage(
              "At least one topic needs to be configured with pattern(s) describing the entities to query changes for. Please refer to documentation for more information.")
        } else {
          cdcTopics.forEach {
            // parse & validate CDC patterns
            try {
              Validators.notBlankOrEmpty().ensureValid(it.key, it.value)

              if (CDC_PATTERNS_REGEX.matches(it.key) || CDC_PATTERN_ARRAY_REGEX.matches(it.key)) {
                try {
                  Pattern.parse(it.value as String?)
                } catch (e: PatternException) {
                  throw ConfigException(it.key, it.value, e.message)
                }
              }
              if (CDC_KEY_STRATEGY_REGEX.matches(it.key)) {
                Validators.enum(Neo4jCdcKeyStrategy::class.java).ensureValid(it.key, it.value)
              }
              if (CDC_VALUE_STRATEGY_REGEX.matches(it.key)) {
                Validators.enum(Neo4jCdcValueStrategy::class.java).ensureValid(it.key, it.value)
              }
            } catch (e: ConfigException) {
              strategy.addErrorMessage(e.message)
            }
          }
        }
      }
    }

    fun config(): ConfigDef =
        Neo4jConfiguration.config()
            .define(
                ConfigKeyBuilder.of(STRATEGY, ConfigDef.Type.STRING) {
                  importance = ConfigDef.Importance.HIGH
                  defaultValue = SourceType.QUERY.name
                  group = Groups.CONNECTOR.title
                  validator = Validators.enum(SourceType::class.java)
                  recommender = Recommenders.enum(SourceType::class.java)
                })
            .define(
                ConfigKeyBuilder.of(START_FROM, ConfigDef.Type.STRING) {
                  importance = ConfigDef.Importance.HIGH
                  defaultValue = StartFrom.NOW.toString()
                  group = Groups.CONNECTOR.title
                  validator = Validators.enum(StartFrom::class.java)
                  recommender = Recommenders.enum(StartFrom::class.java)
                })
            .define(
                ConfigKeyBuilder.of(START_FROM_VALUE, ConfigDef.Type.STRING) {
                  importance = ConfigDef.Importance.HIGH
                  defaultValue = ""
                  group = Groups.CONNECTOR.title
                  recommender =
                      Recommenders.visibleIf(
                          START_FROM, Predicate.isEqual(StartFrom.USER_PROVIDED.name))
                })
            .define(
                ConfigKeyBuilder.of(IGNORE_STORED_OFFSET, ConfigDef.Type.STRING) {
                  importance = ConfigDef.Importance.MEDIUM
                  defaultValue = "false"
                  group = Groups.CONNECTOR_ADVANCED.title
                  validator = Validators.bool()
                  recommender = Recommenders.bool()
                })
            .define(
                ConfigKeyBuilder.of(QUERY, ConfigDef.Type.STRING) {
                  importance = ConfigDef.Importance.HIGH
                  defaultValue = ""
                  group = Groups.CONNECTOR.title
                  recommender =
                      Recommenders.visibleIf(STRATEGY, Predicate.isEqual(SourceType.QUERY.name))
                })
            .define(
                ConfigKeyBuilder.of(QUERY_STREAMING_PROPERTY, ConfigDef.Type.STRING) {
                  importance = ConfigDef.Importance.LOW
                  defaultValue = DEFAULT_STREAMING_PROPERTY
                  group = Groups.CONNECTOR.title
                  recommender =
                      Recommenders.visibleIf(STRATEGY, Predicate.isEqual(SourceType.QUERY.name))
                  validator = Validators.notBlankOrEmpty()
                })
            .define(
                ConfigKeyBuilder.of(QUERY_TOPIC, ConfigDef.Type.STRING) {
                  importance = ConfigDef.Importance.HIGH
                  defaultValue = ""
                  group = Groups.CONNECTOR.title
                  recommender =
                      Recommenders.visibleIf(STRATEGY, Predicate.isEqual(SourceType.QUERY.name))
                })
            .define(
                ConfigKeyBuilder.of(QUERY_TIMEOUT, ConfigDef.Type.STRING) {
                  importance = ConfigDef.Importance.LOW
                  defaultValue = DEFAULT_QUERY_TIMEOUT.toSimpleString()
                  group = Groups.CONNECTOR_ADVANCED.title
                  recommender =
                      Recommenders.visibleIf(STRATEGY, Predicate.isEqual(SourceType.QUERY.name))
                  validator = Validators.pattern(SIMPLE_DURATION_PATTERN)
                })
            .define(
                ConfigKeyBuilder.of(QUERY_POLL_INTERVAL, ConfigDef.Type.STRING) {
                  importance = ConfigDef.Importance.MEDIUM
                  defaultValue = DEFAULT_QUERY_POLL_INTERVAL.toSimpleString()
                  group = Groups.CONNECTOR_ADVANCED.title
                  recommender =
                      Recommenders.visibleIf(STRATEGY, Predicate.isEqual(SourceType.QUERY.name))
                  validator = Validators.pattern(SIMPLE_DURATION_PATTERN)
                })
            .define(
                ConfigKeyBuilder.of(QUERY_POLL_DURATION, ConfigDef.Type.STRING) {
                  importance = ConfigDef.Importance.MEDIUM
                  defaultValue = DEFAULT_QUERY_POLL_DURATION.toSimpleString()
                  group = Groups.CONNECTOR_ADVANCED.title
                  recommender =
                      Recommenders.visibleIf(STRATEGY, Predicate.isEqual(SourceType.QUERY.name))
                  validator = Validators.pattern(SIMPLE_DURATION_PATTERN)
                })
            .define(
                ConfigKeyBuilder.of(QUERY_FORCE_MAPS_AS_STRUCT, ConfigDef.Type.BOOLEAN) {
                  importance = ConfigDef.Importance.LOW
                  defaultValue = DEFAULT_QUERY_FORCE_MAPS_AS_STRUCT
                  group = Groups.CONNECTOR_ADVANCED.title
                  recommender =
                      Recommenders.visibleIf(STRATEGY, Predicate.isEqual(SourceType.QUERY.name))
                })
            .define(
                ConfigKeyBuilder.of(BATCH_SIZE, ConfigDef.Type.INT) {
                  importance = ConfigDef.Importance.MEDIUM
                  defaultValue = DEFAULT_BATCH_SIZE
                  group = Groups.CONNECTOR_ADVANCED.title
                  recommender =
                      Recommenders.visibleIf(STRATEGY, Predicate.isEqual(SourceType.QUERY.name))
                  validator = Range.atLeast(1)
                })
            .define(
                ConfigKeyBuilder.of(CDC_POLL_INTERVAL, ConfigDef.Type.STRING) {
                  importance = ConfigDef.Importance.MEDIUM
                  defaultValue = DEFAULT_CDC_POLL_INTERVAL.toSimpleString()
                  group = Groups.CONNECTOR_ADVANCED.title
                  recommender =
                      Recommenders.visibleIf(STRATEGY, Predicate.isEqual(SourceType.CDC.name))
                  validator = Validators.pattern(SIMPLE_DURATION_PATTERN)
                })
            .define(
                ConfigKeyBuilder.of(CDC_POLL_DURATION, ConfigDef.Type.STRING) {
                  importance = ConfigDef.Importance.MEDIUM
                  defaultValue = DEFAULT_CDC_POLL_DURATION.toSimpleString()
                  group = Groups.CONNECTOR_ADVANCED.title
                  recommender =
                      Recommenders.visibleIf(STRATEGY, Predicate.isEqual(SourceType.CDC.name))
                  validator = Validators.pattern(SIMPLE_DURATION_PATTERN)
                })
            .define(
                ConfigKeyBuilder.of(PAYLOAD_MODE, ConfigDef.Type.STRING) {
                  importance = ConfigDef.Importance.MEDIUM
                  defaultValue = PayloadMode.EXTENDED.name
                  group = Groups.CONNECTOR_ADVANCED.title
                  validator = Validators.enum(PayloadMode::class.java)
                  recommender = Recommenders.enum(PayloadMode::class.java)
                })
  }
}
