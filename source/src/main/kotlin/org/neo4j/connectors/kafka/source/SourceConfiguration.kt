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
import org.neo4j.connectors.kafka.configuration.DeprecatedNeo4jConfiguration
import org.neo4j.connectors.kafka.configuration.Neo4jConfiguration
import org.neo4j.connectors.kafka.configuration.helpers.ConfigKeyBuilder
import org.neo4j.connectors.kafka.configuration.helpers.Recommenders
import org.neo4j.connectors.kafka.configuration.helpers.SIMPLE_DURATION_PATTERN
import org.neo4j.connectors.kafka.configuration.helpers.Validators
import org.neo4j.connectors.kafka.configuration.helpers.Validators.validateNonEmptyIfVisible
import org.neo4j.connectors.kafka.configuration.helpers.parseSimpleString
import org.neo4j.connectors.kafka.configuration.helpers.toSimpleString
import org.neo4j.driver.TransactionConfig

enum class SourceType {
  QUERY,
  CDC
}

enum class StartFrom {
  EARLIEST,
  NOW,
  USER_PROVIDED
}

class SourceConfiguration(originals: Map<*, *>) :
    Neo4jConfiguration(config(), originals, ConnectorType.SOURCE) {

  val startFrom
    get(): StartFrom = StartFrom.valueOf(getString(START_FROM))

  val startFromCustom
    get(): String = getString(START_FROM_VALUE)

  val ignoreStoredOffset
    get(): Boolean = getBoolean(IGNORE_STORED_OFFSET)

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

  val batchSize
    get(): Int = getInt(BATCH_SIZE)

  val queryTimeout
    get(): Duration = Duration.parseSimpleString(getString(QUERY_TIMEOUT))

  val topic
    get(): String = getString(TOPIC)

  val partition
    get(): Map<String, Any> {
      return when (strategy) {
        SourceType.QUERY ->
            mapOf(
                "database" to this.database,
                "type" to "query",
                "query" to query,
                "partition" to 1,
            )
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

        originals()
            .entries
            .filter { CDC_PATTERNS_REGEX.matches(it.key) }
            .flatMap {
              Pattern.parse(it.value as String?).map { key ->
                key to retrieveGroupsFromConfigKey(it.key, CDC_PATTERNS_REGEX).groupValues[1]
              }
            }
            .forEach {
              if (!configMap.containsKey(it.second)) {
                configMap[it.second] = mutableListOf()
              }

              nonPositionalConfigMode[it.second] = true

              val list = configMap[it.second]!!
              list.add(it.first)
            }

        originals()
            .entries
            .filter { CDC_PATTERN_ARRAY_REGEX.matches(it.key) }
            .forEach { mapPositionalPattern(it, nonPositionalConfigMode, configMap) }

        originals()
            .entries
            .filter { CDC_PATTERN_ARRAY_OPERATION_REGEX.matches(it.key) }
            .forEach { mapOperation(it, nonPositionalConfigMode, configMap) }

        originals()
            .entries
            .filter { CDC_PATTERN_ARRAY_CHANGES_TO_REGEX.matches(it.key) }
            .forEach { mapChangesTo(it, nonPositionalConfigMode, configMap) }

        originals()
            .entries
            .filter { CDC_PATTERN_ARRAY_METADATA_REGEX.matches(it.key) }
            .forEach { mapMetadata(it, nonPositionalConfigMode, configMap) }

        pivotMapCdcSelectorMap(configMap)
      }
      else -> emptyMap()
    }
  }

  val cdcTopicsToKeyStrategy: Map<String, Neo4jCdcKeySerializationStrategy> by lazy {
    when (strategy) {
      SourceType.CDC -> {
        originals()
            .entries
            .filter { CDC_KEY_STRATEGY_REGEX.matches(it.key) }
            .associate { mapKeyStrategy(it) }
      }
      else -> emptyMap()
    }
  }

  private fun mapPositionalPattern(
      configEntry: MutableMap.MutableEntry<String, Any>,
      nonPositionalConfigMode: MutableMap<String, Boolean>,
      configMap: MutableMap<String, MutableList<Pattern>>
  ) {
    val matchGroups = retrieveGroupsFromConfigKey(configEntry.key, CDC_PATTERN_ARRAY_REGEX)
    val topicName = matchGroups.groupValues[1]
    if (nonPositionalConfigMode.getOrDefault(topicName, false)) {
      throw ConfigException(
          "It's not allowed to mix positional and non-positional configuration for the same topic.",
      )
    }
    val index = Integer.parseInt(matchGroups.groupValues[3])
    val patterns = Pattern.parse(configEntry.value as String?)
    if (index > patterns.size - 1) {
      throw ConfigException(
          "Index $index out of bounds. Please ensure that you started the definition with a 0-based index.",
      )
    }
    if (patterns.size > 1) {
      throw ConfigException(
          "Too many patterns. Only one pattern allowed for positional pattern configuration.",
      )
    }
    val pattern = patterns.get(0)
    if (!configMap.containsKey(topicName)) {
      configMap[topicName] = mutableListOf()
    }
    val list = configMap[topicName]!!
    list.add(pattern)
  }

  private fun mapOperation(
      configEntry: MutableMap.MutableEntry<String, Any>,
      nonPositionalConfigMode: MutableMap<String, Boolean>,
      configMap: MutableMap<String, MutableList<Pattern>>
  ) {
    val matchGroup = retrieveGroupsFromConfigKey(configEntry.key, CDC_PATTERN_ARRAY_OPERATION_REGEX)
    val (index, patterns) = retrieveIndexAndPattern(matchGroup, nonPositionalConfigMode, configMap)
    val operation =
        when (val value = (configEntry.value as String).lowercase()) {
          "create" -> EntityOperation.CREATE
          "update" -> EntityOperation.UPDATE
          "delete" -> EntityOperation.DELETE
          else -> {
            throw ConfigException(
                "Cannot parse $value as an operation. Allowed operations are create, delete or update.",
            )
          }
        }
    val pattern = patterns.get(index)
    pattern.withOperation(operation)
  }

  private fun mapChangesTo(
      configEntry: MutableMap.MutableEntry<String, Any>,
      nonPositionalConfigMode: MutableMap<String, Boolean>,
      configMap: MutableMap<String, MutableList<Pattern>>
  ) {
    val matchGroup =
        retrieveGroupsFromConfigKey(configEntry.key, CDC_PATTERN_ARRAY_CHANGES_TO_REGEX)
    val (index, patterns) = retrieveIndexAndPattern(matchGroup, nonPositionalConfigMode, configMap)
    val value = configEntry.value as String
    val changesTo = value.splitToSequence(",").map { term -> term.trim() }.toSet()
    val pattern = patterns.get(index)
    pattern.withChangesTo(changesTo)
  }

  private fun mapMetadata(
      configEntry: MutableMap.MutableEntry<String, Any>,
      nonPositionalConfigMode: MutableMap<String, Boolean>,
      configMap: MutableMap<String, MutableList<Pattern>>
  ) {
    val matchGroup = retrieveGroupsFromConfigKey(configEntry.key, CDC_PATTERN_ARRAY_METADATA_REGEX)
    val (index, patterns) = retrieveIndexAndPattern(matchGroup, nonPositionalConfigMode, configMap)
    val keyValue = matchGroup.groupValues[5]
    var value = configEntry.value
    val pattern = patterns.get(index)
    if (keyValue.startsWith(EntitySelector.METADATA_KEY_TX_METADATA + '.')) {
      value =
          mapOf(
              keyValue.removePrefix(EntitySelector.METADATA_KEY_TX_METADATA + '.') to value,
          )
      val metadata =
          mapOf(
              EntitySelector.METADATA_KEY_TX_METADATA to value,
          )
      pattern.withMetadata(metadata)
    } else {
      val metadata =
          mapOf(
              keyValue to value,
          )
      pattern.withMetadata(metadata)
    }
  }

  private fun mapKeyStrategy(
      configEntry: MutableMap.MutableEntry<String, Any>
  ): Pair<String, Neo4jCdcKeySerializationStrategy> {
    val matchGroup = retrieveGroupsFromConfigKey(configEntry.key, CDC_KEY_STRATEGY_REGEX)
    val topicName = matchGroup.groupValues[1]
    val value = configEntry.value
    return topicName to Neo4jCdcKeySerializationStrategy.valueOf(value as String)
  }

  private fun retrieveIndexAndPattern(
      matchGroup: MatchResult,
      nonPositionalConfigMode: MutableMap<String, Boolean>,
      configMap: MutableMap<String, MutableList<Pattern>>
  ): Pair<Int, MutableList<Pattern>> {
    val topicName = matchGroup.groupValues[1]
    if (nonPositionalConfigMode.getOrDefault(topicName, false)) {
      throw ConfigException(
          "It's not allowed to mix positional and non-positional configuration for the same topic.",
      )
    }
    val index = Integer.parseInt(matchGroup.groupValues[3])
    if (!configMap.containsKey(topicName)) {
      throw ConfigException(
          "Cannot assign config value because pattern is not defined for index $index.",
      )
    }
    val patterns = configMap.get(topicName)!!
    if (index > patterns.size - 1) {
      throw ConfigException(
          "Index $index out of bounds. Please ensure that you started the definition with a 0-based index.",
      )
    }
    return Pair(index, patterns)
  }

  private fun retrieveGroupsFromConfigKey(configKey: String, regex: Regex) =
      regex.matchEntire(configKey)!!

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
          selectorBasedMap[selector]!!.add(it.key)
          selectorBasedMap[selector]!!.sort()
        }
      }
    }

    return selectorBasedMap
  }

  val cdcSelectors: Set<Selector> by lazy {
    cdcSelectorsToTopics.keys
        .map {
          when (it) {
            is NodeSelector -> NodeSelector(it.change, it.changesTo, it.labels, it.key, it.metadata)
            is RelationshipSelector ->
                RelationshipSelector(
                    it.change,
                    it.changesTo,
                    it.type,
                    it.start,
                    it.end,
                    it.key,
                    it.metadata,
                )
            is EntitySelector -> EntitySelector(it.change, it.changesTo, it.metadata)
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

    new.withMetadata(buildMap { original.metadata().forEach { (k, v) -> this[k] = v as Any } })

    return new.build()
  }

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

  companion object {
    const val START_FROM = "neo4j.start-from"
    const val START_FROM_VALUE = "neo4j.start-from.value"
    const val IGNORE_STORED_OFFSET = "neo4j.ignore-stored-offset"
    const val STRATEGY = "neo4j.source-strategy"
    const val BATCH_SIZE = "neo4j.batch-size"
    const val QUERY = "neo4j.query"
    const val QUERY_STREAMING_PROPERTY = "neo4j.query.streaming-property"
    const val QUERY_POLL_INTERVAL = "neo4j.query.poll-interval"
    const val QUERY_TIMEOUT = "neo4j.query.timeout"
    const val TOPIC = "topic"
    const val ENFORCE_SCHEMA = "neo4j.enforce-schema"
    const val CDC_POLL_INTERVAL = "neo4j.cdc.poll-interval"
    const val CDC_POLL_DURATION = "neo4j.cdc.poll-duration"
    private val CDC_PATTERNS_REGEX = Regex("^neo4j\\.cdc\\.topic\\.([a-zA-Z0-9._-]+)(\\.patterns)$")
    private val CDC_KEY_STRATEGY_REGEX =
        Regex(
            "^neo4j\\.cdc\\.topic\\.([a-zA-Z0-9._-]+)(\\.key-strategy)$",
        )
    private val CDC_PATTERN_ARRAY_REGEX =
        Regex("^neo4j\\.cdc\\.topic\\.([a-zA-Z0-9._-]+)(\\.patterns)\\.([0-9]+)(\\.pattern)$")
    private val CDC_PATTERN_ARRAY_OPERATION_REGEX =
        Regex("^neo4j\\.cdc\\.topic\\.([a-zA-Z0-9._-]+)(\\.patterns)\\.([0-9]+)(\\.operation)$")
    private val CDC_PATTERN_ARRAY_CHANGES_TO_REGEX =
        Regex("^neo4j\\.cdc\\.topic\\.([a-zA-Z0-9._-]+)(\\.patterns)\\.([0-9]+)(\\.changesTo)$")
    private val CDC_PATTERN_ARRAY_METADATA_REGEX =
        Regex(
            "^neo4j\\.cdc\\.topic\\.([a-zA-Z0-9._-]+)(\\.patterns)\\.([0-9]+)(\\.metadata)\\.([a-zA-Z0-9._-]+)$",
        )

    private val DEFAULT_POLL_INTERVAL = 10.seconds
    private const val DEFAULT_BATCH_SIZE = 1000
    private val DEFAULT_QUERY_TIMEOUT = 0.seconds
    private val DEFAULT_CDC_POLL_INTERVAL = 1.seconds
    private val DEFAULT_CDC_POLL_DURATION = 5.seconds

    @Suppress("DEPRECATION")
    fun migrateSettings(oldSettings: Map<String, Any>): Map<String, String> {
      val migrated = migrateSettings(oldSettings, true).toMutableMap()

      oldSettings.forEach {
        when (it.key) {
          DeprecatedNeo4jSourceConfiguration.STREAMING_FROM ->
              migrated[START_FROM] =
                  when (DeprecatedNeo4jSourceConfiguration.StreamingFrom.valueOf(
                      it.value.toString(),
                  )) {
                    DeprecatedNeo4jSourceConfiguration.StreamingFrom.ALL -> StartFrom.EARLIEST.name
                    DeprecatedNeo4jSourceConfiguration.StreamingFrom.NOW -> StartFrom.NOW.name
                    DeprecatedNeo4jSourceConfiguration.StreamingFrom.LAST_COMMITTED ->
                        StartFrom.NOW.name
                  }
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
          DeprecatedNeo4jConfiguration.BATCH_SIZE -> migrated[BATCH_SIZE] = it.value.toString()
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

    fun validate(config: Config, originals: Map<String, String>) {
      validate(config)

      // START_FROM user defined validation
      config.validateNonEmptyIfVisible(START_FROM_VALUE)

      // COMMON fields
      config.validateNonEmptyIfVisible(BATCH_SIZE)

      // QUERY strategy validation
      config.validateNonEmptyIfVisible(TOPIC)
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
                originals.entries.filter { CDC_PATTERN_ARRAY_METADATA_REGEX.matches(it.key) }
        if (cdcTopics.isEmpty()) {
          strategy.addErrorMessage(
              "At least one topic needs to be configured with pattern(s) describing the entities to query changes for. Please refer to documentation for more information.",
          )
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
                  validator = Validators.enum(SourceType::class.java)
                  recommender = Recommenders.enum(SourceType::class.java)
                },
            )
            .define(
                ConfigKeyBuilder.of(START_FROM, ConfigDef.Type.STRING) {
                  importance = ConfigDef.Importance.HIGH
                  defaultValue = StartFrom.NOW.toString()
                  validator = Validators.enum(StartFrom::class.java)
                  recommender = Recommenders.enum(StartFrom::class.java)
                },
            )
            .define(
                ConfigKeyBuilder.of(START_FROM_VALUE, ConfigDef.Type.STRING) {
                  importance = ConfigDef.Importance.HIGH
                  defaultValue = ""
                  recommender =
                      Recommenders.visibleIf(
                          START_FROM,
                          Predicate.isEqual(StartFrom.USER_PROVIDED.name),
                      )
                },
            )
            .define(
                ConfigKeyBuilder.of(IGNORE_STORED_OFFSET, ConfigDef.Type.BOOLEAN) {
                  importance = ConfigDef.Importance.HIGH
                  defaultValue = false
                },
            )
            .define(
                ConfigKeyBuilder.of(TOPIC, ConfigDef.Type.STRING) {
                  importance = ConfigDef.Importance.HIGH
                  defaultValue = ""
                  recommender =
                      Recommenders.visibleIf(STRATEGY, Predicate.isEqual(SourceType.QUERY.name))
                },
            )
            .define(
                ConfigKeyBuilder.of(QUERY, ConfigDef.Type.STRING) {
                  importance = ConfigDef.Importance.HIGH
                  defaultValue = ""
                  recommender =
                      Recommenders.visibleIf(STRATEGY, Predicate.isEqual(SourceType.QUERY.name))
                },
            )
            .define(
                ConfigKeyBuilder.of(QUERY_STREAMING_PROPERTY, ConfigDef.Type.STRING) {
                  importance = ConfigDef.Importance.HIGH
                  recommender =
                      Recommenders.visibleIf(STRATEGY, Predicate.isEqual(SourceType.QUERY.name))
                  defaultValue = ""
                },
            )
            .define(
                ConfigKeyBuilder.of(QUERY_POLL_INTERVAL, ConfigDef.Type.STRING) {
                  importance = ConfigDef.Importance.HIGH
                  recommender =
                      Recommenders.visibleIf(STRATEGY, Predicate.isEqual(SourceType.QUERY.name))
                  validator = Validators.pattern(SIMPLE_DURATION_PATTERN)
                  defaultValue = DEFAULT_POLL_INTERVAL.toSimpleString()
                },
            )
            .define(
                ConfigKeyBuilder.of(BATCH_SIZE, ConfigDef.Type.INT) {
                  importance = ConfigDef.Importance.HIGH
                  recommender =
                      Recommenders.visibleIf(STRATEGY, Predicate.isEqual(SourceType.QUERY.name))
                  validator = Range.atLeast(1)
                  defaultValue = DEFAULT_BATCH_SIZE
                },
            )
            .define(
                ConfigKeyBuilder.of(QUERY_TIMEOUT, ConfigDef.Type.STRING) {
                  importance = ConfigDef.Importance.HIGH
                  recommender =
                      Recommenders.visibleIf(STRATEGY, Predicate.isEqual(SourceType.QUERY.name))
                  validator = Validators.pattern(SIMPLE_DURATION_PATTERN)
                  defaultValue = DEFAULT_QUERY_TIMEOUT.toSimpleString()
                },
            )
            .define(
                ConfigKeyBuilder.of(CDC_POLL_INTERVAL, ConfigDef.Type.STRING) {
                  importance = ConfigDef.Importance.HIGH
                  recommender =
                      Recommenders.visibleIf(STRATEGY, Predicate.isEqual(SourceType.CDC.name))
                  validator = Validators.pattern(SIMPLE_DURATION_PATTERN)
                  defaultValue = DEFAULT_CDC_POLL_INTERVAL.toSimpleString()
                },
            )
            .define(
                ConfigKeyBuilder.of(CDC_POLL_DURATION, ConfigDef.Type.STRING) {
                  importance = ConfigDef.Importance.HIGH
                  recommender =
                      Recommenders.visibleIf(STRATEGY, Predicate.isEqual(SourceType.CDC.name))
                  validator = Validators.pattern(SIMPLE_DURATION_PATTERN)
                  defaultValue = DEFAULT_CDC_POLL_DURATION.toSimpleString()
                },
            )
            .define(
                ConfigKeyBuilder.of(ENFORCE_SCHEMA, ConfigDef.Type.BOOLEAN) {
                  importance = ConfigDef.Importance.HIGH
                  defaultValue = false
                  validator = ConfigDef.NonNullValidator()
                },
            )
  }
}
