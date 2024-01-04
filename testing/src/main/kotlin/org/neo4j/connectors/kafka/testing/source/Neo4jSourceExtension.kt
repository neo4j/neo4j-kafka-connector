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
package org.neo4j.connectors.kafka.testing.source

import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import java.util.*
import kotlin.jvm.optionals.getOrNull
import kotlin.reflect.KProperty1
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.jupiter.api.extension.AfterEachCallback
import org.junit.jupiter.api.extension.BeforeEachCallback
import org.junit.jupiter.api.extension.ConditionEvaluationResult
import org.junit.jupiter.api.extension.ExecutionCondition
import org.junit.jupiter.api.extension.ExtensionConfigurationException
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.api.extension.ParameterContext
import org.junit.jupiter.api.extension.ParameterResolver
import org.neo4j.connectors.kafka.testing.AnnotationSupport
import org.neo4j.connectors.kafka.testing.AnnotationValueResolver
import org.neo4j.connectors.kafka.testing.DatabaseSupport.createDatabase
import org.neo4j.connectors.kafka.testing.DatabaseSupport.dropDatabase
import org.neo4j.connectors.kafka.testing.DatabaseSupport.enableCdc
import org.neo4j.connectors.kafka.testing.ParameterResolvers
import org.neo4j.driver.AuthToken
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.Session
import org.neo4j.driver.SessionConfig
import org.slf4j.Logger
import org.slf4j.LoggerFactory

internal class Neo4jSourceExtension(
    // visible for testing
    envAccessor: (String) -> String? = System::getenv,
    private val driverFactory: (String, AuthToken) -> Driver = GraphDatabase::driver,
    private val consumerFactory: (Properties, String) -> KafkaConsumer<String, GenericRecord> =
        ::getSubscribedConsumer,
) : ExecutionCondition, BeforeEachCallback, AfterEachCallback, ParameterResolver {

  private val log: Logger = LoggerFactory.getLogger(Neo4jSourceExtension::class.java)

  private val paramResolvers =
      ParameterResolvers(
          mapOf(
              Session::class.java to ::resolveSession,
              KafkaConsumer::class.java to ::resolveConsumer,
          ))

  private lateinit var sourceAnnotation: Neo4jSource

  private lateinit var source: Neo4jSourceRegistration

  private lateinit var driver: Driver

  private lateinit var session: Session

  private lateinit var neo4jDatabase: String

  private val brokerExternalHost =
      AnnotationValueResolver(Neo4jSource::brokerExternalHost, envAccessor)

  private val schemaRegistryUri =
      AnnotationValueResolver(Neo4jSource::schemaControlRegistryUri, envAccessor)

  private val schemaControlRegistryExternalUri =
      AnnotationValueResolver(Neo4jSource::schemaControlRegistryExternalUri, envAccessor)

  private val kafkaConnectExternalUri =
      AnnotationValueResolver(Neo4jSource::kafkaConnectExternalUri, envAccessor)

  private val neo4jUri = AnnotationValueResolver(Neo4jSource::neo4jUri, envAccessor)

  private val neo4jExternalUri = AnnotationValueResolver(Neo4jSource::neo4jExternalUri, envAccessor)

  private val neo4jUser = AnnotationValueResolver(Neo4jSource::neo4jUser, envAccessor)

  private val neo4jPassword = AnnotationValueResolver(Neo4jSource::neo4jPassword, envAccessor)

  private val mandatorySettings =
      listOf(
          brokerExternalHost,
          schemaRegistryUri,
          neo4jUri,
          neo4jUser,
          neo4jPassword,
      )

  override fun evaluateExecutionCondition(context: ExtensionContext?): ConditionEvaluationResult {
    val metadata =
        AnnotationSupport.findAnnotation<Neo4jSource>(context)
            ?: throw ExtensionConfigurationException("@Neo4jSource not found")

    val errors = mutableListOf<String>()
    mandatorySettings.forEach {
      if (!it.isValid(metadata)) {
        errors.add(it.errorMessage())
      }
    }
    if (errors.isNotEmpty()) {
      throw ExtensionConfigurationException(
          "\nMissing settings, see details below:\n\t${errors.joinToString("\n\t")}",
      )
    }

    this.sourceAnnotation = metadata
    return ConditionEvaluationResult.enabled("@Neo4jSource and environment properly configured")
  }

  override fun beforeEach(context: ExtensionContext?) {
    ensureDatabase(context)

    source =
        Neo4jSourceRegistration(
            schemaControlRegistryUri = schemaRegistryUri.resolve(sourceAnnotation),
            neo4jUri = neo4jUri.resolve(sourceAnnotation),
            neo4jUser = neo4jUser.resolve(sourceAnnotation),
            neo4jPassword = neo4jPassword.resolve(sourceAnnotation),
            neo4jDatabase = neo4jDatabase,
            topic = sourceAnnotation.topic,
            streamingProperty = sourceAnnotation.streamingProperty,
            startFrom = sourceAnnotation.startFrom,
            query = sourceAnnotation.query,
            strategy = sourceAnnotation.strategy,
            cdcPatternsIndexed = sourceAnnotation.cdc.patternsIndexed,
            cdcPatterns = sourceAnnotation.cdc.paramAsMap(CdcSourceTopic::patterns),
            cdcOperations = sourceAnnotation.cdc.paramAsMap(CdcSourceTopic::operations),
            cdcChangesTo = sourceAnnotation.cdc.paramAsMap(CdcSourceTopic::changesTo),
            cdcMetadata = sourceAnnotation.cdc.metadataAsMap())
    source.register(kafkaConnectExternalUri.resolve(sourceAnnotation))
  }

  override fun afterEach(context: ExtensionContext?) {
    source.unregister()
    if (this::driver.isInitialized) {
      session.dropDatabase(neo4jDatabase).close()
      driver.close()
    } else {
      createDriver().use { dr -> dr.session().use { it.dropDatabase(neo4jDatabase) } }
    }
  }

  private fun resolveConsumer(
      parameterContext: ParameterContext?,
      extensionContext: ExtensionContext?
  ): KafkaConsumer<String, GenericRecord> {
    val consumerAnnotation = parameterContext?.parameter?.getAnnotation(TopicConsumer::class.java)!!
    val properties = Properties()
    properties.setProperty(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
        brokerExternalHost.resolve(sourceAnnotation),
    )
    properties.setProperty(
        KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
        schemaControlRegistryExternalUri.resolve(sourceAnnotation),
    )
    properties.setProperty(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer::class.java.getName(),
    )
    properties.setProperty(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        KafkaAvroDeserializer::class.java.getName(),
    )
    properties.setProperty(
        ConsumerConfig.GROUP_ID_CONFIG,
        // note: ExtensionContext#getUniqueId() returns null in the CLI
        "${consumerAnnotation.topic}@${extensionContext?.testClass?: ""}#${extensionContext?.displayName}")

    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, consumerAnnotation.offset)
    return consumerFactory(properties, consumerAnnotation.topic)
  }

  private fun resolveSession(
      @Suppress("UNUSED_PARAMETER") parameterContext: ParameterContext?,
      @Suppress("UNUSED_PARAMETER") extensionContext: ExtensionContext?
  ): Any {
    ensureDatabase(extensionContext)
    driver = createDriver()
    session = driver.session(SessionConfig.forDatabase(neo4jDatabase))
    return session
  }

  private fun createDriver(): Driver {
    val uri = neo4jExternalUri.resolve(sourceAnnotation)
    val username = neo4jUser.resolve(sourceAnnotation)
    val password = neo4jPassword.resolve(sourceAnnotation)
    return driverFactory(uri, AuthTokens.basic(username, password))
  }

  private fun ensureDatabase(context: ExtensionContext?) {
    if (this::neo4jDatabase.isInitialized) {
      return
    }
    neo4jDatabase =
        sourceAnnotation.neo4jDatabase.ifEmpty { "test-" + UUID.randomUUID().toString() }
    log.debug(
        "Using database {} for test {}",
        neo4jDatabase,
        "${context?.testClass?.getOrNull()?.simpleName}#${context?.displayName}")
    createDriver().use { driver ->
      driver.verifyConnectivity()
      driver.session().use { session ->
        session.createDatabase(neo4jDatabase)
        if (sourceAnnotation.strategy == SourceStrategy.CDC) {
          session.enableCdc(neo4jDatabase)
        }
      }
    }
  }

  companion object {
    private fun getSubscribedConsumer(
        properties: Properties,
        topic: String
    ): KafkaConsumer<String, GenericRecord> {
      val consumer = KafkaConsumer<String, GenericRecord>(properties)
      consumer.subscribe(listOf(topic))
      return consumer
    }

    private fun CdcSource.paramAsMap(
        property: KProperty1<CdcSourceTopic, Array<CdcSourceParam>>
    ): Map<String, List<String>> {
      val result = mutableMapOf<String, MutableList<String>>()
      this.topics.forEach { topic ->
        property.get(topic).forEach { param ->
          result.computeIfAbsent(topic.topic) { mutableListOf() }.add(param.index, param.value)
        }
      }
      return result
    }

    private fun CdcSource.metadataAsMap(): Map<String, List<Map<String, String>>> {
      val result = mutableMapOf<String, MutableList<MutableMap<String, String>>>()
      this.topics.forEach { topic ->
        topic.metadata.forEach { metadata ->
          val metadataForIndex = result.computeIfAbsent(topic.topic) { mutableListOf() }
          val metadataByKey = metadataForIndex.getOrElse(metadata.index) { mutableMapOf() }
          if (metadataByKey.isEmpty()) {
            metadataForIndex.add(metadata.index, metadataByKey)
          }
          metadataByKey[metadata.key] = metadata.value
        }
      }
      return result
    }
  }

  override fun supportsParameter(
      parameterContext: ParameterContext?,
      extensionContext: ExtensionContext?
  ): Boolean {
    return paramResolvers.supportsParameter(parameterContext, extensionContext)
  }

  override fun resolveParameter(
      parameterContext: ParameterContext?,
      extensionContext: ExtensionContext?
  ): Any {
    return paramResolvers.resolveParameter(parameterContext, extensionContext)
  }
}
