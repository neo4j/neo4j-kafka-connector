package org.neo4j.connectors.kafka.source.testing

import org.junit.jupiter.api.extension.AfterEachCallback
import org.junit.jupiter.api.extension.BeforeEachCallback
import org.junit.jupiter.api.extension.ConditionEvaluationResult
import org.junit.jupiter.api.extension.ExecutionCondition
import org.junit.jupiter.api.extension.ExtensionConfigurationException
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.platform.commons.support.AnnotationSupport
import streams.kafka.connect.source.testing.Neo4jSourceRegistration
import kotlin.jvm.optionals.getOrNull

class Neo4jSourceExtension : ExecutionCondition, BeforeEachCallback, AfterEachCallback {

  private lateinit var metadata: Neo4jSource
  private lateinit var source: Neo4jSourceRegistration

  override fun evaluateExecutionCondition(context: ExtensionContext?): ConditionEvaluationResult {
    val metadata = findAnnotation(context) ?: throw ExtensionConfigurationException("@Neo4jSource not found")
    this.metadata = metadata
    // TODO: check Kafka Connect is up?
    return ConditionEvaluationResult.enabled("@Neo4jSource found")
  }

  override fun beforeEach(context: ExtensionContext?) {
    source =
        Neo4jSourceRegistration(
            topic = metadata.topic,
            neo4jUri = metadata.neo4jUri,
            neo4jUser = metadata.neo4jUser,
            neo4jPassword = metadata.neo4jPassword,
            streamingProperty = metadata.streamingProperty,
            streamingFrom = metadata.streamingFrom,
            streamingQuery = metadata.streamingQuery,
            schemaControlRegistry = metadata.schemaControlRegistryUri,
        )
    source.register(metadata.kafkaConnectUri)
  }

  override fun afterEach(context: ExtensionContext?) {
    source.unregister()
  }

  private fun findAnnotation(context: ExtensionContext?): Neo4jSource? {
    var current = context
    while (current != null) {
      val annotation = AnnotationSupport.findAnnotation(
          current.requiredTestMethod,
          Neo4jSource::class.java,
      )
      if (annotation.isPresent) {
        return annotation.get()
      }
      current = current.parent.getOrNull()
    }
    return null
  }
}
