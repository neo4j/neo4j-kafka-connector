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
package org.neo4j.connectors.kafka.testing.sink

import org.junit.jupiter.api.extension.AfterEachCallback
import org.junit.jupiter.api.extension.BeforeEachCallback
import org.junit.jupiter.api.extension.ConditionEvaluationResult
import org.junit.jupiter.api.extension.ExecutionCondition
import org.junit.jupiter.api.extension.ExtensionConfigurationException
import org.junit.jupiter.api.extension.ExtensionContext
import org.neo4j.connectors.kafka.testing.AnnotationSupport
import org.neo4j.connectors.kafka.testing.EnvBackedSetting
import org.neo4j.connectors.kafka.testing.WordSupport.pluralize

internal class Neo4jSinkExtension(
    // visible for testing
    envAccessor: (String) -> String? = System::getenv
) : ExecutionCondition, BeforeEachCallback, AfterEachCallback {

  private lateinit var sinkAnnotation: Neo4jSink
  private lateinit var sink: Neo4jSinkRegistration

  private val kafkaConnectExternalUri =
      EnvBackedSetting<Neo4jSink>(
          "kafkaConnectExternalUri", { it.kafkaConnectExternalUri }, envAccessor)
  private val neo4jUri = EnvBackedSetting<Neo4jSink>("neo4jUri", { it.neo4jUri }, envAccessor)
  private val neo4jUser = EnvBackedSetting<Neo4jSink>("neo4jUser", { it.neo4jUser }, envAccessor)
  private val neo4jPassword =
      EnvBackedSetting<Neo4jSink>("neo4jPassword", { it.neo4jPassword }, envAccessor)

  private val envSettings =
      listOf(
          kafkaConnectExternalUri,
          neo4jUri,
          neo4jUser,
          neo4jPassword,
      )

  override fun evaluateExecutionCondition(context: ExtensionContext?): ConditionEvaluationResult {
    val metadata =
        AnnotationSupport.findAnnotation<Neo4jSink>(context)
            ?: throw ExtensionConfigurationException("@Neo4jSink not found")

    val errors = mutableListOf<String>()
    envSettings.forEach {
      if (!it.isValid(metadata)) {
        errors.add(it.errorMessage())
      }
    }
    val topicCount = metadata.topics.size
    if (topicCount != metadata.queries.size) {
      errors.add(
          "Expected $topicCount ${pluralize(topicCount, "query", "queries")}, but got ${metadata.queries.size}. There must be as many topics (here: ${topicCount}) as queries defined.")
    }
    if (errors.isNotEmpty()) {
      throw ExtensionConfigurationException(
          "\nMissing settings, see details below:\n\t${errors.joinToString("\n\t")}",
      )
    }
    this.sinkAnnotation = metadata

    return ConditionEvaluationResult.enabled("@Neo4jSink and environment properly configured")
  }

  override fun beforeEach(extensionContext: ExtensionContext?) {
    sink =
        Neo4jSinkRegistration(
            neo4jUri = neo4jUri.read(sinkAnnotation),
            neo4jUser = neo4jUser.read(sinkAnnotation),
            neo4jPassword = neo4jPassword.read(sinkAnnotation),
            topicQuerys = sinkAnnotation.topics.zip(sinkAnnotation.queries).toMap())
    sink.register(kafkaConnectExternalUri.read(sinkAnnotation))
  }

  override fun afterEach(extensionContent: ExtensionContext?) {
    sink.unregister()
  }
}
