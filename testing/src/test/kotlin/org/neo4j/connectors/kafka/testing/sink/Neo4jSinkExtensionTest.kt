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

import kotlin.reflect.jvm.javaMethod
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ConditionEvaluationResult
import org.junit.jupiter.api.extension.ExtensionConfigurationException
import org.junit.jupiter.api.extension.ExtensionContext
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock

class Neo4jSinkExtensionTest {

  @Test
  fun `continues execution evaluation if annotation is found and valid`() {
    val extension = Neo4jSinkExtension()
    val extensionContext =
        mock<ExtensionContext> {
          on { requiredTestMethod } doReturn Neo4jSinkExtensionTest::validAnnotatedMethod.javaMethod
        }

    val result = extension.evaluateExecutionCondition(extensionContext)

    assertIs<ConditionEvaluationResult>(result)
    assertFalse { result.isDisabled }
  }

  @Test
  fun `stops execution evaluation if annotation is not found`() {
    val extension = Neo4jSinkExtension()
    val extensionContext =
        mock<ExtensionContext> {
          on { requiredTestMethod } doReturn Neo4jSinkExtensionTest::notAnnotated.javaMethod
        }

    val exception =
        assertFailsWith<ExtensionConfigurationException> {
          extension.evaluateExecutionCondition(extensionContext)
        }
    assertEquals(exception.message, "@Neo4jSink not found")
  }

  @Test
  fun `stops execution evaluation if neo4j URI is not found`() {
    val fakeEnv =
        mapOf(
            "NEO4J_USER" to "user",
            "NEO4J_PASSWORD" to "password",
        )
    val extension = Neo4jSinkExtension(fakeEnv::get)
    val extensionContext =
        mock<ExtensionContext> {
          on { requiredTestMethod } doReturn Neo4jSinkExtensionTest::missingNeo4jUri.javaMethod
        }

    val exception =
        assertFailsWith<ExtensionConfigurationException> {
          extension.evaluateExecutionCondition(extensionContext)
        }
    assertContains(
        exception.message!!,
        "Both annotation field and environment variable NEO4J_URI are unset. Please specify one")
  }

  @Test
  fun `stops execution evaluation if neo4j user is not found`() {
    val fakeEnv =
        mapOf(
            "NEO4J_URI" to "neo4j://example.com",
            "NEO4J_PASSWORD" to "password",
        )
    val extension = Neo4jSinkExtension(fakeEnv::get)
    val extensionContext =
        mock<ExtensionContext> {
          on { requiredTestMethod } doReturn Neo4jSinkExtensionTest::missingNeo4jUri.javaMethod
        }

    val exception =
        assertFailsWith<ExtensionConfigurationException> {
          extension.evaluateExecutionCondition(extensionContext)
        }
    assertContains(
        exception.message!!,
        "Both annotation field and environment variable NEO4J_USER are unset. Please specify one")
  }

  @Test
  fun `stops execution evaluation if neo4j password is not found`() {
    val fakeEnv =
        mapOf(
            "NEO4J_URI" to "neo4j://example.com",
            "NEO4J_USER" to "user",
        )
    val extension = Neo4jSinkExtension(fakeEnv::get)
    val extensionContext =
        mock<ExtensionContext> {
          on { requiredTestMethod } doReturn Neo4jSinkExtensionTest::missingNeo4jUri.javaMethod
        }

    val exception =
        assertFailsWith<ExtensionConfigurationException> {
          extension.evaluateExecutionCondition(extensionContext)
        }
    assertContains(
        exception.message!!,
        "Both annotation field and environment variable NEO4J_PASSWORD are unset. Please specify one")
  }

  @Suppress("UNUSED") fun notAnnotated() {}

  @Neo4jSink(
      neo4jUri = "neo4j://example.com",
      neo4jUser = "user",
      neo4jPassword = "password",
      topics = ["topic1"],
      queries = ["MERGE ()"])
  @Suppress("UNUSED")
  fun validAnnotatedMethod() {}

  @Neo4jSink(topics = ["topic1"], queries = ["MERGE ()"])
  @Suppress("UNUSED")
  fun missingNeo4jUri() {}
}
