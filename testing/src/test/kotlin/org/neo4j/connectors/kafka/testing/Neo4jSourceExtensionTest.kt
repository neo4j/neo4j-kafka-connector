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
package org.neo4j.connectors.kafka.testing

import java.lang.reflect.Parameter
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.api.extension.ParameterContext
import org.mockito.kotlin.anyOrNull
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.neo4j.driver.Session

class Neo4jSourceExtensionTest {

  @Test
  fun `resolves Neo4j session`() {
    val extension = Neo4jSourceExtension()
    extension.sourceAnnotation =
        Neo4jSource(
            topic = "unused",
            streamingQuery = "unused",
            streamingFrom = "unused",
            streamingProperty = "unused",
            neo4jExternalUri = "neo4j://example.com",
            neo4jUri = "neo4j://example.com",
            neo4jUser = "neo4j",
            neo4jPassword = "s3cr3t!!",
        )
    val parameter = mock<Parameter> { on { getType() } doReturn Session::class.java }
    val parameterContext = mock<ParameterContext> { on { getParameter() } doReturn parameter }
    val extensionContext = mock<ExtensionContext>()

    val session = extension.resolveParameter(parameterContext, extensionContext)

    assertThat(session).isInstanceOf(Session::class.java)
  }

  @Test
  fun `resolves Kafka consumer`() {
    val consumer = mock<KafkaConsumer<String, GenericRecord>>()
    val consumerSupplier =
        mock<ConsumerSupplier<String, GenericRecord>> {
          on { getSubscribed(anyOrNull(), anyOrNull()) } doReturn consumer
        }
    val extension = Neo4jSourceExtension(consumerSupplier)
    extension.sourceAnnotation =
        Neo4jSource(
            topic = "unused",
            streamingQuery = "unused",
            streamingFrom = "unused",
            streamingProperty = "unused",
            neo4jUri = "unused",
            neo4jUser = "unused",
            neo4jPassword = "unused",
            brokerExternalHost = "example.com",
            schemaControlRegistryExternalUri = "example.com")

    val annotation = TopicConsumer(topic = "topic", offset = "earliest")
    val parameter =
        mock<Parameter> {
          on { getType() } doReturn KafkaConsumer::class.java
          on { getAnnotation(TopicConsumer::class.java) } doReturn annotation
        }
    val parameterContext = mock<ParameterContext> { on { getParameter() } doReturn parameter }
    val extensionContext =
        mock<ExtensionContext> { on { displayName } doReturn "some-running-test" }

    val session = extension.resolveParameter(parameterContext, extensionContext)

    assertThat(session).isInstanceOf(KafkaConsumer::class.java)
    verify(consumerSupplier).getSubscribed(anyOrNull(), anyOrNull())
  }
}
