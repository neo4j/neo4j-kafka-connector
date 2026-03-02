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
package org.neo4j.connectors.kafka.sink.strategy

import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.collections.shouldMatchInOrder
import io.kotest.matchers.shouldBe
import java.util.stream.Stream
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.ArgumentsProvider
import org.junit.jupiter.params.provider.ArgumentsSource
import org.junit.jupiter.params.support.ParameterDeclarations
import org.neo4j.caniuse.Neo4j
import org.neo4j.caniuse.Neo4jDeploymentType
import org.neo4j.caniuse.Neo4jEdition
import org.neo4j.caniuse.Neo4jVersion
import org.neo4j.connectors.kafka.sink.SinkStrategy
import org.neo4j.connectors.kafka.sink.strategy.cdc.CdcSchemaEventTransformer

class ApocBatchStrategyTest {

  @ParameterizedTest
  @ArgumentsSource(CypherProvider::class)
  fun `should generate correct statement`(neo4j: Neo4j, expectedQuery: String) {
    val strategy = ApocBatchStrategy(neo4j, 2, "", SinkStrategy.CDC_SCHEMA)
    val transformer = CdcSchemaEventTransformer("my-topic")

    val result =
        strategy.handle(
            listOf(
                TestUtils.newChangeEventMessage(TestUtils.randomChangeEvent(), 0, 0, 0),
                TestUtils.newChangeEventMessage(TestUtils.randomChangeEvent(), 0, 1, 1),
            )
        ) {
          transformer.transform(it)
        }

    result.shouldHaveSize(1)
    val batch = result.first()
    batch.shouldHaveSize(1)
    val changeQuery = batch.first()
    changeQuery.messages shouldHaveSize 2
    changeQuery.query.text() shouldBe expectedQuery
  }

  object CypherProvider : ArgumentsProvider {
    private fun callSubqueryImportWith(): String =
        """
        |UNWIND ${'$'}events AS e
        |WITH e ORDER BY e.offset ASC
        |CALL { WITH e
        |  CALL apoc.cypher.doIt(e.stmt, e.params) YIELD value RETURN COUNT(1) AS total
        |}
        |RETURN COUNT(1) AS total
        """
            .trimMargin()

    private fun callSubqueryWithVariableScope(): String =
        """
        |UNWIND ${'$'}events AS e
        |WITH e ORDER BY e.offset ASC
        |CALL (e) {
        |  CALL apoc.cypher.doIt(e.stmt, e.params) YIELD value RETURN COUNT(1) AS total
        |}
        |FINISH
        """
            .trimMargin()

    override fun provideArguments(
        parameters: ParameterDeclarations?,
        context: ExtensionContext?,
    ): Stream<out Arguments?>? {
      return Stream.of(
          Arguments.of(neo4j4_4, callSubqueryImportWith()),
          Arguments.of(neo4j5_18, callSubqueryImportWith()),
          Arguments.of(neo4j5_26, callSubqueryWithVariableScope()),
          Arguments.of(neo4j5_27_aura, callSubqueryWithVariableScope()),
          Arguments.of(neo4j2026_1, callSubqueryWithVariableScope()),
          Arguments.of(neo4j2026_1_aura, callSubqueryWithVariableScope()),
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(CypherWithEOSOffsetLabelProvider::class)
  fun `should generate correct statement for exactly once semantics`(
      neo4j: Neo4j,
      expectedQuery: String,
  ) {
    val strategy = ApocBatchStrategy(neo4j, 2, "__KafkaOffset", SinkStrategy.CDC_SCHEMA)
    val transformer = CdcSchemaEventTransformer("my-topic")

    val result =
        strategy.handle(
            listOf(
                TestUtils.newChangeEventMessage(TestUtils.randomChangeEvent(), 0, 0, 0),
                TestUtils.newChangeEventMessage(TestUtils.randomChangeEvent(), 0, 1, 1),
            )
        ) {
          transformer.transform(it)
        }

    result.shouldHaveSize(1)
    val batch = result.first()
    batch.shouldHaveSize(1)
    val changeQuery = batch.first()
    changeQuery.messages shouldHaveSize 2
    changeQuery.query.text() shouldBe expectedQuery
  }

  object CypherWithEOSOffsetLabelProvider : ArgumentsProvider {
    private fun callSubqueryImportWith(): String =
        """
        |UNWIND ${'$'}events AS e
        |MERGE (k:__KafkaOffset {strategy: ${'$'}strategy, topic: ${'$'}topic, partition: ${'$'}partition}) ON CREATE SET k.offset = -1
        |WITH k, e WHERE e.offset > k.offset
        |WITH k, e ORDER BY e.offset ASC
        |CALL { WITH e
        |  CALL apoc.cypher.doIt(e.stmt, e.params) YIELD value RETURN COUNT(1) AS total
        |}
        |WITH k, max(e.offset) AS newOffset SET k.offset = newOffset
        |RETURN COUNT(1) AS total
        """
            .trimMargin()

    private fun callSubqueryWithVariableScope(): String =
        """
        |UNWIND ${'$'}events AS e
        |MERGE (k:__KafkaOffset {strategy: ${'$'}strategy, topic: ${'$'}topic, partition: ${'$'}partition}) ON CREATE SET k.offset = -1
        |WITH k, e WHERE e.offset > k.offset
        |WITH k, e ORDER BY e.offset ASC
        |CALL (e) {
        |  CALL apoc.cypher.doIt(e.stmt, e.params) YIELD value RETURN COUNT(1) AS total
        |}
        |WITH k, max(e.offset) AS newOffset SET k.offset = newOffset
        |FINISH
        """
            .trimMargin()

    override fun provideArguments(
        parameters: ParameterDeclarations?,
        context: ExtensionContext?,
    ): Stream<out Arguments?>? {
      return Stream.of(
          Arguments.of(neo4j4_4, callSubqueryImportWith()),
          Arguments.of(neo4j5_18, callSubqueryImportWith()),
          Arguments.of(neo4j5_26, callSubqueryWithVariableScope()),
          Arguments.of(neo4j5_27_aura, callSubqueryWithVariableScope()),
          Arguments.of(neo4j2026_1, callSubqueryWithVariableScope()),
          Arguments.of(neo4j2026_1_aura, callSubqueryWithVariableScope()),
      )
    }
  }

  @Test
  fun `should split changes over batch size`() {
    val strategy = ApocBatchStrategy(neo4j2026_1, 2, "", SinkStrategy.CDC_SCHEMA)
    val transformer = CdcSchemaEventTransformer("my-topic")

    val result =
        strategy.handle(
            listOf(
                TestUtils.newChangeEventMessage(TestUtils.randomChangeEvent(), 0, 0, 0),
                TestUtils.newChangeEventMessage(TestUtils.randomChangeEvent(), 0, 1, 1),
                TestUtils.newChangeEventMessage(TestUtils.randomChangeEvent(), 0, 2, 2),
                TestUtils.newChangeEventMessage(TestUtils.randomChangeEvent(), 1, 0, 3),
                TestUtils.newChangeEventMessage(TestUtils.randomChangeEvent(), 1, 1, 4),
                TestUtils.newChangeEventMessage(TestUtils.randomChangeEvent(), 2, 0, 5),
                TestUtils.newChangeEventMessage(TestUtils.randomChangeEvent(), 3, 0, 6),
            )
        ) {
          transformer.transform(it)
        }

    // With the new apoc.cypher.doIt approach, each batch produces one ChangeQuery
    // So 7 messages with batchSize=2 produces 4 batches (2+2+2+1)
    result.shouldHaveSize(4)
    result.shouldMatchInOrder(
        { batch ->
          batch.shouldHaveSize(1)
          batch.first().messages shouldHaveSize 2
        },
        { batch ->
          batch.shouldHaveSize(1)
          batch.first().messages shouldHaveSize 2
        },
        { batch ->
          batch.shouldHaveSize(1)
          batch.first().messages shouldHaveSize 2
        },
        { batch ->
          batch.shouldHaveSize(1)
          batch.first().messages shouldHaveSize 1
        },
    )
  }

  companion object {
    private val neo4j4_4 =
        Neo4j(Neo4jVersion(4, 4), Neo4jEdition.ENTERPRISE, Neo4jDeploymentType.SELF_MANAGED)
    private val neo4j5_18 =
        Neo4j(Neo4jVersion(5, 18), Neo4jEdition.ENTERPRISE, Neo4jDeploymentType.SELF_MANAGED)
    private val neo4j5_26 =
        Neo4j(Neo4jVersion(5, 26), Neo4jEdition.ENTERPRISE, Neo4jDeploymentType.SELF_MANAGED)
    private val neo4j5_27_aura =
        Neo4j(Neo4jVersion(5, 27), Neo4jEdition.ENTERPRISE, Neo4jDeploymentType.AURA)
    private val neo4j2026_1 =
        Neo4j(Neo4jVersion(2026, 1), Neo4jEdition.ENTERPRISE, Neo4jDeploymentType.SELF_MANAGED)
    private val neo4j2026_1_aura =
        Neo4j(Neo4jVersion(2026, 1), Neo4jEdition.ENTERPRISE, Neo4jDeploymentType.AURA)
  }
}
