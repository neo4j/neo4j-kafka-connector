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
package org.neo4j.connectors.kafka.sink.strategy.cdc

import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.collections.shouldMatchInOrder
import io.kotest.matchers.maps.shouldContain
import io.kotest.matchers.maps.shouldContainKey
import io.kotest.matchers.shouldBe
import java.util.stream.Stream
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
import org.neo4j.connectors.kafka.sink.strategy.TestUtils.createKnowsRelationshipEvent
import org.neo4j.connectors.kafka.sink.strategy.TestUtils.createNodePersonEvent
import org.neo4j.connectors.kafka.sink.strategy.TestUtils.newChangeEventMessage
import org.neo4j.connectors.kafka.sink.strategy.TestUtils.randomChangeEvent

@Suppress("UNCHECKED_CAST")
class NativeBatchStrategyTest {

  @ParameterizedTest
  @ArgumentsSource(CypherProvider::class)
  fun `should generate correct statement`(neo4j: Neo4j, expectedQuery: String) {
    val strategy = NativeBatchStrategy(neo4j, 2, 1000, "", SinkStrategy.CDC_SCHEMA)
    val transformer = CdcSchemaEventTransformer("my-topic")

    val result =
        strategy.handle(
            listOf(
                newChangeEventMessage(createNodePersonEvent("id", 1), 0, 0, 0),
                newChangeEventMessage(createKnowsRelationshipEvent(1, 2, 3), 0, 1, 1),
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

    val params = changeQuery.query.parameters().asMap()
    params shouldContain ("q0" to 1L)
    params shouldContain ("q1" to 0L)
    params shouldContainKey "events"

    val events = params["events"] as List<Map<String, Any>>
    events shouldHaveSize 2
    events[0] shouldContain ("offset" to 0L)
    events[0] shouldContain ("q" to 0L)
    events[0] shouldContainKey "params"

    events[1] shouldContain ("offset" to 1L)
    events[1] shouldContain ("q" to 1L)
    events[1] shouldContainKey "params"
  }

  object CypherProvider : ArgumentsProvider {
    private fun callSubqueryImportWith(): String =
        """
        |UNWIND ${'$'}events AS e
        |WITH e ORDER BY e.offset ASC
        |CALL { WITH e
        |  WITH * WHERE e.q = ${'$'}q0
        |  WITH e.params AS _e MATCH (start:`Person` {`id`: _e.start.matchProperties.`id`}) MATCH (end:`Person` {`id`: _e.end.matchProperties.`id`}) MERGE (start)-[r:`KNOWS` {`id`: _e.matchProperties.`id`}]->(end) SET r += _e.setProperties
        |  RETURN 0 AS x
        |  UNION ALL
        |  WITH * WHERE e.q = ${'$'}q1
        |  WITH e.params AS _e MERGE (n:`Person` {`id`: _e.matchProperties.`id`}) SET n += _e.setProperties
        |  RETURN 1 AS x
        |}
        |RETURN COUNT(1) AS total
        """
            .trimMargin()

    private fun callSubqueryWithVariableScope(): String =
        """
        |UNWIND ${'$'}events AS e
        |WITH e ORDER BY e.offset ASC
        |CALL (e) {
        |  WITH * WHERE e.q = ${'$'}q0
        |  WITH e.params AS _e MATCH (start:`Person` {`id`: _e.start.matchProperties.`id`}) MATCH (end:`Person` {`id`: _e.end.matchProperties.`id`}) MERGE (start)-[r:`KNOWS` {`id`: _e.matchProperties.`id`}]->(end) SET r += _e.setProperties
        |  RETURN 0 AS x
        |  UNION ALL
        |  WITH * WHERE e.q = ${'$'}q1
        |  WITH e.params AS _e MERGE (n:`Person` {`id`: _e.matchProperties.`id`}) SET n += _e.setProperties SET n:$(_e.addLabels) REMOVE n:$(_e.removeLabels)
        |  RETURN 1 AS x
        |}
        |FINISH
        """
            .trimMargin()

    private fun callSubqueryWithConditionals(): String =
        """
        |CYPHER 25
        |UNWIND ${'$'}events AS e
        |WITH e ORDER BY e.offset ASC
        |CALL (e) {
        |  WHEN e.q = ${'$'}q0 THEN {
        |    WITH e.params AS _e MATCH (start:$(_e.start.matchLabels) {`id`: _e.start.matchProperties.`id`}) MATCH (end:$(_e.end.matchLabels) {`id`: _e.end.matchProperties.`id`}) MERGE (start)-[r:$(_e.matchType) {`id`: _e.matchProperties.`id`}]->(end) SET r += _e.setProperties
        |  }
        |  WHEN e.q = ${'$'}q1 THEN {
        |    WITH e.params AS _e MERGE (n:$(_e.matchLabels) {`id`: _e.matchProperties.`id`}) SET n += _e.setProperties SET n:$(_e.addLabels) REMOVE n:$(_e.removeLabels)
        |  }
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
          Arguments.of(neo4j5_27_aura, callSubqueryWithConditionals()),
          Arguments.of(neo4j2026_1, callSubqueryWithConditionals()),
          Arguments.of(neo4j2026_1_aura, callSubqueryWithConditionals()),
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(CypherWithEOSOffsetLabelProvider::class)
  fun `should generate correct statement for exactly once semantics`(
      neo4j: Neo4j,
      expectedQuery: String,
  ) {
    val strategy = NativeBatchStrategy(neo4j, 2, 1000, "__KafkaOffset", SinkStrategy.CDC_SCHEMA)
    val transformer = CdcSchemaEventTransformer("my-topic")

    val result =
        strategy.handle(
            listOf(
                newChangeEventMessage(createNodePersonEvent("id", 1), 0, 0, 0),
                newChangeEventMessage(createKnowsRelationshipEvent(1, 2, 3), 0, 1, 1),
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

    val params = changeQuery.query.parameters().asMap()
    params shouldContain ("q0" to 1L)
    params shouldContain ("q1" to 0L)
    params shouldContainKey "events"

    val events = params["events"] as List<Map<String, Any>>
    events shouldHaveSize 2
    events[0] shouldContain ("offset" to 0L)
    events[0] shouldContain ("q" to 0L)
    events[0] shouldContainKey "params"

    events[1] shouldContain ("offset" to 1L)
    events[1] shouldContain ("q" to 1L)
    events[1] shouldContainKey "params"
  }

  object CypherWithEOSOffsetLabelProvider : ArgumentsProvider {
    private fun callSubqueryImportWith(): String =
        """
        |UNWIND ${'$'}events AS e
        |MERGE (k:__KafkaOffset {strategy: ${'$'}strategy, topic: ${'$'}topic, partition: ${'$'}partition}) ON CREATE SET k.offset = -1
        |WITH k, e WHERE e.offset > k.offset
        |WITH k, e ORDER BY e.offset ASC
        |CALL { WITH e
        |  WITH * WHERE e.q = ${'$'}q0
        |  WITH e.params AS _e MATCH (start:`Person` {`id`: _e.start.matchProperties.`id`}) MATCH (end:`Person` {`id`: _e.end.matchProperties.`id`}) MERGE (start)-[r:`KNOWS` {`id`: _e.matchProperties.`id`}]->(end) SET r += _e.setProperties
        |  RETURN 0 AS x
        |  UNION ALL
        |  WITH * WHERE e.q = ${'$'}q1
        |  WITH e.params AS _e MERGE (n:`Person` {`id`: _e.matchProperties.`id`}) SET n += _e.setProperties
        |  RETURN 1 AS x
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
        |  WITH * WHERE e.q = ${'$'}q0
        |  WITH e.params AS _e MATCH (start:`Person` {`id`: _e.start.matchProperties.`id`}) MATCH (end:`Person` {`id`: _e.end.matchProperties.`id`}) MERGE (start)-[r:`KNOWS` {`id`: _e.matchProperties.`id`}]->(end) SET r += _e.setProperties
        |  RETURN 0 AS x
        |  UNION ALL
        |  WITH * WHERE e.q = ${'$'}q1
        |  WITH e.params AS _e MERGE (n:`Person` {`id`: _e.matchProperties.`id`}) SET n += _e.setProperties SET n:$(_e.addLabels) REMOVE n:$(_e.removeLabels)
        |  RETURN 1 AS x
        |}
        |WITH k, max(e.offset) AS newOffset SET k.offset = newOffset
        |FINISH
        """
            .trimMargin()

    private fun callSubqueryWithConditionals(): String =
        """
        |CYPHER 25
        |UNWIND ${'$'}events AS e
        |MERGE (k:__KafkaOffset {strategy: ${'$'}strategy, topic: ${'$'}topic, partition: ${'$'}partition}) ON CREATE SET k.offset = -1
        |WITH k, e WHERE e.offset > k.offset
        |WITH k, e ORDER BY e.offset ASC
        |CALL (e) {
        |  WHEN e.q = ${'$'}q0 THEN {
        |    WITH e.params AS _e MATCH (start:$(_e.start.matchLabels) {`id`: _e.start.matchProperties.`id`}) MATCH (end:$(_e.end.matchLabels) {`id`: _e.end.matchProperties.`id`}) MERGE (start)-[r:$(_e.matchType) {`id`: _e.matchProperties.`id`}]->(end) SET r += _e.setProperties
        |  }
        |  WHEN e.q = ${'$'}q1 THEN {
        |    WITH e.params AS _e MERGE (n:$(_e.matchLabels) {`id`: _e.matchProperties.`id`}) SET n += _e.setProperties SET n:$(_e.addLabels) REMOVE n:$(_e.removeLabels)
        |  }
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
          Arguments.of(neo4j5_27_aura, callSubqueryWithConditionals()),
          Arguments.of(neo4j2026_1, callSubqueryWithConditionals()),
          Arguments.of(neo4j2026_1_aura, callSubqueryWithConditionals()),
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(EosEnabledProvider::class)
  fun `should split changes over batch size`(eosEnabled: Boolean) {
    val strategy =
        NativeBatchStrategy(
            neo4j2026_1,
            50,
            2,
            if (eosEnabled) "__KafkaOffset" else "",
            SinkStrategy.CDC_SCHEMA,
        )
    val transformer = CdcSchemaEventTransformer("my-topic")

    val result =
        strategy.handle(
            listOf(
                newChangeEventMessage(randomChangeEvent(), 0, 0, 0),
                newChangeEventMessage(randomChangeEvent(), 0, 1, 1),
                newChangeEventMessage(randomChangeEvent(), 0, 2, 2),
                newChangeEventMessage(randomChangeEvent(), 1, 0, 3),
                newChangeEventMessage(randomChangeEvent(), 1, 1, 4),
                newChangeEventMessage(randomChangeEvent(), 2, 0, 5),
                newChangeEventMessage(randomChangeEvent(), 3, 0, 6),
            )
        ) {
          transformer.transform(it)
        }

    result.shouldHaveSize(1)
    result
        .first()
        .shouldHaveSize(4)
        .shouldMatchInOrder(
            { it.messages shouldHaveSize 2 },
            { it.messages shouldHaveSize 2 },
            { it.messages shouldHaveSize 2 },
            { it.messages shouldHaveSize 1 },
        )
  }

  @ParameterizedTest
  @ArgumentsSource(EosEnabledProvider::class)
  fun `should split changes over max batched statement count`(eosEnabled: Boolean) {
    val strategy =
        NativeBatchStrategy(
            neo4j2026_1,
            2,
            1000,
            if (eosEnabled) "__KafkaOffset" else "",
            SinkStrategy.CDC_SCHEMA,
        )
    val transformer = CdcSchemaEventTransformer("my-topic")

    val result =
        strategy.handle(
            listOf(
                newChangeEventMessage(createNodePersonEvent("id", 42), 0, 0, 0),
                newChangeEventMessage(createNodePersonEvent("name", "John"), 0, 1, 1),
                newChangeEventMessage(createNodePersonEvent("id", 23), 1, 0, 2),
                newChangeEventMessage(createKnowsRelationshipEvent(1, 2, 3), 2, 0, 3),
            )
        ) {
          transformer.transform(it)
        }

    result.shouldHaveSize(1)
    result
        .first()
        .shouldHaveSize(2)
        .shouldMatchInOrder({ it.messages shouldHaveSize 3 }, { it.messages shouldHaveSize 1 })
  }

  object EosEnabledProvider : ArgumentsProvider {
    override fun provideArguments(
        parameters: ParameterDeclarations?,
        context: ExtensionContext?,
    ): Stream<out Arguments?>? {
      return Stream.of(Arguments.of(true), Arguments.of(false))
    }
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
