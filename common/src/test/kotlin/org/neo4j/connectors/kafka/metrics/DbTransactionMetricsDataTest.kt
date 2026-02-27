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
package org.neo4j.connectors.kafka.metrics

import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertNotNull
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.neo4j.connectors.kafka.data.TypesTest.Companion.neo4jImage
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.TransactionConfig
import org.testcontainers.containers.Neo4jContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

@OptIn(ExperimentalCoroutinesApi::class)
@Testcontainers
class DbTransactionMetricsDataTest {

  @Test
  fun `should register gauge and poll for transaction id`() = runTest {
    val metrics = mock<Metrics>()

    val refreshInterval = 100.milliseconds
    val transactionConfig = TransactionConfig.builder().build()

    val driver = GraphDatabase.driver(neo4j.boltUrl, AuthTokens.none())
    val dispatcher = this.coroutineContext[kotlinx.coroutines.CoroutineDispatcher]!!

    DbTransactionMetricsData(
            metrics = metrics,
            refreshInterval = refreshInterval,
            neo4jDriver = driver,
            databaseName = "",
            transactionConfig = transactionConfig,
            dispatcher = dispatcher,
        )
        .use {
          val gaugeCaptor = argumentCaptor<() -> Long?>()
          verify(metrics)
              .addGauge(
                  eq("last_db_tx_id"),
                  eq("The transaction commit timestamp of the last processed CDC message"),
                  any(),
                  gaugeCaptor.capture(),
              )

          val initialValue = gaugeCaptor.firstValue()
          assertNotNull(initialValue, "captured transaction id should not be null")

          commitTransaction(driver)
          runCurrent()

          val firstIncrement = gaugeCaptor.firstValue()
          assertNotNull(firstIncrement, "the first increment of transaction id should not be null")
          assertTrue("transaction id should increment") { initialValue < firstIncrement }

          commitTransaction(driver)
          advanceTimeBy(refreshInterval)
          runCurrent()

          val secondIncrement = gaugeCaptor.firstValue()
          assertNotNull(
              secondIncrement,
              "the second increment of transaction id should not be null",
          )
          assertEquals(firstIncrement + 1, secondIncrement, "transaction id should increment")
        }
  }

  @Test
  fun `should stop polling when stop is called`() = runTest {
    val metrics = mock<Metrics>()

    val refreshInterval = 100.milliseconds
    val transactionConfig = TransactionConfig.builder().build()

    val driver = GraphDatabase.driver(neo4j.boltUrl, AuthTokens.none())
    val dispatcher = this.coroutineContext[kotlinx.coroutines.CoroutineDispatcher]!!

    DbTransactionMetricsData(
            metrics = metrics,
            refreshInterval = refreshInterval,
            neo4jDriver = driver,
            databaseName = "",
            transactionConfig = transactionConfig,
            dispatcher = dispatcher,
        )
        .use { data ->
          val gaugeCaptor = argumentCaptor<() -> Long?>()
          verify(metrics)
              .addGauge(
                  eq("last_db_tx_id"),
                  eq("The transaction commit timestamp of the last processed CDC message"),
                  any(),
                  gaugeCaptor.capture(),
              )

          val initialValue = gaugeCaptor.firstValue()
          assertNotNull(initialValue, "captured transaction id should not be null")

          commitTransaction(driver)
          runCurrent()

          val firstIncrement = gaugeCaptor.firstValue()
          assertNotNull(firstIncrement, "the first increment of transaction id should not be null")
          assertTrue("transaction id should increment") { initialValue < firstIncrement }

          data.close()
          commitTransaction(driver)
          advanceTimeBy(refreshInterval)
          runCurrent()

          val secondIncrement = gaugeCaptor.firstValue()
          assertNotNull(
              secondIncrement,
              "the second increment of transaction id should not be null",
          )
          assertEquals(firstIncrement, secondIncrement, "transaction id should not increment")
        }
  }

  companion object {

    @Container
    val neo4j: Neo4jContainer<*> =
        Neo4jContainer(neo4jImage())
            .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
            .withExposedPorts(7687)
            .withoutAuthentication()

    private fun commitTransaction(driver: Driver) {
      driver.session().use { session ->
        session
            .run("CREATE (n: TestNode {id: ${'$'}id})", mapOf("id" to UUID.randomUUID().toString()))
            .consume()
      }
    }
  }
}
