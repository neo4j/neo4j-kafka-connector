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
package org.neo4j.connectors.kafka.sink.strategy.cud

import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.neo4j.caniuse.Neo4j
import org.neo4j.caniuse.Neo4jDetector
import org.neo4j.connectors.kafka.sink.strategy.NativeBatchStrategy
import org.neo4j.connectors.kafka.testing.neo4jDatabase
import org.neo4j.connectors.kafka.testing.neo4jImage
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.testcontainers.containers.Neo4jContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

@Testcontainers
abstract class CudHandlerNativeIT(eosOffsetLabel: String) :
    CudHandlerIT(eosOffsetLabel, NativeBatchStrategy::class) {
  companion object {
    @Container
    val container: Neo4jContainer<*> =
        Neo4jContainer(neo4jImage())
            .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
            .withExposedPorts(7687)
            .withoutAuthentication()
            .waitingFor(neo4jDatabase())

    private lateinit var driver: Driver
    private lateinit var neo4j: Neo4j

    @BeforeAll
    @JvmStatic
    fun setUpContainer() {
      driver = GraphDatabase.driver(container.boltUrl, AuthTokens.none())
      neo4j = Neo4jDetector.detect(driver)
    }

    @AfterAll
    @JvmStatic
    fun tearDownContainer() {
      driver.close()
    }
  }

  override fun container() = container

  override fun driver(): Driver = driver

  override fun neo4j(): Neo4j = neo4j
}

class CudHandlerNativeWithEosOffsetIT : CudHandlerNativeIT("__KafkaOffset") {}

class CudHandlerNativeWithoutEosOffsetIT : CudHandlerNativeIT("") {}
