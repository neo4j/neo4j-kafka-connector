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
package org.neo4j.connectors.kafka.configuration

import org.neo4j.connectors.driver.auth.AuthConfig
import org.neo4j.connectors.driver.auth.AuthContext
import org.neo4j.connectors.driver.auth.AuthTokenManagerFactory
import org.neo4j.driver.AuthTokenManager
import org.neo4j.driver.AuthTokenManagers
import org.neo4j.driver.AuthTokens

/**
 * Stands in for a third-party provider, registered through `META-INF/services` in the test
 * resources. It records the configuration it was created with.
 */
class FakeAuthTokenManagerFactory : AuthTokenManagerFactory {
  override fun getName(): String = NAME

  override fun create(config: AuthConfig, context: AuthContext): AuthTokenManager {
    lastConfig = config
    val principal = config.require("principal")
    return AuthTokenManagers.basic { AuthTokens.custom(principal, "", null, NAME) }
  }

  companion object {
    const val NAME = "fake"

    @Volatile var lastConfig: AuthConfig? = null
  }
}
