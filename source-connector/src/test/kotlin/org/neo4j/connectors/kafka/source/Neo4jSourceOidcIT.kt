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
package org.neo4j.connectors.kafka.source

import io.kotest.matchers.shouldBe
import java.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.delay
import org.junit.jupiter.api.Test
import org.neo4j.connectors.kafka.testing.AuthenticationSetting
import org.neo4j.connectors.kafka.testing.KeycloakSupport
import org.neo4j.connectors.kafka.testing.MapSupport.excludingKeys
import org.neo4j.connectors.kafka.testing.TestSupport.runTest
import org.neo4j.connectors.kafka.testing.assertions.TopicVerifier
import org.neo4j.connectors.kafka.testing.format.KafkaConverter.AVRO
import org.neo4j.connectors.kafka.testing.format.KeyValueConverter
import org.neo4j.connectors.kafka.testing.kafka.ConvertingKafkaConsumer
import org.neo4j.connectors.kafka.testing.source.Neo4jSource
import org.neo4j.connectors.kafka.testing.source.SourceStrategy
import org.neo4j.connectors.kafka.testing.source.TopicConsumer
import org.neo4j.driver.Session

/**
 * Authenticates to Neo4j with tokens obtained from the test environment's Keycloak, through the
 * `oidc` provider. Declared and undeclared `neo4j.authentication.oidc.*` settings are both used.
 */
@KeyValueConverter(key = AVRO, value = AVRO)
class Neo4jSourceOidcIT {
  companion object {
    private const val TOPIC = "oidc"
  }

  @Neo4jSource(
      authentication =
          [
              AuthenticationSetting("type", "oidc"),
              AuthenticationSetting("oidc.issuer", KeycloakSupport.ISSUER),
              AuthenticationSetting("oidc.grantType", "password"),
              AuthenticationSetting("oidc.clientId", KeycloakSupport.CLIENT_ID),
              AuthenticationSetting("oidc.clientSecret", KeycloakSupport.CLIENT_SECRET),
              AuthenticationSetting("oidc.username", KeycloakSupport.USERNAME),
              AuthenticationSetting("oidc.password", KeycloakSupport.PASSWORD),
              AuthenticationSetting("oidc.scope", "openid email roles"),
              AuthenticationSetting("oidc.trustStoreFile", KeycloakSupport.TRUST_STORE_FILE),
              AuthenticationSetting(
                  "oidc.trustStorePassword",
                  KeycloakSupport.TRUST_STORE_PASSWORD,
              ),
              // tokens live for three seconds, so the default skew would treat each as expired
              AuthenticationSetting("oidc.refreshSkewSeconds", "0"),
          ],
      topic = TOPIC,
      strategy = SourceStrategy.QUERY,
      streamingProperty = "timestamp",
      startFrom = "EARLIEST",
      query =
          "MATCH (ts:TestSource) WHERE ts.timestamp > \$lastCheck RETURN ts.name AS name, ts.timestamp AS timestamp",
  )
  @Test
  fun `should keep reading after access tokens expire`(
      @TopicConsumer(topic = TOPIC, offset = "earliest") consumer: ConvertingKafkaConsumer,
      session: Session,
  ) = runTest {
    session.run("CREATE (:TestSource {name: 'jane', timestamp: datetime().epochMillis})").consume()

    TopicVerifier.createForMap(consumer)
        .assertMessageValue { it.excludingKeys("timestamp") shouldBe mapOf("name" to "jane") }
        .verifyWithin(Duration.ofSeconds(30))

    // outlive the token the source polled with, so that the driver has to re-authenticate
    delay((KeycloakSupport.ACCESS_TOKEN_LIFESPAN_SECONDS + 1).seconds)
    session.run("CREATE (:TestSource {name: 'john', timestamp: datetime().epochMillis})").consume()

    TopicVerifier.createForMap(consumer)
        .assertMessageValue { it.excludingKeys("timestamp") shouldBe mapOf("name" to "john") }
        .verifyWithin(Duration.ofSeconds(30))
  }
}
