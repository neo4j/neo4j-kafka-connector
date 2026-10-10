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
package org.neo4j.connectors.kafka.sink

import io.kotest.assertions.nondeterministic.eventually
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.delay
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.junit.jupiter.api.Test
import org.neo4j.connectors.kafka.testing.AuthenticationSetting
import org.neo4j.connectors.kafka.testing.KeycloakSupport
import org.neo4j.connectors.kafka.testing.TestSupport.runTest
import org.neo4j.connectors.kafka.testing.format.KafkaConverter.AVRO
import org.neo4j.connectors.kafka.testing.format.KeyValueConverter
import org.neo4j.connectors.kafka.testing.kafka.ConvertingKafkaProducer
import org.neo4j.connectors.kafka.testing.sink.CypherStrategy
import org.neo4j.connectors.kafka.testing.sink.Neo4jSink
import org.neo4j.connectors.kafka.testing.sink.Neo4jSinkRegistration
import org.neo4j.connectors.kafka.testing.sink.TopicProducer
import org.neo4j.driver.Session

/**
 * Authenticates to Neo4j with tokens obtained from the test environment's Keycloak, through the
 * `oidc` provider. Declared and undeclared `neo4j.authentication.oidc.*` settings are both used.
 */
@KeyValueConverter(key = AVRO, value = AVRO)
class Neo4jSinkOidcIT {
  companion object {
    private const val TOPIC = "oidc"
  }

  @Neo4jSink(
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
      cypher = [CypherStrategy(TOPIC, "CREATE (p:Person) SET p.id = event.id")],
  )
  @Test
  fun `should keep writing after access tokens expire`(
      @TopicProducer(TOPIC) producer: ConvertingKafkaProducer,
      session: Session,
      sink: Neo4jSinkRegistration,
  ) = runTest {
    val schema = SchemaBuilder.struct().field("id", Schema.INT64_SCHEMA).build()
    val count = 4

    // spread the records over several token lifespans, so that the driver has to re-authenticate
    (1..count).forEach { id ->
      producer.publish(valueSchema = schema, value = Struct(schema).put("id", id.toLong()))
      eventually(30.seconds) {
        session
            .run("MATCH (p:Person {id: \$id}) RETURN count(p) AS c", mapOf("id" to id))
            .single()
            .get("c")
            .asLong() shouldBe 1
      }
      delay((KeycloakSupport.ACCESS_TOKEN_LIFESPAN_SECONDS + 1).seconds)
    }

    session.run("MATCH (p:Person) RETURN count(p) AS c").single().get("c").asLong() shouldBe
        count.toLong()
    val tasks = sink.getConnectorTasksForStatusCheck()
    tasks shouldHaveSize 1
    tasks.get(0).get("state").asText() shouldBe "RUNNING"
  }
}
