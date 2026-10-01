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
package org.neo4j.connectors.kafka.testing

/**
 * The Keycloak test realm of the test environment, see `docker/keycloak`. Addresses are the ones
 * Kafka Connect reaches Keycloak at, inside the docker network.
 */
object KeycloakSupport {
  const val ISSUER = "https://keycloak:8443/realms/neo4j-sso-test"
  const val CLIENT_ID = "neo4j-commons-client"
  const val CLIENT_SECRET = "QNrSpbh0mxhnlYlI21UcBaz3Htb734vi"
  const val USERNAME = "john-tester"
  const val PASSWORD = "testerpwd"
  const val TRUST_STORE_FILE = "/tmp/keycloak/keycloak.jks"
  const val TRUST_STORE_PASSWORD = "testpwd"

  /** The realm caps the client's access tokens at this many seconds. */
  const val ACCESS_TOKEN_LIFESPAN_SECONDS = 3L
}
