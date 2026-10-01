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
 * A connector setting used to authenticate to Neo4j. [name] is relative to `neo4j.authentication.`,
 * for example `AuthenticationSetting("type", "oidc")` or `AuthenticationSetting("oidc.clientId",
 * "client")`. When a connector declares none, it uses basic authentication with the test
 * environment's Neo4j user.
 */
annotation class AuthenticationSetting(val name: String, val value: String)

/** The connector settings, with their full names. */
internal fun Array<AuthenticationSetting>.toSettings(): Map<String, String> = associate {
  "neo4j.authentication.${it.name}" to it.value
}
