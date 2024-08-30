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

import io.kotest.assertions.throwables.shouldNotThrowAny
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.throwable.shouldHaveMessage
import java.io.File
import java.net.URI
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import org.apache.kafka.common.config.ConfigException
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Config.TrustStrategy.Strategy

class Neo4jConfigurationTest {

  @Test
  fun `config should be successful`() {
    shouldNotThrowAny { Neo4jConfiguration.config() }
  }

  @ParameterizedTest
  @EnumSource(ConnectorType::class)
  fun `invalid config`(type: ConnectorType) {
    shouldThrow<ConfigException> {
      Neo4jConfiguration(
          Neo4jConfiguration.config(),
          mapOf(Neo4jConfiguration.URI to "bolt+routing://localhost"),
          type)
    } shouldHaveMessage
        "Invalid value bolt+routing://localhost for configuration neo4j.uri: Scheme must be one of: 'neo4j', 'neo4j+s', 'neo4j+ssc', 'bolt', 'bolt+s', 'bolt+ssc'."

    shouldThrow<ConfigException> {
      Neo4jConfiguration(
          Neo4jConfiguration.config(),
          mapOf(Neo4jConfiguration.URI to "neo4j://localhost,https://localhost"),
          type)
    } shouldHaveMessage
        "Invalid value https://localhost for configuration neo4j.uri: Scheme must be one of: 'neo4j', 'neo4j+s', 'neo4j+ssc', 'bolt', 'bolt+s', 'bolt+ssc'."

    shouldThrow<ConfigException> {
      Neo4jConfiguration(
          Neo4jConfiguration.config(),
          mapOf(
              Neo4jConfiguration.URI to "bolt://localhost",
              Neo4jConfiguration.AUTHENTICATION_TYPE to "unsupported"),
          type)
    } shouldHaveMessage
        "Invalid value unsupported for configuration neo4j.authentication.type: Must be one of: 'NONE', 'BASIC', 'KERBEROS', 'BEARER', 'CUSTOM'."

    shouldThrow<ConfigException> {
      Neo4jConfiguration(
          Neo4jConfiguration.config(),
          mapOf(
              Neo4jConfiguration.URI to "bolt://localhost",
              Neo4jConfiguration.AUTHENTICATION_TYPE to "NONE",
              Neo4jConfiguration.MAX_TRANSACTION_RETRY_TIMEOUT to "5"),
          type)
    } shouldHaveMessage
        "Invalid value 5 for configuration neo4j.max-retry-time: Must match pattern '(\\d+(ms|s|m|h|d))+'."

    shouldThrow<ConfigException> {
      Neo4jConfiguration(
          Neo4jConfiguration.config(),
          mapOf(
              Neo4jConfiguration.URI to "bolt://localhost",
              Neo4jConfiguration.AUTHENTICATION_TYPE to "NONE",
              Neo4jConfiguration.MAX_TRANSACTION_RETRY_TIMEOUT to "5s",
              Neo4jConfiguration.CONNECTION_TIMEOUT to "1"),
          type)
    } shouldHaveMessage
        "Invalid value 1 for configuration neo4j.connection-timeout: Must match pattern '(\\d+(ms|s|m|h|d))+'."

    shouldThrow<ConfigException> {
      Neo4jConfiguration(
          Neo4jConfiguration.config(),
          mapOf(
              Neo4jConfiguration.URI to "bolt://localhost",
              Neo4jConfiguration.AUTHENTICATION_TYPE to "NONE",
              Neo4jConfiguration.MAX_TRANSACTION_RETRY_TIMEOUT to "5s",
              Neo4jConfiguration.CONNECTION_TIMEOUT to "1m",
              Neo4jConfiguration.POOL_MAX_CONNECTION_POOL_SIZE to 0),
          type)
    } shouldHaveMessage
        "Invalid value 0 for configuration neo4j.pool.max-connection-pool-size: Value must be at least 1"

    shouldThrow<ConfigException> {
      Neo4jConfiguration(
          Neo4jConfiguration.config(),
          mapOf(
              Neo4jConfiguration.URI to "bolt://localhost",
              Neo4jConfiguration.AUTHENTICATION_TYPE to "NONE",
              Neo4jConfiguration.MAX_TRANSACTION_RETRY_TIMEOUT to "5s",
              Neo4jConfiguration.CONNECTION_TIMEOUT to "1m",
              Neo4jConfiguration.POOL_MAX_CONNECTION_POOL_SIZE to 5,
              Neo4jConfiguration.POOL_CONNECTION_ACQUISITION_TIMEOUT to "5k"),
          type)
    } shouldHaveMessage
        "Invalid value 5k for configuration neo4j.pool.connection-acquisition-timeout: Must match pattern '(\\d+(ms|s|m|h|d))+'."

    shouldThrow<ConfigException> {
      Neo4jConfiguration(
          Neo4jConfiguration.config(),
          mapOf(
              Neo4jConfiguration.URI to "bolt://localhost",
              Neo4jConfiguration.AUTHENTICATION_TYPE to "NONE",
              Neo4jConfiguration.MAX_TRANSACTION_RETRY_TIMEOUT to "5s",
              Neo4jConfiguration.CONNECTION_TIMEOUT to "1m",
              Neo4jConfiguration.POOL_MAX_CONNECTION_POOL_SIZE to 5,
              Neo4jConfiguration.POOL_CONNECTION_ACQUISITION_TIMEOUT to "5m",
              Neo4jConfiguration.POOL_IDLE_TIME_BEFORE_TEST to "1ns"),
          type)
    } shouldHaveMessage
        "Invalid value 1ns for configuration neo4j.pool.idle-time-before-connection-test: Must match pattern '(\\d+(ms|s|m|h|d))+'."

    shouldThrow<ConfigException> {
      Neo4jConfiguration(
          Neo4jConfiguration.config(),
          mapOf(
              Neo4jConfiguration.URI to "bolt://localhost",
              Neo4jConfiguration.AUTHENTICATION_TYPE to "NONE",
              Neo4jConfiguration.MAX_TRANSACTION_RETRY_TIMEOUT to "5s",
              Neo4jConfiguration.CONNECTION_TIMEOUT to "1m",
              Neo4jConfiguration.POOL_MAX_CONNECTION_POOL_SIZE to 5,
              Neo4jConfiguration.POOL_CONNECTION_ACQUISITION_TIMEOUT to "5m",
              Neo4jConfiguration.POOL_IDLE_TIME_BEFORE_TEST to "1h",
              Neo4jConfiguration.POOL_MAX_CONNECTION_LIFETIME to "1w"),
          type)
    } shouldHaveMessage
        "Invalid value 1w for configuration neo4j.pool.max-connection-lifetime: Must match pattern '(\\d+(ms|s|m|h|d))+'."

    shouldThrow<ConfigException> {
      Neo4jConfiguration(
          Neo4jConfiguration.config(),
          mapOf(
              Neo4jConfiguration.URI to "bolt://localhost",
              Neo4jConfiguration.AUTHENTICATION_TYPE to "NONE",
              Neo4jConfiguration.MAX_TRANSACTION_RETRY_TIMEOUT to "5s",
              Neo4jConfiguration.CONNECTION_TIMEOUT to "1m",
              Neo4jConfiguration.POOL_MAX_CONNECTION_POOL_SIZE to 5,
              Neo4jConfiguration.POOL_CONNECTION_ACQUISITION_TIMEOUT to "5m",
              Neo4jConfiguration.POOL_IDLE_TIME_BEFORE_TEST to "1h",
              Neo4jConfiguration.POOL_MAX_CONNECTION_LIFETIME to "8h",
              Neo4jConfiguration.SECURITY_ENCRYPTED to "enabled"),
          type)
    } shouldHaveMessage
        "Invalid value enabled for configuration neo4j.security.encrypted: Must be one of: 'true', 'false'."

    shouldThrow<ConfigException> {
      Neo4jConfiguration(
          Neo4jConfiguration.config(),
          mapOf(
              Neo4jConfiguration.URI to "bolt://localhost",
              Neo4jConfiguration.AUTHENTICATION_TYPE to "NONE",
              Neo4jConfiguration.MAX_TRANSACTION_RETRY_TIMEOUT to "5s",
              Neo4jConfiguration.CONNECTION_TIMEOUT to "1m",
              Neo4jConfiguration.POOL_MAX_CONNECTION_POOL_SIZE to 5,
              Neo4jConfiguration.POOL_CONNECTION_ACQUISITION_TIMEOUT to "5m",
              Neo4jConfiguration.POOL_IDLE_TIME_BEFORE_TEST to "1h",
              Neo4jConfiguration.POOL_MAX_CONNECTION_LIFETIME to "8h",
              Neo4jConfiguration.SECURITY_ENCRYPTED to "true",
              Neo4jConfiguration.SECURITY_TRUST_STRATEGY to "unknown"),
          type)
    } shouldHaveMessage
        "Invalid value unknown for configuration neo4j.security.trust-strategy: Must be one of: 'TRUST_ALL_CERTIFICATES', 'TRUST_CUSTOM_CA_SIGNED_CERTIFICATES', 'TRUST_SYSTEM_CA_SIGNED_CERTIFICATES'."

    shouldThrow<ConfigException> {
      Neo4jConfiguration(
          Neo4jConfiguration.config(),
          mapOf(
              Neo4jConfiguration.URI to "bolt://localhost",
              Neo4jConfiguration.AUTHENTICATION_TYPE to "NONE",
              Neo4jConfiguration.MAX_TRANSACTION_RETRY_TIMEOUT to "5s",
              Neo4jConfiguration.CONNECTION_TIMEOUT to "1m",
              Neo4jConfiguration.POOL_MAX_CONNECTION_POOL_SIZE to 5,
              Neo4jConfiguration.POOL_CONNECTION_ACQUISITION_TIMEOUT to "5m",
              Neo4jConfiguration.POOL_IDLE_TIME_BEFORE_TEST to "1h",
              Neo4jConfiguration.POOL_MAX_CONNECTION_LIFETIME to "8h",
              Neo4jConfiguration.SECURITY_ENCRYPTED to "true",
              Neo4jConfiguration.SECURITY_TRUST_STRATEGY to "TRUST_SYSTEM_CA_SIGNED_CERTIFICATES",
              Neo4jConfiguration.SECURITY_HOST_NAME_VERIFICATION_ENABLED to "disabled"),
          type)
    } shouldHaveMessage
        "Invalid value disabled for configuration neo4j.security.hostname-verification-enabled: Must be one of: 'true', 'false'."

    shouldThrow<ConfigException> {
      Neo4jConfiguration(
          Neo4jConfiguration.config(),
          mapOf(
              Neo4jConfiguration.URI to "bolt://localhost",
              Neo4jConfiguration.AUTHENTICATION_TYPE to "NONE",
              Neo4jConfiguration.MAX_TRANSACTION_RETRY_TIMEOUT to "5s",
              Neo4jConfiguration.CONNECTION_TIMEOUT to "1m",
              Neo4jConfiguration.POOL_MAX_CONNECTION_POOL_SIZE to 5,
              Neo4jConfiguration.POOL_CONNECTION_ACQUISITION_TIMEOUT to "5m",
              Neo4jConfiguration.POOL_IDLE_TIME_BEFORE_TEST to "1h",
              Neo4jConfiguration.POOL_MAX_CONNECTION_LIFETIME to "8h",
              Neo4jConfiguration.SECURITY_ENCRYPTED to "true",
              Neo4jConfiguration.SECURITY_TRUST_STRATEGY to "TRUST_CUSTOM_CA_SIGNED_CERTIFICATES",
              Neo4jConfiguration.SECURITY_HOST_NAME_VERIFICATION_ENABLED to "false",
              Neo4jConfiguration.SECURITY_CERT_FILES to "non-existing-file.txt"),
          type)
    } shouldHaveMessage
        "Invalid value non-existing-file.txt for configuration neo4j.security.cert-files: Must be an absolute path."
  }

  @ParameterizedTest
  @EnumSource(ConnectorType::class)
  fun `valid config`(type: ConnectorType) {
    val f1 = newTempFile()
    val f2 = newTempFile()

    val config =
        Neo4jConfiguration(
            Neo4jConfiguration.config(),
            mapOf(
                Neo4jConfiguration.URI to "bolt://localhost",
                Neo4jConfiguration.AUTHENTICATION_TYPE to "NONE",
                Neo4jConfiguration.MAX_TRANSACTION_RETRY_TIMEOUT to "5s",
                Neo4jConfiguration.CONNECTION_TIMEOUT to "1m",
                Neo4jConfiguration.POOL_MAX_CONNECTION_POOL_SIZE to 5,
                Neo4jConfiguration.POOL_CONNECTION_ACQUISITION_TIMEOUT to "5m",
                Neo4jConfiguration.POOL_IDLE_TIME_BEFORE_TEST to "1h",
                Neo4jConfiguration.POOL_MAX_CONNECTION_LIFETIME to "8h",
                Neo4jConfiguration.SECURITY_ENCRYPTED to "true",
                Neo4jConfiguration.SECURITY_TRUST_STRATEGY to "TRUST_CUSTOM_CA_SIGNED_CERTIFICATES",
                Neo4jConfiguration.SECURITY_HOST_NAME_VERIFICATION_ENABLED to "false",
                Neo4jConfiguration.SECURITY_CERT_FILES to "${f1.absolutePath},${f2.absolutePath}"),
            type)

    assertEquals(listOf(URI("bolt://localhost")), config.uris)
    assertEquals(AuthTokens.none(), config.authenticationToken)
    assertEquals(5.seconds, config.maxRetryTime)
    assertEquals(1.minutes, config.connectionTimeout)
    assertEquals(5, config.maxConnectionPoolSize)
    assertEquals(5.minutes, config.connectionAcquisitionTimeout)
    assertEquals(1.hours, config.idleTimeBeforeTest)
    assertEquals(8.hours, config.maxConnectionLifetime)
    assertTrue(config.encrypted)
    config.trustStrategy.run {
      assertEquals(Strategy.TRUST_CUSTOM_CA_SIGNED_CERTIFICATES, this.strategy())
      assertFalse(this.isHostnameVerificationEnabled)
      assertEquals(listOf(f1, f2), this.certFiles())
    }
  }

  @Test
  fun `auth tokens`() {
    Neo4jConfiguration(
            Neo4jConfiguration.config(),
            mapOf(
                Neo4jConfiguration.URI to "bolt://localhost",
                Neo4jConfiguration.AUTHENTICATION_TYPE to "NONE"),
            ConnectorType.SINK)
        .run { assertEquals(AuthTokens.none(), this.authenticationToken) }

    Neo4jConfiguration(
            Neo4jConfiguration.config(),
            mapOf(
                Neo4jConfiguration.URI to "bolt://localhost",
                Neo4jConfiguration.AUTHENTICATION_TYPE to "BASIC",
                Neo4jConfiguration.AUTHENTICATION_BASIC_USERNAME to "neo4j",
                Neo4jConfiguration.AUTHENTICATION_BASIC_PASSWORD to "password",
                Neo4jConfiguration.AUTHENTICATION_BASIC_REALM to "realm"),
            ConnectorType.SINK)
        .run {
          assertEquals(AuthTokens.basic("neo4j", "password", "realm"), this.authenticationToken)
        }

    Neo4jConfiguration(
            Neo4jConfiguration.config(),
            mapOf(
                Neo4jConfiguration.URI to "bolt://localhost",
                Neo4jConfiguration.AUTHENTICATION_TYPE to "KERBEROS",
                Neo4jConfiguration.AUTHENTICATION_KERBEROS_TICKET to "ticket"),
            ConnectorType.SINK)
        .run { assertEquals(AuthTokens.kerberos("ticket"), this.authenticationToken) }

    Neo4jConfiguration(
            Neo4jConfiguration.config(),
            mapOf(
                Neo4jConfiguration.URI to "bolt://localhost",
                Neo4jConfiguration.AUTHENTICATION_TYPE to "BEARER",
                Neo4jConfiguration.AUTHENTICATION_BEARER_TOKEN to "token"),
            ConnectorType.SINK)
        .run { assertEquals(AuthTokens.bearer("token"), this.authenticationToken) }

    Neo4jConfiguration(
            Neo4jConfiguration.config(),
            mapOf(
                Neo4jConfiguration.URI to "bolt://localhost",
                Neo4jConfiguration.AUTHENTICATION_TYPE to "CUSTOM",
                Neo4jConfiguration.AUTHENTICATION_CUSTOM_PRINCIPAL to "principal",
                Neo4jConfiguration.AUTHENTICATION_CUSTOM_CREDENTIALS to "creds",
                Neo4jConfiguration.AUTHENTICATION_CUSTOM_REALM to "realm",
                Neo4jConfiguration.AUTHENTICATION_CUSTOM_SCHEME to "scheme",
            ),
            ConnectorType.SINK)
        .run {
          assertEquals(
              AuthTokens.custom("principal", "creds", "realm", "scheme"), this.authenticationToken)
        }
  }

  companion object {
    fun newTempFile(prefix: String = "test", suffix: String = ".tmp"): File {
      val f = File.createTempFile(prefix, suffix)
      f.deleteOnExit()
      return f
    }
  }
}
