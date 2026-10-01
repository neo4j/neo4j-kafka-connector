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
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.throwable.shouldHaveMessage
import java.io.File
import java.net.URI
import java.util.Optional
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import org.apache.kafka.common.config.Config
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.config.ConfigValue
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.connect.errors.ConnectException
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.junit.jupiter.params.provider.ValueSource
import org.neo4j.connectors.driver.auth.oidc.clientauth.ClientAuthMethod
import org.neo4j.connectors.driver.auth.oidc.grant.GrantType
import org.neo4j.driver.AuthToken
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
          type,
      )
    } shouldHaveMessage
        "Invalid value bolt+routing://localhost for configuration neo4j.uri: Scheme must be one of: 'neo4j', 'neo4j+s', 'neo4j+ssc', 'bolt', 'bolt+s', 'bolt+ssc'."

    shouldThrow<ConfigException> {
      Neo4jConfiguration(
          Neo4jConfiguration.config(),
          mapOf(Neo4jConfiguration.URI to "neo4j://localhost,https://localhost"),
          type,
      )
    } shouldHaveMessage
        "Invalid value https://localhost for configuration neo4j.uri: Scheme must be one of: 'neo4j', 'neo4j+s', 'neo4j+ssc', 'bolt', 'bolt+s', 'bolt+ssc'."

    shouldThrow<ConfigException> {
      Neo4jConfiguration(
          Neo4jConfiguration.config(),
          mapOf(
              Neo4jConfiguration.URI to "bolt://localhost",
              Neo4jConfiguration.AUTHENTICATION_TYPE to "unsupported",
          ),
          type,
      )
    } shouldHaveMessage
        "Invalid value unsupported for configuration neo4j.authentication.type: No authentication provider is registered under this name, available names are 'basic', 'bearer', 'custom', 'fake', 'kerberos', 'none', 'oidc'."

    shouldThrow<ConfigException> {
      Neo4jConfiguration(
          Neo4jConfiguration.config(),
          mapOf(
              Neo4jConfiguration.URI to "bolt://localhost",
              Neo4jConfiguration.AUTHENTICATION_TYPE to "NONE",
              Neo4jConfiguration.MAX_TRANSACTION_RETRY_TIMEOUT to "5",
          ),
          type,
      )
    } shouldHaveMessage
        "Invalid value 5 for configuration neo4j.max-retry-time: Must match pattern '(\\d+(ms|s|m|h|d))+'."

    shouldThrow<ConfigException> {
      Neo4jConfiguration(
          Neo4jConfiguration.config(),
          mapOf(
              Neo4jConfiguration.URI to "bolt://localhost",
              Neo4jConfiguration.AUTHENTICATION_TYPE to "NONE",
              Neo4jConfiguration.MAX_TRANSACTION_RETRY_TIMEOUT to "5s",
              Neo4jConfiguration.CONNECTION_TIMEOUT to "1",
          ),
          type,
      )
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
              Neo4jConfiguration.POOL_MAX_CONNECTION_POOL_SIZE to 0,
          ),
          type,
      )
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
              Neo4jConfiguration.POOL_CONNECTION_ACQUISITION_TIMEOUT to "5k",
          ),
          type,
      )
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
              Neo4jConfiguration.POOL_IDLE_TIME_BEFORE_TEST to "1ns",
          ),
          type,
      )
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
              Neo4jConfiguration.POOL_MAX_CONNECTION_LIFETIME to "1w",
          ),
          type,
      )
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
              Neo4jConfiguration.SECURITY_ENCRYPTED to "enabled",
          ),
          type,
      )
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
              Neo4jConfiguration.SECURITY_TRUST_STRATEGY to "unknown",
          ),
          type,
      )
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
              Neo4jConfiguration.SECURITY_HOST_NAME_VERIFICATION_ENABLED to "disabled",
          ),
          type,
      )
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
              Neo4jConfiguration.SECURITY_CERT_FILES to "non-existing-file.txt",
          ),
          type,
      )
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
                Neo4jConfiguration.SECURITY_CERT_FILES to "${f1.absolutePath},${f2.absolutePath}",
            ),
            type,
        )

    assertEquals(listOf(URI("bolt://localhost")), config.uris)
    assertEquals(AuthTokens.none(), config.authToken())
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

  @ParameterizedTest
  @ValueSource(strings = ["NONE", "none"])
  fun `none auth token should be backward compatible`(authType: String) {
    authToken(Neo4jConfiguration.AUTHENTICATION_TYPE to authType) shouldBe AuthTokens.none()
  }

  @ParameterizedTest
  @ValueSource(strings = ["BASIC", "basic"])
  fun `basic auth token should be backward compatible`(authType: String) {
    authToken(
        Neo4jConfiguration.AUTHENTICATION_TYPE to authType,
        Neo4jConfiguration.AUTHENTICATION_BASIC_USERNAME to "neo4j",
        Neo4jConfiguration.AUTHENTICATION_BASIC_PASSWORD to "password",
        Neo4jConfiguration.AUTHENTICATION_BASIC_REALM to "realm",
    ) shouldBe AuthTokens.basic("neo4j", "password", "realm")

    // the realm defaults to "", which is not passed on as a realm
    authToken(
        Neo4jConfiguration.AUTHENTICATION_TYPE to authType,
        Neo4jConfiguration.AUTHENTICATION_BASIC_USERNAME to "neo4j",
        Neo4jConfiguration.AUTHENTICATION_BASIC_PASSWORD to "password",
    ) shouldBe AuthTokens.basic("neo4j", "password")
  }

  @Test
  fun `basic auth type should be the default`() {
    authToken(
        Neo4jConfiguration.AUTHENTICATION_BASIC_USERNAME to "neo4j",
        Neo4jConfiguration.AUTHENTICATION_BASIC_PASSWORD to "password",
    ) shouldBe AuthTokens.basic("neo4j", "password")
  }

  @ParameterizedTest
  @ValueSource(strings = ["KERBEROS", "kerberos"])
  fun `kerberos auth token should be backward compatible`(authType: String) {
    authToken(
        Neo4jConfiguration.AUTHENTICATION_TYPE to authType,
        Neo4jConfiguration.AUTHENTICATION_KERBEROS_TICKET to "ticket",
    ) shouldBe AuthTokens.kerberos("ticket")
  }

  @ParameterizedTest
  @ValueSource(strings = ["BEARER", "bearer"])
  fun `bearer auth token should be backward compatible`(authType: String) {
    authToken(
        Neo4jConfiguration.AUTHENTICATION_TYPE to authType,
        Neo4jConfiguration.AUTHENTICATION_BEARER_TOKEN to "token",
    ) shouldBe AuthTokens.bearer("token")
  }

  @ParameterizedTest
  @ValueSource(strings = ["CUSTOM", "custom"])
  fun `custom auth token should be backward compatible`(authType: String) {
    authToken(
        Neo4jConfiguration.AUTHENTICATION_TYPE to authType,
        Neo4jConfiguration.AUTHENTICATION_CUSTOM_PRINCIPAL to "principal",
        Neo4jConfiguration.AUTHENTICATION_CUSTOM_CREDENTIALS to "creds",
        Neo4jConfiguration.AUTHENTICATION_CUSTOM_REALM to "realm",
        Neo4jConfiguration.AUTHENTICATION_CUSTOM_SCHEME to "scheme",
    ) shouldBe AuthTokens.custom("principal", "creds", "realm", "scheme")

    // the realm defaults to "", which is not passed on as a realm
    authToken(
        Neo4jConfiguration.AUTHENTICATION_TYPE to authType,
        Neo4jConfiguration.AUTHENTICATION_CUSTOM_PRINCIPAL to "principal",
        Neo4jConfiguration.AUTHENTICATION_CUSTOM_CREDENTIALS to "creds",
        Neo4jConfiguration.AUTHENTICATION_CUSTOM_SCHEME to "scheme",
    ) shouldBe AuthTokens.custom("principal", "creds", null, "scheme")
  }

  @Test
  fun `should report invalid provider configuration as connect exception`() {
    val config =
        configuration(
            Neo4jConfiguration.AUTHENTICATION_TYPE to "BASIC",
            Neo4jConfiguration.AUTHENTICATION_BASIC_USERNAME to "neo4j",
        )

    shouldThrow<ConnectException> { config.createAuthTokenManager() } shouldHaveMessage
        "Invalid authentication configuration for 'basic': Authentication scheme 'basic' requires a password but none was configured"
  }

  @Test
  fun `should pass prefixed keys to the provider`() {
    val authConfig =
        Neo4jConfiguration.authConfig(
            mapOf(
                "neo4j.authentication.type" to "oidc",
                "neo4j.authentication.oidc.clientId" to "client",
                "neo4j.authentication.oidc.clientSecret" to Password("secret"),
                "neo4j.authentication.oidc.extraParam.foo" to "bar",
                "neo4j.authentication.oidc.assertion.claim.sub" to "subject",
                "neo4j.authentication.oidc.username" to "user",
                "neo4j.authentication.oidc.password" to "pass",
                "neo4j.authentication.oidc.scope" to "",
                "neo4j.authentication.oidc.audience" to " ",
                "neo4j.authentication.oidc.profile" to null,
                "neo4j.authentication.oidcx.clientId" to "other",
                "neo4j.authentication.basic.username" to "neo4j",
                "neo4j.uri" to "neo4j://localhost",
            ),
            "oidc",
        )

    authConfig.username() shouldBe Optional.of("user")
    authConfig.password() shouldBe Optional.of("pass")
    authConfig.asMap() shouldBe
        mapOf(
            "clientId" to "client",
            "clientSecret" to "secret",
            "extraParam.foo" to "bar",
            "assertion.claim.sub" to "subject",
        )
  }

  @Test
  fun `should leave username and password empty when not configured`() {
    val authConfig =
        Neo4jConfiguration.authConfig(
            mapOf(
                "neo4j.authentication.basic.username" to "",
                "neo4j.authentication.basic.realm" to "",
            ),
            "basic",
        )

    authConfig.username() shouldBe Optional.empty()
    authConfig.password() shouldBe Optional.empty()
    authConfig.asMap() shouldBe emptyMap()
  }

  @Test
  fun `should resolve auth type`() {
    val names = setOf("basic", "oidc", "Dup", "dup", "DUP-2", "dup-2")

    Neo4jConfiguration.resolveAuthType("basic", names) shouldBe "basic"
    Neo4jConfiguration.resolveAuthType("BASIC", names) shouldBe "basic"
    Neo4jConfiguration.resolveAuthType("OIDC", names) shouldBe "oidc"
    Neo4jConfiguration.resolveAuthType("Dup", names) shouldBe "Dup"
    Neo4jConfiguration.resolveAuthType("dup", names) shouldBe "dup"

    shouldThrow<ConfigException> {
      Neo4jConfiguration.resolveAuthType("DUP", names)
    } shouldHaveMessage
        "Invalid value DUP for configuration neo4j.authentication.type: Name matches more than one authentication provider: 'Dup', 'dup'."

    shouldThrow<ConfigException> {
      Neo4jConfiguration.resolveAuthType("saml", names)
    } shouldHaveMessage
        "Invalid value saml for configuration neo4j.authentication.type: No authentication provider is registered under this name, available names are 'basic', 'oidc', 'Dup', 'dup', 'DUP-2', 'dup-2'."
  }

  @Test
  fun `should recommend registered auth types`() {
    Neo4jConfiguration.config()
        .configKeys()[Neo4jConfiguration.AUTHENTICATION_TYPE]!!
        .recommender
        .validValues(Neo4jConfiguration.AUTHENTICATION_TYPE, mutableMapOf()) shouldBe
        listOf("basic", "bearer", "custom", "fake", "kerberos", "none", "oidc")
  }

  @ParameterizedTest
  @ValueSource(strings = ["OIDC", "oidc"])
  fun `should show oidc fields only for oidc`(authType: String) {
    val oidcFields =
        Neo4jConfiguration.config().configKeys().keys.filter {
          it.startsWith("neo4j.authentication.oidc.")
        }
    oidcFields shouldHaveSize 17

    val shown =
        Neo4jConfiguration.config()
            .validate(
                mapOf(
                    Neo4jConfiguration.URI to "neo4j://localhost",
                    Neo4jConfiguration.AUTHENTICATION_TYPE to authType,
                )
            )
            .associateBy { it.name() }
    oidcFields.forEach { shown.getValue(it).visible() shouldBe true }
    shown.getValue(Neo4jConfiguration.AUTHENTICATION_BASIC_USERNAME).visible() shouldBe false

    val hidden =
        Neo4jConfiguration.config()
            .validate(mapOf(Neo4jConfiguration.URI to "neo4j://localhost"))
            .associateBy { it.name() }
    oidcFields.forEach { hidden.getValue(it).visible() shouldBe false }
  }

  @Test
  fun `should declare oidc secrets as passwords`() {
    Neo4jConfiguration.config()
        .configKeys()
        .values
        .filter { it.name.startsWith("neo4j.authentication.oidc.") }
        .filter { it.type == ConfigDef.Type.PASSWORD }
        .map { it.name } shouldBe
        listOf(
            Neo4jConfiguration.AUTHENTICATION_OIDC_CLIENT_SECRET,
            Neo4jConfiguration.AUTHENTICATION_OIDC_PASSWORD,
            Neo4jConfiguration.AUTHENTICATION_OIDC_REFRESH_TOKEN,
            Neo4jConfiguration.AUTHENTICATION_OIDC_TRUST_STORE_PASSWORD,
        )
  }

  @Test
  fun `should recommend the values the oidc provider accepts`() {
    // the lists are kept here so that the provider stays a runtime dependency, check them for drift
    Neo4jConfiguration.OIDC_GRANT_TYPES shouldBe GrantType.entries.map { it.settingValue() }
    Neo4jConfiguration.OIDC_CLIENT_AUTH_METHODS shouldBe
        ClientAuthMethod.entries.map { it.settingValue() }
    Neo4jConfiguration.OIDC_PROFILES.forEach {
      javaClass.classLoader.getResource("profiles/$it.properties").shouldNotBeNull()
    }

    val values =
        Neo4jConfiguration.config()
            .validate(
                mapOf(
                    Neo4jConfiguration.URI to "neo4j://localhost",
                    Neo4jConfiguration.AUTHENTICATION_TYPE to "oidc",
                )
            )
            .associateBy { it.name() }
    values.getValue(Neo4jConfiguration.AUTHENTICATION_OIDC_GRANT_TYPE).recommendedValues() shouldBe
        Neo4jConfiguration.OIDC_GRANT_TYPES
    values
        .getValue(Neo4jConfiguration.AUTHENTICATION_OIDC_CLIENT_AUTH_METHOD)
        .recommendedValues() shouldBe Neo4jConfiguration.OIDC_CLIENT_AUTH_METHODS
    values.getValue(Neo4jConfiguration.AUTHENTICATION_OIDC_PROFILE).recommendedValues() shouldBe
        Neo4jConfiguration.OIDC_PROFILES
  }

  @Test
  fun `should document every setting`() {
    Neo4jConfiguration.config()
        .configKeys()
        .values
        .filter { it.documentation.isNullOrBlank() }
        .map { it.name } shouldBe emptyList()
  }

  @Test
  fun `should pass declared and undeclared oidc settings to the provider`() {
    val authConfig =
        Neo4jConfiguration.authConfig(
            configuration(
                    Neo4jConfiguration.AUTHENTICATION_TYPE to "oidc",
                    Neo4jConfiguration.AUTHENTICATION_OIDC_CLIENT_ID to "client",
                    Neo4jConfiguration.AUTHENTICATION_OIDC_CLIENT_SECRET to "secret",
                    Neo4jConfiguration.AUTHENTICATION_OIDC_USERNAME to "user",
                    Neo4jConfiguration.AUTHENTICATION_OIDC_PASSWORD to "pass",
                    "neo4j.authentication.oidc.extraParam.foo" to "bar",
                )
                .originals(),
            "oidc",
        )

    authConfig.username() shouldBe Optional.of("user")
    authConfig.password() shouldBe Optional.of("pass")
    authConfig.asMap() shouldBe
        mapOf("clientId" to "client", "clientSecret" to "secret", "extraParam.foo" to "bar")
  }

  @ParameterizedTest
  @ValueSource(strings = ["BASIC", "basic", "Basic"])
  fun `should show auth type fields case-insensitively`(authType: String) {
    val values =
        Neo4jConfiguration.config()
            .validate(
                mapOf(
                    Neo4jConfiguration.URI to "neo4j://localhost",
                    Neo4jConfiguration.AUTHENTICATION_TYPE to authType,
                )
            )
            .associateBy { it.name() }

    values[Neo4jConfiguration.AUTHENTICATION_BASIC_USERNAME]!!.visible() shouldBe true
    values[Neo4jConfiguration.AUTHENTICATION_BASIC_PASSWORD]!!.visible() shouldBe true
    values[Neo4jConfiguration.AUTHENTICATION_BASIC_REALM]!!.visible() shouldBe true
    values[Neo4jConfiguration.AUTHENTICATION_KERBEROS_TICKET]!!.visible() shouldBe false
    values[Neo4jConfiguration.AUTHENTICATION_BEARER_TOKEN]!!.visible() shouldBe false
    values[Neo4jConfiguration.AUTHENTICATION_CUSTOM_PRINCIPAL]!!.visible() shouldBe false
  }

  @Test
  fun `should create oidc token manager from passthrough keys`() {
    shouldNotThrowAny {
      configuration(
              Neo4jConfiguration.AUTHENTICATION_TYPE to "oidc",
              "neo4j.authentication.oidc.tokenEndpoint" to "https://idp.example.com/token",
              "neo4j.authentication.oidc.clientId" to "client",
              "neo4j.authentication.oidc.clientSecret" to "secret",
          )
          .createAuthTokenManager()
    }
  }

  @Test
  fun `should create token manager from a third-party provider`() {
    FakeAuthTokenManagerFactory.lastConfig = null

    authToken(
        Neo4jConfiguration.AUTHENTICATION_TYPE to "FAKE",
        "neo4j.authentication.fake.principal" to "someone",
        "neo4j.authentication.fake.nested.key" to "value",
        "neo4j.authentication.fake.password" to "secret",
    ) shouldBe AuthTokens.custom("someone", "", null, "fake")

    FakeAuthTokenManagerFactory.lastConfig.shouldNotBeNull {
      password() shouldBe Optional.of("secret")
      asMap() shouldBe mapOf("principal" to "someone", "nested.key" to "value")
    }
  }

  @Test
  fun `validate should not duplicate errors of declared parameters`() {
    val values =
        validate(
            Neo4jConfiguration.AUTHENTICATION_TYPE to "BASIC",
            Neo4jConfiguration.AUTHENTICATION_BASIC_PASSWORD to "password",
        )

    values.errors(Neo4jConfiguration.AUTHENTICATION_BASIC_USERNAME) shouldBe
        listOf(
            "Invalid value for configuration neo4j.authentication.basic.username: Must not be blank."
        )
    values.errors(Neo4jConfiguration.AUTHENTICATION_TYPE) shouldBe emptyList()
  }

  @Test
  fun `validate should report unknown auth type without adding errors`() {
    // Kafka repeats a parse error once for every key that lists this one as a dependent
    validate(Neo4jConfiguration.AUTHENTICATION_TYPE to "saml")
        .errors(Neo4jConfiguration.AUTHENTICATION_TYPE)
        .toSet() shouldBe
        setOf(
            "Invalid value saml for configuration neo4j.authentication.type: No authentication provider is registered under this name, available names are 'basic', 'bearer', 'custom', 'fake', 'kerberos', 'none', 'oidc'."
        )
  }

  @ParameterizedTest
  @ValueSource(strings = ["NONE", "basic"])
  fun `validate should accept valid built-in configuration`(authType: String) {
    validate(
            Neo4jConfiguration.AUTHENTICATION_TYPE to authType,
            Neo4jConfiguration.AUTHENTICATION_BASIC_USERNAME to "neo4j",
            Neo4jConfiguration.AUTHENTICATION_BASIC_PASSWORD to "password",
        )
        .values
        .flatMap { it.errorMessages() } shouldBe emptyList()
  }

  // The messages below come from the provider and are shown to the user as they are.

  @Test
  fun `validate should report missing oidc endpoint`() {
    val values =
        validate(
            Neo4jConfiguration.AUTHENTICATION_TYPE to "oidc",
            "neo4j.authentication.oidc.clientId" to "client",
            "neo4j.authentication.oidc.clientSecret" to "secret",
        )

    values.errors(Neo4jConfiguration.AUTHENTICATION_TYPE) shouldBe
        listOf(
            "oidc: One of the authentication parameters 'issuer', 'discoveryUrl' or 'tokenEndpoint' is required, but none was configured"
        )
    listOf(
            Neo4jConfiguration.AUTHENTICATION_OIDC_ISSUER,
            Neo4jConfiguration.AUTHENTICATION_OIDC_DISCOVERY_URL,
            Neo4jConfiguration.AUTHENTICATION_OIDC_TOKEN_ENDPOINT,
        )
        .forEach {
          values.errors(it) shouldBe
              listOf(
                  "One of the authentication parameters 'issuer', 'discoveryUrl' or 'tokenEndpoint' is required, but none was configured"
              )
        }
  }

  @Test
  fun `validate should report missing oidc client id`() {
    val values =
        validate(
            Neo4jConfiguration.AUTHENTICATION_TYPE to "oidc",
            "neo4j.authentication.oidc.tokenEndpoint" to "https://idp.example.com/token",
            "neo4j.authentication.oidc.clientSecret" to "secret",
        )

    values.errors(Neo4jConfiguration.AUTHENTICATION_TYPE) shouldBe
        listOf("oidc: Authentication parameter 'clientId' is required but was not configured")
    values.errors(Neo4jConfiguration.AUTHENTICATION_OIDC_CLIENT_ID) shouldBe
        listOf("Authentication parameter 'clientId' is required but was not configured")
  }

  @Test
  fun `validate should report invalid oidc grant type`() {
    val values =
        validate(
            Neo4jConfiguration.AUTHENTICATION_TYPE to "oidc",
            "neo4j.authentication.oidc.tokenEndpoint" to "https://idp.example.com/token",
            "neo4j.authentication.oidc.clientId" to "client",
            "neo4j.authentication.oidc.clientSecret" to "secret",
            "neo4j.authentication.oidc.grantType" to "magic",
        )

    values.errors(Neo4jConfiguration.AUTHENTICATION_TYPE) shouldBe
        listOf(
            "oidc: Authentication parameter 'grantType' has invalid value 'magic'; expected one of client_credentials, password, refresh_token, jwt_bearer, token_exchange"
        )
    values.errors(Neo4jConfiguration.AUTHENTICATION_OIDC_GRANT_TYPE) shouldBe
        listOf(
            "Authentication parameter 'grantType' has invalid value 'magic'; expected one of client_credentials, password, refresh_token, jwt_bearer, token_exchange"
        )
  }

  @Test
  fun `validate should not read oidc files or start http clients`() {
    val missing = File(System.getProperty("java.io.tmpdir"), "missing-${UUID.randomUUID()}")
    val httpClientThreads = {
      Thread.getAllStackTraces().keys.count { it.name.startsWith("HttpClient") }
    }
    val threadsBefore = httpClientThreads()

    validate(
            Neo4jConfiguration.AUTHENTICATION_TYPE to "oidc",
            "neo4j.authentication.oidc.tokenEndpoint" to "https://idp.example.com/token",
            "neo4j.authentication.oidc.clientId" to "client",
            "neo4j.authentication.oidc.clientAuthMethod" to "private_key_jwt",
            "neo4j.authentication.oidc.privateKeyFile" to "${missing.absolutePath}.pem",
            "neo4j.authentication.oidc.trustStoreFile" to "${missing.absolutePath}.p12",
        )
        .values
        .flatMap { it.errorMessages() } shouldBe emptyList()

    httpClientThreads() shouldBe threadsBefore
  }

  @Test
  fun `validate should validate third-party providers without creating token managers`() {
    FakeAuthTokenManagerFactory.lastConfig = null

    validate(Neo4jConfiguration.AUTHENTICATION_TYPE to "fake")
        .errors(Neo4jConfiguration.AUTHENTICATION_TYPE) shouldBe
        listOf("fake: Authentication parameter 'principal' is required but was not configured")

    validate(
            Neo4jConfiguration.AUTHENTICATION_TYPE to "fake",
            "neo4j.authentication.fake.principal" to "someone",
        )
        .values
        .flatMap { it.errorMessages() } shouldBe emptyList()

    FakeAuthTokenManagerFactory.lastConfig shouldBe null
  }

  @Test
  fun `validate should report plain provider errors on auth type only`() {
    validate(
            Neo4jConfiguration.AUTHENTICATION_TYPE to "fake",
            "neo4j.authentication.fake.plainError" to "something is wrong",
        )
        .errors(Neo4jConfiguration.AUTHENTICATION_TYPE) shouldBe listOf("fake: something is wrong")
  }

  @Test
  fun `should add authentication error to type and to declared parameters among keys`() {
    val values =
        listOf(
                Neo4jConfiguration.AUTHENTICATION_TYPE,
                "neo4j.authentication.oidc.clientId",
                "neo4j.authentication.oidc.clientSecret",
                "neo4j.authentication.oidc.grantType",
                "neo4j.authentication.basic.clientId",
            )
            .associateWith { ConfigValue(it) }
    val message = "Something is wrong with 'grantType'"

    Neo4jConfiguration.addAuthenticationError(
        values,
        "oidc",
        message,
        listOf("clientId", "clientSecret", "clientId", "undeclared.key"),
    )

    values.errors(Neo4jConfiguration.AUTHENTICATION_TYPE) shouldBe listOf("oidc: $message")
    values.errors("neo4j.authentication.oidc.clientId") shouldBe listOf(message)
    values.errors("neo4j.authentication.oidc.clientSecret") shouldBe listOf(message)
    // only keys count, not words quoted in the message
    values.errors("neo4j.authentication.oidc.grantType") shouldBe emptyList()
    values.errors("neo4j.authentication.basic.clientId") shouldBe emptyList()
  }

  @Test
  fun `internal variables`() {
    Neo4jConfiguration(
            Neo4jConfiguration.config(),
            mapOf(
                Neo4jConfiguration.URI to "bolt://localhost",
                Neo4jConfiguration.TASK_ID to "1",
                Neo4jConfiguration.CONNECTOR_NAME to "neo4j-connector",
            ),
            ConnectorType.SINK,
        )
        .run {
          assertEquals("1", this.taskId)
          assertEquals("neo4j-connector", this.connectorName)
        }
  }

  private fun validate(vararg settings: Pair<String, String>): Map<String, ConfigValue> {
    val originals = mapOf(Neo4jConfiguration.URI to "bolt://localhost", *settings)
    val config = Config(Neo4jConfiguration.config().validate(originals))
    Neo4jConfiguration.validate(config, originals)
    return config.configValues().associateBy { it.name() }
  }

  private fun Map<String, ConfigValue>.errors(name: String): List<String> =
      this.getValue(name).errorMessages()

  private fun configuration(vararg settings: Pair<String, Any>): Neo4jConfiguration =
      Neo4jConfiguration(
          Neo4jConfiguration.config(),
          mapOf(Neo4jConfiguration.URI to "bolt://localhost", *settings),
          ConnectorType.SINK,
      )

  private fun authToken(vararg settings: Pair<String, Any>): AuthToken =
      configuration(*settings).authToken()

  private fun Neo4jConfiguration.authToken(): AuthToken =
      createAuthTokenManager().token.toCompletableFuture().get()

  companion object {
    fun newTempFile(prefix: String = "test", suffix: String = ".tmp"): File {
      val f = File.createTempFile(prefix, suffix)
      f.deleteOnExit()
      return f
    }
  }
}
