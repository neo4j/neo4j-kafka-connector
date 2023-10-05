/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.connectors.kafka.configuration.helpers

import java.io.File
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigException
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.neo4j.connectors.kafka.configuration.AuthenticationType
import org.neo4j.connectors.kafka.configuration.ConnectorType

class ValidatorsTest {

  @Test
  fun `and should chain validators correctly`() {
    assertDoesNotThrow {
      Validators.and().ensureValid("my.property", "value")
      Validators.and(ConfigDef.Validator { _, _ -> run {} }).ensureValid("my.property", "value")
      Validators.and(ConfigDef.Validator { _, _ -> run {} }, ConfigDef.Validator { _, _ -> run {} })
          .ensureValid("my.property", "value")
    }

    assertFailsWith(ConfigException::class) {
          Validators.and(
                  ConfigDef.Validator { name, value ->
                    throw ConfigException(name, value, "Invalid data")
                  })
              .ensureValid("my.property", "abc")
        }
        .also {
          assertEquals("Invalid value abc for configuration my.property: Invalid data", it.message)
        }

    assertFailsWith(ConfigException::class) {
          Validators.and(
                  ConfigDef.Validator { _, _ -> run {} },
                  ConfigDef.Validator { name, value ->
                    throw ConfigException(name, value, "Invalid data")
                  })
              .ensureValid("my.property", "abc")
        }
        .also {
          assertEquals("Invalid value abc for configuration my.property: Invalid data", it.message)
        }
  }

  @Test
  fun `or should chain validators correctly`() {
    assertDoesNotThrow {
      Validators.or().ensureValid("my.property", "value")
      Validators.or(ConfigDef.Validator { _, _ -> run {} }).ensureValid("my.property", "value")
      Validators.or(
              ConfigDef.Validator { _, _ -> run {} },
              ConfigDef.Validator { name, value ->
                throw ConfigException(name, value, "Invalid data")
              })
          .ensureValid("my.property", "value")
      Validators.or(ConfigDef.Validator { _, _ -> run {} }, ConfigDef.Validator { _, _ -> run {} })
          .ensureValid("my.property", "value")
    }

    assertFailsWith(ConfigException::class) {
          Validators.or(
                  ConfigDef.Validator { name, value ->
                    throw ConfigException(name, value, "Invalid data 1")
                  },
                  ConfigDef.Validator { name, value ->
                    throw ConfigException(name, value, "Invalid data 2")
                  })
              .ensureValid("my.property", "abc")
        }
        .also {
          assertEquals(
              "Invalid value abc for configuration my.property: Invalid data 2", it.message)
        }
  }

  @Test
  fun `blank should validate correctly`() {
    assertDoesNotThrow {
      Validators.blank().ensureValid("my.property", "")
      Validators.blank().ensureValid("my.property", listOf<Any>())
    }

    assertFailsWith(ConfigException::class) { Validators.blank().ensureValid("my.property", "abc") }
        .also {
          assertEquals(
              "Invalid value abc for configuration my.property: Must be blank.", it.message)
        }

    assertFailsWith(ConfigException::class) {
          Validators.blank().ensureValid("my.property", listOf("abc"))
        }
        .also {
          assertEquals(
              "Invalid value [abc] for configuration my.property: Must be empty.", it.message)
        }
  }

  @Test
  fun `string should validate against valid entries`() {
    listOf(null, 1, true, ConnectorType.SINK).forEach { v ->
      assertFailsWith(ConfigException::class) {
            Validators.string().apply { this.ensureValid("my.property", v) }
          }
          .also {
            assertEquals(
                "Invalid value $v for configuration my.property: Must be a String or a List.",
                it.message)
          }
    }

    listOf("SAML", listOf("NONE", "SAML")).forEach { v ->
      assertFailsWith(ConfigException::class) {
            Validators.string("NONE", "BASIC").apply { this.ensureValid("my.property", v) }
          }
          .also {
            assertEquals(
                "Invalid value SAML for configuration my.property: Must be one of: 'NONE', 'BASIC'.",
                it.message)
          }
    }

    assertDoesNotThrow {
      Validators.string("NONE", "CUSTOM").ensureValid("my.property", "NONE")
      Validators.string("NONE", "CUSTOM").ensureValid("my.property", "CUSTOM")
      Validators.string("NONE", "CUSTOM").ensureValid("my.property", listOf("CUSTOM"))
      Validators.string("NONE", "CUSTOM").ensureValid("my.property", listOf("CUSTOM", "NONE"))
    }
  }

  @Test
  fun `enum should validate against valid entries`() {
    listOf(null, 1, true, ConnectorType.SINK).forEach { v ->
      assertFailsWith(ConfigException::class) {
            Validators.enum(AuthenticationType::class.java).apply {
              this.ensureValid("my.property", v)
            }
          }
          .also {
            assertEquals(
                "Invalid value $v for configuration my.property: Must be a String or a List.",
                it.message)
          }
    }

    listOf("SAML", listOf("NONE", "SAML")).forEach { v ->
      assertFailsWith(ConfigException::class) {
            Validators.enum(AuthenticationType::class.java).apply {
              this.ensureValid("my.property", v)
            }
          }
          .also {
            assertEquals(
                "Invalid value SAML for configuration my.property: Must be one of: 'NONE', 'BASIC', 'KERBEROS', 'BEARER', 'CUSTOM'.",
                it.message)
          }
    }

    assertDoesNotThrow {
      Validators.enum(AuthenticationType::class.java).ensureValid("my.property", "NONE")
      Validators.enum(AuthenticationType::class.java).ensureValid("my.property", "CUSTOM")
      Validators.enum(AuthenticationType::class.java).ensureValid("my.property", listOf("CUSTOM"))
      Validators.enum(AuthenticationType::class.java)
          .ensureValid("my.property", listOf("CUSTOM", "BEARER"))
    }
  }

  @Test
  fun `pattern should validate against provided regex`() {
    listOf(null, 1, true, ConnectorType.SINK).forEach { v ->
      assertFailsWith(ConfigException::class) {
            Validators.pattern(".*").apply { this.ensureValid("my.property", v) }
          }
          .also {
            assertEquals(
                "Invalid value $v for configuration my.property: Must be a String or a List.",
                it.message)
          }
    }

    listOf("abc", listOf("abc"), listOf("123", "abc")).forEach { v ->
      assertFailsWith(ConfigException::class) {
            Validators.pattern("\\d+").apply { this.ensureValid("my.property", v) }
          }
          .also {
            assertEquals(
                "Invalid value abc for configuration my.property: Must match pattern '\\d+'.",
                it.message)
          }
    }

    assertDoesNotThrow {
      Validators.pattern("\\d+").ensureValid("my.property", "1")
      Validators.pattern("\\d+").ensureValid("my.property", listOf("1"))
      Validators.pattern("\\d+").ensureValid("my.property", listOf("1", "2"))
    }
  }

  @Test
  fun `uri should validate uris and schemes when provided`() {
    listOf(null, 1, true, ConnectorType.SINK).forEach { v ->
      assertFailsWith(ConfigException::class) {
            Validators.uri().apply { this.ensureValid("my.property", v) }
          }
          .also {
            assertEquals(
                "Invalid value $v for configuration my.property: Must be a String or a List.",
                it.message)
          }
    }

    listOf("", listOf<Any>()).forEach { v ->
      assertFailsWith(ConfigException::class) {
            Validators.uri().apply { this.ensureValid("my.property", v) }
          }
          .also {
            assertEquals(
                "Invalid value $v for configuration my.property: Must be non-empty.", it.message)
          }
    }

    assertFailsWith(ConfigException::class) {
          Validators.uri().apply { this.ensureValid("my.property", "fxz:\\sab.set") }
        }
        .also { assertContains(it.message!!, "Must be a valid URI") }

    listOf(
            "ftp://localhost/a.tar",
            listOf("ftp://localhost/a.tar"),
            listOf("http://localhost", "ftp://localhost/a.tar"))
        .forEach { v ->
          assertFailsWith(ConfigException::class) {
                Validators.uri("http", "https").apply { this.ensureValid("my.property", v) }
              }
              .also {
                assertEquals(
                    "Invalid value ftp://localhost/a.tar for configuration my.property: Scheme must be one of: 'http', 'https'.",
                    it.message)
              }
        }

    assertDoesNotThrow {
      Validators.uri("http", "https").ensureValid("my.property", "http://localhost")
      Validators.uri("http", "https").ensureValid("my.property", "https://localhost")
      Validators.uri("http", "https").ensureValid("my.property", listOf("https://localhost"))
      Validators.uri("http", "https")
          .ensureValid("my.property", listOf("http://localhost", "https://localhost"))
    }
  }

  @Test
  fun `file should validate correctly`() {
    listOf(null, 1, true, ConnectorType.SINK).forEach { v ->
      assertFailsWith(ConfigException::class) { Validators.file().ensureValid("my.property", v) }
          .also {
            assertEquals(
                "Invalid value $v for configuration my.property: Must be a String or a List.",
                it.message)
          }
    }

    listOf("", listOf<Any>()).forEach { v ->
      assertFailsWith(ConfigException::class) { Validators.file().ensureValid("my.property", v) }
          .also {
            assertEquals(
                "Invalid value $v for configuration my.property: Must be non-empty.", it.message)
          }
    }

    assertFailsWith(ConfigException::class) {
          Validators.file().ensureValid("my.property", "deneme.txt")
        }
        .also {
          assertEquals(
              "Invalid value deneme.txt for configuration my.property: Must be an absolute path.",
              it.message)
        }

    assertFailsWith(ConfigException::class) {
          Validators.file().ensureValid("my.property", listOf("deneme.txt"))
        }
        .also {
          assertEquals(
              "Invalid value deneme.txt for configuration my.property: Must be an absolute path.",
              it.message)
        }

    assertFailsWith(ConfigException::class) {
          val f = File.createTempFile("test", ".tmp")
          f.deleteOnExit()

          Validators.file().ensureValid("my.property", f.parentFile.absolutePath)
        }
        .also { assertContains(it.message!!, "Must be a file") }

    assertDoesNotThrow {
      val f = File.createTempFile("test", ".tmp")
      f.deleteOnExit()

      Validators.file().ensureValid("my.property", f.absolutePath)
      Validators.file().ensureValid("my.property", listOf(f.absolutePath))
    }
  }
}
