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
package org.neo4j.connectors.kafka.configuration.helpers

import java.io.File
import java.net.URI
import java.net.URISyntaxException
import java.util.regex.Pattern
import org.apache.kafka.common.config.Config
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.config.types.Password

object Validators {

  fun and(vararg validators: ConfigDef.Validator): ConfigDef.Validator {
    return ConfigDef.Validator { name, value -> validators.forEach { it.ensureValid(name, value) } }
  }

  fun or(vararg validators: ConfigDef.Validator): ConfigDef.Validator {
    return ConfigDef.Validator { name, value ->
      var lastError: ConfigException? = null

      for (i in validators) {
        try {
          i.ensureValid(name, value)
          return@Validator
        } catch (t: ConfigException) {
          lastError = t
        }
      }

      if (lastError != null) {
        throw lastError
      }
    }
  }

  fun blank(): ConfigDef.Validator {
    return ConfigDef.Validator { name, value ->
      if (value is String) {
        if (value.isNotBlank()) {
          throw ConfigException(name, value, "Must be blank.")
        }
      } else if (value is List<*>) {
        if (value.any()) {
          throw ConfigException(name, value, "Must be empty.")
        }
      } else {
        throw ConfigException(name, value, "Must be a String or a List.")
      }
    }
  }

  fun notBlankOrEmpty(): ConfigDef.Validator {
    return ConfigDef.Validator { name, value ->
      if (value is String) {
        if (value.isBlank()) {
          throw ConfigException(name, value, "Must not be blank.")
        }
      } else if (value is List<*>) {
        if (value.isEmpty()) {
          throw ConfigException(name, value, "Must not be empty.")
        }
      } else {
        throw ConfigException(name, value, "Must be a String or a List.")
      }
    }
  }

  fun string(vararg values: String): ConfigDef.Validator {
    return object : ConfigDef.Validator {
      override fun ensureValid(name: String?, value: Any?) {
        if (value is String) {
          if (values.isNotEmpty() && !values.contains(value)) {
            throw ConfigException(
                name, value, "Must be one of: ${values.joinToString { "'$it'" }}.")
          }
        } else if (value is List<*>) {
          value.forEach { ensureValid(name, it) }
        } else {
          throw ConfigException(name, value, "Must be a String or a List.")
        }
      }
    }
  }

  fun <T : Enum<T>> enum(cls: Class<T>, vararg exclude: T): ConfigDef.Validator {
    return string(
        *cls.enumConstants.filterNot { exclude.contains(it) }.map { it.name }.toTypedArray())
  }

  fun pattern(pattern: String): ConfigDef.Validator {
    return pattern(Pattern.compile(pattern))
  }

  fun pattern(pattern: Pattern): ConfigDef.Validator {
    val predicate = pattern.asPredicate()

    return object : ConfigDef.Validator {
      override fun ensureValid(name: String?, value: Any?) {
        if (value is String) {
          if (!predicate.test(value)) {
            throw ConfigException(name, value, "Must match pattern '${pattern.pattern()}'.")
          }
        } else if (value is List<*>) {
          value.forEach { ensureValid(name, it) }
        } else {
          throw ConfigException(name, value, "Must be a String or a List.")
        }
      }
    }
  }

  fun uri(vararg schemes: String): ConfigDef.Validator {
    return object : ConfigDef.Validator {
      override fun ensureValid(name: String?, value: Any?) {
        notBlankOrEmpty().ensureValid(name, value)

        when (value) {
          is String -> {
            try {
              val parsed = URI(value)

              if (schemes.isNotEmpty() && !schemes.contains(parsed.scheme)) {
                throw ConfigException(
                    name, value, "Scheme must be one of: ${schemes.joinToString { "'$it'" }}.")
              }
            } catch (t: URISyntaxException) {
              throw ConfigException(name, value, "Must be a valid URI: ${t.message}")
            }
          }
          is List<*> -> {
            value.forEach { ensureValid(name, it) }
          }
        }
      }
    }
  }

  fun file(readable: Boolean = true, writable: Boolean = false): ConfigDef.Validator {
    return object : ConfigDef.Validator {
      override fun ensureValid(name: String?, value: Any?) {
        notBlankOrEmpty().ensureValid(name, value)

        when (value) {
          is String -> {
            val file = File(value)
            if (!file.isAbsolute) {
              throw ConfigException(name, value, "Must be an absolute path.")
            }
            if (!file.isFile) {
              throw ConfigException(name, value, "Must be a file.")
            }
            if (readable && !file.canRead()) {
              throw ConfigException(name, value, "Must be readable.")
            }
            if (writable && !file.canWrite()) {
              throw ConfigException(name, value, "Must be writable.")
            }
          }
          is List<*> -> {
            value.forEach { ensureValid(name, it) }
          }
        }
      }
    }
  }

  fun Config.validateNonEmptyIfVisible(name: String) {
    this.configValues()
        .first { it.name() == name }
        .let { config ->
          if (config.visible() &&
              (when (val value = config.value()) {
                null -> true
                is Int -> false
                is Boolean -> false
                is String -> value.isEmpty()
                is Password -> value.value().isNullOrEmpty()
                is List<*> -> value.isEmpty()
                else ->
                    throw IllegalArgumentException(
                        "unexpected value '$value' for configuration $name")
              })) {
            config.addErrorMessage("Invalid value for configuration $name: Must not be blank.")
          }
        }
  }
}
