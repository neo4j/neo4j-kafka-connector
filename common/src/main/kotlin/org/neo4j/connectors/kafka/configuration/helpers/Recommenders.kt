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

import java.util.function.Predicate
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigException

interface DependentRecommender {
  val dependsOn: Set<String>
}

object Recommenders {

  fun and(vararg recommenders: ConfigDef.Recommender): ConfigDef.Recommender {
    return object : ConfigDef.Recommender, DependentRecommender {
      override fun validValues(name: String?, parsedConfig: Map<String, Any>?): List<Any> =
          recommenders.flatMap { it.validValues(name, parsedConfig) }

      override fun visible(name: String?, parsedConfig: MutableMap<String, Any>?): Boolean {
        return recommenders.all { it.visible(name, parsedConfig) }
      }

      override val dependsOn: Set<String>
        get() =
            recommenders
                .filter { it is DependentRecommender }
                .flatMap { (it as DependentRecommender).dependsOn }
                .toSet()
    }
  }

  fun <T : Enum<T>> enum(cls: Class<T>, vararg exclude: T): ConfigDef.Recommender {
    val values = cls.enumConstants.filterNot { exclude.contains(it) }.map { it.name }

    return object : ConfigDef.Recommender {
      override fun validValues(
          name: String?,
          parsedConfig: MutableMap<String, Any>?,
      ): MutableList<Any> {
        return values.toMutableList()
      }

      override fun visible(name: String?, parsedConfig: MutableMap<String, Any>?): Boolean {
        return true
      }
    }
  }

  fun bool(): ConfigDef.Recommender {
    val values = listOf("true", "false")

    return object : ConfigDef.Recommender {
      override fun validValues(
          name: String?,
          parsedConfig: MutableMap<String, Any>?,
      ): MutableList<Any> {
        return values.toMutableList()
      }

      override fun visible(name: String?, parsedConfig: MutableMap<String, Any>?): Boolean {
        return true
      }
    }
  }

  fun visibleIf(dependent: String, valueMatcher: Predicate<Any?>): ConfigDef.Recommender {
    return object : ConfigDef.Recommender, DependentRecommender {
      override fun validValues(
          name: String?,
          parsedConfig: MutableMap<String, Any>?,
      ): MutableList<Any> = mutableListOf()

      override fun visible(name: String?, parsedConfig: MutableMap<String, Any>?): Boolean {
        val dependentValue = parsedConfig?.get(dependent)
        return valueMatcher.test(dependentValue)
      }

      override val dependsOn: Set<String>
        get() = setOf(dependent)
    }
  }

  fun visibleIfNotEmpty(dependentPredicate: Predicate<String>): ConfigDef.Recommender {
    return object : ConfigDef.Recommender {
      override fun validValues(
          name: String?,
          parsedConfig: MutableMap<String, Any>?,
      ): MutableList<Any> = mutableListOf()

      override fun visible(name: String?, parsedConfig: MutableMap<String, Any>?): Boolean {
        return parsedConfig
            ?.filterKeys { dependentPredicate.test(it) }
            ?.any {
              when (val dependentValue = parsedConfig[it.key]) {
                null -> false
                is String -> {
                  return dependentValue.isNotBlank()
                }
                is List<*> -> {
                  return dependentValue.isNotEmpty()
                }
                else -> {
                  throw ConfigException("Must be a String or a List.")
                }
              }
            } ?: false
      }
    }
  }
}
