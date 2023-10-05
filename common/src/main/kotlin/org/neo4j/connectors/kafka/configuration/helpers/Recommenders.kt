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

import java.util.Objects
import org.apache.kafka.common.config.ConfigDef

object Recommenders {
  fun <T : Enum<T>> enum(cls: Class<T>, vararg exclude: T): ConfigDef.Recommender {
    val values = cls.enumConstants.filterNot { exclude.contains(it) }.map { it.name }

    return object : ConfigDef.Recommender {
      override fun validValues(
          name: String?,
          parsedConfig: MutableMap<String, Any>?
      ): MutableList<Any> {
        return values.toMutableList()
      }

      override fun visible(name: String?, parsedConfig: MutableMap<String, Any>?): Boolean {
        return true
      }
    }
  }

  fun visibleIf(dependent: String, value: Any): ConfigDef.Recommender {
    return object : ConfigDef.Recommender {
      override fun validValues(
          name: String?,
          parsedConfig: MutableMap<String, Any>?
      ): MutableList<Any> = mutableListOf()

      override fun visible(name: String?, parsedConfig: MutableMap<String, Any>?): Boolean {
        val dependentValue = parsedConfig?.getValue(dependent)
        return Objects.equals(value, dependentValue)
      }
    }
  }
}
