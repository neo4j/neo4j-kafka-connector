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

import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.neo4j.connectors.kafka.utils.PropertiesUtil

object ConfigUtils {
  inline fun <reified E : Enum<E>> getEnum(config: AbstractConfig, key: String): E {
    return enumValueOf<E>(config.getString(key))
  }
}

class ConfigKeyBuilder private constructor(val name: String, val type: ConfigDef.Type) {
  var documentation: String = ""
  var defaultValue: Any = ConfigDef.NO_DEFAULT_VALUE
  var validator: ConfigDef.Validator? = null
  var importance: ConfigDef.Importance? = null
  var group: String = ""
  var orderInGroup: Int = -1
  var width: ConfigDef.Width = ConfigDef.Width.NONE
  var displayName: String = name
  var dependents: Set<String> = emptySet()
  var recommender: ConfigDef.Recommender? = null
  var internalConfig: Boolean = true

  fun build(): ConfigDef.ConfigKey {
    return ConfigDef.ConfigKey(
        name,
        type,
        defaultValue,
        validator,
        importance,
        documentation.ifBlank { PropertiesUtil.getProperty(name) },
        group,
        orderInGroup,
        width,
        displayName,
        recommender
            .let {
              when (it) {
                is DependentRecommender -> it.dependsOn
                else -> emptySet()
              }
            }
            .union(dependents)
            .toList(),
        recommender,
        internalConfig,
    )
  }

  companion object {
    fun of(
        name: String,
        type: ConfigDef.Type,
        init: ConfigKeyBuilder.() -> Unit
    ): ConfigDef.ConfigKey {
      val builder = ConfigKeyBuilder(name, type)
      init(builder)
      return builder.build()
    }
  }
}
