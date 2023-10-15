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

object ConfigUtils {
  inline fun <reified E : Enum<E>> getEnum(config: AbstractConfig, key: String): E {
    return enumValueOf<E>(config.getString(key))
  }
}

class ConfigKeyBuilder private constructor(name: String, type: ConfigDef.Type) {
  val name: String
  val type: ConfigDef.Type
  var documentation: String
  var defaultValue: Any
  var validator: ConfigDef.Validator? = null
  var importance: ConfigDef.Importance? = null
  var group: String
  var orderInGroup: Int
  var width: ConfigDef.Width
  var displayName: String
  var dependents: List<String>
  var recommender: ConfigDef.Recommender? = null
  var internalConfig: Boolean

  init {
    documentation = ""
    defaultValue = ConfigDef.NO_DEFAULT_VALUE
    group = ""
    orderInGroup = -1
    width = ConfigDef.Width.NONE
    dependents = emptyList()
    internalConfig = true
    this.name = name
    displayName = name
    this.type = type
  }

  fun build(): ConfigDef.ConfigKey {
    return ConfigDef.ConfigKey(
        name,
        type,
        defaultValue,
        validator,
        importance,
        documentation,
        group,
        orderInGroup,
        width,
        displayName,
        dependents,
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
