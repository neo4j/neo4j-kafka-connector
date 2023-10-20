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
package org.neo4j.connectors.kafka.testing

import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.api.extension.ParameterContext
import org.junit.jupiter.api.extension.ParameterResolver

internal class ParameterResolvers(
    private val parameterResolvers: Map<Class<*>, (ParameterContext?, ExtensionContext?) -> Any>
) : ParameterResolver {

  override fun supportsParameter(
      parameterContext: ParameterContext?,
      ignored: ExtensionContext?
  ): Boolean {
    return parameterResolvers.containsKey(parameterContext!!.parameter.type)
  }

  override fun resolveParameter(
      parameterContext: ParameterContext?,
      extensionContext: ExtensionContext?
  ): Any {
    val resolver = parameterResolvers[parameterContext!!.parameter.type]!!
    return resolver(parameterContext, extensionContext)
  }

  companion object {}
}
