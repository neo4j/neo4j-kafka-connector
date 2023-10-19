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

import org.neo4j.connectors.kafka.testing.WordSupport.camelCaseToUpperSnakeCase
import org.neo4j.connectors.kafka.testing.source.DEFAULT_TO_ENV

/**
 * Setting represents an annotation attribute that is potentially backed by an environment variable
 * Such attributes hold a specific value that will trigger the search of the corresponding
 * environment variable The corresponding environment variable is automatically derived from the
 * annotation attribute name, following a simple camel case to upper snake case conversion
 */
internal class Setting<T : Annotation>(
    private val name: String,
    private val envAccessor: (String) -> String? = System::getenv,
) {
  private val fieldValueOf: (T) -> String = resolveFieldAccessor(name)

  private val envVarName = camelCaseToUpperSnakeCase(name)

  fun isValid(annotation: T): Boolean {
    return fieldValueOf(annotation) != DEFAULT_TO_ENV || envAccessor(envVarName) != null
  }

  fun read(annotation: T): String {
    val fieldValue = fieldValueOf(annotation)
    if (fieldValue != DEFAULT_TO_ENV) {
      return fieldValue
    }
    return envAccessor(envVarName)!!
  }

  fun errorMessage(): String {
    return "Both annotation field and environment variable $envVarName are unset. Please specify one"
  }

  override fun toString(): String {
    return "EnvBackedSetting(name='$name', envVarName='$envVarName')"
  }

  private fun resolveFieldAccessor(name: String): (T) -> String {
    return { annotation ->
      annotation::class.members.first { member -> member.name == name }.call(annotation) as String
    }
  }
}
