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

internal const val DEFAULT_TO_ENV = "___UNSET___"

/**
 * AnnotationValueResolver resolves an annotation attribute value in at most two steps. If the value
 * has been explicitly initialized on the annotation T, it is returned. If the value holds a
 * specific default value, the resolver will look for the corresponding environment variable and
 * return the value. The environment variable name results from the camel case to upper snake case
 * conversion operated on the attribute name.
 */
internal class AnnotationValueResolver<T : Annotation>(
    private val name: String,
    private val envAccessor: (String) -> String?,
) {
  private val fieldValueOf: (T) -> String = resolveFieldAccessor(name)

  private val envVarName = camelCaseToUpperSnakeCase(name)

  /** Determines whether the value is resolvable. */
  fun isValid(annotation: T): Boolean {
    return fieldValueOf(annotation) != DEFAULT_TO_ENV || envAccessor(envVarName) != null
  }

  /**
   * Resolves the value of the provided annotation's attribute. This assumes [isValid] has been
   * called first and returned true.
   */
  fun resolve(annotation: T): String {
    val fieldValue = fieldValueOf(annotation)
    if (fieldValue != DEFAULT_TO_ENV) {
      return fieldValue
    }
    return envAccessor(envVarName)!!
  }

  fun errorMessage(): String {
    return "Both annotation field $name and environment variable $envVarName are unset. Please specify one"
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
