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

import kotlin.math.absoluteValue

internal object WordSupport {

  // implementation note: this is a naive pluralization helper, which is not meant to work outside
  // English
  fun pluralize(count: Int, singular: String, plural: String): String {
    if (count.absoluteValue < 2) {
      return singular
    }
    return plural
  }

  fun camelCaseToUpperSnakeCase(word: String): String {
    fun convertChar(char: Char): List<Char> {
      return if (Character.isUpperCase(char)) {
        listOf('_', char)
      } else listOf(char.uppercaseChar())
    }

    return word.flatMap { convertChar(it) }.joinToString("")
  }
}
