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

package org.neo4j.connectors.kafka.testing.format

import com.google.protobuf.DynamicMessage

object DynamicMessageSupport {

  fun DynamicMessage.asMap(): Map<String, Any> {
    return this.allFields.entries
        .filter { field -> field.value != null }
        .associate { field -> field.key.name to castMapValue(field.value) }
  }

  private fun castMapValue(value: Any): Any =
      when (value) {
        is Long -> value
        else -> value.toString()
      }
}
