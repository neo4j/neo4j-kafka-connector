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
package org.neo4j.connectors.kafka.testing.format.string

import com.fasterxml.jackson.databind.ObjectMapper
import org.neo4j.connectors.kafka.testing.format.KafkaRecordDeserializer
import org.neo4j.connectors.kafka.testing.format.MappingException

object StringDeserializer : KafkaRecordDeserializer {
  val mapper = ObjectMapper()

  @Suppress("UNCHECKED_CAST")
  override fun <K> deserialize(sourceValue: Any?, targetClass: Class<K>): K? {
    if (sourceValue == null) {
      return null
    }

    if (targetClass.isAssignableFrom(String::class.java)) {
      return when (sourceValue) {
        is String -> sourceValue as K
        else -> sourceValue.toString() as K
      }
    }

    throw MappingException("unexpected target type ${targetClass.name} for value $sourceValue")
  }
}
