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

package org.neo4j.connectors.kafka.testing.format.mapper

import com.google.protobuf.DynamicMessage
import org.neo4j.cdc.client.model.ChangeEvent
import org.neo4j.connectors.kafka.testing.format.ChangeEventSupport.mapToChangeEvent
import org.neo4j.connectors.kafka.testing.format.DynamicMessageSupport.asMap

object ProtobufMapper : KafkaRecordMapper {

  @Suppress("UNCHECKED_CAST", "IMPLICIT_CAST_TO_ANY")
  override fun <K> map(sourceValue: Any?, targetClass: Class<K>): K? {
    if (sourceValue == null) {
      return null
    }

    val resultValue =
        when (sourceValue) {
          is DynamicMessage ->
              when (targetClass) {
                Map::class.java -> sourceValue.asMap()
                ChangeEvent::class.java -> mapToChangeEvent(sourceValue.asMap())
                String::class.java -> sourceValue.allFields.entries.first().value.toString()
                else -> throw MappingException(sourceValue, targetClass)
              }
          else -> throw MappingException(sourceValue)
        }

    return resultValue as K?
  }
}
