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
package org.neo4j.connectors.kafka.testing.format.avro

import org.apache.avro.generic.GenericArray
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.connect.data.Struct
import org.neo4j.cdc.client.model.ChangeEvent
import org.neo4j.connectors.kafka.testing.format.ChangeEventSupport.mapToChangeEvent
import org.neo4j.connectors.kafka.testing.format.KafkaRecordDeserializer
import org.neo4j.connectors.kafka.testing.format.MappingException
import org.neo4j.connectors.kafka.testing.format.avro.GenericRecordSupport.asList
import org.neo4j.connectors.kafka.testing.format.avro.GenericRecordSupport.asMap
import org.neo4j.connectors.kafka.testing.format.avro.GenericRecordSupport.asStruct

object AvroDeserializer : KafkaRecordDeserializer {

  @Suppress("UNCHECKED_CAST")
  override fun <K> deserialize(sourceValue: Any?, targetClass: Class<K>): K? {
    if (sourceValue == null) {
      return null
    }
    val resultValue =
        when (sourceValue) {
          is GenericRecord ->
              when (targetClass) {
                Map::class.java -> sourceValue.asMap()
                ChangeEvent::class.java -> mapToChangeEvent(sourceValue.asMap())
                Struct::class.java -> sourceValue.asStruct()
                else -> throw MappingException(sourceValue, targetClass)
              }
          is GenericArray<*> ->
              when (targetClass) {
                List::class.java -> sourceValue.asList()
                else -> throw MappingException(sourceValue, targetClass)
              }
          is String -> sourceValue
          else -> throw MappingException(sourceValue)
        }

    return resultValue as K?
  }
}
