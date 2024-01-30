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

import org.apache.avro.generic.GenericRecord
import org.neo4j.connectors.kafka.testing.GenericRecordSupport.asMap
import org.neo4j.connectors.kafka.testing.format.KafkaRecordMapper
import java.security.InvalidParameterException


object AvroMapper : KafkaRecordMapper {

  @Suppress("UNCHECKED_CAST")
  override fun <K> map(sourceValue: Any?, targetClass: Class<K>): K? {
    if (sourceValue == null) {
      return null
    }
    if (sourceValue !is GenericRecord) {
      throw InvalidParameterException(
          "AvroMapper expects source value to be GenericRecord, but it was ${sourceValue::class.java}")
    }
    val resultValue =
        when (targetClass) {
          Map::class.java -> sourceValue.asMap()
          // TODO CDC Events
          else -> null
        }
    return resultValue as K?
  }
}
