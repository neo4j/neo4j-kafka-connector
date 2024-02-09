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

package org.neo4j.connectors.kafka.testing.format.avro

import io.confluent.connect.avro.AvroData
import org.apache.kafka.connect.data.Schema
import org.neo4j.connectors.kafka.testing.format.KafkaRecordSerializer

object AvroSerializer : KafkaRecordSerializer {

  private const val CACHE_SIZE = 100

  private val avroData = AvroData(CACHE_SIZE)

  override fun serialize(value: Any, schema: Schema): Any {
    return avroData.fromConnectData(schema, value)
  }
}
