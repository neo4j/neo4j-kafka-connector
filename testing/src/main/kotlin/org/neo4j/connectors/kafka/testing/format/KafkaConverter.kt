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

import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.neo4j.connectors.kafka.testing.format.mapper.AvroMapper
import org.neo4j.connectors.kafka.testing.format.mapper.JsonMapper

enum class KafkaConverter(
    val className: String,
    val deserializerClass: Class<out Deserializer<*>>,
    val mapper: KafkaRecordMapper,
    val supportsSchemaRegistry: Boolean = true
) {
  AVRO(
      className = "io.confluent.connect.avro.AvroConverter",
      deserializerClass = KafkaAvroDeserializer::class.java,
      mapper = AvroMapper),
  JSON_SCHEMA(
      className = "io.confluent.connect.json.JsonSchemaConverter",
      deserializerClass = KafkaJsonSchemaDeserializer::class.java,
      mapper = JsonMapper),
  PROTOBUF(
      className = "io.confluent.connect.protobuf.ProtobufConverter",
      deserializerClass = KafkaProtobufDeserializer::class.java,
      mapper = AvroMapper),
  STRING(
      className = "org.apache.kafka.connect.storage.StringConverter",
      deserializerClass = StringDeserializer::class.java,
      mapper = AvroMapper,
      supportsSchemaRegistry = false)
}

@Target(AnnotationTarget.FUNCTION, AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class KeyValueConverter(val key: KafkaConverter, val value: KafkaConverter)

interface KafkaRecordMapper {
  fun <K> map(sourceValue: Any?, targetClass: Class<K>): K?
}
