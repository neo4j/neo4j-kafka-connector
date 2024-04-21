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
import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serializer
import org.junit.jupiter.api.extension.ExtensionContext
import org.neo4j.connectors.kafka.testing.AnnotationSupport
import org.neo4j.connectors.kafka.testing.format.avro.AvroDeserializer
import org.neo4j.connectors.kafka.testing.format.avro.AvroSerializer
import org.neo4j.connectors.kafka.testing.format.json.JsonSchemaDeserializer
import org.neo4j.connectors.kafka.testing.format.json.JsonSchemaSerializer
import org.neo4j.connectors.kafka.testing.format.protobuf.ProtobufDeserializer
import org.neo4j.connectors.kafka.testing.format.protobuf.ProtobufSerializer
import org.neo4j.connectors.kafka.testing.format.string.StringDeserializer
import org.neo4j.connectors.kafka.testing.format.string.StringSerializer

private val PROTOBUF_OPTIONS = mapOf("optional.for.nullables" to "true")

enum class KafkaConverter(
    val className: String,
    val deserializerClass: Class<out Deserializer<*>>,
    val serializerClass: Class<out Serializer<*>>,
    val testShimDeserializer: KafkaRecordDeserializer,
    val testShimSerializer: KafkaRecordSerializer,
    val supportsSchemaRegistry: Boolean = true,
    val additionalProperties: Map<String, Any> = mapOf(),
) {
  AVRO(
      className = "io.confluent.connect.avro.AvroConverter",
      deserializerClass = KafkaAvroDeserializer::class.java,
      serializerClass = KafkaAvroSerializer::class.java,
      testShimDeserializer = AvroDeserializer,
      testShimSerializer = AvroSerializer),
  JSON_SCHEMA(
      className = "io.confluent.connect.json.JsonSchemaConverter",
      deserializerClass = KafkaJsonSchemaDeserializer::class.java,
      serializerClass = KafkaJsonSchemaSerializer::class.java,
      testShimDeserializer = JsonSchemaDeserializer,
      testShimSerializer = JsonSchemaSerializer),
  PROTOBUF(
      className = "io.confluent.connect.protobuf.ProtobufConverter",
      deserializerClass = KafkaProtobufDeserializer::class.java,
      serializerClass = KafkaProtobufSerializer::class.java,
      testShimDeserializer = ProtobufDeserializer,
      testShimSerializer = ProtobufSerializer(PROTOBUF_OPTIONS),
      additionalProperties = PROTOBUF_OPTIONS),
  STRING(
      className = "org.apache.kafka.connect.storage.StringConverter",
      deserializerClass = org.apache.kafka.common.serialization.StringDeserializer::class.java,
      serializerClass = org.apache.kafka.common.serialization.StringSerializer::class.java,
      testShimDeserializer = StringDeserializer,
      testShimSerializer = StringSerializer)
}

@Target(AnnotationTarget.FUNCTION, AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class KeyValueConverter(val key: KafkaConverter, val value: KafkaConverter)

class KeyValueConverterResolver {

  private lateinit var keyConverter: KafkaConverter
  private lateinit var valueConverter: KafkaConverter

  fun resolveKeyConverter(context: ExtensionContext?): KafkaConverter {
    initializeKeyValueConverters(context)
    return keyConverter
  }

  fun resolveValueConverter(context: ExtensionContext?): KafkaConverter {
    initializeKeyValueConverters(context)
    return keyConverter
  }

  private fun initializeKeyValueConverters(context: ExtensionContext?) {
    if (this::keyConverter.isInitialized && this::valueConverter.isInitialized) {
      return
    }
    val annotation: KeyValueConverter? =
        AnnotationSupport.findAnnotation<KeyValueConverter>(context)
    if (annotation == null) {
      keyConverter = KafkaConverter.AVRO
      valueConverter = KafkaConverter.AVRO
    } else {
      keyConverter = annotation.key
      valueConverter = annotation.value
    }
  }
}
