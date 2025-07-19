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
package org.neo4j.connectors.kafka.testing.format

import io.confluent.connect.avro.AvroConverter
import io.confluent.connect.json.JsonSchemaConverter
import io.confluent.connect.protobuf.ProtobufConverter
import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.confluent.kafka.serializers.KafkaJsonSerializer
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.connect.json.JsonConverter
import org.apache.kafka.connect.storage.Converter
import org.apache.kafka.connect.storage.StringConverter
import org.junit.jupiter.api.extension.ExtensionContext
import org.neo4j.connectors.kafka.configuration.PayloadMode
import org.neo4j.connectors.kafka.testing.AnnotationSupport
import org.neo4j.connectors.kafka.testing.format.avro.AvroSerializer
import org.neo4j.connectors.kafka.testing.format.json.JsonEmbeddedSerializer
import org.neo4j.connectors.kafka.testing.format.json.JsonRawSerializer
import org.neo4j.connectors.kafka.testing.format.json.JsonSchemaSerializer
import org.neo4j.connectors.kafka.testing.format.protobuf.ProtobufSerializer
import org.neo4j.connectors.kafka.testing.format.string.StringSerializer

private val PROTOBUF_OPTIONS =
    mapOf("enhanced.protobuf.schema.support" to "true", "optional.for.nullables" to "true")

private val JSON_RAW_OPTIONS = mapOf("schemas.enable" to "false")

private val JSON_EMBEDDED_OPTIONS = mapOf("schemas.enable" to "true")

enum class KafkaConverter(
    val className: String,
    val converterProvider: () -> Converter,
    val serializerClass: Class<out Serializer<*>>,
    val testShimSerializer: KafkaRecordSerializer,
    val supportsSchemaRegistry: Boolean = true,
    val additionalProperties: Map<String, Any> = mapOf(),
) {
  AVRO(
      className = "io.confluent.connect.avro.AvroConverter",
      converterProvider = { AvroConverter() },
      serializerClass = KafkaAvroSerializer::class.java,
      testShimSerializer = AvroSerializer,
  ),
  JSON_SCHEMA(
      className = "io.confluent.connect.json.JsonSchemaConverter",
      converterProvider = { JsonSchemaConverter() },
      serializerClass = KafkaJsonSchemaSerializer::class.java,
      testShimSerializer = JsonSchemaSerializer,
  ),
  JSON_EMBEDDED(
      className = "org.apache.kafka.connect.json.JsonConverter",
      converterProvider = { JsonConverter() },
      serializerClass = KafkaJsonSerializer::class.java,
      testShimSerializer = JsonEmbeddedSerializer,
      additionalProperties = JSON_EMBEDDED_OPTIONS,
  ),
  JSON_RAW(
      className = "org.apache.kafka.connect.json.JsonConverter",
      converterProvider = { JsonConverter() },
      serializerClass = KafkaJsonSerializer::class.java,
      testShimSerializer = JsonRawSerializer,
      supportsSchemaRegistry = false,
      additionalProperties = JSON_RAW_OPTIONS,
  ),
  PROTOBUF(
      className = "io.confluent.connect.protobuf.ProtobufConverter",
      converterProvider = { ProtobufConverter() },
      serializerClass = KafkaProtobufSerializer::class.java,
      testShimSerializer = ProtobufSerializer(PROTOBUF_OPTIONS),
      additionalProperties = PROTOBUF_OPTIONS,
  ),
  STRING(
      className = "org.apache.kafka.connect.storage.StringConverter",
      converterProvider = { StringConverter() },
      serializerClass = org.apache.kafka.common.serialization.StringSerializer::class.java,
      testShimSerializer = StringSerializer,
  ),
}

@Target(AnnotationTarget.FUNCTION, AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class KeyValueConverter(
    val key: KafkaConverter,
    val value: KafkaConverter,
    val payloadMode: PayloadMode = PayloadMode.EXTENDED,
)

class KeyValueConverterResolver {

  private lateinit var keyConverter: KafkaConverter
  private lateinit var valueConverter: KafkaConverter
  private lateinit var payloadMode: PayloadMode

  fun resolveKeyConverter(context: ExtensionContext?): KafkaConverter {
    initializeKeyValueConverters(context)
    return keyConverter
  }

  fun resolveValueConverter(context: ExtensionContext?): KafkaConverter {
    initializeKeyValueConverters(context)
    return valueConverter
  }

  fun resolvePayloadMode(context: ExtensionContext?): PayloadMode {
    initializeKeyValueConverters(context)
    return payloadMode
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
      payloadMode = PayloadMode.EXTENDED
    } else {
      keyConverter = annotation.key
      valueConverter = annotation.value
      payloadMode = annotation.payloadMode
    }
  }
}
