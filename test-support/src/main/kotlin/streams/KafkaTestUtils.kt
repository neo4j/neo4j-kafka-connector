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
package streams

import java.util.Properties
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer

object KafkaTestUtils {
  fun <K, V> createConsumer(
    bootstrapServers: String,
    schemaRegistryUrl: String? = null,
    keyDeserializer: String = StringDeserializer::class.java.name,
    valueDeserializer: String = ByteArrayDeserializer::class.java.name,
    vararg topics: String = emptyArray()
  ): KafkaConsumer<K, V> {
    val props = Properties()
    props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
    props["group.id"] = "neo4j" // UUID.randomUUID().toString()
    props["enable.auto.commit"] = "true"
    props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = keyDeserializer
    props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = valueDeserializer
    props["auto.offset.reset"] = "earliest"
    if (schemaRegistryUrl != null) {
      props["schema.registry.url"] = schemaRegistryUrl
    }
    val consumer = KafkaConsumer<K, V>(props)
    if (!topics.isNullOrEmpty()) {
      consumer.subscribe(topics.toList())
    }
    return consumer
  }

  fun <K, V> createProducer(
    bootstrapServers: String,
    schemaRegistryUrl: String? = null,
    keySerializer: String = StringSerializer::class.java.name,
    valueSerializer: String = ByteArraySerializer::class.java.name
  ): KafkaProducer<K, V> {
    val props = Properties()
    props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
    props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = keySerializer
    props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = valueSerializer
    if (!schemaRegistryUrl.isNullOrBlank()) {
      props["schema.registry.url"] = schemaRegistryUrl
    }
    return KafkaProducer(props)
  }
}
