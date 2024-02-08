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

package org.neo4j.connectors.kafka.testing.kafka

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.connect.data.Schema
import org.neo4j.connectors.kafka.testing.format.KafkaConverter

data class ConvertingKafkaProducer(
    val keyConverter: KafkaConverter,
    val valueConverter: KafkaConverter,
    val kafkaProducer: KafkaProducer<Any, Any>,
    val topicRegistry: TopicRegistry
) {

  fun publish(topic: String, value: Any, schema: Schema) {
    val serialisedValue = valueConverter.testShimSerializer.serialize(value, schema)
    val record: ProducerRecord<Any, Any> =
        ProducerRecord(topicRegistry.resolveTopic(topic), serialisedValue)
    kafkaProducer.send(record)
  }
}
