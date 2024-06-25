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
package org.neo4j.connectors.kafka.sink.legacy

import org.apache.kafka.connect.sink.SinkRecord
import org.neo4j.connectors.kafka.sink.legacy.strategy.StreamsSinkEntity
import org.neo4j.connectors.kafka.sink.utils.toStreamsSinkEntity

class EventBuilder {
  private var batchSize: Int? = null
  private lateinit var sinkRecords: Collection<SinkRecord>

  fun withBatchSize(batchSize: Int): EventBuilder {
    this.batchSize = batchSize
    return this
  }

  fun withSinkRecords(sinkRecords: Collection<SinkRecord>): EventBuilder {
    this.sinkRecords = sinkRecords
    return this
  }

  fun build(): Map<String, List<List<StreamsSinkEntity>>> { // <Topic, List<List<SinkRecord>>
    val batchSize = this.batchSize!!
    return this.sinkRecords
        .groupBy { it.topic() }
        .mapValues { entry ->
          val value = entry.value.map { it.toStreamsSinkEntity() }
          if (batchSize > value.size) listOf(value) else value.chunked(batchSize)
        }
  }
}
