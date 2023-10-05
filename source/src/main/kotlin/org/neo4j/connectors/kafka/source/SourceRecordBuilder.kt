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
package org.neo4j.connectors.kafka.source

import kotlin.properties.Delegates
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.source.SourceRecord
import org.neo4j.driver.Record
import streams.kafka.connect.utils.asJsonString
import streams.kafka.connect.utils.asStruct

class SourceRecordBuilder {

  private lateinit var topic: String

  private lateinit var streamingProperty: String

  private var timestamp by Delegates.notNull<Long>()

  private lateinit var sourcePartition: Map<String, Any>

  private lateinit var record: Record

  private var enforceSchema: Boolean = false

  fun withTopic(topic: String): SourceRecordBuilder {
    this.topic = topic
    return this
  }

  fun withStreamingProperty(streamingProperty: String): SourceRecordBuilder {
    this.streamingProperty = streamingProperty
    return this
  }

  fun withTimestamp(timestamp: Long): SourceRecordBuilder {
    this.timestamp = timestamp
    return this
  }

  fun withSourcePartition(sourcePartition: Map<String, Any>): SourceRecordBuilder {
    this.sourcePartition = sourcePartition
    return this
  }

  fun withRecord(record: Record): SourceRecordBuilder {
    this.record = record
    return this
  }

  fun withEnforceSchema(enforceSchema: Boolean): SourceRecordBuilder {
    this.enforceSchema = enforceSchema
    return this
  }

  fun build(): SourceRecord {
    val sourceOffset =
        mapOf("property" to streamingProperty.ifBlank { "undefined" }, "value" to timestamp)
    val (struct, schema) =
        when (enforceSchema) {
          true -> {
            val st = record.asStruct()
            val sc = st.schema()
            st to sc
          }
          else -> record.asJsonString() to Schema.STRING_SCHEMA
        }
    return SourceRecord(sourcePartition, sourceOffset, topic, schema, struct, schema, struct)
  }
}
