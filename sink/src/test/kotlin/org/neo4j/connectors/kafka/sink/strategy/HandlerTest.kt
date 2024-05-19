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
package org.neo4j.connectors.kafka.sink.strategy

import java.time.Instant
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.header.Header
import org.apache.kafka.connect.sink.SinkRecord
import org.neo4j.connectors.kafka.sink.SinkMessage

open class HandlerTest {

  protected fun newMessage(
      valueSchema: Schema?,
      value: Any?,
      keySchema: Schema? = null,
      key: Any? = null,
      headers: Iterable<Header> = emptyList()
  ): SinkMessage {
    return SinkMessage(
        SinkRecord(
            "my-topic",
            0,
            keySchema,
            key,
            valueSchema,
            value,
            0,
            TIMESTAMP,
            TimestampType.CREATE_TIME,
            headers))
  }

  companion object {
    val TIMESTAMP: Long = Instant.now().toEpochMilli()
  }
}
