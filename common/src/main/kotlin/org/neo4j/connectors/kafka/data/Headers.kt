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
package org.neo4j.connectors.kafka.data

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaAndValue
import org.apache.kafka.connect.header.Header
import org.apache.kafka.connect.sink.SinkRecord
import org.neo4j.cdc.client.model.ChangeEvent
import org.neo4j.connectors.kafka.connect.ConnectHeader

object Headers {
  const val KEY_CDC_ID = "neo4j.source.cdc.id"
  const val KEY_CDC_TX_ID = "neo4j.source.cdc.tx-id"
  const val KEY_CDC_TX_SEQ = "neo4j.source.cdc.tx-seq"

  fun from(event: ChangeEvent): List<Header> =
      listOf(
          ConnectHeader(KEY_CDC_ID, SchemaAndValue(Schema.STRING_SCHEMA, event.id.id)),
          ConnectHeader(KEY_CDC_TX_ID, SchemaAndValue(Schema.INT64_SCHEMA, event.txId)),
          ConnectHeader(KEY_CDC_TX_SEQ, SchemaAndValue(Schema.INT32_SCHEMA, event.seq)))
}

fun SinkRecord.isCdcMessage(): Boolean =
    this.headers()?.any { header: Header? -> header?.key() == Headers.KEY_CDC_ID } ?: false

fun SinkRecord.cdcTxId(): Long? =
    this.headers()?.singleOrNull { it.key() == Headers.KEY_CDC_TX_ID }?.value() as Long?

fun SinkRecord.cdcTxSeq(): Int? =
    this.headers()?.singleOrNull { it.key() == Headers.KEY_CDC_TX_SEQ }?.value() as Int?
