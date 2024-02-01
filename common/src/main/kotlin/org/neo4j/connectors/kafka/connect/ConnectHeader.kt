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
package org.neo4j.connectors.kafka.connect

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaAndValue
import org.apache.kafka.connect.header.Header

data class ConnectHeader(private val key: String, private val schemaAndValue: SchemaAndValue) :
    Header {

  override fun key(): String = key

  override fun schema(): Schema = schemaAndValue.schema()

  override fun value(): Any = schemaAndValue.value()

  override fun with(schema: Schema?, value: Any?): Header =
      ConnectHeader(key, SchemaAndValue(schema!!, value!!))

  override fun rename(key: String?): Header = ConnectHeader(key!!, schemaAndValue)
}
