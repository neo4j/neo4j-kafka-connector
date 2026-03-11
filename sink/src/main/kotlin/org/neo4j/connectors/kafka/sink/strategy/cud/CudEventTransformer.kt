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
package org.neo4j.connectors.kafka.sink.strategy.cud

import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.errors.DataException
import org.neo4j.connectors.kafka.sink.SinkMessage
import org.neo4j.connectors.kafka.sink.strategy.SinkAction
import org.neo4j.connectors.kafka.sink.strategy.SinkEventTransformer

@Suppress("UNCHECKED_CAST")
class CudEventTransformer : SinkEventTransformer {
  override fun transform(message: SinkMessage): SinkAction {
    val value =
        when (val value = message.valueFromConnectValue()) {
          is Map<*, *> -> value as Map<String, Any?>
          else -> throw ConnectException("Message value must be convertible to a Map.")
        }
    val cud = Operation.from(value)
    return try {
      cud.toAction()
    } catch (e: IllegalArgumentException) {
      throw DataException("Failed to transform message.", e)
    }
  }
}
