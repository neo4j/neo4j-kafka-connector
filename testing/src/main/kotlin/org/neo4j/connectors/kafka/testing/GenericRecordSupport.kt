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
package org.neo4j.connectors.kafka.testing

import org.apache.avro.generic.GenericArray
import org.apache.avro.generic.GenericRecord

object GenericRecordSupport {

  fun GenericRecord.asMap(): Map<String, Any> {
    // FIXME: properly convert values
    return this.schema.fields.associate { field ->
      field.name() to castMapValue(this.get(field.name()))
    }
  }

  fun GenericRecord.getRecord(k: String): GenericRecord? = this.get(k) as? GenericRecord

  fun GenericRecord.getString(k: String): String? = this.get(k)?.toString()

  @Suppress("UNCHECKED_CAST")
  fun GenericRecord.getMap(k: String): Map<String, Any>? =
      (this.get(k) as? Map<Any, Any>)?.map { it.key.toString() to castMapValue(it.value) }?.toMap()

  @Suppress("UNCHECKED_CAST")
  fun <T> GenericRecord.getArray(k: String): GenericArray<T>? = this.get(k) as? GenericArray<T>

  private fun castMapValue(value: Any): Any =
      when (value) {
        is Long,
        is GenericArray<*>,
        is GenericRecord -> value
        else -> value.toString()
      }
}
