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
package org.neo4j.connectors.kafka.data.converter

import org.apache.kafka.connect.data.Schema
import org.neo4j.connectors.kafka.data.ValueConverter
import org.neo4j.connectors.kafka.utils.JSONUtils

class RawJsonStringValueConverter : ValueConverter {

  override fun schema(value: Any?, optional: Boolean, forceMapsAsStruct: Boolean): Schema {
    return Schema.STRING_SCHEMA
  }

  override fun value(schema: Schema, value: Any?): Any? {
    if (value == null) {
      return null
    }

    return JSONUtils.writeValueAsString(value)
  }
}
