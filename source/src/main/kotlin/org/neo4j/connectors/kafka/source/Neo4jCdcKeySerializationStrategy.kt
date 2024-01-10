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

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaAndValue
import org.apache.kafka.connect.data.Struct

enum class Neo4jCdcKeySerializationStrategy {
  SKIP {
    override fun schema(message: SchemaAndValue): Schema? {
      return null
    }

    override fun value(message: SchemaAndValue): Any? {
      return null
    }
  },
  ELEMENT_ID {
    override fun schema(message: SchemaAndValue): Schema? {
      return Schema.OPTIONAL_STRING_SCHEMA
    }

    override fun value(message: SchemaAndValue): Any? {
      val value = message.value()
      if (value !is Struct) {
        throw IllegalArgumentException(
            "expected value to be a struct, but got: ${value?.javaClass}")
      }
      val eventData = value.get("event")
      if (eventData !is Struct) {
        throw IllegalArgumentException(
            "expected value to be a struct, but got: ${value?.javaClass}")
      }
      return eventData.get("elementId")
    }
  },
  ENTITY_KEYS {
    override fun schema(message: SchemaAndValue): Schema? {
      TODO("Not yet implemented")
    }

    override fun value(message: SchemaAndValue): Any? {
      TODO("Not yet implemented")
    }
  },
  WHOLE_VALUE {
    override fun schema(message: SchemaAndValue): Schema? {
      return message.schema()
    }

    override fun value(message: SchemaAndValue): Any? {
      return message.value()
    }
  };

  abstract fun schema(message: SchemaAndValue): Schema?

  abstract fun value(message: SchemaAndValue): Any?
}
