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
package org.neo4j.connectors.kafka.source

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaAndValue
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.neo4j.connectors.kafka.data.extractEventSchema
import org.neo4j.connectors.kafka.data.extractEventValue

enum class Neo4jCdcKeyStrategy {
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
      return message.extractEventSchema().field("elementId").schema()
    }

    override fun value(message: SchemaAndValue): Any? {
      return message.extractEventValue().get("elementId")
    }
  },
  ENTITY_KEYS {
    override fun schema(message: SchemaAndValue): Schema? {
      val keysSchema = message.extractEventSchema().field("keys").schema()
      return SchemaBuilder.struct().field("keys", keysSchema).optional().build()
    }

    override fun value(message: SchemaAndValue): Any? =
        message
            .extractEventValue()
            .getArray<Struct>("keys")
            // an entity without keys is either null, empty, or a name without any rows
            ?.takeIf { entries -> entries.any { it.getArray<Struct>("rows").isNotEmpty() } }
            ?.let { Struct(schema(message)).put("keys", it) }
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
