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

enum class Neo4jCdcKeySerializationStrategy {
  SKIP {
    override fun schema(transformedValue: SchemaAndValue): Schema? {
      return null
    }

    override fun value(transformedValue: SchemaAndValue): Any? {
      return null
    }
  },
  ELEMENT_ID {
    override fun schema(transformedValue: SchemaAndValue): Schema? {
      TODO("string schema")
    }

    override fun value(transformedValue: SchemaAndValue): Any? {
      TODO("element id extraction")
    }
  },
  ENTITY_KEYS {
    override fun schema(transformedValue: SchemaAndValue): Schema? {
      TODO("Not yet implemented")
    }

    override fun value(transformedValue: SchemaAndValue): Any? {
      TODO("Not yet implemented")
    }
  },
  WHOLE_VALUE {
    override fun schema(transformedValue: SchemaAndValue): Schema? {
      return transformedValue.schema()
    }

    override fun value(transformedValue: SchemaAndValue): Any? {
      return transformedValue.value()
    }
  };

  abstract fun schema(transformedValue: SchemaAndValue): Schema?

  abstract fun value(transformedValue: SchemaAndValue): Any?
}
