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
package org.neo4j.connectors.kafka.configuration

/**
 * How a Neo4j map is described in a Kafka Connect schema. The row a query returns is always a
 * STRUCT with one field per column, whatever this says.
 */
enum class MapEncoding {
  /** Every Neo4j map becomes a STRUCT with one field per key. */
  STRUCT,

  /**
   * Every Neo4j map becomes a MAP. A map whose values have no shared schema has no valid MAP schema
   * and is rejected.
   */
  MAP,

  /**
   * A Neo4j map becomes a MAP when its values have a shared schema, and a STRUCT otherwise, so the
   * encoding depends on the data rather than on the configuration.
   */
  LEGACY,
}
