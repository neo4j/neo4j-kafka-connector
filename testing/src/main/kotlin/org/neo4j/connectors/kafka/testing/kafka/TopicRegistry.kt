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
package org.neo4j.connectors.kafka.testing.kafka

import java.util.UUID
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class TopicRegistry {

  private val log: Logger = LoggerFactory.getLogger(this::class.java)

  private val aliases: MutableMap<String, String> = mutableMapOf()

  fun resolveTopic(alias: String?): String {
    if (alias.isNullOrBlank()) {
      return ""
    }
    return aliases.computeIfAbsent(alias) { UUID.randomUUID().toString() }
  }

  fun log() {
    log.info(
        "Using the following topic mapping:\n${aliases.entries.joinToString("\n") { 
          " * '${it.key}' -> '${it.value}'" 
        }}"
    )
  }

  fun clear() {
    aliases.clear()
  }
}
