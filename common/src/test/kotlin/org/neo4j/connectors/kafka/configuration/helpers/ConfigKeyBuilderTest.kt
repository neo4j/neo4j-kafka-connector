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
package org.neo4j.connectors.kafka.configuration.helpers

import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldNotBe
import org.apache.kafka.common.config.ConfigDef
import org.junit.jupiter.api.Test
import org.neo4j.connectors.kafka.configuration.AuthenticationType

class ConfigKeyBuilderTest {

  @Test
  fun `should use documentation from property files by default`() {
    ConfigKeyBuilder.of("neo4j.uri", ConfigDef.Type.STRING) {}.documentation shouldNotBe ""
  }

  @Test
  fun `should collect dependents from supported recommenders`() {
    ConfigKeyBuilder.of("neo4j.uri", ConfigDef.Type.STRING) {
          recommender = Recommenders.visibleIf("neo4j.other") { true }
        }
        .dependents shouldContainExactly listOf("neo4j.other")

    ConfigKeyBuilder.of("neo4j.uri", ConfigDef.Type.STRING) {
          recommender =
              Recommenders.and(
                  Recommenders.visibleIf("neo4j.other") { true },
                  Recommenders.visibleIf("neo4j.another") { true },
                  Recommenders.enum(AuthenticationType::class.java))
        }
        .dependents shouldContainExactly listOf("neo4j.other", "neo4j.another")
  }
}
