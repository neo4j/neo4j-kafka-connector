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

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class WordSupportTest {

  @Test
  fun pluralizes() {
    assertEquals("flowers", WordSupport.pluralize(2, "flower", "flowers"))
    assertEquals("alumnus", WordSupport.pluralize(1, "alumnus", "alumni"))
    assertEquals("query", WordSupport.pluralize(0, "query", "queries"))
    assertEquals("query", WordSupport.pluralize(0, "query", "queries"))
    assertEquals("dollar", WordSupport.pluralize(-1, "dollar", "dollars"))
    assertEquals("years", WordSupport.pluralize(-2, "year", "years"))
  }

  @Test
  fun `converts camel case to upper snake case`() {
    assertEquals("EASY", WordSupport.camelCaseToUpperSnakeCase("easy"))
    assertEquals("THIS_IS_BANANAS", WordSupport.camelCaseToUpperSnakeCase("thisIsBananas"))
  }
}
