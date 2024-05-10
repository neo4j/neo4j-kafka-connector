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
package org.neo4j.connectors.kafka.sink.strategy.pattern

import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldStartWith
import io.kotest.matchers.throwable.shouldHaveMessage
import kotlin.test.assertFailsWith
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

class PatternTest {

  @Nested
  inner class NodePattern {

    @Test
    fun `should extract all params`() {
      val pattern = "(:LabelA:LabelB{!id,*})"

      val result = Pattern.parse(pattern)

      result shouldBe NodePattern(setOf("LabelA", "LabelB"), setOf("id"), setOf("*"), emptySet())
    }

    @Test
    fun `should extract all fixed params`() {
      val pattern = "(:LabelA{!id,foo,bar})"

      val result = Pattern.parse(pattern)

      result shouldBe NodePattern(setOf("LabelA"), setOf("id"), setOf("foo", "bar"), emptySet())
    }

    @Test
    fun `should extract complex params`() {
      val pattern = "(:LabelA{!id,foo.bar})"

      val result = Pattern.parse(pattern)

      result shouldBe NodePattern(setOf("LabelA"), setOf("id"), setOf("foo.bar"), emptySet())
    }

    @Test
    fun `should extract composite keys with fixed params`() {
      val pattern = "(:LabelA{!idA,!idB,foo,bar})"

      val result = Pattern.parse(pattern)

      result shouldBe
          NodePattern(setOf("LabelA"), setOf("idA", "idB"), setOf("foo", "bar"), emptySet())
    }

    @Test
    fun `should extract all excluded params`() {
      val pattern = "(:LabelA{!id,-foo,-bar})"

      val result = Pattern.parse(pattern)

      result shouldBe NodePattern(setOf("LabelA"), setOf("id"), emptySet(), setOf("foo", "bar"))
    }

    @Test
    fun `should throw an exception because of mixed configuration`() {
      val pattern = "(:LabelA{!id,-foo,bar})"

      assertFailsWith(PatternException::class) { Pattern.parse(pattern) } shouldHaveMessage
          "Property inclusions and exclusions are mutually exclusive."
    }

    @Test
    fun `should throw an exception because of invalid pattern`() {
      val pattern = "(LabelA{!id,-foo,bar})"

      assertFailsWith(PatternException::class) { Pattern.parse(pattern) }.message shouldStartWith
          "Invalid pattern:"
    }

    @Test
    fun `should throw an exception because the pattern should contain a key`() {
      val pattern = "(:LabelA{id,bar})"

      assertFailsWith(PatternException::class) { Pattern.parse(pattern) } shouldHaveMessage
          "At least one key selector must be specified in node patterns."
    }

    @Test
    fun `should throw an exception because the pattern should contain a key when selectors are empty`() {
      val pattern = "(:LabelA{})"

      assertFailsWith(PatternException::class) { Pattern.parse(pattern) } shouldHaveMessage
          "At least one key selector must be specified in node patterns."
    }

    @Test
    fun `should extract all params - simple`() {
      val pattern = "LabelA:LabelB{!id,*}"

      val result = Pattern.parse(pattern)

      result shouldBe NodePattern(setOf("LabelA", "LabelB"), setOf("id"), setOf("*"), emptySet())
    }

    @Test
    fun `should extract all fixed params - simple`() {
      val pattern = "LabelA{!id,foo,bar}"

      val result = Pattern.parse(pattern)

      result shouldBe NodePattern(setOf("LabelA"), setOf("id"), setOf("foo", "bar"), emptySet())
    }

    @Test
    fun `should extract complex params - simple`() {
      val pattern = "LabelA{!id,foo.bar}"

      val result = Pattern.parse(pattern)

      result shouldBe NodePattern(setOf("LabelA"), setOf("id"), setOf("foo.bar"), emptySet())
    }

    @Test
    fun `should extract composite keys with fixed params - simple`() {
      val pattern = "LabelA{!idA,!idB,foo,bar}"

      val result = Pattern.parse(pattern)

      result shouldBe
          NodePattern(setOf("LabelA"), setOf("idA", "idB"), setOf("foo", "bar"), emptySet())
    }

    @Test
    fun `should extract all excluded params - simple`() {
      val pattern = "LabelA{!id,-foo,-bar}"

      val result =
          Pattern.parse(
              pattern,
          )

      result shouldBe NodePattern(setOf("LabelA"), setOf("id"), emptySet(), setOf("foo", "bar"))
    }

    @Test
    fun `should throw an exception because of mixed configuration - simple`() {
      val pattern = "LabelA{!id,-foo,bar}"

      assertFailsWith(PatternException::class) {
        Pattern.parse(
            pattern,
        )
      } shouldHaveMessage "Property inclusions and exclusions are mutually exclusive."
    }

    @Test
    fun `should throw an exception because the pattern should contain a key - simple`() {
      val pattern = "LabelA{id,foo,bar}"

      assertFailsWith(PatternException::class) { Pattern.parse(pattern) } shouldHaveMessage
          "At least one key selector must be specified in node patterns."
    }

    @Test
    fun `should throw an exception because the pattern should contain a key when selectors are empty - simple`() {
      val pattern = "LabelA{}"

      assertFailsWith(PatternException::class) { Pattern.parse(pattern) } shouldHaveMessage
          "At least one key selector must be specified in node patterns."
    }
  }

  @Nested
  inner class RelationshipPattern {

    @Test
    fun `should extract all params`() {
      val pattern = "(:LabelA{!idA,aa})-[:REL_TYPE]->(:LabelB{!idB,bb})"

      val result = Pattern.parse(pattern)

      result shouldBe
          RelationshipPattern(
              "REL_TYPE",
              NodePattern(setOf("LabelA"), setOf("idA"), setOf("aa"), emptySet()),
              NodePattern(setOf("LabelB"), setOf("idB"), setOf("bb"), emptySet()),
              emptySet(),
              emptySet(),
              emptySet())
    }

    @Test
    fun `should extract all params with reverse source and target`() {
      val pattern = "(:LabelA{!idA,aa})<-[:REL_TYPE]-(:LabelB{!idB,bb})"

      val result = Pattern.parse(pattern)

      result shouldBe
          RelationshipPattern(
              "REL_TYPE",
              NodePattern(setOf("LabelB"), setOf("idB"), setOf("bb"), emptySet()),
              NodePattern(setOf("LabelA"), setOf("idA"), setOf("aa"), emptySet()),
              emptySet(),
              emptySet(),
              emptySet())
    }

    @Test
    fun `should extract all fixed params`() {
      val pattern = "(:LabelA{!idA})-[:REL_TYPE{foo, BAR}]->(:LabelB{!idB})"

      val result = Pattern.parse(pattern)

      result shouldBe
          RelationshipPattern(
              "REL_TYPE",
              NodePattern(setOf("LabelA"), setOf("idA"), emptySet(), emptySet()),
              NodePattern(setOf("LabelB"), setOf("idB"), emptySet(), emptySet()),
              emptySet(),
              setOf("foo", "BAR"),
              emptySet())
    }

    @Test
    fun `should extract complex params`() {
      val pattern = "(:LabelA{!idA})-[:REL_TYPE{foo.BAR, BAR.foo}]->(:LabelB{!idB})"

      val result = Pattern.parse(pattern)

      result shouldBe
          RelationshipPattern(
              "REL_TYPE",
              NodePattern(setOf("LabelA"), setOf("idA"), emptySet(), emptySet()),
              NodePattern(setOf("LabelB"), setOf("idB"), emptySet(), emptySet()),
              emptySet(),
              setOf("foo.BAR", "BAR.foo"),
              emptySet())
    }

    @Test
    fun `should extract all excluded params`() {
      val pattern = "(:LabelA{!idA})-[:REL_TYPE{-foo, -BAR}]->(:LabelB{!idB})"

      val result = Pattern.parse(pattern)

      result shouldBe
          RelationshipPattern(
              "REL_TYPE",
              NodePattern(setOf("LabelA"), setOf("idA"), emptySet(), emptySet()),
              NodePattern(setOf("LabelB"), setOf("idB"), emptySet(), emptySet()),
              emptySet(),
              emptySet(),
              setOf("foo", "BAR"))
    }

    @Test
    fun `should throw an exception because of unspecified direction`() {
      val pattern = "(:LabelA{!id})-[:REL_TYPE{foo}]-(:LabelB{!idB,-exclude})"

      assertFailsWith(PatternException::class) { Pattern.parse(pattern) } shouldHaveMessage
          "Direction of relationship pattern must be explicitly set."
    }

    @Test
    fun `should throw an exception because of property exclusion`() {
      val pattern = "(:LabelA{!id})-[:REL_TYPE{foo}]->(:LabelB{!idB,-exclude})"

      assertFailsWith(PatternException::class) { Pattern.parse(pattern) } shouldHaveMessage
          "Property exclusions are not allowed on start and end node patterns."
    }

    @Test
    fun `should throw an exception because of wildcard property inclusion`() {
      val pattern = "(:LabelA{!id})-[:REL_TYPE{foo}]->(:LabelB{!idB,*})"

      assertFailsWith(PatternException::class) { Pattern.parse(pattern) } shouldHaveMessage
          "Wildcard property inclusion is not allowed on start and end node patterns."
    }

    @Test
    fun `should throw an exception because of mixed configuration`() {
      val pattern = "(:LabelA{!id})-[:REL_TYPE{foo, -BAR}]->(:LabelB{!idB})"

      assertFailsWith(PatternException::class) { Pattern.parse(pattern) } shouldHaveMessage
          "Property inclusions and exclusions are mutually exclusive."
    }

    @Test
    fun `should throw an exception because the pattern should contain nodes with ids`() {
      val pattern = "(:LabelA{id})-[:REL_TYPE{foo,BAR}]->(:LabelB{!idB})"

      assertFailsWith(PatternException::class) { Pattern.parse(pattern) } shouldHaveMessage
          "At least one key selector must be specified in node patterns."
    }

    @Test
    fun `should throw an exception because the pattern should contain nodes with ids when selectors are empty`() {
      val pattern = "(:LabelA{})-[:REL_TYPE{foo,BAR}]->(:LabelB{!idB})"

      assertFailsWith(PatternException::class) { Pattern.parse(pattern) } shouldHaveMessage
          "At least one key selector must be specified in node patterns."
    }

    @Test
    fun `should throw an exception because the pattern is invalid`() {
      val pattern = "(LabelA{!id})-[:REL_TYPE{foo,BAR}]->(:LabelB{!idB})"

      assertFailsWith(PatternException::class) { Pattern.parse(pattern) }.message shouldStartWith
          "Invalid pattern:"
    }

    @Test
    fun `should extract all params - simple`() {
      val pattern = "LabelA{!idA,aa} REL_TYPE LabelB{!idB,bb}"

      val result = Pattern.parse(pattern)

      result shouldBe
          RelationshipPattern(
              "REL_TYPE",
              NodePattern(setOf("LabelA"), setOf("idA"), setOf("aa"), emptySet()),
              NodePattern(setOf("LabelB"), setOf("idB"), setOf("bb"), emptySet()),
              emptySet(),
              emptySet(),
              emptySet())
    }

    @Test
    fun `should extract all fixed params - simple`() {
      val pattern = "LabelA{!idA} REL_TYPE{foo, BAR} LabelB{!idB}"

      val result = Pattern.parse(pattern)

      result shouldBe
          RelationshipPattern(
              "REL_TYPE",
              NodePattern(setOf("LabelA"), setOf("idA"), emptySet(), emptySet()),
              NodePattern(setOf("LabelB"), setOf("idB"), emptySet(), emptySet()),
              emptySet(),
              setOf("foo", "BAR"),
              emptySet())
    }

    @Test
    fun `should extract complex params - simple`() {
      val pattern = "LabelA{!idA} REL_TYPE{foo.BAR, BAR.foo} LabelB{!idB}"

      val result = Pattern.parse(pattern)

      result shouldBe
          RelationshipPattern(
              "REL_TYPE",
              NodePattern(setOf("LabelA"), setOf("idA"), emptySet(), emptySet()),
              NodePattern(setOf("LabelB"), setOf("idB"), emptySet(), emptySet()),
              emptySet(),
              setOf("foo.BAR", "BAR.foo"),
              emptySet())
    }

    @Test
    fun `should extract all excluded params - simple`() {
      val pattern = "LabelA{!idA} REL_TYPE{-foo, -BAR} LabelB{!idB}"

      val result = Pattern.parse(pattern)

      result shouldBe
          RelationshipPattern(
              "REL_TYPE",
              NodePattern(setOf("LabelA"), setOf("idA"), emptySet(), emptySet()),
              NodePattern(setOf("LabelB"), setOf("idB"), emptySet(), emptySet()),
              emptySet(),
              emptySet(),
              setOf("foo", "BAR"))
    }

    @Test
    fun `should throw an exception because of property exclusion - simple`() {
      val pattern = "LabelA{!id} REL_TYPE{foo} LabelB{!idB,-exclude}"

      assertFailsWith(PatternException::class) { Pattern.parse(pattern) } shouldHaveMessage
          "Property exclusions are not allowed on start and end node patterns."
    }

    @Test
    fun `should throw an exception because of wildcard property inclusion - simple`() {
      val pattern = "LabelA{!id} REL_TYPE{foo} LabelB{!idB,*}"

      assertFailsWith(PatternException::class) { Pattern.parse(pattern) } shouldHaveMessage
          "Wildcard property inclusion is not allowed on start and end node patterns."
    }

    @Test
    fun `should throw an exception because of mixed configuration - simple`() {
      val pattern = "LabelA{!id} REL_TYPE{foo, -BAR} LabelB{!idB}"

      assertFailsWith(PatternException::class) { Pattern.parse(pattern) } shouldHaveMessage
          "Property inclusions and exclusions are mutually exclusive."
    }

    @Test
    fun `should throw an exception because the pattern should contain nodes with only ids - simple`() {
      val pattern = "LabelA{id} REL_TYPE{foo,BAR} LabelB{!idB}"

      assertFailsWith(PatternException::class) { Pattern.parse(pattern) } shouldHaveMessage
          "At least one key selector must be specified in node patterns."
    }

    @Test
    fun `should throw an exception because the pattern should contain nodes with ids when selectors are empty - simple`() {
      val pattern = "LabelA{} REL_TYPE{foo,BAR} LabelB{!idB}"

      assertFailsWith(PatternException::class) { Pattern.parse(pattern) } shouldHaveMessage
          "At least one key selector must be specified in node patterns."
    }

    @Test
    fun `should throw an exception because the pattern is invalid - simple`() {
      val pattern = "LabelA{id} :REL_TYPE{foo,BAR} LabelB{!idB}"

      assertFailsWith(PatternException::class) { Pattern.parse(pattern) }.message shouldStartWith
          "Invalid pattern:"
    }
  }
}
