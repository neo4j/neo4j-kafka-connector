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

      result shouldBe
          NodePattern(setOf("LabelA", "LabelB"), mapOf("id" to "id"), mapOf("*" to "*"), emptySet())
    }

    @Test
    fun `should extract all fixed params`() {
      val pattern = "(:LabelA{!id,foo,bar})"

      val result = Pattern.parse(pattern)

      result shouldBe
          NodePattern(
              setOf("LabelA"),
              mapOf("id" to "id"),
              mapOf("foo" to "foo", "bar" to "bar"),
              emptySet())
    }

    @Test
    fun `should extract complex params`() {
      val pattern = "(:LabelA{!id,foo.bar})"

      val result = Pattern.parse(pattern)

      result shouldBe
          NodePattern(
              setOf("LabelA"), mapOf("id" to "id"), mapOf("foo.bar" to "foo.bar"), emptySet())
    }

    @Test
    fun `should extract composite keys with fixed params`() {
      val pattern = "(:LabelA{!idA,!idB,foo,bar})"

      val result = Pattern.parse(pattern)

      result shouldBe
          NodePattern(
              setOf("LabelA"),
              mapOf("idA" to "idA", "idB" to "idB"),
              mapOf("foo" to "foo", "bar" to "bar"),
              emptySet())
    }

    @Test
    fun `should extract all excluded params`() {
      val pattern = "(:LabelA{!id,-foo,-bar})"

      val result = Pattern.parse(pattern)

      result shouldBe
          NodePattern(setOf("LabelA"), mapOf("id" to "id"), emptyMap(), setOf("foo", "bar"))
    }

    @Test
    fun `should extract all params aliased`() {
      val pattern = "(:LabelA:LabelB{!id: customer.id,*})"

      val result = Pattern.parse(pattern)

      result shouldBe
          NodePattern(
              setOf("LabelA", "LabelB"),
              mapOf("id" to "customer.id"),
              mapOf("*" to "*"),
              emptySet())
    }

    @Test
    fun `should extract all fixed params aliased`() {
      val pattern = "(:LabelA{!id:product.id,foo: product.foo,bar:product.bar})"

      val result = Pattern.parse(pattern)

      result shouldBe
          NodePattern(
              setOf("LabelA"),
              mapOf("id" to "product.id"),
              mapOf("foo" to "product.foo", "bar" to "product.bar"),
              emptySet())
    }

    @Test
    fun `should extract complex params aliased`() {
      val pattern = "(:LabelA{!id:product.id,foo.bar: product.foo.bar})"

      val result = Pattern.parse(pattern)

      result shouldBe
          NodePattern(
              setOf("LabelA"),
              mapOf("id" to "product.id"),
              mapOf("foo.bar" to "product.foo.bar"),
              emptySet())
    }

    @Test
    fun `should extract composite keys with fixed params aliased`() {
      val pattern = "(:LabelA{!idA: product.id,!idB: stock.id,foo: product.foo,bar})"

      val result = Pattern.parse(pattern)

      result shouldBe
          NodePattern(
              setOf("LabelA"),
              mapOf("idA" to "product.id", "idB" to "stock.id"),
              mapOf("foo" to "product.foo", "bar" to "bar"),
              emptySet())
    }

    @Test
    fun `should extract all excluded params aliased`() {
      val pattern = "(:LabelA{!id:product.id,-foo,-bar})"

      val result = Pattern.parse(pattern)

      result shouldBe
          NodePattern(setOf("LabelA"), mapOf("id" to "product.id"), emptyMap(), setOf("foo", "bar"))
    }

    @Test
    fun `should throw an exception because of mixed configuration`() {
      val pattern = "(:LabelA{!id,-foo,bar})"

      assertFailsWith(PatternException::class) { Pattern.parse(pattern) } shouldHaveMessage
          "Property inclusions and exclusions are mutually exclusive."
    }

    @Test
    fun `should throw an exception because of invalid label pattern`() {
      val pattern = "(LabelA{!id,-foo,bar})"

      assertFailsWith(PatternException::class) { Pattern.parse(pattern) }.message shouldStartWith
          "Invalid pattern:"
    }

    @Test
    fun `should throw an exception because of invalid alias`() {
      val pattern = "(:LabelA{!id,-foo: xyz})"

      assertFailsWith(PatternException::class) { Pattern.parse(pattern) }.message shouldStartWith
          "Invalid pattern:"
    }

    @Test
    fun `should throw an exception because the pattern is missing a key`() {
      val pattern = "(:LabelA{id,bar})"

      assertFailsWith(PatternException::class) { Pattern.parse(pattern) } shouldHaveMessage
          "At least one key selector must be specified in node patterns."
    }

    @Test
    fun `should throw an exception because the pattern is missing a key when selectors are empty`() {
      val pattern = "(:LabelA{})"

      assertFailsWith(PatternException::class) { Pattern.parse(pattern) } shouldHaveMessage
          "At least one key selector must be specified in node patterns."
    }

    @Test
    fun `should extract all params - simple`() {
      val pattern = "LabelA:LabelB{!id,*}"

      val result = Pattern.parse(pattern)

      result shouldBe
          NodePattern(setOf("LabelA", "LabelB"), mapOf("id" to "id"), mapOf("*" to "*"), emptySet())
    }

    @Test
    fun `should extract all fixed params - simple`() {
      val pattern = "LabelA{!id,foo,bar}"

      val result = Pattern.parse(pattern)

      result shouldBe
          NodePattern(
              setOf("LabelA"),
              mapOf("id" to "id"),
              mapOf("foo" to "foo", "bar" to "bar"),
              emptySet())
    }

    @Test
    fun `should extract complex params - simple`() {
      val pattern = "LabelA{!id,foo.bar}"

      val result = Pattern.parse(pattern)

      result shouldBe
          NodePattern(
              setOf("LabelA"), mapOf("id" to "id"), mapOf("foo.bar" to "foo.bar"), emptySet())
    }

    @Test
    fun `should extract composite keys with fixed params - simple`() {
      val pattern = "LabelA{!idA,!idB,foo,bar}"

      val result = Pattern.parse(pattern)

      result shouldBe
          NodePattern(
              setOf("LabelA"),
              mapOf("idA" to "idA", "idB" to "idB"),
              mapOf("foo" to "foo", "bar" to "bar"),
              emptySet())
    }

    @Test
    fun `should extract all excluded params - simple`() {
      val pattern = "LabelA{!id,-foo,-bar}"

      val result =
          Pattern.parse(
              pattern,
          )

      result shouldBe
          NodePattern(setOf("LabelA"), mapOf("id" to "id"), emptyMap(), setOf("foo", "bar"))
    }

    @Test
    fun `should extract all params - simple aliased`() {
      val pattern = "LabelA:LabelB{!id:product.id,*}"

      val result = Pattern.parse(pattern)

      result shouldBe
          NodePattern(
              setOf("LabelA", "LabelB"), mapOf("id" to "product.id"), mapOf("*" to "*"), emptySet())
    }

    @Test
    fun `should extract all fixed params - simple aliased`() {
      val pattern = "LabelA{!id:product.id,foo:product.foo,bar:product.bar}"

      val result = Pattern.parse(pattern)

      result shouldBe
          NodePattern(
              setOf("LabelA"),
              mapOf("id" to "product.id"),
              mapOf("foo" to "product.foo", "bar" to "product.bar"),
              emptySet())
    }

    @Test
    fun `should extract complex params - simple aliased`() {
      val pattern = "LabelA{!id:product.id,foo.bar:product.foo.bar}"

      val result = Pattern.parse(pattern)

      result shouldBe
          NodePattern(
              setOf("LabelA"),
              mapOf("id" to "product.id"),
              mapOf("foo.bar" to "product.foo.bar"),
              emptySet())
    }

    @Test
    fun `should extract composite keys with fixed params - simple aliased`() {
      val pattern = "LabelA{!idA:product.id,!idB:stock.id,foo:product.foo,bar}"

      val result = Pattern.parse(pattern)

      result shouldBe
          NodePattern(
              setOf("LabelA"),
              mapOf("idA" to "product.id", "idB" to "stock.id"),
              mapOf("foo" to "product.foo", "bar" to "bar"),
              emptySet())
    }

    @Test
    fun `should extract all excluded params - simple aliased`() {
      val pattern = "LabelA{!id:product.id,-foo,-bar}"

      val result =
          Pattern.parse(
              pattern,
          )

      result shouldBe
          NodePattern(setOf("LabelA"), mapOf("id" to "product.id"), emptyMap(), setOf("foo", "bar"))
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
    fun `should throw an exception because of invalid alias - simple`() {
      val pattern = "LabelA{!id,-foo:product.foo}"

      assertFailsWith(PatternException::class) {
            Pattern.parse(
                pattern,
            )
          }
          .message shouldStartWith "Invalid pattern:"
    }

    @Test
    fun `should throw an exception because the pattern is missing a key - simple`() {
      val pattern = "LabelA{id,foo,bar}"

      assertFailsWith(PatternException::class) { Pattern.parse(pattern) } shouldHaveMessage
          "At least one key selector must be specified in node patterns."
    }

    @Test
    fun `should throw an exception because the pattern is missing a key when selectors are empty - simple`() {
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
              NodePattern(setOf("LabelA"), mapOf("idA" to "idA"), mapOf("aa" to "aa"), emptySet()),
              NodePattern(setOf("LabelB"), mapOf("idB" to "idB"), mapOf("bb" to "bb"), emptySet()),
              emptyMap(),
              emptyMap(),
              emptySet())
    }

    @Test
    fun `should extract all params with reverse source and target`() {
      val pattern = "(:LabelA{!idA,aa})<-[:REL_TYPE]-(:LabelB{!idB,bb})"

      val result = Pattern.parse(pattern)

      result shouldBe
          RelationshipPattern(
              "REL_TYPE",
              NodePattern(setOf("LabelB"), mapOf("idB" to "idB"), mapOf("bb" to "bb"), emptySet()),
              NodePattern(setOf("LabelA"), mapOf("idA" to "idA"), mapOf("aa" to "aa"), emptySet()),
              emptyMap(),
              emptyMap(),
              emptySet())
    }

    @Test
    fun `should extract all fixed params`() {
      val pattern = "(:LabelA{!idA})-[:REL_TYPE{foo, BAR}]->(:LabelB{!idB})"

      val result = Pattern.parse(pattern)

      result shouldBe
          RelationshipPattern(
              "REL_TYPE",
              NodePattern(setOf("LabelA"), mapOf("idA" to "idA"), emptyMap(), emptySet()),
              NodePattern(setOf("LabelB"), mapOf("idB" to "idB"), emptyMap(), emptySet()),
              emptyMap(),
              mapOf("foo" to "foo", "BAR" to "BAR"),
              emptySet())
    }

    @Test
    fun `should extract complex params`() {
      val pattern = "(:LabelA{!idA})-[:REL_TYPE{foo.BAR, BAR.foo}]->(:LabelB{!idB})"

      val result = Pattern.parse(pattern)

      result shouldBe
          RelationshipPattern(
              "REL_TYPE",
              NodePattern(setOf("LabelA"), mapOf("idA" to "idA"), emptyMap(), emptySet()),
              NodePattern(setOf("LabelB"), mapOf("idB" to "idB"), emptyMap(), emptySet()),
              emptyMap(),
              mapOf("foo.BAR" to "foo.BAR", "BAR.foo" to "BAR.foo"),
              emptySet())
    }

    @Test
    fun `should extract all excluded params`() {
      val pattern = "(:LabelA{!idA})-[:REL_TYPE{-foo, -BAR}]->(:LabelB{!idB})"

      val result = Pattern.parse(pattern)

      result shouldBe
          RelationshipPattern(
              "REL_TYPE",
              NodePattern(setOf("LabelA"), mapOf("idA" to "idA"), emptyMap(), emptySet()),
              NodePattern(setOf("LabelB"), mapOf("idB" to "idB"), emptyMap(), emptySet()),
              emptyMap(),
              emptyMap(),
              setOf("foo", "BAR"))
    }

    @Test
    fun `should extract all params aliased`() {
      val pattern =
          "(:LabelA{!id:start.id,aa:start.aa})-[:REL_TYPE]->(:LabelB{!id:end.id,bb:end.bb})"

      val result = Pattern.parse(pattern)

      result shouldBe
          RelationshipPattern(
              "REL_TYPE",
              NodePattern(
                  setOf("LabelA"),
                  mapOf("id" to "start.id"),
                  mapOf("aa" to "start.aa"),
                  emptySet()),
              NodePattern(
                  setOf("LabelB"), mapOf("id" to "end.id"), mapOf("bb" to "end.bb"), emptySet()),
              emptyMap(),
              emptyMap(),
              emptySet())
    }

    @Test
    fun `should extract all params with reverse source and target aliased`() {
      val pattern =
          "(:LabelA{!id:end.id,aa:end.aa})<-[:REL_TYPE]-(:LabelB{!id:start.id,bb:start.bb})"

      val result = Pattern.parse(pattern)

      result shouldBe
          RelationshipPattern(
              "REL_TYPE",
              NodePattern(
                  setOf("LabelB"),
                  mapOf("id" to "start.id"),
                  mapOf("bb" to "start.bb"),
                  emptySet()),
              NodePattern(
                  setOf("LabelA"), mapOf("id" to "end.id"), mapOf("aa" to "end.aa"), emptySet()),
              emptyMap(),
              emptyMap(),
              emptySet())
    }

    @Test
    fun `should extract all fixed params aliased`() {
      val pattern =
          "(:LabelA{!id:start.id})-[:REL_TYPE{foo:rel.foo, BAR:rel.bar}]->(:LabelB{!id:end.id})"

      val result = Pattern.parse(pattern)

      result shouldBe
          RelationshipPattern(
              "REL_TYPE",
              NodePattern(setOf("LabelA"), mapOf("id" to "start.id"), emptyMap(), emptySet()),
              NodePattern(setOf("LabelB"), mapOf("id" to "end.id"), emptyMap(), emptySet()),
              emptyMap(),
              mapOf("foo" to "rel.foo", "BAR" to "rel.bar"),
              emptySet())
    }

    @Test
    fun `should extract complex params aliased`() {
      val pattern =
          "(:LabelA{!id:start.id})-[:REL_TYPE{foo.BAR:rel.foo.BAR, BAR.foo:rel.BAR.foo}]->(:LabelB{!id:end.id})"

      val result = Pattern.parse(pattern)

      result shouldBe
          RelationshipPattern(
              "REL_TYPE",
              NodePattern(setOf("LabelA"), mapOf("id" to "start.id"), emptyMap(), emptySet()),
              NodePattern(setOf("LabelB"), mapOf("id" to "end.id"), emptyMap(), emptySet()),
              emptyMap(),
              mapOf("foo.BAR" to "rel.foo.BAR", "BAR.foo" to "rel.BAR.foo"),
              emptySet())
    }

    @Test
    fun `should extract all excluded params aliased`() {
      val pattern = "(:LabelA{!id:start.id})-[:REL_TYPE{-foo, -BAR}]->(:LabelB{!id:end.id})"

      val result = Pattern.parse(pattern)

      result shouldBe
          RelationshipPattern(
              "REL_TYPE",
              NodePattern(setOf("LabelA"), mapOf("id" to "start.id"), emptyMap(), emptySet()),
              NodePattern(setOf("LabelB"), mapOf("id" to "end.id"), emptyMap(), emptySet()),
              emptyMap(),
              emptyMap(),
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
    fun `should throw an exception because the node pattern is missing a key`() {
      val pattern = "(:LabelA{id})-[:REL_TYPE{foo,BAR}]->(:LabelB{!idB})"

      assertFailsWith(PatternException::class) { Pattern.parse(pattern) } shouldHaveMessage
          "At least one key selector must be specified in node patterns."
    }

    @Test
    fun `should throw an exception because the node pattern is missing a key when selectors are empty`() {
      val pattern = "(:LabelA{})-[:REL_TYPE{foo,BAR}]->(:LabelB{!idB})"

      assertFailsWith(PatternException::class) { Pattern.parse(pattern) } shouldHaveMessage
          "At least one key selector must be specified in node patterns."
    }

    @Test
    fun `should throw an exception because of invalid label pattern`() {
      val pattern = "(LabelA{!id})-[:REL_TYPE{foo,BAR}]->(:LabelB{!idB})"

      assertFailsWith(PatternException::class) { Pattern.parse(pattern) }.message shouldStartWith
          "Invalid pattern:"
    }

    @Test
    fun `should throw an exception because the pattern has aliased exclusion`() {
      val pattern = "(LabelA{!id})-[:REL_TYPE{-foo:abc}]->(:LabelB{!idB})"

      assertFailsWith(PatternException::class) { Pattern.parse(pattern) }.message shouldStartWith
          "Invalid pattern:"
    }

    @Test
    fun `should throw an exception because the pattern has aliased exclusion on node pattern`() {
      val pattern = "(LabelA{!id})-[:REL_TYPE{-foo}]->(:LabelB{!idB,-prop: abc})"

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
              NodePattern(setOf("LabelA"), mapOf("idA" to "idA"), mapOf("aa" to "aa"), emptySet()),
              NodePattern(setOf("LabelB"), mapOf("idB" to "idB"), mapOf("bb" to "bb"), emptySet()),
              emptyMap(),
              emptyMap(),
              emptySet())
    }

    @Test
    fun `should extract all fixed params - simple`() {
      val pattern = "LabelA{!idA} REL_TYPE{foo, BAR} LabelB{!idB}"

      val result = Pattern.parse(pattern)

      result shouldBe
          RelationshipPattern(
              "REL_TYPE",
              NodePattern(setOf("LabelA"), mapOf("idA" to "idA"), emptyMap(), emptySet()),
              NodePattern(setOf("LabelB"), mapOf("idB" to "idB"), emptyMap(), emptySet()),
              emptyMap(),
              mapOf("foo" to "foo", "BAR" to "BAR"),
              emptySet())
    }

    @Test
    fun `should extract complex params - simple`() {
      val pattern = "LabelA{!idA} REL_TYPE{foo.BAR, BAR.foo} LabelB{!idB}"

      val result = Pattern.parse(pattern)

      result shouldBe
          RelationshipPattern(
              "REL_TYPE",
              NodePattern(setOf("LabelA"), mapOf("idA" to "idA"), emptyMap(), emptySet()),
              NodePattern(setOf("LabelB"), mapOf("idB" to "idB"), emptyMap(), emptySet()),
              emptyMap(),
              mapOf("foo.BAR" to "foo.BAR", "BAR.foo" to "BAR.foo"),
              emptySet())
    }

    @Test
    fun `should extract all excluded params - simple`() {
      val pattern = "LabelA{!idA} REL_TYPE{-foo, -BAR} LabelB{!idB}"

      val result = Pattern.parse(pattern)

      result shouldBe
          RelationshipPattern(
              "REL_TYPE",
              NodePattern(setOf("LabelA"), mapOf("idA" to "idA"), emptyMap(), emptySet()),
              NodePattern(setOf("LabelB"), mapOf("idB" to "idB"), emptyMap(), emptySet()),
              emptyMap(),
              emptyMap(),
              setOf("foo", "BAR"))
    }

    @Test
    fun `should extract all params - simple aliased`() {
      val pattern = "LabelA{!id:start.id,aa:start.aa} REL_TYPE LabelB{!id:end.id,bb:end.bb}"

      val result = Pattern.parse(pattern)

      result shouldBe
          RelationshipPattern(
              "REL_TYPE",
              NodePattern(
                  setOf("LabelA"),
                  mapOf("id" to "start.id"),
                  mapOf("aa" to "start.aa"),
                  emptySet()),
              NodePattern(
                  setOf("LabelB"), mapOf("id" to "end.id"), mapOf("bb" to "end.bb"), emptySet()),
              emptyMap(),
              emptyMap(),
              emptySet())
    }

    @Test
    fun `should extract all fixed params - simple aliased`() {
      val pattern = "LabelA{!id:start.id} REL_TYPE{foo:rel.foo, BAR:rel.BAR} LabelB{!id:end.id}"

      val result = Pattern.parse(pattern)

      result shouldBe
          RelationshipPattern(
              "REL_TYPE",
              NodePattern(setOf("LabelA"), mapOf("id" to "start.id"), emptyMap(), emptySet()),
              NodePattern(setOf("LabelB"), mapOf("id" to "end.id"), emptyMap(), emptySet()),
              emptyMap(),
              mapOf("foo" to "rel.foo", "BAR" to "rel.BAR"),
              emptySet())
    }

    @Test
    fun `should extract complex params - simple aliased`() {
      val pattern =
          "LabelA{!id:start.id} REL_TYPE{foo.BAR:rel.foo.BAR, BAR.foo:rel.BAR.foo} LabelB{!id:end.id}"

      val result = Pattern.parse(pattern)

      result shouldBe
          RelationshipPattern(
              "REL_TYPE",
              NodePattern(setOf("LabelA"), mapOf("id" to "start.id"), emptyMap(), emptySet()),
              NodePattern(setOf("LabelB"), mapOf("id" to "end.id"), emptyMap(), emptySet()),
              emptyMap(),
              mapOf("foo.BAR" to "rel.foo.BAR", "BAR.foo" to "rel.BAR.foo"),
              emptySet())
    }

    @Test
    fun `should extract all excluded params - simple aliased`() {
      val pattern = "LabelA{!id:start.id} REL_TYPE{-foo, -BAR} LabelB{!id:end.id}"

      val result = Pattern.parse(pattern)

      result shouldBe
          RelationshipPattern(
              "REL_TYPE",
              NodePattern(setOf("LabelA"), mapOf("id" to "start.id"), emptyMap(), emptySet()),
              NodePattern(setOf("LabelB"), mapOf("id" to "end.id"), emptyMap(), emptySet()),
              emptyMap(),
              emptyMap(),
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

    @Test
    fun `should throw an exception because the pattern has aliased exclusion - simple`() {
      val pattern = "LabelA{id} :REL_TYPE{-bar:abc} LabelB{!idB}"

      assertFailsWith(PatternException::class) { Pattern.parse(pattern) }.message shouldStartWith
          "Invalid pattern:"
    }

    @Test
    fun `should throw an exception because the pattern has aliased exclusion on node pattern - simple`() {
      val pattern = "LabelA{id} :REL_TYPE{foo,BAR} LabelB{!idB,-bb:bb}"

      assertFailsWith(PatternException::class) { Pattern.parse(pattern) }.message shouldStartWith
          "Invalid pattern:"
    }
  }
}
