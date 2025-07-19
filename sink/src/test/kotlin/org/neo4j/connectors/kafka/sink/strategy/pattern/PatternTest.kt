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
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

class PatternTest {

  @Nested
  inner class NodePattern {

    @ParameterizedTest
    @ValueSource(strings = ["(:LabelA:LabelB{!id,*})", "LabelA:LabelB{!id,*}"])
    fun `should extract all params`(pattern: String) {
      val result = Pattern.parse(pattern)

      result shouldBe
          NodePattern(
              setOf("LabelA", "LabelB"),
              true,
              setOf(PropertyMapping("id", "id")),
              emptySet(),
              emptySet(),
          )
      result.text shouldBe "(:LabelA:LabelB {!id: id, *})"
    }

    @ParameterizedTest
    @ValueSource(strings = ["(:LabelA{!id,foo,bar})", "LabelA{!id,foo,bar}"])
    fun `should extract all fixed params`(pattern: String) {
      val result = Pattern.parse(pattern)

      result shouldBe
          NodePattern(
              setOf("LabelA"),
              false,
              setOf(PropertyMapping("id", "id")),
              setOf(PropertyMapping("foo", "foo"), PropertyMapping("bar", "bar")),
              emptySet(),
          )
      result.text shouldBe "(:LabelA {!id: id, foo: foo, bar: bar})"
    }

    @ParameterizedTest
    @ValueSource(
        strings =
            [
                "(:`LabelA`:`Label B`{!`id`,`a foo`,`b bar`,`c dar`: `c bar`})",
                "`LabelA`:`Label B`{!`id`,`a foo`,`b bar`,`c dar`: `c bar`}",
            ]
    )
    fun `should extract escaped labels and properties`(pattern: String) {
      val result = Pattern.parse(pattern)

      result shouldBe
          NodePattern(
              setOf("LabelA", "Label B"),
              false,
              setOf(PropertyMapping("id", "id")),
              setOf(
                  PropertyMapping("a foo", "a foo"),
                  PropertyMapping("b bar", "b bar"),
                  PropertyMapping("c bar", "c dar"),
              ),
              emptySet(),
          )
      result.text shouldBe
          "(:LabelA:`Label B` {!id: id, `a foo`: `a foo`, `b bar`: `b bar`, `c dar`: `c bar`})"
    }

    @ParameterizedTest
    @ValueSource(strings = ["(:LabelA{!id,foo.bar})", "LabelA{!id,foo.bar}"])
    fun `should extract complex params`(pattern: String) {
      val result = Pattern.parse(pattern)

      result shouldBe
          NodePattern(
              setOf("LabelA"),
              false,
              setOf(PropertyMapping("id", "id")),
              setOf(PropertyMapping("foo.bar", "foo.bar")),
              emptySet(),
          )
      result.text shouldBe "(:LabelA {!id: id, `foo.bar`: `foo.bar`})"
    }

    @ParameterizedTest
    @ValueSource(strings = ["(:LabelA{!idA,!idB,foo,bar})", "LabelA{!idA,!idB,foo,bar}"])
    fun `should extract composite keys with fixed params`(pattern: String) {
      val result = Pattern.parse(pattern)

      result shouldBe
          NodePattern(
              setOf("LabelA"),
              false,
              setOf(PropertyMapping("idA", "idA"), PropertyMapping("idB", "idB")),
              setOf(PropertyMapping("foo", "foo"), PropertyMapping("bar", "bar")),
              emptySet(),
          )
      result.text shouldBe "(:LabelA {!idA: idA, !idB: idB, foo: foo, bar: bar})"
    }

    @ParameterizedTest
    @ValueSource(strings = ["(:LabelA{!id,-foo,-bar})", "LabelA{!id,-foo,-bar}"])
    fun `should extract all excluded params`(pattern: String) {
      val result = Pattern.parse(pattern)

      result shouldBe
          NodePattern(
              setOf("LabelA"),
              true,
              setOf(PropertyMapping("id", "id")),
              emptySet(),
              setOf("foo", "bar"),
          )
      result.text shouldBe "(:LabelA {!id: id, -foo, -bar, *})"
    }

    @ParameterizedTest
    @ValueSource(
        strings = ["(:LabelA:LabelB{!id: customer.id,*})", "LabelA:LabelB{!id: customer.id,*}"]
    )
    fun `should extract all params aliased`(pattern: String) {
      val result = Pattern.parse(pattern)

      result shouldBe
          NodePattern(
              setOf("LabelA", "LabelB"),
              true,
              setOf(PropertyMapping("customer.id", "id")),
              emptySet(),
              emptySet(),
          )
      result.text shouldBe "(:LabelA:LabelB {!id: `customer.id`, *})"
    }

    @ParameterizedTest
    @ValueSource(
        strings =
            [
                "(:LabelA{!id:product.id,foo: product.foo,bar:product.bar})",
                "LabelA{!id:product.id,foo: product.foo,bar:product.bar}",
            ]
    )
    fun `should extract all fixed params aliased`(pattern: String) {
      val result = Pattern.parse(pattern)

      result shouldBe
          NodePattern(
              setOf("LabelA"),
              false,
              setOf(PropertyMapping("product.id", "id")),
              setOf(PropertyMapping("product.foo", "foo"), PropertyMapping("product.bar", "bar")),
              emptySet(),
          )
      result.text shouldBe "(:LabelA {!id: `product.id`, foo: `product.foo`, bar: `product.bar`})"
    }

    @ParameterizedTest
    @ValueSource(
        strings =
            [
                "(:LabelA{!id:product.id,foo.bar: product.foo.bar})",
                "LabelA{!id:product.id,foo.bar:product.foo.bar}",
            ]
    )
    fun `should extract complex params aliased`(pattern: String) {
      val result = Pattern.parse(pattern)

      result shouldBe
          NodePattern(
              setOf("LabelA"),
              false,
              setOf(PropertyMapping("product.id", "id")),
              setOf(PropertyMapping("product.foo.bar", "foo.bar")),
              emptySet(),
          )
      result.text shouldBe "(:LabelA {!id: `product.id`, `foo.bar`: `product.foo.bar`})"
    }

    @ParameterizedTest
    @ValueSource(
        strings =
            [
                "(:LabelA{!idA: product.id,!idB: stock.id,foo: product.foo,bar})",
                "LabelA{!idA:product.id,!idB:stock.id,foo:product.foo,bar}",
            ]
    )
    fun `should extract composite keys with fixed params aliased`(pattern: String) {
      val result = Pattern.parse(pattern)

      result shouldBe
          NodePattern(
              setOf("LabelA"),
              false,
              setOf(PropertyMapping("product.id", "idA"), PropertyMapping("stock.id", "idB")),
              setOf(PropertyMapping("product.foo", "foo"), PropertyMapping("bar", "bar")),
              emptySet(),
          )
      result.text shouldBe
          "(:LabelA {!idA: `product.id`, !idB: `stock.id`, foo: `product.foo`, bar: bar})"
    }

    @ParameterizedTest
    @ValueSource(
        strings = ["(:LabelA{!id:product.id,-foo,-bar})", "LabelA{!id:product.id,-foo,-bar}"]
    )
    fun `should extract all excluded params aliased`(pattern: String) {
      val result = Pattern.parse(pattern)

      result shouldBe
          NodePattern(
              setOf("LabelA"),
              true,
              setOf(PropertyMapping("product.id", "id")),
              emptySet(),
              setOf("foo", "bar"),
          )
      result.text shouldBe "(:LabelA {!id: `product.id`, -foo, -bar, *})"
    }

    @ParameterizedTest
    @ValueSource(strings = ["(:LabelA{!id,-foo,bar})", "LabelA{!id,-foo,bar}"])
    fun `should throw an exception because of mixed configuration`(pattern: String) {
      assertFailsWith(PatternException::class) { Pattern.parse(pattern) } shouldHaveMessage
          "Property inclusions and exclusions are mutually exclusive."
    }

    @Test
    fun `should throw an exception because of invalid label pattern`() {
      val pattern = "(LabelA{!id,-foo,bar})"

      assertFailsWith(PatternException::class) { Pattern.parse(pattern) }.message shouldStartWith
          "Invalid pattern:"
    }

    @ParameterizedTest
    @ValueSource(strings = ["(:LabelA{!id,-foo: xyz})", "LabelA{!id,-foo:product.foo}"])
    fun `should throw an exception because of invalid alias`(pattern: String) {
      assertFailsWith(PatternException::class) { Pattern.parse(pattern) }.message shouldStartWith
          "Invalid pattern:"
    }

    @ParameterizedTest
    @ValueSource(strings = ["(:LabelA{id,bar})", "LabelA{id,foo,bar}"])
    fun `should throw an exception because the pattern is missing a key`(pattern: String) {
      assertFailsWith(PatternException::class) { Pattern.parse(pattern) } shouldHaveMessage
          "At least one key selector must be specified in node patterns."
    }

    @ParameterizedTest
    @ValueSource(strings = ["(:LabelA{})", "LabelA{}"])
    fun `should throw an exception because the pattern is missing a key when selectors are empty`(
        pattern: String
    ) {
      assertFailsWith(PatternException::class) { Pattern.parse(pattern) } shouldHaveMessage
          "At least one key selector must be specified in node patterns."
    }
  }

  @Nested
  inner class RelationshipPattern {

    @ParameterizedTest
    @ValueSource(
        strings =
            [
                "(:LabelA{!idA,aa})-[:REL_TYPE]->(:LabelB{!idB,bb})",
                "LabelA{!idA,aa} REL_TYPE LabelB{!idB,bb}",
            ]
    )
    fun `should extract all params`(pattern: String) {
      val result = Pattern.parse(pattern)

      result shouldBe
          RelationshipPattern(
              "REL_TYPE",
              NodePattern(
                  setOf("LabelA"),
                  false,
                  setOf(PropertyMapping("idA", "idA")),
                  setOf(PropertyMapping("aa", "aa")),
                  emptySet(),
              ),
              NodePattern(
                  setOf("LabelB"),
                  false,
                  setOf(PropertyMapping("idB", "idB")),
                  setOf(PropertyMapping("bb", "bb")),
                  emptySet(),
              ),
              true,
              emptySet(),
              emptySet(),
              emptySet(),
          )
      result.text shouldBe
          "(:LabelA {!idA: idA, aa: aa})-[:REL_TYPE {*}]->(:LabelB {!idB: idB, bb: bb})"
    }

    @Test
    fun `should extract all params with reverse source and target`() {
      val pattern = "(:LabelA{!idA,aa})<-[:REL_TYPE]-(:LabelB{!idB,bb})"

      val result = Pattern.parse(pattern)

      result shouldBe
          RelationshipPattern(
              "REL_TYPE",
              NodePattern(
                  setOf("LabelB"),
                  false,
                  setOf(PropertyMapping("idB", "idB")),
                  setOf(PropertyMapping("bb", "bb")),
                  emptySet(),
              ),
              NodePattern(
                  setOf("LabelA"),
                  false,
                  setOf(PropertyMapping("idA", "idA")),
                  setOf(PropertyMapping("aa", "aa")),
                  emptySet(),
              ),
              true,
              emptySet(),
              emptySet(),
              emptySet(),
          )
      result.text shouldBe
          "(:LabelB {!idB: idB, bb: bb})-[:REL_TYPE {*}]->(:LabelA {!idA: idA, aa: aa})"
    }

    @ParameterizedTest
    @ValueSource(
        strings =
            [
                "(:LabelA{!idA})-[:REL_TYPE{foo, BAR}]->(:LabelB{!idB})",
                "LabelA{!idA} REL_TYPE{foo, BAR} LabelB{!idB}",
            ]
    )
    fun `should extract all fixed params`(pattern: String) {
      val result = Pattern.parse(pattern)

      result shouldBe
          RelationshipPattern(
              "REL_TYPE",
              NodePattern(
                  setOf("LabelA"),
                  false,
                  setOf(PropertyMapping("idA", "idA")),
                  emptySet(),
                  emptySet(),
              ),
              NodePattern(
                  setOf("LabelB"),
                  false,
                  setOf(PropertyMapping("idB", "idB")),
                  emptySet(),
                  emptySet(),
              ),
              false,
              emptySet(),
              setOf(PropertyMapping("foo", "foo"), PropertyMapping("BAR", "BAR")),
              emptySet(),
          )
      result.text shouldBe
          "(:LabelA {!idA: idA})-[:REL_TYPE {foo: foo, BAR: BAR}]->(:LabelB {!idB: idB})"
    }

    @ParameterizedTest
    @ValueSource(
        strings =
            [
                "(:LabelA{!idA})-[:REL_TYPE{foo.BAR, BAR.foo}]->(:LabelB{!idB})",
                "LabelA{!idA} REL_TYPE{foo.BAR, BAR.foo} LabelB{!idB}",
            ]
    )
    fun `should extract complex params`(pattern: String) {
      val result = Pattern.parse(pattern)

      result shouldBe
          RelationshipPattern(
              "REL_TYPE",
              NodePattern(
                  setOf("LabelA"),
                  false,
                  setOf(PropertyMapping("idA", "idA")),
                  emptySet(),
                  emptySet(),
              ),
              NodePattern(
                  setOf("LabelB"),
                  false,
                  setOf(PropertyMapping("idB", "idB")),
                  emptySet(),
                  emptySet(),
              ),
              false,
              emptySet(),
              setOf(PropertyMapping("foo.BAR", "foo.BAR"), PropertyMapping("BAR.foo", "BAR.foo")),
              emptySet(),
          )
      result.text shouldBe
          "(:LabelA {!idA: idA})-[:REL_TYPE {`foo.BAR`: `foo.BAR`, `BAR.foo`: `BAR.foo`}]->(:LabelB {!idB: idB})"
    }

    @ParameterizedTest
    @ValueSource(
        strings =
            [
                "(:LabelA{!idA})-[:REL_TYPE{-foo, -BAR}]->(:LabelB{!idB})",
                "LabelA{!idA} REL_TYPE{-foo, -BAR} LabelB{!idB}",
            ]
    )
    fun `should extract all excluded params`(pattern: String) {
      val result = Pattern.parse(pattern)

      result shouldBe
          RelationshipPattern(
              "REL_TYPE",
              NodePattern(
                  setOf("LabelA"),
                  false,
                  setOf(PropertyMapping("idA", "idA")),
                  emptySet(),
                  emptySet(),
              ),
              NodePattern(
                  setOf("LabelB"),
                  false,
                  setOf(PropertyMapping("idB", "idB")),
                  emptySet(),
                  emptySet(),
              ),
              true,
              emptySet(),
              emptySet(),
              setOf("foo", "BAR"),
          )
      result.text shouldBe
          "(:LabelA {!idA: idA})-[:REL_TYPE {-foo, -BAR, *}]->(:LabelB {!idB: idB})"
    }

    @ParameterizedTest
    @ValueSource(
        strings =
            [
                "(:LabelA{!id:start.id,aa:start.aa})-[:REL_TYPE]->(:LabelB{!id:end.id,bb:end.bb})",
                "LabelA{!id:start.id,aa:start.aa} REL_TYPE LabelB{!id:end.id,bb:end.bb}",
            ]
    )
    fun `should extract all params aliased`(pattern: String) {
      val result = Pattern.parse(pattern)

      result shouldBe
          RelationshipPattern(
              "REL_TYPE",
              NodePattern(
                  setOf("LabelA"),
                  false,
                  setOf(PropertyMapping("start.id", "id")),
                  setOf(PropertyMapping("start.aa", "aa")),
                  emptySet(),
              ),
              NodePattern(
                  setOf("LabelB"),
                  false,
                  setOf(PropertyMapping("end.id", "id")),
                  setOf(PropertyMapping("end.bb", "bb")),
                  emptySet(),
              ),
              true,
              emptySet(),
              emptySet(),
              emptySet(),
          )
      result.text shouldBe
          "(:LabelA {!id: `start.id`, aa: `start.aa`})-[:REL_TYPE {*}]->(:LabelB {!id: `end.id`, bb: `end.bb`})"
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
                  false,
                  setOf(PropertyMapping("start.id", "id")),
                  setOf(PropertyMapping("start.bb", "bb")),
                  emptySet(),
              ),
              NodePattern(
                  setOf("LabelA"),
                  false,
                  setOf(PropertyMapping("end.id", "id")),
                  setOf(PropertyMapping("end.aa", "aa")),
                  emptySet(),
              ),
              true,
              emptySet(),
              emptySet(),
              emptySet(),
          )
      result.text shouldBe
          "(:LabelB {!id: `start.id`, bb: `start.bb`})-[:REL_TYPE {*}]->(:LabelA {!id: `end.id`, aa: `end.aa`})"
    }

    @ParameterizedTest
    @ValueSource(
        strings =
            [
                "(:LabelA{!id:start.id})-[:REL_TYPE{foo:rel.foo, BAR:rel.BAR}]->(:LabelB{!id:end.id})",
                "LabelA{!id:start.id} REL_TYPE{foo:rel.foo, BAR:rel.BAR} LabelB{!id:end.id}",
            ]
    )
    fun `should extract all fixed params aliased`(pattern: String) {
      val result = Pattern.parse(pattern)

      result shouldBe
          RelationshipPattern(
              "REL_TYPE",
              NodePattern(
                  setOf("LabelA"),
                  false,
                  setOf(PropertyMapping("start.id", "id")),
                  emptySet(),
                  emptySet(),
              ),
              NodePattern(
                  setOf("LabelB"),
                  false,
                  setOf(PropertyMapping("end.id", "id")),
                  emptySet(),
                  emptySet(),
              ),
              false,
              emptySet(),
              setOf(PropertyMapping("rel.foo", "foo"), PropertyMapping("rel.BAR", "BAR")),
              emptySet(),
          )
      result.text shouldBe
          "(:LabelA {!id: `start.id`})-[:REL_TYPE {foo: `rel.foo`, BAR: `rel.BAR`}]->(:LabelB {!id: `end.id`})"
    }

    @ParameterizedTest
    @ValueSource(
        strings =
            [
                "(:LabelA{!id:start.id})-[:REL_TYPE{foo.BAR:rel.foo.BAR, BAR.foo:rel.BAR.foo}]->(:LabelB{!id:end.id})",
                "LabelA{!id:start.id} REL_TYPE{foo.BAR:rel.foo.BAR, BAR.foo:rel.BAR.foo} LabelB{!id:end.id}",
            ]
    )
    fun `should extract complex params aliased`(pattern: String) {
      val result = Pattern.parse(pattern)

      result shouldBe
          RelationshipPattern(
              "REL_TYPE",
              NodePattern(
                  setOf("LabelA"),
                  false,
                  setOf(PropertyMapping("start.id", "id")),
                  emptySet(),
                  emptySet(),
              ),
              NodePattern(
                  setOf("LabelB"),
                  false,
                  setOf(PropertyMapping("end.id", "id")),
                  emptySet(),
                  emptySet(),
              ),
              false,
              emptySet(),
              setOf(
                  PropertyMapping("rel.foo.BAR", "foo.BAR"),
                  PropertyMapping("rel.BAR.foo", "BAR.foo"),
              ),
              emptySet(),
          )
      result.text shouldBe
          "(:LabelA {!id: `start.id`})-[:REL_TYPE {`foo.BAR`: `rel.foo.BAR`, `BAR.foo`: `rel.BAR.foo`}]->(:LabelB {!id: `end.id`})"
    }

    @ParameterizedTest
    @ValueSource(
        strings =
            [
                "(:LabelA{!id:start.id})-[:REL_TYPE{-foo, -BAR}]->(:LabelB{!id:end.id})",
                "LabelA{!id:start.id} REL_TYPE{-foo, -BAR} LabelB{!id:end.id}",
            ]
    )
    fun `should extract all excluded params aliased`(pattern: String) {
      val result = Pattern.parse(pattern)

      result shouldBe
          RelationshipPattern(
              "REL_TYPE",
              NodePattern(
                  setOf("LabelA"),
                  false,
                  setOf(PropertyMapping("start.id", "id")),
                  emptySet(),
                  emptySet(),
              ),
              NodePattern(
                  setOf("LabelB"),
                  false,
                  setOf(PropertyMapping("end.id", "id")),
                  emptySet(),
                  emptySet(),
              ),
              true,
              emptySet(),
              emptySet(),
              setOf("foo", "BAR"),
          )
      result.text shouldBe
          "(:LabelA {!id: `start.id`})-[:REL_TYPE {-foo, -BAR, *}]->(:LabelB {!id: `end.id`})"
    }

    @Test
    fun `should throw an exception because of unspecified direction`() {
      val pattern = "(:LabelA{!id})-[:REL_TYPE{foo}]-(:LabelB{!idB,-exclude})"

      assertFailsWith(PatternException::class) { Pattern.parse(pattern) } shouldHaveMessage
          "Direction of relationship pattern must be explicitly set."
    }

    @ParameterizedTest
    @ValueSource(
        strings =
            [
                "(:LabelA{!id})-[:REL_TYPE{foo}]->(:LabelB{!idB,-exclude})",
                "LabelA{!id} REL_TYPE{foo} LabelB{!idB,-exclude}",
            ]
    )
    fun `should throw an exception because of property exclusion`(pattern: String) {
      assertFailsWith(PatternException::class) { Pattern.parse(pattern) } shouldHaveMessage
          "Property exclusions are not allowed on start and end node patterns."
    }

    @ParameterizedTest
    @ValueSource(
        strings =
            [
                "(:LabelA{!id})-[:REL_TYPE{foo}]->(:LabelB{!idB,*})",
                "LabelA{!id} REL_TYPE{foo} LabelB{!idB,*}",
            ]
    )
    fun `should throw an exception because of wildcard property inclusion`(pattern: String) {
      assertFailsWith(PatternException::class) { Pattern.parse(pattern) } shouldHaveMessage
          "Wildcard property inclusion is not allowed on start and end node patterns."
    }

    @ParameterizedTest
    @ValueSource(
        strings =
            [
                "(:LabelA{!id})-[:REL_TYPE{foo, -BAR}]->(:LabelB{!idB})",
                "LabelA{!id} REL_TYPE{foo, -BAR} LabelB{!idB}",
            ]
    )
    fun `should throw an exception because of mixed configuration`(pattern: String) {
      assertFailsWith(PatternException::class) { Pattern.parse(pattern) } shouldHaveMessage
          "Property inclusions and exclusions are mutually exclusive."
    }

    @ParameterizedTest
    @ValueSource(
        strings =
            [
                "(:LabelA{id})-[:REL_TYPE{foo,BAR}]->(:LabelB{!idB})",
                "LabelA{id} REL_TYPE{foo,BAR} LabelB{!idB}",
            ]
    )
    fun `should throw an exception because the node pattern is missing a key`(pattern: String) {
      assertFailsWith(PatternException::class) { Pattern.parse(pattern) } shouldHaveMessage
          "At least one key selector must be specified in node patterns."
    }

    @ParameterizedTest
    @ValueSource(
        strings =
            [
                "(:LabelA{})-[:REL_TYPE{foo,BAR}]->(:LabelB{!idB})",
                "LabelA{} REL_TYPE{foo,BAR} LabelB{!idB}",
            ]
    )
    fun `should throw an exception because the node pattern is missing a key when selectors are empty`(
        pattern: String
    ) {
      assertFailsWith(PatternException::class) { Pattern.parse(pattern) } shouldHaveMessage
          "At least one key selector must be specified in node patterns."
    }

    @Test
    fun `should throw an exception because of invalid label pattern`() {
      val pattern = "(LabelA{!id})-[:REL_TYPE{foo,BAR}]->(:LabelB{!idB})"

      assertFailsWith(PatternException::class) { Pattern.parse(pattern) }.message shouldStartWith
          "Invalid pattern:"
    }

    @ParameterizedTest
    @ValueSource(
        strings =
            [
                "(:LabelA{!id})-[REL_TYPE{foo,BAR}]->(:LabelB{!idB})",
                "LabelA{id} :REL_TYPE{foo,BAR} LabelB{!idB}",
            ]
    )
    fun `should throw an exception because of invalid relationship pattern`(pattern: String) {
      assertFailsWith(PatternException::class) { Pattern.parse(pattern) }.message shouldStartWith
          "Invalid pattern:"
    }

    @ParameterizedTest
    @ValueSource(
        strings =
            [
                "(LabelA{!id})-[:REL_TYPE{-foo:abc}]->(:LabelB{!idB})",
                "LabelA{id} :REL_TYPE{-bar:abc} LabelB{!idB}",
            ]
    )
    fun `should throw an exception because the pattern has aliased exclusion`(pattern: String) {
      assertFailsWith(PatternException::class) { Pattern.parse(pattern) }.message shouldStartWith
          "Invalid pattern:"
    }

    @ParameterizedTest
    @ValueSource(
        strings =
            [
                "(LabelA{!id})-[:REL_TYPE{-foo}]->(:LabelB{!idB,-prop: abc})",
                "LabelA{id} :REL_TYPE{foo,BAR} LabelB{!idB,-bb:bb}",
            ]
    )
    fun `should throw an exception because the pattern has aliased exclusion on node pattern`(
        pattern: String
    ) {
      assertFailsWith(PatternException::class) { Pattern.parse(pattern) }.message shouldStartWith
          "Invalid pattern:"
    }
  }
}
