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
package org.neo4j.cdc.client.pattern

import kotlin.math.pow
import kotlin.streams.toList
import org.antlr.v4.runtime.*
import org.neo4j.cdc.client.NodeSelector
import org.neo4j.cdc.client.RelationshipNodeSelector
import org.neo4j.cdc.client.RelationshipSelector
import org.neo4j.cdc.client.Selector

data class NodePattern(
  val labels: Set<String>,
  val keyFilters: Map<String, Any>,
  val includeProperties: Set<String>,
  val excludeProperties: Set<String>
) : Pattern {
  override fun toSelector(): Set<Selector> {
    return setOf(
      NodeSelector().also {
        it.labels = labels
        it.key = keyFilters
        it.includeProperties = includeProperties
        it.excludeProperties = excludeProperties
      })
  }
}

data class RelationshipPattern(
  val type: String,
  val start: NodePattern,
  val end: NodePattern,
  val bidirectional: Boolean,
  val keyFilters: Map<String, Any>,
  val includeProperties: Set<String>,
  val excludeProperties: Set<String>
) : Pattern {
  override fun toSelector(): Set<Selector> {
    fun toSelector(reverse: Boolean): Selector {
      return RelationshipSelector().also {
        it.type = type
        it.start =
          RelationshipNodeSelector().also { n ->
            val source = if (reverse) end else start

            n.labels = source.labels
            n.key = source.keyFilters
          }
        it.end =
          RelationshipNodeSelector().also { n ->
            val source = if (reverse) start else end

            n.labels = source.labels
            n.key = source.keyFilters
          }
        it.key = keyFilters
        it.includeProperties = includeProperties
        it.excludeProperties = excludeProperties
      }
    }

    if (bidirectional) {
      return setOf(toSelector(false), toSelector(true))
    }

    return setOf(toSelector(false))
  }
}

interface Pattern {

  fun toSelector(): Set<Selector>

  companion object {
    fun parse(source: String): List<Pattern> {
      val input = CharStreams.fromString(source)
      val lexer = PatternLexer(input)
      val tokens = CommonTokenStream(lexer)
      val parser = PatternParser(tokens)

      val errors = mutableListOf<String>()
      parser.addErrorListener(
        object : BaseErrorListener() {
          override fun syntaxError(
            recognizer: Recognizer<*, *>?,
            offendingSymbol: Any?,
            line: Int,
            charPositionInLine: Int,
            msg: String?,
            e: RecognitionException?
          ) {
            errors.add("line $line:$charPositionInLine, $msg")
          }
        })
      val patternList = parser.patternList()

      if (errors.any()) {
        throw PatternException(errors.joinToString())
      }

      return patternList.pattern()?.map { ctx -> PatternVisitor.visitPattern(ctx) }?.toList()
        ?: emptyList()
    }
  }

  private object PatternVisitor : PatternParserBaseVisitor<Pattern>() {
    override fun visitPattern(ctx: PatternParser.PatternContext?): Pattern {
      return if (ctx!!.relationshipPattern() != null) {
        val relPattern = ctx.relationshipPattern()
        val bidirectional =
          (relPattern.leftArrow() != null && relPattern.rightArrow() != null) ||
            (relPattern.leftArrow() == null && relPattern.rightArrow() == null)
        val keyFilters = mutableMapOf<String, Any>()
        val includeProperties = mutableSetOf<String>()
        val excludeProperties = mutableSetOf<String>()

        extractPropertySelectors(
          ctx.relationshipPattern().properties()?.propertySelector()?.keyFilterOrPropSelector(),
          keyFilters,
          includeProperties,
          excludeProperties)

        val startNode =
          visitNodePattern(
            ctx.nodePattern(if (bidirectional || relPattern.rightArrow() != null) 0 else 1))
        if (startNode.excludeProperties.any() || startNode.includeProperties.any()) {
          throw PatternException(
            "property selectors are not allowed in node part of relationship patterns")
        }

        val endNode =
          visitNodePattern(
            ctx.nodePattern(if (bidirectional || relPattern.rightArrow() != null) 1 else 0))
        if (endNode.excludeProperties.any() || endNode.includeProperties.any()) {
          throw PatternException(
            "property selectors are not allowed in node part of relationship patterns")
        }

        RelationshipPattern(
          LabelOrRelTypeVisitor.visitLabelOrRelType(relPattern.labelOrRelType()),
          startNode,
          endNode,
          bidirectional,
          keyFilters.toMap(),
          includeProperties.toSet(),
          excludeProperties.toSet())
      } else {
        visitNodePattern(ctx.nodePattern(0))
      }
    }

    override fun visitNodePattern(ctx: PatternParser.NodePatternContext?): NodePattern {
      val labels = NodeLabelsVisitor.visitNodeLabels(ctx!!.nodeLabels())
      val keyFilters = mutableMapOf<String, Any>()
      val includeProperties = mutableSetOf<String>()
      val excludeProperties = mutableSetOf<String>()

      extractPropertySelectors(
        ctx.properties()?.propertySelector()?.keyFilterOrPropSelector(),
        keyFilters,
        includeProperties,
        excludeProperties)

      return NodePattern(
        labels, keyFilters.toMap(), includeProperties.toSet(), excludeProperties.toSet())
    }

    private fun extractPropertySelectors(
      selectors: List<PatternParser.KeyFilterOrPropSelectorContext>?,
      keyFilters: MutableMap<String, Any>,
      includeProperties: MutableSet<String>,
      excludeProperties: MutableSet<String>
    ) {
      selectors?.forEach { child ->
        if (child.keyFilter() != null) {
          val propName =
            PropertyKeyNameVisitor.visitPropertyKeyName(child.keyFilter().propertyKeyName())
          val value = ExpressionVisitor.visitExpression(child.keyFilter().expression())

          keyFilters[propName] = value
        } else if (child.propSelector() != null) {
          if (child.propSelector().TIMES() != null) {
            includeProperties.add("*")
          } else if (child.propSelector().MINUS() != null) {
            excludeProperties.add(
              PropertyKeyNameVisitor.visitPropertyKeyName(child.propSelector().propertyKeyName()))
          } else {
            includeProperties.add(
              PropertyKeyNameVisitor.visitPropertyKeyName(child.propSelector().propertyKeyName()))
          }
        }
      }
    }
  }

  private object NodeLabelsVisitor : PatternParserBaseVisitor<Set<String>>() {
    override fun visitNodeLabels(ctx: PatternParser.NodeLabelsContext?): Set<String> {
      return ctx?.labelOrRelType()?.map(LabelOrRelTypeVisitor::visitLabelOrRelType)?.toSet()
        ?: emptySet()
    }
  }

  private object LabelOrRelTypeVisitor : PatternParserBaseVisitor<String>() {
    override fun visitLabelOrRelType(ctx: PatternParser.LabelOrRelTypeContext?): String {
      return SymbolicNameStringVisitor.visitSymbolicNameString(ctx!!.symbolicNameString())
    }
  }

  private object PropertyKeyNameVisitor : PatternParserBaseVisitor<String>() {
    override fun visitPropertyKeyName(ctx: PatternParser.PropertyKeyNameContext?): String {
      return SymbolicNameStringVisitor.visitSymbolicNameString(ctx!!.symbolicNameString())
    }
  }

  private object SymbolicNameStringVisitor : PatternParserBaseVisitor<String>() {
    override fun visitSymbolicNameString(ctx: PatternParser.SymbolicNameStringContext?): String {
      return super.visitSymbolicNameString(ctx)
    }

    override fun visitEscapedSymbolicNameString(
      ctx: PatternParser.EscapedSymbolicNameStringContext?
    ): String {
      return ctx!!.text
    }

    override fun visitUnescapedSymbolicNameString(
      ctx: PatternParser.UnescapedSymbolicNameStringContext?
    ): String {
      return ctx!!.text
    }
  }

  private object ExpressionVisitor : PatternParserBaseVisitor<Any>() {

    override fun visitExpression(ctx: PatternParser.ExpressionContext?): Any {
      if (ctx!!.literal() != null) {
        return ctx.literal().accept(LiteralVisitor)
      } else {
        throw PatternException("unsupported expression: '$ctx'")
      }
    }
  }

  private object LiteralVisitor : PatternParserBaseVisitor<Any>() {

    override fun visitKeywordLiteral(ctx: PatternParser.KeywordLiteralContext?): Any {
      return if (ctx!!.INF() != null || ctx.INFINITY() != null) {
        Double.POSITIVE_INFINITY
      } else if (ctx.NAN() != null) {
        Double.NaN
      } else {
        throw PatternException("unsupported keyword literal: '$ctx'")
      }
    }

    override fun visitBooleanLiteral(ctx: PatternParser.BooleanLiteralContext?): Any {
      return ctx!!.TRUE() != null
    }

    override fun visitOtherLiteral(ctx: PatternParser.OtherLiteralContext?): Any {
      if (ctx!!.mapLiteral() != null) {
        return ctx
          .mapLiteral()
          .propertyKeyName()
          .mapIndexed { i, propCtx ->
            SymbolicNameStringVisitor.visit(propCtx.symbolicNameString()) to
              ExpressionVisitor.visitExpression(ctx.mapLiteral().expression(i))
          }
          .toMap()
      } else if (ctx.listLiteral() != null) {
        return ctx
          .listLiteral()
          .expression()
          .map { exp -> ExpressionVisitor.visitExpression(exp) }
          .toList()
      } else {
        throw PatternException("unsupported literal: '$ctx'")
      }
    }

    override fun visitStringsLiteral(ctx: PatternParser.StringsLiteralContext?): Any {
      val text = ctx!!.text

      return text.substring(1, text.length - 1)
    }

    @OptIn(ExperimentalStdlibApi::class)
    override fun visitNumericLiteral(ctx: PatternParser.NumericLiteralContext?): Any {
      val number = ctx!!.numberLiteral()
      val multiplier = if (number.MINUS() != null) -1 else 1

      return if (number.DECIMAL_DOUBLE() != null) {
        number.DECIMAL_DOUBLE().text.toDouble() * multiplier
      } else if (number.UNSIGNED_DECIMAL_INTEGER() != null) {
        number.UNSIGNED_DECIMAL_INTEGER().text.toLong() * multiplier
      } else if (number.UNSIGNED_HEX_INTEGER() != null) {
        number.UNSIGNED_HEX_INTEGER().text.substring(2).hexToLong(HexFormat.Default) * multiplier
      } else if (number.UNSIGNED_OCTAL_INTEGER() != null) {
        number
          .UNSIGNED_OCTAL_INTEGER()
          .text
          .substring(2)
          .reversed()
          .map { c -> c.digitToInt().toLong() }
          .reduceIndexed { index, acc, value -> acc + (value * 8.0.pow(index).toLong()) } *
          multiplier
      } else {
        throw PatternException("unexpected number literal '$number'")
      }
    }

    fun accOctal(index: Int, value: Int, acc: Long): Long {
      println("i: $index, value: $value, acc: $acc")
      return acc + (value * 8.0.pow(index).toInt())
    }
  }
}
