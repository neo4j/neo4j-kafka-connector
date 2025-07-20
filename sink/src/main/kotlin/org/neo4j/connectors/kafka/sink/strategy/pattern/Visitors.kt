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

import org.antlr.v4.runtime.BaseErrorListener
import org.antlr.v4.runtime.CharStreams
import org.antlr.v4.runtime.CommonTokenStream
import org.antlr.v4.runtime.RecognitionException
import org.antlr.v4.runtime.Recognizer
import org.neo4j.connectors.kafka.sink.strategy.pattern.PatternParser.PropertyKeyNameOrAliasContext

internal object Visitors {
  fun parse(source: String?): Pattern {
    val input = CharStreams.fromString(source)
    val lexer = PatternLexer(input)
    val tokens = CommonTokenStream(lexer)
    val parser = PatternParser(tokens)

    val errors = mutableListOf<String>()
    parser.addErrorListener(
        object : BaseErrorListener() {
          override fun syntaxError(
              recognizer: Recognizer<*, *>?,
              offendingSymbol: Any,
              line: Int,
              charPositionInLine: Int,
              msg: String,
              e: RecognitionException?,
          ) {
            errors.add(String.format("line %d:%d, %s", line, charPositionInLine, msg))
          }
        }
    )

    val pattern = parser.pattern()

    if (errors.isNotEmpty()) {
      throw PatternException("Invalid pattern: ${errors.joinToString(", ")}.")
    }

    return PatternVisitor.visitPattern(pattern)
  }

  private object PatternVisitor : PatternParserBaseVisitor<Pattern>() {
    override fun visitPattern(ctx: PatternParser.PatternContext): Pattern {
      if (ctx.cypherPattern() != null) {
        return visitCypherPattern(ctx.cypherPattern())
      }

      return visitSimplePattern(ctx.simplePattern())
    }

    override fun visitCypherPattern(ctx: PatternParser.CypherPatternContext): Pattern {
      if (ctx.cypherRelationshipPattern() != null) {
        val relPattern = ctx.cypherRelationshipPattern()
        val bidirectional =
            ((relPattern.leftArrow() != null && relPattern.rightArrow() != null) ||
                (relPattern.leftArrow() == null && relPattern.rightArrow() == null))
        if (bidirectional) {
          throw PatternException("Direction of relationship pattern must be explicitly set.")
        }

        val keyProperties = mutableSetOf<PropertyMapping>()
        val includeProperties = mutableSetOf<PropertyMapping>()
        val excludeProperties = mutableSetOf<String>()

        if (relPattern.properties() != null && relPattern.properties().propertySelector() != null) {
          extractPropertySelectors(
              relPattern.properties().propertySelector().propSelector(),
              keyProperties,
              includeProperties,
              excludeProperties,
          )
        }

        val startNode =
            enforceKeyProperties(
                visitCypherNodePattern(
                    ctx.cypherNodePattern(if ((relPattern.rightArrow() != null)) 0 else 1)
                )
            )

        val endNode =
            enforceKeyProperties(
                visitCypherNodePattern(
                    ctx.cypherNodePattern(if ((relPattern.rightArrow() != null)) 1 else 0)
                )
            )

        return ensureImplicitWildcard(
            RelationshipPattern(
                LabelOrRelTypeVisitor.visitLabelOrRelType(relPattern.labelOrRelType()),
                enforceExplicitInclusionOnly(startNode),
                enforceExplicitInclusionOnly(endNode),
                includeProperties.contains(PropertyMapping.WILDCARD),
                keyProperties,
                includeProperties.filter { it != PropertyMapping.WILDCARD }.toSet(),
                excludeProperties,
            )
        )
      }

      return ensureImplicitWildcard(
          enforceKeyProperties(visitCypherNodePattern(ctx.cypherNodePattern(0)))
      )
    }

    override fun visitSimplePattern(ctx: PatternParser.SimplePatternContext): Pattern {
      if (ctx.simpleRelationshipPattern() != null) {
        val relPattern = ctx.simpleRelationshipPattern()
        val keyProperties = mutableSetOf<PropertyMapping>()
        val includeProperties = mutableSetOf<PropertyMapping>()
        val excludeProperties = mutableSetOf<String>()

        if (relPattern.properties() != null && relPattern.properties().propertySelector() != null) {
          extractPropertySelectors(
              relPattern.properties().propertySelector().propSelector(),
              keyProperties,
              includeProperties,
              excludeProperties,
          )
        }

        val startNode = enforceKeyProperties(visitSimpleNodePattern(ctx.simpleNodePattern(0)))
        val endNode = enforceKeyProperties(visitSimpleNodePattern(ctx.simpleNodePattern(1)))

        return ensureImplicitWildcard(
            RelationshipPattern(
                SymbolicNameStringVisitor.visitSymbolicNameString(relPattern.symbolicNameString()),
                enforceExplicitInclusionOnly(startNode),
                enforceExplicitInclusionOnly(endNode),
                includeProperties.contains(PropertyMapping.WILDCARD),
                keyProperties,
                includeProperties.filter { it != PropertyMapping.WILDCARD }.toSet(),
                excludeProperties,
            )
        )
      }

      return ensureImplicitWildcard(
          enforceKeyProperties(visitSimpleNodePattern(ctx.simpleNodePattern(0)))
      )
    }

    @Suppress("UNCHECKED_CAST")
    private fun <T : Pattern> ensureImplicitWildcard(pattern: T): T {
      when (pattern) {
        is NodePattern -> {
          if (pattern.includeProperties.isEmpty()) {
            return NodePattern(
                pattern.labels,
                true,
                pattern.keyProperties,
                emptySet(),
                pattern.excludeProperties,
            )
                as T
          }
        }
        is RelationshipPattern -> {
          if (pattern.includeProperties.isEmpty()) {
            return RelationshipPattern(
                pattern.type,
                pattern.start,
                pattern.end,
                true,
                pattern.keyProperties,
                emptySet(),
                pattern.excludeProperties,
            )
                as T
          }
        }
        else ->
            throw IllegalArgumentException("Unsupported pattern type: ${pattern.javaClass.name}.")
      }

      return pattern
    }

    private fun enforceKeyProperties(pattern: NodePattern): NodePattern {
      if (pattern.keyProperties.isEmpty()) {
        throw PatternException("At least one key selector must be specified in node patterns.")
      }

      return pattern
    }

    private fun enforceExplicitInclusionOnly(pattern: NodePattern): NodePattern {
      if (pattern.excludeProperties.isNotEmpty()) {
        throw PatternException(
            "Property exclusions are not allowed on start and end node patterns."
        )
      }

      if (pattern.includeAllValueProperties) {
        throw PatternException(
            "Wildcard property inclusion is not allowed on start and end node patterns."
        )
      }

      return pattern
    }

    override fun visitCypherNodePattern(ctx: PatternParser.CypherNodePatternContext): NodePattern {
      val labels = NodeLabelsVisitor.visitNodeLabels(ctx.nodeLabels())
      return nodePattern(labels, ctx.properties())
    }

    override fun visitSimpleNodePattern(ctx: PatternParser.SimpleNodePatternContext): NodePattern {
      val labels =
          setOf(SymbolicNameStringVisitor.visitSymbolicNameString(ctx.symbolicNameString())) +
              NodeLabelsVisitor.visitNodeLabels(ctx.nodeLabels())
      return nodePattern(labels, ctx.properties())
    }

    private fun nodePattern(
        labels: Set<String>,
        properties: PatternParser.PropertiesContext?,
    ): NodePattern {
      val keyProperties = mutableSetOf<PropertyMapping>()
      val includeProperties = mutableSetOf<PropertyMapping>()
      val excludeProperties = mutableSetOf<String>()

      if (properties?.propertySelector() != null) {
        extractPropertySelectors(
            properties.propertySelector().propSelector(),
            keyProperties,
            includeProperties,
            excludeProperties,
        )
      }

      return NodePattern(
          labels,
          includeProperties.contains(PropertyMapping.WILDCARD),
          keyProperties,
          includeProperties.filter { it != PropertyMapping.WILDCARD }.toSet(),
          excludeProperties,
      )
    }

    private fun extractPropertySelectors(
        selectors: List<PatternParser.PropSelectorContext>,
        keyProperties: MutableSet<PropertyMapping>,
        includeProperties: MutableSet<PropertyMapping>,
        excludeProperties: MutableSet<String>,
    ) {
      selectors.forEach { child ->
        if (child.TIMES() != null) {
          includeProperties.add(PropertyMapping.WILDCARD)
        } else if (child.MINUS() != null) {
          excludeProperties.add(
              PropertyKeyNameVisitor.visitPropertyKeyName(child.propertyKeyName())
          )
        } else if (child.EXCLAMATION() != null) {
          keyProperties += extractPropertyNameOrAlias(child.propertyKeyNameOrAlias())
        } else {
          includeProperties += extractPropertyNameOrAlias(child.propertyKeyNameOrAlias())
        }
      }

      if (includeProperties.isNotEmpty() && excludeProperties.isNotEmpty()) {
        if (!includeProperties.contains(PropertyMapping.WILDCARD)) {
          throw PatternException("Property inclusions and exclusions are mutually exclusive.")
        }
      }
    }
  }

  private fun extractPropertyNameOrAlias(ctx: PropertyKeyNameOrAliasContext): PropertyMapping {
    return if (ctx.propertyKeyName() != null) {
      val property = PropertyKeyNameVisitor.visitPropertyKeyName(ctx.propertyKeyName())
      PropertyMapping(property, property)
    } else {
      PropertyMapping(
          PropertyKeyNameVisitor.visitPropertyKeyName(
              ctx.aliasedPropertyKeyName().propertyKeyName(1)
          ),
          PropertyKeyNameVisitor.visitPropertyKeyName(
              ctx.aliasedPropertyKeyName().propertyKeyName(0)
          ),
      )
    }
  }

  private object NodeLabelsVisitor : PatternParserBaseVisitor<Set<String?>>() {
    override fun visitNodeLabels(ctx: PatternParser.NodeLabelsContext?): Set<String> {
      if (ctx?.labelOrRelType() == null) {
        return emptySet<String>()
      }

      return ctx.labelOrRelType().map { LabelOrRelTypeVisitor.visitLabelOrRelType(it) }.toSet()
    }
  }

  private object LabelOrRelTypeVisitor : PatternParserBaseVisitor<String?>() {
    override fun visitLabelOrRelType(ctx: PatternParser.LabelOrRelTypeContext): String {
      return SymbolicNameStringVisitor.visitSymbolicNameString(ctx.symbolicNameString())
    }
  }

  private object PropertyKeyNameVisitor : PatternParserBaseVisitor<String>() {
    override fun visitPropertyKeyName(ctx: PatternParser.PropertyKeyNameContext): String {
      return SymbolicNameStringVisitor.visitSymbolicNameString(ctx.symbolicNameString())
    }
  }

  private object SymbolicNameStringVisitor : PatternParserBaseVisitor<String>() {
    override fun visitSymbolicNameString(ctx: PatternParser.SymbolicNameStringContext): String {
      return super.visitSymbolicNameString(ctx)
    }

    override fun visitEscapedSymbolicNameString(
        ctx: PatternParser.EscapedSymbolicNameStringContext
    ): String {
      return ctx.text.substring(1, ctx.text.length - 1)
    }

    override fun visitUnescapedSymbolicNameString(
        ctx: PatternParser.UnescapedSymbolicNameStringContext
    ): String {
      return ctx.text
    }
  }
}
