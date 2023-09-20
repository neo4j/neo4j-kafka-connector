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
package org.neo4j.cdc.client.pattern;

import java.util.*;
import java.util.stream.Collectors;
import org.antlr.v4.runtime.*;

class Visitors {

    static List<Pattern> parse(String source) {
        var input = CharStreams.fromString(source);
        var lexer = new PatternLexer(input);
        var tokens = new CommonTokenStream(lexer);
        var parser = new PatternParser(tokens);

        var errors = new ArrayList<String>();
        parser.addErrorListener(new BaseErrorListener() {
            @Override
            public void syntaxError(
                    Recognizer<?, ?> recognizer,
                    Object offendingSymbol,
                    int line,
                    int charPositionInLine,
                    String msg,
                    RecognitionException e) {
                errors.add(String.format("line %d:%d, %s", line, charPositionInLine, msg));
            }
        });

        var patternList = parser.patternList();

        if (!errors.isEmpty()) {
            throw new PatternException(String.join(", ", errors));
        }

        return patternList.pattern().stream()
                .map(PatternVisitor.INSTANCE::visitPattern)
                .collect(Collectors.toList());
    }

    private static class PatternVisitor extends PatternParserBaseVisitor<Pattern> {
        public static final PatternVisitor INSTANCE = new PatternVisitor();

        @Override
        public Pattern visitPattern(PatternParser.PatternContext ctx) {
            if (ctx.relationshipPattern() != null) {
                var relPattern = ctx.relationshipPattern();
                var bidirectional = (relPattern.leftArrow() != null && relPattern.rightArrow() != null)
                        || (relPattern.leftArrow() == null && relPattern.rightArrow() == null);
                var keyFilters = new HashMap<String, Object>();
                var includeProperties = new HashSet<String>();
                var excludeProperties = new HashSet<String>();

                if (relPattern.properties() != null && relPattern.properties().propertySelector() != null) {
                    extractPropertySelectors(
                            relPattern.properties().propertySelector().keyFilterOrPropSelector(),
                            keyFilters,
                            includeProperties,
                            excludeProperties);
                }

                var startNode =
                        visitNodePattern(ctx.nodePattern((bidirectional || relPattern.rightArrow() != null) ? 0 : 1));
                if (!startNode.getExcludeProperties().isEmpty()
                        || !startNode.getIncludeProperties().isEmpty()) {
                    throw new PatternException(
                            "property selectors are not allowed in node part of relationship patterns");
                }

                var endNode =
                        visitNodePattern(ctx.nodePattern((bidirectional || relPattern.rightArrow() != null) ? 1 : 0));
                if (!endNode.getExcludeProperties().isEmpty()
                        || !endNode.getIncludeProperties().isEmpty()) {
                    throw new PatternException(
                            "property selectors are not allowed in node part of relationship patterns");
                }

                return new RelationshipPattern(
                        LabelOrRelTypeVisitor.INSTANCE.visitLabelOrRelType(relPattern.labelOrRelType()),
                        startNode,
                        endNode,
                        bidirectional,
                        keyFilters,
                        includeProperties,
                        excludeProperties);
            }

            return visitNodePattern(ctx.nodePattern(0));
        }

        @Override
        public NodePattern visitNodePattern(PatternParser.NodePatternContext ctx) {
            var labels = NodeLabelsVisitor.INSTANCE.visitNodeLabels(ctx.nodeLabels());
            var keyFilters = new HashMap<String, Object>();
            var includeProperties = new HashSet<String>();
            var excludeProperties = new HashSet<String>();

            if (ctx.properties() != null && ctx.properties().propertySelector() != null) {
                extractPropertySelectors(
                        ctx.properties().propertySelector().keyFilterOrPropSelector(),
                        keyFilters,
                        includeProperties,
                        excludeProperties);
            }

            return new NodePattern(labels, keyFilters, includeProperties, excludeProperties);
        }

        private void extractPropertySelectors(
                List<PatternParser.KeyFilterOrPropSelectorContext> selectors,
                Map<String, Object> keyFilters,
                Set<String> includeProperties,
                Set<String> excludeProperties) {
            selectors.forEach(child -> {
                if (child.keyFilter() != null) {
                    var propName = PropertyKeyNameVisitor.INSTANCE.visitPropertyKeyName(
                            child.keyFilter().propertyKeyName());
                    var value = ExpressionVisitor.INSTANCE.visitExpression(
                            child.keyFilter().expression());

                    keyFilters.put(propName, value);
                } else if (child.propSelector() != null) {
                    if (child.propSelector().TIMES() != null) {
                        includeProperties.add("*");
                    } else if (child.propSelector().MINUS() != null) {
                        excludeProperties.add(PropertyKeyNameVisitor.INSTANCE.visitPropertyKeyName(
                                child.propSelector().propertyKeyName()));
                    } else {
                        includeProperties.add(PropertyKeyNameVisitor.INSTANCE.visitPropertyKeyName(
                                child.propSelector().propertyKeyName()));
                    }
                }
            });
        }
    }

    private static class NodeLabelsVisitor extends PatternParserBaseVisitor<Set<String>> {
        public static final NodeLabelsVisitor INSTANCE = new NodeLabelsVisitor();

        public Set<String> visitNodeLabels(PatternParser.NodeLabelsContext ctx) {
            if (ctx == null || ctx.labelOrRelType() == null) {
                return Set.of();
            }

            return ctx.labelOrRelType().stream()
                    .map(LabelOrRelTypeVisitor.INSTANCE::visitLabelOrRelType)
                    .collect(Collectors.toSet());
        }
    }

    private static class LabelOrRelTypeVisitor extends PatternParserBaseVisitor<String> {
        public static final LabelOrRelTypeVisitor INSTANCE = new LabelOrRelTypeVisitor();

        public String visitLabelOrRelType(PatternParser.LabelOrRelTypeContext ctx) {
            if (ctx == null) {
                return null;
            }

            return SymbolicNameStringVisitor.INSTANCE.visitSymbolicNameString(ctx.symbolicNameString());
        }
    }

    private static class PropertyKeyNameVisitor extends PatternParserBaseVisitor<String> {
        public static final PropertyKeyNameVisitor INSTANCE = new PropertyKeyNameVisitor();

        public String visitPropertyKeyName(PatternParser.PropertyKeyNameContext ctx) {
            return SymbolicNameStringVisitor.INSTANCE.visitSymbolicNameString(ctx.symbolicNameString());
        }
    }

    private static class SymbolicNameStringVisitor extends PatternParserBaseVisitor<String> {
        public static final SymbolicNameStringVisitor INSTANCE = new SymbolicNameStringVisitor();

        public String visitSymbolicNameString(PatternParser.SymbolicNameStringContext ctx) {
            return super.visitSymbolicNameString(ctx);
        }

        public String visitEscapedSymbolicNameString(PatternParser.EscapedSymbolicNameStringContext ctx) {
            return ctx.getText();
        }

        public String visitUnescapedSymbolicNameString(PatternParser.UnescapedSymbolicNameStringContext ctx) {
            return ctx.getText();
        }
    }

    private static class ExpressionVisitor extends PatternParserBaseVisitor<Object> {
        public static final ExpressionVisitor INSTANCE = new ExpressionVisitor();

        public Object visitExpression(PatternParser.ExpressionContext ctx) {
            if (ctx.literal() != null) {
                return ctx.literal().accept(LiteralVisitor.INSTANCE);
            } else {
                throw new PatternException(String.format("unsupported expression: '%s'", ctx));
            }
        }
    }

    private static class LiteralVisitor extends PatternParserBaseVisitor<Object> {
        private static final LiteralVisitor INSTANCE = new LiteralVisitor();

        public Object visitKeywordLiteral(PatternParser.KeywordLiteralContext ctx) {
            if (ctx.INF() != null || ctx.INFINITY() != null) {
                return Double.POSITIVE_INFINITY;
            } else if (ctx.NAN() != null) {
                return Double.NaN;
            } else {
                throw new PatternException(String.format("unsupported keyword literal: '%s'", ctx));
            }
        }

        public Object visitBooleanLiteral(PatternParser.BooleanLiteralContext ctx) {
            return ctx.TRUE() != null;
        }

        public Object visitOtherLiteral(PatternParser.OtherLiteralContext ctx) {
            if (ctx.mapLiteral() != null) {
                var result = new HashMap<String, Object>();
                var mapProps = ctx.mapLiteral().propertyKeyName();
                var mapExps = ctx.mapLiteral().expression();
                for (int i = 0; i < mapProps.size(); i++) {
                    result.put(
                            SymbolicNameStringVisitor.INSTANCE.visit(mapProps.get(i)),
                            ExpressionVisitor.INSTANCE.visitExpression(mapExps.get(i)));
                }
                return result;
            } else if (ctx.listLiteral() != null) {
                return ctx.listLiteral().expression().stream()
                        .map(ExpressionVisitor.INSTANCE::visitExpression)
                        .collect(Collectors.toList());
            } else {
                throw new PatternException(String.format("unsupported literal: '%s'", ctx));
            }
        }

        public Object visitStringsLiteral(PatternParser.StringsLiteralContext ctx) {
            var text = ctx.getText();
            return text.substring(1, text.length() - 1);
        }

        public Object visitNumericLiteral(PatternParser.NumericLiteralContext ctx) {
            var number = ctx.numberLiteral();
            var multiplier = number.MINUS() != null ? -1 : 1;

            if (number.DECIMAL_DOUBLE() != null) {
                return Double.parseDouble(number.DECIMAL_DOUBLE().getText()) * multiplier;
            } else if (number.UNSIGNED_DECIMAL_INTEGER() != null) {
                return Long.parseLong(number.UNSIGNED_DECIMAL_INTEGER().getText(), 10) * multiplier;
            } else if (number.UNSIGNED_HEX_INTEGER() != null) {
                return Long.parseLong(number.UNSIGNED_HEX_INTEGER().getText().substring(2), 16) * multiplier;
            } else if (number.UNSIGNED_OCTAL_INTEGER() != null) {
                return Long.parseLong(number.UNSIGNED_OCTAL_INTEGER().getText().substring(2), 8) * multiplier;
            } else {
                throw new PatternException(String.format("unexpected number literal '%s'", ctx));
            }
        }
    }
}
