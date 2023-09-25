/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
parser grammar PatternParser;

options { tokenVocab = PatternLexer; }

patternList:
   (pattern (COMMA pattern)*)? EOF;

pattern:
   nodePattern (relationshipPattern nodePattern)?;

nodePattern:
   LPAREN nodeLabels? properties? RPAREN;

nodeLabels:
   labelOrRelType+;

labelOrRelType:
   COLON symbolicNameString;

properties:
   propertySelector;

propertySelector:
   LCURLY keyFilterOrPropSelector? (COMMA keyFilterOrPropSelector)* RCURLY;

keyFilterOrPropSelector:
    keyFilter | propSelector;

keyFilter:
    propertyKeyName COLON expression;

propSelector:
    TIMES | PLUS? propertyKeyName | MINUS propertyKeyName;

relationshipPattern:
   leftArrow? arrowLine (
        LBRACKET
        labelOrRelType? properties?
        RBRACKET
   )? arrowLine rightArrow?;

leftArrow:
   (LT | ARROW_LEFT_HEAD);

arrowLine:
   (ARROW_LINE | MINUS);

rightArrow:
   (GT | ARROW_RIGHT_HEAD);

expression:
   literal;

literal:
   numberLiteral    #NumericLiteral
   | stringLiteral    #StringsLiteral
   | mapLiteral       #OtherLiteral
   | listLiteral      #OtherLiteral
   | TRUE             #BooleanLiteral
   | FALSE            #BooleanLiteral
   | (INFINITY | INF) #KeywordLiteral
   | NAN              #KeywordLiteral;

mapLiteral:
   LCURLY (propertyKeyName COLON expression)? (COMMA propertyKeyName COLON expression)* RCURLY;

stringLiteral:
   stringToken;

numberLiteral:
   MINUS? (DECIMAL_DOUBLE | UNSIGNED_DECIMAL_INTEGER | UNSIGNED_HEX_INTEGER | UNSIGNED_OCTAL_INTEGER);

listLiteral:
   LBRACKET expression? (COMMA expression)* RBRACKET;

propertyKeyName:
   symbolicNameString;

stringToken:
   (STRING_LITERAL1 | STRING_LITERAL2);

symbolicNameString:
   (escapedSymbolicNameString | unescapedSymbolicNameString);

escapedSymbolicNameString:
   ESCAPED_SYMBOLIC_NAME;

unescapedSymbolicNameString:
   IDENTIFIER;
