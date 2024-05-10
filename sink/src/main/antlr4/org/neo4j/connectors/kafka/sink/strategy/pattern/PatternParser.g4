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

pattern:
    cypherPattern | simplePattern EOF;

cypherPattern:
   cypherNodePattern (cypherRelationshipPattern cypherNodePattern)?;

simplePattern:
   simpleNodePattern (simpleRelationshipPattern simpleNodePattern)?;

cypherNodePattern:
   LPAREN nodeLabels? properties RPAREN;

simpleNodePattern:
    symbolicNameString nodeLabels? properties ;

nodeLabels:
   labelOrRelType+;

labelOrRelType:
   COLON symbolicNameString;

properties:
   propertySelector;

propertySelector:
   LCURLY propSelector? (COMMA propSelector)* RCURLY;

propSelector:
    TIMES | PLUS? propertyKeyName | MINUS propertyKeyName | EXCLAMATION propertyKeyName;

cypherRelationshipPattern:
   leftArrow? arrowLine (
        LBRACKET
        labelOrRelType properties?
        RBRACKET
   )? arrowLine rightArrow?;

leftArrow:
   (LT | ARROW_LEFT_HEAD);

arrowLine:
   (ARROW_LINE | MINUS);

rightArrow:
   (GT | ARROW_RIGHT_HEAD);

simpleRelationshipPattern:
   (
        symbolicNameString properties?
   )?;

propertyKeyName:
   symbolicNameString;

symbolicNameString:
   (escapedSymbolicNameString | unescapedSymbolicNameString);

escapedSymbolicNameString:
   ESCAPED_SYMBOLIC_NAME;

unescapedSymbolicNameString:
   IDENTIFIER;
