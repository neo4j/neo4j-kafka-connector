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

data class RelationshipPattern(
    val type: String?,
    val start: NodePattern,
    val end: NodePattern,
    override val includeAllValueProperties: Boolean,
    override val keyProperties: Set<PropertyMapping>,
    override val includeProperties: Set<PropertyMapping>,
    override val excludeProperties: Set<String>
) : Pattern {}
