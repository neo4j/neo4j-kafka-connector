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
package org.neo4j.cdc.client

import com.fasterxml.jackson.annotation.JsonFormat
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import java.time.ZonedDateTime

data class ChangeIdentifier(val id: String)

data class ChangeEvent(
    val id: ChangeIdentifier,
    val txId: Long,
    val seq: Int,
    val metadata: Metadata,
    val event: Event
)

data class Metadata(
    val executingUser: String,
    val connectionClient: String,
    val authenticatedUser: String,
    val captureMode: CaptureMode,
    val serverId: String,
    val connectionType: String, // needs enum type
    val connectionServer: String,
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSX")
    val txStartTime: ZonedDateTime,
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSX")
    val txCommitTime: ZonedDateTime
)

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.EXISTING_PROPERTY,
    property = "eventType",
    visible = true,
)
@JsonSubTypes(
    JsonSubTypes.Type(value = NodeEvent::class, name = "n"),
    JsonSubTypes.Type(value = RelationshipEvent::class, name = "r"),
)
abstract class Event(
    open val elementId: String,
    open val eventType: String, // needs type
    open val state: State,
    open val operation: String, // needs enum type
)

data class NodeEvent(
    val keys: Map<String, Any>,
    val labels: List<String>,
    override val elementId: String,
    override val eventType: String,
    override val state: State,
    override val operation: String
) : Event(elementId, eventType, state, operation)

data class RelationshipEvent(
    val start: Node,
    val end: Node,
    val type: String,
    val key: Map<String, Any>,
    override val elementId: String,
    override val eventType: String,
    override val state: State,
    override val operation: String
) : Event(elementId, eventType, state, operation)

data class Node(val elementId: String, val keys: Map<String, Any>, val labels: List<String>)

data class State(
    val before: Map<String, Any>?, // needs type
    val after: Map<String, Any>?, // needs type
)

enum class CaptureMode {
  OFF,
  DIFF,
  FULL
}
