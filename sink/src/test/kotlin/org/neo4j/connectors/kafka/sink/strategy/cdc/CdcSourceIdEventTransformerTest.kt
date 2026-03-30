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
package org.neo4j.connectors.kafka.sink.strategy.cdc

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import io.kotest.matchers.throwable.shouldHaveMessage
import java.time.ZonedDateTime
import org.junit.jupiter.api.Test
import org.neo4j.cdc.client.model.CaptureMode
import org.neo4j.cdc.client.model.ChangeEvent
import org.neo4j.cdc.client.model.ChangeIdentifier
import org.neo4j.cdc.client.model.EntityOperation
import org.neo4j.cdc.client.model.Event
import org.neo4j.cdc.client.model.Metadata
import org.neo4j.cdc.client.model.Node
import org.neo4j.cdc.client.model.NodeEvent
import org.neo4j.cdc.client.model.NodeState
import org.neo4j.cdc.client.model.RelationshipEvent
import org.neo4j.cdc.client.model.RelationshipState
import org.neo4j.connectors.kafka.exceptions.InvalidDataException
import org.neo4j.connectors.kafka.sink.strategy.DeleteNodeSinkAction
import org.neo4j.connectors.kafka.sink.strategy.DeleteRelationshipSinkAction
import org.neo4j.connectors.kafka.sink.strategy.LookupMode
import org.neo4j.connectors.kafka.sink.strategy.MergeNodeSinkAction
import org.neo4j.connectors.kafka.sink.strategy.MergeRelationshipSinkAction
import org.neo4j.connectors.kafka.sink.strategy.NodeMatcher
import org.neo4j.connectors.kafka.sink.strategy.RelationshipMatcher
import org.neo4j.connectors.kafka.sink.strategy.SinkActionNodeReference

class CdcSourceIdEventTransformerTest {
  private val transformer = CdcSourceIdEventTransformer("my-topic", "SourceLabel", "sourceId")

  @Test
  fun `should transform node creation event`() {
    val event =
        NodeEvent(
            "node-element-id",
            EntityOperation.CREATE,
            listOf("Person"),
            mapOf("Person" to listOf(mapOf("id" to 1))),
            null,
            NodeState(listOf("Person"), mapOf("id" to 1, "name" to "John")),
        )

    transformer.transform(changeEvent(event)) shouldBe
        MergeNodeSinkAction(
            NodeMatcher.ByLabelsAndProperties(
                setOf("SourceLabel"),
                mapOf("sourceId" to "node-element-id"),
            ),
            mapOf("id" to 1, "name" to "John"),
            setOf("Person"),
            emptySet(),
        )
  }

  @Test
  fun `should transform node creation event without keys`() {
    val event =
        NodeEvent(
            "node-element-id",
            EntityOperation.CREATE,
            listOf("Person"),
            emptyMap(),
            null,
            NodeState(listOf("Person"), mapOf("id" to 1, "name" to "John")),
        )

    transformer.transform(changeEvent(event)) shouldBe
        MergeNodeSinkAction(
            NodeMatcher.ByLabelsAndProperties(
                setOf("SourceLabel"),
                mapOf("sourceId" to "node-element-id"),
            ),
            mapOf("id" to 1, "name" to "John"),
            setOf("Person"),
            emptySet(),
        )
  }

  @Test
  fun `should transform node update event`() {
    val event =
        NodeEvent(
            "node-element-id",
            EntityOperation.UPDATE,
            listOf("Person"),
            mapOf("Person" to listOf(mapOf("id" to 1))),
            NodeState(listOf("Person"), mapOf("id" to 1, "name" to "John")),
            NodeState(
                listOf("Person", "Employee"),
                mapOf("id" to 1, "name" to "John", "salary" to 1000),
            ),
        )

    transformer.transform(changeEvent(event)) shouldBe
        MergeNodeSinkAction(
            NodeMatcher.ByLabelsAndProperties(
                setOf("SourceLabel"),
                mapOf("sourceId" to "node-element-id"),
            ),
            mapOf("salary" to 1000),
            setOf("Employee"),
            emptySet(),
        )
  }

  @Test
  fun `should transform node update event without keys`() {
    val event =
        NodeEvent(
            "node-element-id",
            EntityOperation.UPDATE,
            listOf("Person"),
            emptyMap(),
            NodeState(listOf("Person"), mapOf("id" to 1, "name" to "John")),
            NodeState(
                listOf("Person", "Employee"),
                mapOf("id" to 1, "name" to "John", "salary" to 1000),
            ),
        )

    transformer.transform(changeEvent(event)) shouldBe
        MergeNodeSinkAction(
            NodeMatcher.ByLabelsAndProperties(
                setOf("SourceLabel"),
                mapOf("sourceId" to "node-element-id"),
            ),
            mapOf("salary" to 1000),
            setOf("Employee"),
            emptySet(),
        )
  }

  @Test
  fun `should transform node deletion event`() {
    val event =
        NodeEvent(
            "node-element-id",
            EntityOperation.DELETE,
            listOf("Person"),
            mapOf("Person" to listOf(mapOf("id" to 1))),
            NodeState(listOf("Person"), mapOf("id" to 1, "name" to "John")),
            null,
        )

    transformer.transform(changeEvent(event)) shouldBe
        DeleteNodeSinkAction(
            NodeMatcher.ByLabelsAndProperties(
                setOf("SourceLabel"),
                mapOf("sourceId" to "node-element-id"),
            )
        )
  }

  @Test
  fun `should transform node deletion event without keys`() {
    val event =
        NodeEvent(
            "node-element-id",
            EntityOperation.DELETE,
            listOf("Person"),
            emptyMap(),
            NodeState(listOf("Person"), mapOf("id" to 1, "name" to "John")),
            null,
        )

    transformer.transform(changeEvent(event)) shouldBe
        DeleteNodeSinkAction(
            NodeMatcher.ByLabelsAndProperties(
                setOf("SourceLabel"),
                mapOf("sourceId" to "node-element-id"),
            )
        )
  }

  @Test
  fun `should transform relationship creation event`() {
    val event =
        RelationshipEvent(
            "rel-element-id",
            "KNOWS",
            Node("s-element-id", listOf("Person"), emptyMap()),
            Node("e-element-id", listOf("Person"), emptyMap()),
            emptyList(),
            EntityOperation.CREATE,
            null,
            RelationshipState(mapOf("since" to 2020)),
        )

    transformer.transform(changeEvent(event)) shouldBe
        MergeRelationshipSinkAction(
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(
                    setOf("SourceLabel"),
                    mapOf("sourceId" to "s-element-id"),
                ),
                LookupMode.MERGE,
            ),
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(
                    setOf("SourceLabel"),
                    mapOf("sourceId" to "e-element-id"),
                ),
                LookupMode.MERGE,
            ),
            RelationshipMatcher.ByTypeAndProperties(
                "KNOWS",
                mapOf("sourceId" to "rel-element-id"),
                true,
            ),
            mapOf("since" to 2020),
        )
  }

  @Test
  fun `should transform relationship creation event with keys`() {
    val event =
        RelationshipEvent(
            "rel-element-id",
            "KNOWS",
            Node("s-element-id", listOf("Person"), emptyMap()),
            Node("e-element-id", listOf("Person"), emptyMap()),
            listOf(mapOf("relId" to "R1")),
            EntityOperation.CREATE,
            null,
            RelationshipState(mapOf("since" to 2020)),
        )

    transformer.transform(changeEvent(event)) shouldBe
        MergeRelationshipSinkAction(
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(
                    setOf("SourceLabel"),
                    mapOf("sourceId" to "s-element-id"),
                ),
                LookupMode.MERGE,
            ),
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(
                    setOf("SourceLabel"),
                    mapOf("sourceId" to "e-element-id"),
                ),
                LookupMode.MERGE,
            ),
            RelationshipMatcher.ByTypeAndProperties(
                "KNOWS",
                mapOf("sourceId" to "rel-element-id"),
                true,
            ),
            mapOf("since" to 2020),
        )
  }

  @Test
  fun `should transform relationship creation event with node keys`() {
    val event =
        RelationshipEvent(
            "rel-element-id",
            "KNOWS",
            Node("s-element-id", listOf("Person"), mapOf("Person" to listOf(mapOf("id" to 1)))),
            Node("e-element-id", listOf("Person"), mapOf("Person" to listOf(mapOf("id" to 2)))),
            emptyList(),
            EntityOperation.CREATE,
            null,
            RelationshipState(mapOf("since" to 2020)),
        )

    transformer.transform(changeEvent(event)) shouldBe
        MergeRelationshipSinkAction(
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(
                    setOf("SourceLabel"),
                    mapOf("sourceId" to "s-element-id"),
                ),
                LookupMode.MERGE,
            ),
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(
                    setOf("SourceLabel"),
                    mapOf("sourceId" to "e-element-id"),
                ),
                LookupMode.MERGE,
            ),
            RelationshipMatcher.ByTypeAndProperties(
                "KNOWS",
                mapOf("sourceId" to "rel-element-id"),
                true,
            ),
            mapOf("since" to 2020),
        )
  }

  @Test
  fun `should transform relationship update event`() {
    val event =
        RelationshipEvent(
            "rel-element-id",
            "KNOWS",
            Node("s-element-id", listOf("Person"), emptyMap()),
            Node("e-element-id", listOf("Person"), emptyMap()),
            emptyList(),
            EntityOperation.UPDATE,
            RelationshipState(mapOf("since" to 2020)),
            RelationshipState(mapOf("since" to 2021, "rating" to 5)),
        )

    transformer.transform(changeEvent(event)) shouldBe
        MergeRelationshipSinkAction(
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(
                    setOf("SourceLabel"),
                    mapOf("sourceId" to "s-element-id"),
                ),
                LookupMode.MERGE,
            ),
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(
                    setOf("SourceLabel"),
                    mapOf("sourceId" to "e-element-id"),
                ),
                LookupMode.MERGE,
            ),
            RelationshipMatcher.ByTypeAndProperties(
                "KNOWS",
                mapOf("sourceId" to "rel-element-id"),
                true,
            ),
            mapOf("since" to 2021, "rating" to 5),
        )
  }

  @Test
  fun `should transform relationship update event with keys`() {
    val event =
        RelationshipEvent(
            "rel-element-id",
            "KNOWS",
            Node("s-element-id", listOf("Person"), emptyMap()),
            Node("e-element-id", listOf("Person"), emptyMap()),
            listOf(mapOf("relId" to "R1")),
            EntityOperation.UPDATE,
            RelationshipState(mapOf("since" to 2020)),
            RelationshipState(mapOf("since" to 2021, "rating" to 5)),
        )

    transformer.transform(changeEvent(event)) shouldBe
        MergeRelationshipSinkAction(
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(
                    setOf("SourceLabel"),
                    mapOf("sourceId" to "s-element-id"),
                ),
                LookupMode.MERGE,
            ),
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(
                    setOf("SourceLabel"),
                    mapOf("sourceId" to "e-element-id"),
                ),
                LookupMode.MERGE,
            ),
            RelationshipMatcher.ByTypeAndProperties(
                "KNOWS",
                mapOf("sourceId" to "rel-element-id"),
                true,
            ),
            mapOf("since" to 2021, "rating" to 5),
        )
  }

  @Test
  fun `should transform relationship update event with node keys`() {
    val event =
        RelationshipEvent(
            "rel-element-id",
            "KNOWS",
            Node("s-element-id", listOf("Person"), mapOf("Person" to listOf(mapOf("id" to 1)))),
            Node("e-element-id", listOf("Person"), mapOf("Person" to listOf(mapOf("id" to 2)))),
            emptyList(),
            EntityOperation.UPDATE,
            RelationshipState(mapOf("since" to 2020)),
            RelationshipState(mapOf("since" to 2021, "rating" to 5)),
        )

    transformer.transform(changeEvent(event)) shouldBe
        MergeRelationshipSinkAction(
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(
                    setOf("SourceLabel"),
                    mapOf("sourceId" to "s-element-id"),
                ),
                LookupMode.MERGE,
            ),
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(
                    setOf("SourceLabel"),
                    mapOf("sourceId" to "e-element-id"),
                ),
                LookupMode.MERGE,
            ),
            RelationshipMatcher.ByTypeAndProperties(
                "KNOWS",
                mapOf("sourceId" to "rel-element-id"),
                true,
            ),
            mapOf("since" to 2021, "rating" to 5),
        )
  }

  @Test
  fun `should transform relationship deletion event`() {
    val event =
        RelationshipEvent(
            "rel-element-id",
            "KNOWS",
            Node("s-element-id", listOf("Person"), emptyMap()),
            Node("e-element-id", listOf("Person"), emptyMap()),
            emptyList(),
            EntityOperation.DELETE,
            RelationshipState(mapOf("since" to 2020)),
            null,
        )

    transformer.transform(changeEvent(event)) shouldBe
        DeleteRelationshipSinkAction(
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(
                    setOf("SourceLabel"),
                    mapOf("sourceId" to "s-element-id"),
                ),
                LookupMode.MATCH,
            ),
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(
                    setOf("SourceLabel"),
                    mapOf("sourceId" to "e-element-id"),
                ),
                LookupMode.MATCH,
            ),
            RelationshipMatcher.ByTypeAndProperties(
                "KNOWS",
                mapOf("sourceId" to "rel-element-id"),
                true,
            ),
        )
  }

  @Test
  fun `should transform relationship deletion event with keys`() {
    val event =
        RelationshipEvent(
            "rel-element-id",
            "KNOWS",
            Node("s-element-id", listOf("Person"), emptyMap()),
            Node("e-element-id", listOf("Person"), emptyMap()),
            listOf(mapOf("relId" to "R1")),
            EntityOperation.DELETE,
            RelationshipState(mapOf("since" to 2020)),
            null,
        )

    transformer.transform(changeEvent(event)) shouldBe
        DeleteRelationshipSinkAction(
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(
                    setOf("SourceLabel"),
                    mapOf("sourceId" to "s-element-id"),
                ),
                LookupMode.MATCH,
            ),
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(
                    setOf("SourceLabel"),
                    mapOf("sourceId" to "e-element-id"),
                ),
                LookupMode.MATCH,
            ),
            RelationshipMatcher.ByTypeAndProperties(
                "KNOWS",
                mapOf("sourceId" to "rel-element-id"),
                true,
            ),
        )
  }

  @Test
  fun `should transform relationship deletion event with node keys`() {
    val event =
        RelationshipEvent(
            "rel-element-id",
            "KNOWS",
            Node("s-element-id", listOf("Person"), mapOf("Person" to listOf(mapOf("id" to 1)))),
            Node("e-element-id", listOf("Person"), mapOf("Person" to listOf(mapOf("id" to 2)))),
            emptyList(),
            EntityOperation.DELETE,
            RelationshipState(mapOf("since" to 2020)),
            null,
        )

    transformer.transform(changeEvent(event)) shouldBe
        DeleteRelationshipSinkAction(
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(
                    setOf("SourceLabel"),
                    mapOf("sourceId" to "s-element-id"),
                ),
                LookupMode.MATCH,
            ),
            SinkActionNodeReference(
                NodeMatcher.ByLabelsAndProperties(
                    setOf("SourceLabel"),
                    mapOf("sourceId" to "e-element-id"),
                ),
                LookupMode.MATCH,
            ),
            RelationshipMatcher.ByTypeAndProperties(
                "KNOWS",
                mapOf("sourceId" to "rel-element-id"),
                true,
            ),
        )
  }

  @Test
  fun `should fail on null after field with node create operation`() {
    val event = NodeEvent("id", EntityOperation.CREATE, emptyList(), emptyMap(), null, null)
    shouldThrow<InvalidDataException> {
      transformer.transform(changeEvent(event))
    } shouldHaveMessage "create operation requires 'after' field in the event object."
  }

  @Test
  fun `should fail on non-null before field with node create operation`() {
    val event =
        NodeEvent(
            "id",
            EntityOperation.CREATE,
            emptyList(),
            emptyMap(),
            NodeState(emptyList(), emptyMap()),
            NodeState(emptyList(), emptyMap()),
        )
    shouldThrow<InvalidDataException> {
      transformer.transform(changeEvent(event))
    } shouldHaveMessage "create operation requires 'before' field to be unset in the event object."
  }

  @Test
  fun `should fail on null before field with node update operation`() {
    val event =
        NodeEvent(
            "id",
            EntityOperation.UPDATE,
            emptyList(),
            emptyMap(),
            null,
            NodeState(emptyList(), emptyMap()),
        )
    shouldThrow<InvalidDataException> {
      transformer.transform(changeEvent(event))
    } shouldHaveMessage "update operation requires 'before' field in the event object."
  }

  @Test
  fun `should fail on null after field with node update operation`() {
    val event =
        NodeEvent(
            "id",
            EntityOperation.UPDATE,
            emptyList(),
            emptyMap(),
            NodeState(emptyList(), emptyMap()),
            null,
        )
    shouldThrow<InvalidDataException> {
      transformer.transform(changeEvent(event))
    } shouldHaveMessage "update operation requires 'after' field in the event object."
  }

  @Test
  fun `should fail on null before field with node delete operation`() {
    val event = NodeEvent("id", EntityOperation.DELETE, emptyList(), emptyMap(), null, null)
    shouldThrow<InvalidDataException> {
      transformer.transform(changeEvent(event))
    } shouldHaveMessage "delete operation requires 'before' field in the event object."
  }

  @Test
  fun `should fail on non-null after field with node delete operation`() {
    val event =
        NodeEvent(
            "id",
            EntityOperation.DELETE,
            emptyList(),
            emptyMap(),
            NodeState(emptyList(), emptyMap()),
            NodeState(emptyList(), emptyMap()),
        )
    shouldThrow<InvalidDataException> {
      transformer.transform(changeEvent(event))
    } shouldHaveMessage "delete operation requires 'after' field to be unset in the event object."
  }

  @Test
  fun `should fail on null after field with relationship create operation`() {
    val event =
        RelationshipEvent(
            "id",
            "KNOWS",
            Node("start-id", listOf("Person"), mapOf("Person" to listOf(mapOf("id" to 1L)))),
            Node("end-id", listOf("Person"), mapOf("Person" to listOf(mapOf("id" to 2L)))),
            emptyList(),
            EntityOperation.CREATE,
            null,
            null,
        )
    shouldThrow<InvalidDataException> {
      transformer.transform(changeEvent(event))
    } shouldHaveMessage "create operation requires 'after' field in the event object."
  }

  @Test
  fun `should fail on non-null before field with relationship create operation`() {
    val event =
        RelationshipEvent(
            "id",
            "KNOWS",
            Node("start-id", listOf("Person"), mapOf("Person" to listOf(mapOf("id" to 1L)))),
            Node("end-id", listOf("Person"), mapOf("Person" to listOf(mapOf("id" to 2L)))),
            emptyList(),
            EntityOperation.CREATE,
            RelationshipState(emptyMap()),
            RelationshipState(emptyMap()),
        )
    shouldThrow<InvalidDataException> {
      transformer.transform(changeEvent(event))
    } shouldHaveMessage "create operation requires 'before' field to be unset in the event object."
  }

  @Test
  fun `should fail on null before field with relationship update operation`() {
    val event =
        RelationshipEvent(
            "id",
            "KNOWS",
            Node("start-id", listOf("Person"), mapOf("Person" to listOf(mapOf("id" to 1L)))),
            Node("end-id", listOf("Person"), mapOf("Person" to listOf(mapOf("id" to 2L)))),
            emptyList(),
            EntityOperation.UPDATE,
            null,
            RelationshipState(emptyMap()),
        )
    shouldThrow<InvalidDataException> {
      transformer.transform(changeEvent(event))
    } shouldHaveMessage "update operation requires 'before' field in the event object."
  }

  @Test
  fun `should fail on null after field with relationship update operation`() {
    val event =
        RelationshipEvent(
            "id",
            "KNOWS",
            Node("start-id", listOf("Person"), mapOf("Person" to listOf(mapOf("id" to 1L)))),
            Node("end-id", listOf("Person"), mapOf("Person" to listOf(mapOf("id" to 2L)))),
            emptyList(),
            EntityOperation.UPDATE,
            RelationshipState(emptyMap()),
            null,
        )
    shouldThrow<InvalidDataException> {
      transformer.transform(changeEvent(event))
    } shouldHaveMessage "update operation requires 'after' field in the event object."
  }

  @Test
  fun `should fail on null before field with relationship delete operation`() {
    val event =
        RelationshipEvent(
            "id",
            "KNOWS",
            Node("start-id", listOf("Person"), mapOf("Person" to listOf(mapOf("id" to 1L)))),
            Node("end-id", listOf("Person"), mapOf("Person" to listOf(mapOf("id" to 2L)))),
            emptyList(),
            EntityOperation.DELETE,
            null,
            null,
        )
    shouldThrow<InvalidDataException> {
      transformer.transform(changeEvent(event))
    } shouldHaveMessage "delete operation requires 'before' field in the event object."
  }

  @Test
  fun `should fail on non-null after field with relationship delete operation`() {
    val event =
        RelationshipEvent(
            "id",
            "KNOWS",
            Node("start-id", listOf("Person"), mapOf("Person" to listOf(mapOf("id" to 1L)))),
            Node("end-id", listOf("Person"), mapOf("Person" to listOf(mapOf("id" to 2L)))),
            emptyList(),
            EntityOperation.DELETE,
            RelationshipState(emptyMap()),
            RelationshipState(emptyMap()),
        )
    shouldThrow<InvalidDataException> {
      transformer.transform(changeEvent(event))
    } shouldHaveMessage "delete operation requires 'after' field to be unset in the event object."
  }

  private fun changeEvent(event: Event): ChangeEvent =
      ChangeEvent(
          ChangeIdentifier("change-id"),
          1,
          1,
          Metadata(
              "service",
              "neo4j",
              "server-1",
              "neo4j",
              CaptureMode.DIFF,
              "bolt",
              "127.0.0.1:32000",
              "127.0.0.1:7687",
              ZonedDateTime.now().minusSeconds(1),
              ZonedDateTime.now(),
              mapOf("user" to "app_user", "app" to "hr"),
              emptyMap(),
          ),
          event,
      )
}
