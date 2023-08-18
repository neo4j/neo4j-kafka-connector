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

import org.junit.jupiter.api.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull

/**
 *
 * @author Gerrit Meier
 */
class ResultMapperTest {

  @Test
  fun shouldParseChangeIdentifier() {
    val changeIdentifierValue = "AlvVSy-4s0yaia31SXmHHz8AAAAAAAAACgAAAAAAAAAA"
    val result = ResultMapper.parseChangeIdentifier(mapOf("id" to changeIdentifierValue))
    assertEquals(result.id, changeIdentifierValue)
  }

  @Test
  fun shouldParseCompleteChangeNodeEventRecord() {
    val message = mapOf(
        "id" to "AlvVSy-4s0yaia31SXmHHz8AAAAAAAAACgAAAAAAAAAA",
        "txId" to 3L,
        "seq" to 1,
        "metadata" to """
          {
            "executingUser": "neo4j",
            "connectionClient": "172.17.0.1:44484",
            "authenticatedUser": "neo4j",
            "captureMode": "FULL",
            "serverId": "60b75468",
            "connectionType": "bolt",
            "connectionServer": "172.17.0.2:7687",
            "txStartTime": "2023-08-17T09:14:35.636000000Z",
            "txCommitTime": "2023-08-17T09:14:35.666000000Z"
          }
        """.trimIndent(),
        "event" to """
          {
            "elementId": "4:5bd54b2f-b8b3-4c9a-89ad-f54979871f3f:0",
            "keys": {

            },
            "eventType": "n",
            "state": {
              "before": null,
              "after": {
                "properties": {
                  "name": "someone",
                  "real_name": "Some real name"
                },
                "labels": [
                  "User"
                ]
              }
            },
            "operation": "c",
            "labels": [
              "User"
            ]
          }
        """.trimIndent(),
    )

    val changeEvent = ResultMapper.parseChangeEvent(message)
    assertEquals(changeEvent.id.id, "AlvVSy-4s0yaia31SXmHHz8AAAAAAAAACgAAAAAAAAAA")
    assertEquals(changeEvent.txId, 3L)
    assertEquals(changeEvent.seq, 1)
    val metadata = changeEvent.metadata
    checkMetadata(metadata)
    val nodeEvent = changeEvent.event
    assertIs<NodeEvent>(nodeEvent)
    assertEquals(nodeEvent.elementId, "4:5bd54b2f-b8b3-4c9a-89ad-f54979871f3f:0")
    assertEquals(nodeEvent.keys, emptyMap())
    assertEquals(nodeEvent.eventType, "n")
    assertContains(nodeEvent.labels, "User")
    assertEquals(nodeEvent.operation, "c")
    val state = nodeEvent.state
    assertNotNull(state)
    assertNull(state.before)
    assertEquals((state.after["properties"] as Map<String, Any>)["name"], "someone")
    assertEquals((state.after["properties"] as Map<String, Any>)["real_name"], "Some real name")
    assertContains((state.after["labels"] as List<String>), "User")

  }

  @Test
  fun shouldParseCompleteChangeRelationshipEventRecord() {
    val message = mapOf(
        "id" to "AlvVSy-4s0yaia31SXmHHz8AAAAAAAAACgAAAAAAAAAA",
        "txId" to 4L,
        "seq" to 2,
        "metadata" to """
          {
            "executingUser": "neo4j",
            "connectionClient": "172.17.0.1:44484",
            "authenticatedUser": "neo4j",
            "captureMode": "FULL",
            "serverId": "60b75468",
            "connectionType": "bolt",
            "connectionServer": "172.17.0.2:7687",
            "txStartTime": "2023-08-17T09:14:35.636000000Z",
            "txCommitTime": "2023-08-17T09:14:35.666000000Z"
          }
        """.trimIndent(),
        "event" to """
         {
          "elementId": "5:6a4af4ff-da3a-49e7-ae71-2c0ac3c1fc1a:0",
          "start": {
            "elementId": "4:6a4af4ff-da3a-49e7-ae71-2c0ac3c1fc1a:0",
            "keys": {},
            "labels": ["PERSON"]
          },
          "end": {
           "elementId": "4:6a4af4ff-da3a-49e7-ae71-2c0ac3c1fc1a:1",
           "keys": {},
           "labels": ["MOVIE"] 
           },
           "eventType": "r",
           "state": {
            "before": null, 
            "after": {
             "properties": {
               "roles": "Jack Swigert"
              }
            } 
          },
          "type": "ACTED_IN",
          "operation": "c",
          "key": {}
        }
        """.trimIndent(),
    )

    val changeEvent = ResultMapper.parseChangeEvent(message)
    assertEquals(changeEvent.id.id, "AlvVSy-4s0yaia31SXmHHz8AAAAAAAAACgAAAAAAAAAA")
    assertEquals(changeEvent.txId, 4L)
    assertEquals(changeEvent.seq, 2)
    checkMetadata(changeEvent.metadata)
    val relationshipEvent = changeEvent.event
    assertIs<RelationshipEvent>(relationshipEvent)
    assertEquals(relationshipEvent.elementId, "5:6a4af4ff-da3a-49e7-ae71-2c0ac3c1fc1a:0")
    assertEquals(relationshipEvent.type, "ACTED_IN")
    assertEquals(relationshipEvent.operation, "c")
    assertEquals(relationshipEvent.eventType, "r")
    assertNull(relationshipEvent.state.before)
    val after = relationshipEvent.state.after
    assertEquals((after["properties"] as Map<String, Any>)["roles"], "Jack Swigert")

    val startElement = relationshipEvent.start
    assertEquals(startElement.elementId, "4:6a4af4ff-da3a-49e7-ae71-2c0ac3c1fc1a:0")
    assertEquals(startElement.keys, emptyMap())
    assertContains(startElement.labels, "PERSON")

    val endElement = relationshipEvent.end
    assertEquals(endElement.elementId, "4:6a4af4ff-da3a-49e7-ae71-2c0ac3c1fc1a:1")
    assertEquals(endElement.keys, emptyMap())
    assertContains(endElement.labels, "MOVIE")
  }

  private fun checkMetadata(metadata: Metadata) {
    assertEquals(metadata.executingUser, "neo4j")
    assertEquals(metadata.connectionClient, "172.17.0.1:44484")
    assertEquals(metadata.authenticatedUser, "neo4j")
    assertEquals(metadata.captureMode, CaptureMode.FULL)
    assertEquals(metadata.serverId, "60b75468")
    assertEquals(metadata.connectionType, "bolt")
    assertEquals(metadata.connectionServer, "172.17.0.2:7687")
    assertEquals(metadata.txStartTime, "2023-08-17T09:14:35.636000000Z")
    assertEquals(metadata.txCommitTime, "2023-08-17T09:14:35.666000000Z")
  }
}
