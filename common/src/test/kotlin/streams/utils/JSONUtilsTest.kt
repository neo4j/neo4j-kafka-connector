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
package streams.utils

import java.time.OffsetTime
import java.time.ZoneOffset.UTC
import java.time.ZonedDateTime
import kotlin.test.assertEquals
import kotlin.test.assertFails
import org.junit.jupiter.api.Test
import org.neo4j.connectors.kafka.events.EntityType
import org.neo4j.connectors.kafka.events.Meta
import org.neo4j.connectors.kafka.events.NodeChange
import org.neo4j.connectors.kafka.events.NodePayload
import org.neo4j.connectors.kafka.events.OperationType
import org.neo4j.connectors.kafka.events.Schema
import org.neo4j.connectors.kafka.events.StreamsTransactionEvent
import org.neo4j.connectors.kafka.utils.JSONUtils
import org.neo4j.driver.Values

class JSONUtilsTest {
  val Cartesian_Code = 7203
  val Cartesian_3D_Code = 9157
  val WGS84_Code = 4326
  val WGS84_3D_Code = 4979

  @Test
  fun `should serialize driver Point Data Types`() {
    // Given
    val expected =
        "{\"point2dCartesian\":{\"crs\":\"cartesian\",\"x\":1.0,\"y\":2.0,\"z\":null}," +
            "\"point3dCartesian\":{\"crs\":\"cartesian-3d\",\"x\":1.0,\"y\":2.0,\"z\":3.0}," +
            "\"point2dWgs84\":{\"crs\":\"wgs-84\",\"latitude\":1.0,\"longitude\":2.0,\"height\":null}," +
            "\"point3dWgs84\":{\"crs\":\"wgs-84-3d\",\"latitude\":1.0,\"longitude\":2.0,\"height\":3.0}," +
            "\"time\":\"14:01:01.000000001Z\",\"dateTime\":\"2017-12-17T17:14:35.123456789Z\"}"

    val map =
        linkedMapOf<String, Any>(
            "point2dCartesian" to Values.point(Cartesian_Code, 1.0, 2.0),
            "point3dCartesian" to Values.point(Cartesian_3D_Code, 1.0, 2.0, 3.0),
            "point2dWgs84" to Values.point(WGS84_Code, 1.0, 2.0),
            "point3dWgs84" to Values.point(WGS84_3D_Code, 1.0, 2.0, 3.0),
            "time" to Values.value(OffsetTime.of(14, 1, 1, 1, UTC)),
            "dateTime" to Values.value(ZonedDateTime.of(2017, 12, 17, 17, 14, 35, 123456789, UTC)))

    // When
    val jsonString = JSONUtils.writeValueAsString(map)

    // Then
    assertEquals(expected, jsonString)
  }

  @Test
  fun `should convert data to StreamsTransactionEvent`() {
    // given
    val timestamp = System.currentTimeMillis()
    val cdcData =
        StreamsTransactionEvent(
            meta =
                Meta(
                    timestamp = timestamp,
                    username = "user",
                    txId = 1,
                    txEventId = 0,
                    txEventsCount = 1,
                    operation = OperationType.created),
            payload =
                NodePayload(
                    id = "0",
                    before = null,
                    after =
                        NodeChange(
                            properties = mapOf("prop1" to "foo", "bar" to 1),
                            labels = listOf("LabelCDC")),
                    type = EntityType.node),
            schema = Schema())
    val cdcMap =
        mapOf<String, Any>(
            "meta" to
                mapOf(
                    "timestamp" to timestamp,
                    "username" to "user",
                    "txId" to 1,
                    "txEventId" to 0,
                    "txEventsCount" to 1,
                    "operation" to OperationType.created),
            "payload" to
                mapOf(
                    "id" to "0",
                    "before" to null,
                    "after" to
                        NodeChange(
                            properties = mapOf("prop1" to "foo", "bar" to 1),
                            labels = listOf("LabelCDC")),
                    "type" to EntityType.node),
            "schema" to emptyMap<String, Any>())
    val cdcString =
        """{
            |"meta":{"timestamp":$timestamp,"username":"user","txId":1,"txEventId":0,"txEventsCount":1,"operation":"created"},
            |"payload":{"id":"0","before":null,"after":{"properties":{"prop1":"foo","bar":1},"labels":["LabelCDC"]},"type":"node"},
            |"schema":{}
            |}"""
            .trimMargin()

    val fromMap = JSONUtils.asStreamsTransactionEvent(cdcMap)
    val fromString = JSONUtils.asStreamsTransactionEvent(cdcString)
    assertEquals(cdcData, fromMap)
    assertEquals(cdcData, fromString)
  }

  @Test
  fun `should deserialize plain values`() {
    assertEquals("a", JSONUtils.readValue("a", stringWhenFailure = true))
    assertEquals(42, JSONUtils.readValue("42"))
    assertEquals(true, JSONUtils.readValue("true"))
    assertFails { JSONUtils.readValue("a") }
    // assertEquals(null, JSONUtils.readValue("null"))
  }
}
