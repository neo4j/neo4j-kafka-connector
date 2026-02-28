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
package org.neo4j.connectors.kafka.metrics

import java.lang.management.ManagementFactory
import javax.management.MBeanServer
import javax.management.ObjectName
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.neo4j.connectors.kafka.configuration.Neo4jConfiguration

class JmxMetricsTest {

  private val config =
      mock<Neo4jConfiguration> {
        on { connectorName } doReturn "my-connector"
        on { taskId } doReturn "0"
      }

  private lateinit var metrics: JmxMetrics

  @AfterEach
  fun tearDown() {
    metrics.close()
  }

  @Test
  fun `should register MBean on initialization`() {
    // given MBean is not registered
    assertFalse(mbs.isRegistered(objectName))

    // when
    metrics = JmxMetrics(config)

    // then
    assertTrue(mbs.isRegistered(objectName))
  }

  @Test
  fun `should unregister MBean on close`() {
    // given
    metrics = JmxMetrics(config)
    assertTrue(mbs.isRegistered(objectName))

    // when
    metrics.close()

    // then
    assertFalse(mbs.isRegistered(objectName))
  }

  @Test
  fun `should provide gauge value via JMX`() {
    // given
    metrics = JmxMetrics(config)

    // when
    metrics.addGauge("test_gauge", "A test gauge", linkedMapOf()) { 42 }

    // then
    val value = mbs.getAttribute(objectName, "test_gauge")
    assertEquals(42, value)
  }

  @Test
  fun `should expose MBean info with attributes`() {
    // given
    metrics = JmxMetrics(config)

    // when
    metrics.addGauge("gauge_1", "Description 1", linkedMapOf()) { 1 }
    metrics.addGauge("gauge_2", "Description 2", linkedMapOf()) { 2 }

    // then
    val info = metrics.getMBeanInfo()
    assertNotNull(info)
    assertEquals(2, info.attributes.size)

    val attr1 = info.attributes.find { it.name == "gauge_1" }
    assertNotNull(attr1)
    assertEquals("Description 1", attr1.description)
    assertEquals("java.lang.Number", attr1.type)

    val attr2 = info.attributes.find { it.name == "gauge_2" }
    assertNotNull(attr2)
    assertEquals("Description 2", attr2.description)
    assertEquals("java.lang.Number", attr2.type)
  }

  @Test
  fun `should handle re-registration`() {
    // given
    metrics = JmxMetrics(config)
    assertTrue(mbs.isRegistered(objectName))

    // create another instance with the same name, should unregister old and register new
    val metrics2 = JmxMetrics(config)
    assertTrue(mbs.isRegistered(objectName))

    metrics2.close()
    assertFalse(mbs.isRegistered(objectName))

    // original metrics close should not fail even if already unregistered
    metrics.close()
  }

  companion object {
    val mbs: MBeanServer = ManagementFactory.getPlatformMBeanServer()
    val objectName: ObjectName =
        ObjectName("kafka.connect:type=plugins,connector=my-connector,task=0")
  }
}
