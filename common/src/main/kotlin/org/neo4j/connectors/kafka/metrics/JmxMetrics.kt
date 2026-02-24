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
import java.util.Hashtable
import java.util.concurrent.ConcurrentHashMap
import javax.management.Attribute
import javax.management.AttributeList
import javax.management.DynamicMBean
import javax.management.MBeanAttributeInfo
import javax.management.MBeanInfo
import javax.management.ObjectName
import org.neo4j.connectors.kafka.configuration.Neo4jConfiguration

class JmxMetrics(config: Neo4jConfiguration) : Metrics, DynamicMBean {

  private val connectorName: String = config.connectorName
  private val taskId: String = config.taskId
  private val objectName: ObjectName =
      ObjectName(
          "kafka.connect",
          Hashtable<String, String>().apply {
            put("type", "plugins")
            put("connector", connectorName)
            put("task", taskId)
          },
      )

  private val gauges = ConcurrentHashMap<String, Gauge<*>>()
  private val mbs = ManagementFactory.getPlatformMBeanServer()

  init {
    if (mbs.isRegistered(objectName)) {
      mbs.unregisterMBean(objectName)
    }
    mbs.registerMBean(this, objectName)
  }

  override fun <T : Number> addGauge(
      name: String,
      description: String,
      tags: LinkedHashMap<String, String>,
      valueProvider: () -> T?,
  ) {
    gauges[name] = Gauge(name, description, valueProvider)
  }

  override fun getAttribute(attribute: String?): Any? {
    return gauges[attribute]?.valueProvider?.invoke()
  }

  override fun setAttribute(attribute: Attribute?) {
    throw UnsupportedOperationException("Attributes are read-only")
  }

  override fun getAttributes(attributes: Array<out String>?): AttributeList {
    val list = AttributeList()
    attributes?.forEach { name ->
      getAttribute(name)?.let { value -> list.add(Attribute(name, value)) }
    }
    return list
  }

  override fun setAttributes(attributes: AttributeList?): AttributeList {
    throw UnsupportedOperationException("Attributes are read-only")
  }

  override fun invoke(
      actionName: String?,
      params: Array<out Any>?,
      signature: Array<out String>?,
  ): Any {
    throw UnsupportedOperationException("Operations are not supported")
  }

  override fun getMBeanInfo(): MBeanInfo {
    val attrs =
        gauges.values.map { gauge ->
          MBeanAttributeInfo(gauge.name, "java.lang.Number", gauge.description, true, false, false)
        }

    return MBeanInfo(
        this.javaClass.name,
        "Neo4j Kafka Connector JMX Metrics",
        attrs.toTypedArray(),
        null,
        null,
        null,
    )
  }

  private class Gauge<T : Number>(
      val name: String,
      val description: String,
      val valueProvider: () -> T?,
  )

  override fun close() {
    mbs.unregisterMBean(objectName)
  }
}
