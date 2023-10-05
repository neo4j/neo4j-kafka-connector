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
package org.neo4j.connectors.kafka.sink.converters

import java.math.BigDecimal
import java.time.LocalTime
import java.time.ZoneId
import java.util.Date
import java.util.concurrent.TimeUnit
import org.apache.kafka.connect.data.Struct
import org.neo4j.driver.Value
import org.neo4j.driver.Values

class Neo4jValueConverter : MapValueConverter<Value>() {

  companion object {
    @JvmStatic private val UTC = ZoneId.of("UTC")
  }

  override fun setValue(result: MutableMap<String, Value?>?, fieldName: String, value: Any?) {
    if (result != null) {
      result[fieldName] = Values.value(value) ?: Values.NULL
    }
  }

  override fun newValue(): MutableMap<String, Value?> {
    return mutableMapOf()
  }

  override fun setDecimalField(
      result: MutableMap<String, Value?>?,
      fieldName: String,
      value: BigDecimal
  ) {
    val doubleValue = value.toDouble()
    val fitsScale =
        doubleValue != Double.POSITIVE_INFINITY &&
            doubleValue != Double.NEGATIVE_INFINITY &&
            value.compareTo(doubleValue.let { BigDecimal.valueOf(it) }) == 0
    if (fitsScale) {
      setValue(result, fieldName, doubleValue)
    } else {
      setValue(result, fieldName, value.toPlainString())
    }
  }

  override fun setTimestampField(
      result: MutableMap<String, Value?>?,
      fieldName: String,
      value: Date
  ) {
    val localDate = value.toInstant().atZone(UTC).toLocalDateTime()
    setValue(result, fieldName, localDate)
  }

  override fun setTimeField(result: MutableMap<String, Value?>?, fieldName: String, value: Date) {
    val time = LocalTime.ofNanoOfDay(TimeUnit.MILLISECONDS.toNanos(value.time))
    setValue(result, fieldName, time)
  }

  override fun setDateField(result: MutableMap<String, Value?>?, fieldName: String, value: Date) {
    val localDate = value.toInstant().atZone(UTC).toLocalDate()
    setValue(result, fieldName, localDate)
  }

  override fun setStructField(
      result: MutableMap<String, Value?>?,
      fieldName: String,
      value: Struct
  ) {
    val converted =
        convert(value).mapValues { it.value?.asObject() }.toMutableMap() as MutableMap<Any?, Any?>
    setMap(result, fieldName, null, converted)
  }
}
