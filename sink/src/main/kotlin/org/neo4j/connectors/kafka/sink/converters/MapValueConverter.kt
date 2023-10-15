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
import java.util.*
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct

@Suppress("UNCHECKED_CAST")
open class MapValueConverter<T> : ValueConverter<MutableMap<String, T?>>() {

  open fun setValue(result: MutableMap<String, T?>?, fieldName: String, value: Any?) {
    if (result != null) {
      result[fieldName] = value as T
    }
  }

  override fun newValue(): MutableMap<String, T?> {
    return mutableMapOf()
  }

  override fun setBytesField(
      result: MutableMap<String, T?>?,
      fieldName: String,
      value: ByteArray?
  ) {
    setValue(result, fieldName, value)
  }

  override fun setStringField(result: MutableMap<String, T?>?, fieldName: String, value: String?) {
    setValue(result, fieldName, value)
  }

  override fun setFloat32Field(result: MutableMap<String, T?>?, fieldName: String, value: Float?) {
    setValue(result, fieldName, value)
  }

  override fun setInt32Field(result: MutableMap<String, T?>?, fieldName: String, value: Int?) {
    setValue(result, fieldName, value)
  }

  override fun setArray(
      result: MutableMap<String, T?>?,
      fieldName: String,
      schema: Schema?,
      array: MutableList<Any?>?
  ) {
    val convertedArray = array?.map { convertInner(it) }
    setValue(result, fieldName, convertedArray)
  }

  override fun setTimestampField(result: MutableMap<String, T?>?, fieldName: String, value: Date) {
    setValue(result, fieldName, value)
  }

  override fun setTimeField(result: MutableMap<String, T?>?, fieldName: String, value: Date) {
    setValue(result, fieldName, value)
  }

  override fun setInt8Field(result: MutableMap<String, T?>?, fieldName: String, value: Byte) {
    setValue(result, fieldName, value)
  }

  override fun setStructField(result: MutableMap<String, T?>?, fieldName: String, value: Struct) {
    val converted = convert(value) as MutableMap<Any?, Any?>
    setMap(result, fieldName, null, converted)
  }

  override fun setMap(
      result: MutableMap<String, T?>?,
      fieldName: String,
      schema: Schema?,
      value: MutableMap<Any?, Any?>?
  ) {
    if (value != null) {
      val converted = convert(value) as MutableMap<Any?, Any?>
      setValue(result, fieldName, converted)
    } else {
      setNullField(result, fieldName)
    }
  }

  override fun setNullField(result: MutableMap<String, T?>?, fieldName: String) {
    setValue(result, fieldName, null)
  }

  override fun setFloat64Field(result: MutableMap<String, T?>?, fieldName: String, value: Double) {
    setValue(result, fieldName, value)
  }

  override fun setInt16Field(result: MutableMap<String, T?>?, fieldName: String, value: Short) {
    setValue(result, fieldName, value)
  }

  override fun setInt64Field(result: MutableMap<String, T?>?, fieldName: String, value: Long) {
    setValue(result, fieldName, value)
  }

  override fun setBooleanField(result: MutableMap<String, T?>?, fieldName: String, value: Boolean) {
    setValue(result, fieldName, value)
  }

  override fun setDecimalField(
      result: MutableMap<String, T?>?,
      fieldName: String,
      value: BigDecimal
  ) {
    setValue(result, fieldName, value)
  }

  override fun setDateField(result: MutableMap<String, T?>?, fieldName: String, value: Date) {
    setValue(result, fieldName, value)
  }

  open fun convertInner(value: Any?): Any? {
    return when (value) {
      is Struct,
      is Map<*, *> -> convert(value)
      is Collection<*> -> value.map(::convertInner)
      is Array<*> ->
          if (value.javaClass.componentType.isPrimitive) value else value.map(::convertInner)
      else -> value
    }
  }
}
