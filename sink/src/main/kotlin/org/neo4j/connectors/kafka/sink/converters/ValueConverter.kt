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
import java.math.BigInteger
import java.util.*
import org.apache.kafka.connect.data.Decimal
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.data.Time
import org.apache.kafka.connect.data.Timestamp
import org.apache.kafka.connect.errors.DataException
import org.slf4j.LoggerFactory

abstract class ValueConverter<T> {
  protected abstract fun newValue(): T

  protected abstract fun setStringField(result: T?, fieldName: String, value: String?)

  protected abstract fun setFloat32Field(result: T?, fieldName: String, value: Float?)

  protected abstract fun setFloat64Field(result: T?, fieldName: String, value: Double)

  protected abstract fun setTimestampField(result: T?, fieldName: String, value: Date)

  protected abstract fun setDateField(result: T?, fieldName: String, value: Date)

  protected abstract fun setTimeField(result: T?, fieldName: String, value: Date)

  protected abstract fun setInt8Field(result: T?, fieldName: String, value: Byte)

  protected abstract fun setInt16Field(result: T?, fieldName: String, value: Short)

  protected abstract fun setInt32Field(result: T?, fieldName: String, value: Int?)

  protected abstract fun setInt64Field(result: T?, fieldName: String, value: Long)

  protected abstract fun setBytesField(result: T?, fieldName: String, value: ByteArray?)

  protected abstract fun setDecimalField(result: T?, fieldName: String, value: BigDecimal)

  protected abstract fun setBooleanField(result: T?, fieldName: String, value: Boolean)

  protected abstract fun setStructField(result: T?, fieldName: String, value: Struct)

  protected abstract fun setArray(
      result: T?,
      fieldName: String,
      schema: Schema?,
      array: MutableList<Any?>?
  )

  protected abstract fun setMap(
      result: T?,
      fieldName: String,
      schema: Schema?,
      map: MutableMap<Any?, Any?>?
  )

  protected abstract fun setNullField(result: T?, fieldName: String)

  fun convert(value: Any): T {
    val result = newValue()
    when (value) {
      is Struct -> {
        convertStruct(result, value)
      }
      is Map<*, *> -> {
        convertMap(result, value)
      }
      else -> {
        throw DataException(
            String.format(
                "Only Schema (%s) or Schema less (%s) are supported. %s is not a supported type.",
                Struct::class.java.getName(),
                MutableMap::class.java.getName(),
                value.javaClass.getName(),
            ),
        )
      }
    }
    return result
  }

  @Suppress("UNCHECKED_CAST")
  fun convertMap(result: T, value: Map<*, *>) {
    for (key in value.keys) {
      val fieldName = key as String
      val fieldValue = value[key]
      try {
        if (null == fieldValue) {
          log.trace("convertStruct() - Setting '{}' to null.", fieldName)
          setNullField(result, fieldName)
          continue
        }
        if (fieldValue is String) {
          log.trace("convertStruct() - Processing '{}' as string.", fieldName)
          setStringField(result, fieldName, fieldValue as String?)
        } else if (fieldValue is Byte) {
          log.trace("convertStruct() - Processing '{}' as int8.", fieldName)
          setInt8Field(result, fieldName, fieldValue)
        } else if (fieldValue is Short) {
          log.trace("convertStruct() - Processing '{}' as int16.", fieldName)
          setInt16Field(result, fieldName, fieldValue)
        } else if (fieldValue is Int) {
          log.trace("convertStruct() - Processing '{}' as int32.", fieldName)
          setInt32Field(result, fieldName, fieldValue as Int?)
        } else if (fieldValue is Long) {
          log.trace("convertStruct() - Processing '{}' as long.", fieldName)
          setInt64Field(result, fieldName, fieldValue)
        } else if (fieldValue is BigInteger) {
          log.trace("convertStruct() - Processing '{}' as long.", fieldName)
          setInt64Field(result, fieldName, fieldValue.toLong())
        } else if (fieldValue is Double) {
          log.trace("convertStruct() - Processing '{}' as float64.", fieldName)
          setFloat64Field(result, fieldName, fieldValue)
        } else if (fieldValue is Float) {
          log.trace("convertStruct() - Processing '{}' as float32.", fieldName)
          setFloat32Field(result, fieldName, fieldValue as Float?)
        } else if (fieldValue is BigDecimal) {
          log.trace("convertStruct() - Processing '{}' as decimal.", fieldName)
          setDecimalField(result, fieldName, fieldValue)
        } else if (fieldValue is Boolean) {
          log.trace("convertStruct() - Processing '{}' as boolean.", fieldName)
          setBooleanField(result, fieldName, fieldValue)
        } else if (fieldValue is java.util.Date) {
          log.trace("convertStruct() - Processing '{}' as timestamp.", fieldName)
          setTimestampField(result, fieldName, fieldValue)
        } else if (fieldValue is ByteArray) {
          log.trace("convertStruct() - Processing '{}' as bytes.", fieldName)
          setBytesField(result, fieldName, fieldValue as ByteArray?)
        } else if (fieldValue is List<*>) {
          log.trace("convertStruct() - Processing '{}' as array.", fieldName)
          setArray(result, fieldName, null, fieldValue as MutableList<Any?>?)
        } else if (fieldValue is Map<*, *>) {
          log.trace("convertStruct() - Processing '{}' as map.", fieldName)
          setMap(result, fieldName, null, fieldValue as MutableMap<Any?, Any?>?)
        } else {
          throw DataException(
              String.format(
                  "%s is not a supported data type.",
                  fieldValue.javaClass.getName(),
              ),
          )
        }
      } catch (ex: Exception) {
        throw DataException(
            String.format("Exception thrown while processing field '%s'", fieldName),
            ex,
        )
      }
    }
  }

  @Suppress("UNCHECKED_CAST")
  fun convertStruct(result: T, struct: Struct) {
    val schema = struct.schema()
    for (field in schema.fields()) {
      val fieldName = field.name()
      log.trace("convertStruct() - Processing '{}'", field.name())
      val fieldValue = struct[field]
      try {
        if (null == fieldValue) {
          log.trace("convertStruct() - Setting '{}' to null.", fieldName)
          setNullField(result, fieldName)
          continue
        }
        log.trace(
            "convertStruct() - Field '{}'.field().schema().type() = '{}'",
            fieldName,
            field.schema().type(),
        )
        when (field.schema().type()) {
          Schema.Type.STRING -> {
            log.trace("convertStruct() - Processing '{}' as string.", fieldName)
            setStringField(result, fieldName, fieldValue as String)
          }
          Schema.Type.INT8 -> {
            log.trace("convertStruct() - Processing '{}' as int8.", fieldName)
            setInt8Field(result, fieldName, fieldValue as Byte)
          }
          Schema.Type.INT16 -> {
            log.trace("convertStruct() - Processing '{}' as int16.", fieldName)
            setInt16Field(result, fieldName, fieldValue as Short)
          }
          Schema.Type.INT32 ->
              if (org.apache.kafka.connect.data.Date.LOGICAL_NAME == field.schema().name()) {
                log.trace("convertStruct() - Processing '{}' as date.", fieldName)
                setDateField(result, fieldName, fieldValue as java.util.Date)
              } else if (Time.LOGICAL_NAME == field.schema().name()) {
                log.trace("convertStruct() - Processing '{}' as time.", fieldName)
                setTimeField(result, fieldName, fieldValue as java.util.Date)
              } else {
                val int32Value = fieldValue as Int
                log.trace("convertStruct() - Processing '{}' as int32.", fieldName)
                setInt32Field(result, fieldName, int32Value)
              }
          Schema.Type.INT64 ->
              if (Timestamp.LOGICAL_NAME == field.schema().name()) {
                log.trace("convertStruct() - Processing '{}' as timestamp.", fieldName)
                setTimestampField(result, fieldName, fieldValue as java.util.Date)
              } else {
                val int64Value = fieldValue as Long
                log.trace("convertStruct() - Processing '{}' as int64.", fieldName)
                setInt64Field(result, fieldName, int64Value)
              }
          Schema.Type.BYTES ->
              if (Decimal.LOGICAL_NAME == field.schema().name()) {
                log.trace("convertStruct() - Processing '{}' as decimal.", fieldName)
                setDecimalField(result, fieldName, fieldValue as BigDecimal)
              } else {
                val bytes = fieldValue as ByteArray
                log.trace("convertStruct() - Processing '{}' as bytes.", fieldName)
                setBytesField(result, fieldName, bytes)
              }
          Schema.Type.FLOAT32 -> {
            log.trace("convertStruct() - Processing '{}' as float32.", fieldName)
            setFloat32Field(result, fieldName, fieldValue as Float)
          }
          Schema.Type.FLOAT64 -> {
            log.trace("convertStruct() - Processing '{}' as float64.", fieldName)
            setFloat64Field(result, fieldName, fieldValue as Double)
          }
          Schema.Type.BOOLEAN -> {
            log.trace("convertStruct() - Processing '{}' as boolean.", fieldName)
            setBooleanField(result, fieldName, fieldValue as Boolean)
          }
          Schema.Type.STRUCT -> {
            log.trace("convertStruct() - Processing '{}' as struct.", fieldName)
            setStructField(result, fieldName, fieldValue as Struct)
          }
          Schema.Type.ARRAY -> {
            log.trace("convertStruct() - Processing '{}' as array.", fieldName)
            setArray(result, fieldName, schema, fieldValue as MutableList<Any?>?)
          }
          Schema.Type.MAP -> {
            log.trace("convertStruct() - Processing '{}' as map.", fieldName)
            setMap(result, fieldName, schema, fieldValue as MutableMap<Any?, Any?>?)
          }
          else -> throw DataException("Unsupported schema.type(): " + schema.type())
        }
      } catch (ex: Exception) {
        throw DataException(
            String.format("Exception thrown while processing field '%s'", fieldName),
            ex,
        )
      }
    }
  }

  companion object {
    private val log = LoggerFactory.getLogger(ValueConverter::class.java)
  }
}
