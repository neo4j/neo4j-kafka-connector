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
package org.neo4j.connectors.kafka.data.converter

import java.nio.ByteBuffer
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.OffsetTime
import java.time.ZonedDateTime
import java.util.UUID
import kotlin.reflect.KClass
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.neo4j.connectors.kafka.configuration.MapEncoding
import org.neo4j.connectors.kafka.data.DAYS
import org.neo4j.connectors.kafka.data.DIMENSION
import org.neo4j.connectors.kafka.data.MONTHS
import org.neo4j.connectors.kafka.data.NANOS
import org.neo4j.connectors.kafka.data.PropertyType
import org.neo4j.connectors.kafka.data.SECONDS
import org.neo4j.connectors.kafka.data.SR_ID
import org.neo4j.connectors.kafka.data.Schemas
import org.neo4j.connectors.kafka.data.THREE_D
import org.neo4j.connectors.kafka.data.TWO_D
import org.neo4j.connectors.kafka.data.ValueConverter
import org.neo4j.connectors.kafka.data.X
import org.neo4j.connectors.kafka.data.Y
import org.neo4j.connectors.kafka.data.Z
import org.neo4j.connectors.kafka.exceptions.InvalidDataException
import org.neo4j.driver.types.IsoDuration
import org.neo4j.driver.types.Node
import org.neo4j.driver.types.Point
import org.neo4j.driver.types.Relationship

class ExtendedValueConverter(private val mapEncoding: MapEncoding = MapEncoding.STRUCT) :
    ValueConverter {

  override fun rowSchema(row: Map<String, Any?>, optional: Boolean): Schema =
      structSchema(row.mapValues { schema(it.value, optional) }, optional, sortFields = false)

  override fun schema(value: Any?, optional: Boolean): Schema {
    return when (value) {
      null -> PropertyType.schema
      is Boolean,
      is Float,
      is Double,
      is Number,
      is Char,
      is LocalDate,
      is LocalDateTime,
      is LocalTime,
      is OffsetDateTime,
      is ZonedDateTime,
      is OffsetTime,
      is IsoDuration,
      is Point,
      is CharArray,
      is CharSequence,
      is UUID,
      is ByteBuffer,
      is ByteArray,
      is ShortArray,
      is IntArray,
      is LongArray,
      is FloatArray,
      is DoubleArray,
      is BooleanArray -> PropertyType.schema

      is Array<*> ->
          if (value::class.java.componentType.kotlin.isSimplePropertyType()) {
            PropertyType.schema
          } else {
            elementsSchema(value.asList(), optional)
          }

      is Node ->
          SchemaBuilder.struct()
              .apply {
                field("<elementId>", Schema.STRING_SCHEMA)
                field("<labels>", SchemaBuilder.array(Schema.STRING_SCHEMA).build())

                value.keys().forEach { field(it, PropertyType.schema) }

                if (optional) optional()
              }
              .build()

      is Relationship ->
          SchemaBuilder.struct()
              .apply {
                field("<elementId>", Schema.STRING_SCHEMA)
                field("<type>", Schema.STRING_SCHEMA)
                field("<start.elementId>", Schema.STRING_SCHEMA)
                field("<end.elementId>", Schema.STRING_SCHEMA)

                value.keys().forEach { field(it, PropertyType.schema) }

                if (optional) optional()
              }
              .build()

      is Collection<*> -> {
        val elementTypes = value.map { it?.javaClass?.kotlin }.toSet()
        if (elementTypes.isEmpty()) {
          return PropertyType.schema
        }

        val elementType = elementTypes.singleOrNull()
        if (elementType != null && elementType.isSimplePropertyType()) {
          return PropertyType.schema
        }

        elementsSchema(value.toList(), optional)
      }

      is Map<*, *> -> mapSchema(value, optional)

      else -> throw IllegalArgumentException("unsupported type ${value.javaClass.name}")
    }
  }

  /**
   * An array of the one schema every element fits. Elements with no shared schema fall back to a
   * STRUCT with one field per position, so that a collection of unrelated values is still
   * representable.
   */
  private fun elementsSchema(elements: List<Any?>, optional: Boolean): Schema {
    // A null element is described by the property-type schema here, which no map or list schema
    // combines with, so null elements are left out of the comparison. They stay valid, because an
    // element schema that stands for several elements is optional.
    val present = elements.filterNotNull()

    // An empty collection has no element to look at, so it is described the way one holding
    // only nulls is.
    if (present.isEmpty()) return array(PropertyType.schema, optional)

    val elementSchemas = present.map { schema(it, optional) }.distinct()
    if (elementSchemas.size == 1) {
      val element = elementSchemas.single()
      // a null element left out above still has to fit the element schema
      return array(
          if (present.size < elements.size) Schemas.makeOptional(element) else element,
          optional,
      )
    }

    // One schema now stands for several elements, so everything it describes is optional: a later
    // element may leave out a key, and a null element has to stay valid.
    val combined = Schemas.combineAll(present.map { schema(it, optional = true) }.distinct())
    if (combined == null) {
      return SchemaBuilder.struct()
          .apply {
            elements.forEachIndexed { index, element ->
              field("e${index}", schema(element, optional))
            }
            if (optional) optional()
          }
          .build()
    }

    return array(Schemas.makeOptional(Schemas.normalize(combined)), optional)
  }

  private fun array(elementSchema: Schema, optional: Boolean): Schema =
      SchemaBuilder.array(elementSchema).apply { if (optional) optional() }.build()

  /** Describes a Neo4j map as the configured [MapEncoding] says, at every level of nesting. */
  private fun mapSchema(value: Map<*, *>, optional: Boolean): Schema {
    val entries = value.mapKeys { stringKey(it.key) }.mapValues { schema(it.value, optional) }

    return when (mapEncoding) {
      MapEncoding.STRUCT -> structSchema(entries, optional, sortFields = true)
      MapEncoding.MAP ->
          kafkaMap(valueSchema(value, entries) ?: throw noSharedValueSchema(entries), optional)

      // A map whose values have no shared schema, an empty map and a map holding only nulls
      // leave no value type for a MAP to carry, so they become STRUCTs.
      MapEncoding.LEGACY -> {
        val valueSchema = valueSchema(value, entries)
        if (
            valueSchema == null || Schemas.isUnknown(valueSchema) || value.values.all { it == null }
        ) {
          structSchema(entries, optional, sortFields = true)
        } else {
          kafkaMap(valueSchema, optional)
        }
      }
    }
  }

  /**
   * The one schema the values of a Neo4j map all fit, or `null` when they have none. Every property
   * value has the same property-type schema here, including a null one, so whether an entry may be
   * null has to come from the values rather than from their schemas.
   */
  private fun valueSchema(value: Map<*, *>, entries: Map<String, Schema>): Schema? =
      Schemas.combineAll(entries.values)?.let {
        if (value.values.any { entry -> entry == null }) Schemas.makeOptional(it) else it
      }

  private fun kafkaMap(valueSchema: Schema, optional: Boolean): Schema =
      SchemaBuilder.map(Schema.STRING_SCHEMA, valueSchema)
          .apply { if (optional) optional() }
          .build()

  /**
   * Every field is optional, so a later record may omit any key. The fields of a Neo4j map are
   * sorted by name, so the order the keys arrived in cannot spawn a second schema for the same
   * data; the columns of a row keep the order the query returned them in.
   */
  private fun structSchema(
      fields: Map<String, Schema>,
      optional: Boolean,
      sortFields: Boolean,
  ): Schema {
    val names = if (sortFields) fields.keys.sorted() else fields.keys.toList()
    return SchemaBuilder.struct()
        .apply {
          names.forEach { field(it, Schemas.makeOptional(fields.getValue(it))) }
          if (optional) optional()
        }
        .build()
  }

  private fun noSharedValueSchema(entries: Map<String, Schema>): InvalidDataException =
      InvalidDataException(
          "unable to describe a map as a MAP, because its values have no shared type: " +
              entries.entries.joinToString(", ") { "'${it.key}' is ${describe(it.value)}" } +
              ". Use 'neo4j.query.map-encoding' of 'STRUCT' or 'LEGACY', or a 'neo4j.payload-mode' of " +
              "'EXTENDED' or 'RAW_JSON_STRING'."
      )

  private fun describe(schema: Schema): String = schema.name() ?: schema.type().name

  private fun stringKey(key: Any?): String =
      key as? String
          ?: throw IllegalArgumentException("unsupported map key type ${key?.javaClass?.name}")

  override fun value(schema: Schema, value: Any?): Any? {
    if (value == null) {
      return null
    }

    if (schema == PropertyType.schema) {
      return PropertyType.toConnectValue(value)
    }

    return when (schema.type()) {
      Schema.Type.ARRAY ->
          when (value) {
            is Collection<*> -> value.map { value(schema.valueSchema(), it) }
            is Array<*> -> value.map { value(schema.valueSchema(), it) }.toList()
            is ShortArray -> value.map { s -> s.toLong() }.toList()
            is IntArray -> value.map { i -> i.toLong() }.toList()
            is FloatArray -> value.map { f -> f.toDouble() }.toList()
            is BooleanArray -> value.toList()
            is LongArray -> value.toList()
            is DoubleArray -> value.toList()
            else -> throw IllegalArgumentException("unsupported array type ${value.javaClass.name}")
          }

      Schema.Type.MAP ->
          when (value) {
            is Map<*, *> -> value.mapValues { value(schema.valueSchema(), it.value) }
            else -> throw IllegalArgumentException("unsupported map type ${value.javaClass.name}")
          }

      Schema.Type.STRUCT ->
          when (value) {
            is IsoDuration ->
                Struct(schema)
                    .put(MONTHS, value.months())
                    .put(DAYS, value.days())
                    .put(SECONDS, value.seconds())
                    .put(NANOS, value.nanoseconds())

            is Point ->
                Struct(schema).put(SR_ID, value.srid()).put(X, value.x()).put(Y, value.y()).also {
                  it.put(DIMENSION, if (value.z().isNaN()) TWO_D else THREE_D)
                  if (!value.z().isNaN()) {
                    it.put(Z, value.z())
                  }
                }

            is Node ->
                Struct(schema).apply {
                  put("<elementId>", value.elementId())
                  put("<labels>", value.labels())
                  value
                      .asMap { it.asObject() }
                      .forEach { e -> put(e.key, value(schema.field(e.key).schema(), e.value)) }
                }

            is Relationship ->
                Struct(schema).apply {
                  put("<elementId>", value.elementId())
                  put("<type>", value.type())
                  put("<start.elementId>", value.startNodeElementId())
                  put("<end.elementId>", value.endNodeElementId())
                  value
                      .asMap { it.asObject() }
                      .forEach { e -> put(e.key, value(schema.field(e.key).schema(), e.value)) }
                }

            is Map<*, *> ->
                Struct(schema).apply {
                  schema.fields().forEach { put(it.name(), value(it.schema(), value[it.name()])) }
                }

            is Collection<*> ->
                Struct(schema).apply {
                  schema.fields().forEach {
                    put(
                        it.name(),
                        value(it.schema(), value.elementAt(it.name().substring(1).toInt())),
                    )
                  }
                }

            else ->
                throw IllegalArgumentException("unsupported struct type ${value.javaClass.name}")
          }

      else -> value
    }
  }

  private fun KClass<*>.isSimplePropertyType(): Boolean =
      when (this) {
        Boolean::class,
        Byte::class,
        Short::class,
        Int::class,
        Long::class,
        Float::class,
        Double::class,
        String::class,
        UUID::class,
        LocalDate::class,
        LocalDateTime::class,
        LocalTime::class,
        OffsetDateTime::class,
        ZonedDateTime::class,
        OffsetTime::class -> true

        else ->
            if (IsoDuration::class.java.isAssignableFrom(this.java)) {
              true
            } else if (Point::class.java.isAssignableFrom(this.java)) {
              true
            } else {
              false
            }
      }
}
