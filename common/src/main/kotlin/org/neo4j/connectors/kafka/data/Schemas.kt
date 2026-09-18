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
package org.neo4j.connectors.kafka.data

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder

/**
 * Schema arithmetic shared by the value converters: [combine] finds the one schema that a group of
 * values all fit, and [normalize] settles the field order and optionality of the STRUCTs built from
 * Neo4j maps.
 */
object Schemas {

  /** The placeholder for a value that has only ever been seen as null. */
  val UNKNOWN: Schema = SimpleTypes.NULL.schema(true)

  fun isUnknown(schema: Schema): Boolean = SimpleTypes.NULL.matches(schema)

  /**
   * The narrowest schema that values of both [a] and [b] fit, or `null` when they have none.
   *
   * Nothing here changes a value's type. It supplies a type that was previously unknown, reconciles
   * whether a value may be null, and adds fields, so a value described by the result is still
   * delivered as the type it had.
   */
  fun combine(a: Schema, b: Schema): Schema? =
      when {
        // Checked first: UNKNOWN is itself a STRUCT and carries a type name of its own. A key seen
        // only as null may be absent later, so what it combines with becomes optional.
        isUnknown(a) && isUnknown(b) -> UNKNOWN
        isUnknown(a) -> makeOptional(b)
        isUnknown(b) -> makeOptional(a)
        a == b -> a
        a.type() != b.type() -> null
        a.name() != b.name() -> null
        a.version() != b.version() || a.doc() != b.doc() || a.parameters() != b.parameters() -> null
        else ->
            when (a.type()) {
              Schema.Type.STRUCT -> combineStructs(a, b)
              Schema.Type.ARRAY ->
                  combine(a.valueSchema(), b.valueSchema())?.let {
                    finish(SchemaBuilder.array(it), a, b)
                  }

              Schema.Type.MAP ->
                  combine(a.valueSchema(), b.valueSchema())?.let {
                    finish(SchemaBuilder.map(a.keySchema(), it), a, b)
                  }

              // Same type, same name and same metadata, so the two differ only in optionality.
              else -> makeOptional(a)
            }
      }

  /**
   * The one schema that every schema in [schemas] fits, or `null` when they have none. An empty
   * group gives [UNKNOWN]. The result does not depend on the order of the group, and repeated
   * schemas make no difference.
   */
  fun combineAll(schemas: Iterable<Schema>): Schema? {
    val iterator = schemas.iterator()
    if (!iterator.hasNext()) return UNKNOWN

    var combined: Schema? = iterator.next()
    while (iterator.hasNext() && combined != null) {
      combined = combine(combined, iterator.next())
    }
    return combined
  }

  /**
   * Settles the shape of a STRUCT built from Neo4j maps: every field becomes optional, so a later
   * record may omit any key, and the fields of a plain STRUCT are sorted by name, so the order the
   * keys arrived in cannot spawn a second schema for the same data.
   *
   * A node's or a relationship's STRUCT keeps its field order, because its metadata fields come
   * first by contract. Anything that is not a plain STRUCT is returned untouched.
   */
  fun normalize(schema: Schema): Schema {
    // Points, durations and the EXTENDED property-type envelope are fixed by their type.
    if (schema.name() != null) return schema

    return when {
      schema.type() == Schema.Type.ARRAY ->
          optionalityOf(SchemaBuilder.array(normalize(schema.valueSchema())), schema)

      schema.type() == Schema.Type.MAP ->
          optionalityOf(
              SchemaBuilder.map(schema.keySchema(), normalize(schema.valueSchema())),
              schema,
          )

      // The `{e0, e1, …}` fallback names its fields after list positions, so sorting them by name
      // would put e10 before e2.
      schema.type() == Schema.Type.STRUCT && !isIndexed(schema) -> {
        val fields =
            if (isEntity(schema)) schema.fields() else schema.fields().sortedBy { it.name() }
        optionalityOf(
            SchemaBuilder.struct().apply {
              fields.forEach { field(it.name(), makeOptional(normalize(it.schema()))) }
            },
            schema,
        )
      }

      else -> schema
    }
  }

  private fun optionalityOf(builder: SchemaBuilder, schema: Schema): Schema =
      builder.apply { if (schema.isOptional) optional() }.build()

  fun makeOptional(schema: Schema): Schema {
    if (schema.isOptional) return schema
    val builder =
        when (schema.type()) {
          Schema.Type.STRUCT ->
              SchemaBuilder.struct().apply {
                schema.fields().forEach { field(it.name(), it.schema()) }
              }

          Schema.Type.ARRAY -> SchemaBuilder.array(schema.valueSchema())
          Schema.Type.MAP -> SchemaBuilder.map(schema.keySchema(), schema.valueSchema())
          else -> SchemaBuilder.type(schema.type())
        }
    // name() carries the Neo4j logical type marker Schema.matches() keys off, so it and the rest
    // of the metadata have to survive the rebuild.
    return builder
        .name(schema.name())
        .version(schema.version())
        .doc(schema.doc())
        .apply {
          schema.parameters()?.let { parameters(it) }
          optional()
        }
        .build()
  }

  /** The fields a node's or a relationship's STRUCT is recognised by, such as `<elementId>`. */
  private fun metadataFieldNames(schema: Schema): Set<String> =
      schema.fields().map { it.name() }.filter { it.startsWith("<") && it.endsWith(">") }.toSet()

  private fun isEntity(schema: Schema): Boolean = metadataFieldNames(schema).isNotEmpty()

  /**
   * True for the `{e0, e1, …}` STRUCT a list falls back to, whose field names are list positions.
   * Two of those must not combine: the result would name a position the shorter list does not have.
   */
  private fun isIndexed(schema: Schema): Boolean =
      schema.fields().isNotEmpty() && schema.fields().all { INDEXED_FIELD.matches(it.name()) }

  private fun combineStructs(a: Schema, b: Schema): Schema? {
    // Points, durations and the EXTENDED property-type envelope are STRUCTs carrying a type name.
    // Only identical ones combine, which combine() has already handled.
    if (a.name() != null || b.name() != null) return null
    if (isIndexed(a) || isIndexed(b)) return null
    if (metadataFieldNames(a) != metadataFieldNames(b)) return null

    val aNames = a.fields().map { it.name() }
    val bNames = b.fields().map { it.name() }
    // Two entity STRUCTs combine only when they describe the same fields. Their fields are never
    // sorted, so a union of two different field sets would come out in an order that depends on
    // which value arrived first.
    if (isEntity(a) && aNames.toSet() != bNames.toSet()) return null

    val builder = SchemaBuilder.struct()
    for (name in aNames + bNames.filterNot { aNames.contains(it) }) {
      val fieldOfA = a.field(name)?.schema()
      val fieldOfB = b.field(name)?.schema()
      val combined =
          if (fieldOfA != null && fieldOfB != null) {
            combine(fieldOfA, fieldOfB) ?: return null
          } else {
            // A field only one side has may be absent from a later value.
            makeOptional(fieldOfA ?: fieldOfB!!)
          }
      builder.field(name, combined)
    }
    return finish(builder, a, b)
  }

  private fun finish(builder: SchemaBuilder, a: Schema, b: Schema): Schema =
      builder
          .name(a.name())
          .version(a.version())
          .doc(a.doc())
          .apply {
            a.parameters()?.let { parameters(it) }
            if (a.isOptional || b.isOptional) optional()
          }
          .build()

  private val INDEXED_FIELD = Regex("e\\d+")
}
