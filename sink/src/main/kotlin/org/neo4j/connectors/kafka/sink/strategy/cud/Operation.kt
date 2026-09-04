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
package org.neo4j.connectors.kafka.sink.strategy.cud

import com.fasterxml.jackson.databind.JsonNode
import com.networknt.schema.InputFormat
import com.networknt.schema.Schema
import com.networknt.schema.SchemaRegistry
import com.networknt.schema.SchemaRegistryConfig
import com.networknt.schema.SpecificationVersion
import com.networknt.schema.path.PathType
import java.io.IOException
import org.neo4j.connectors.kafka.exceptions.InvalidDataException
import org.neo4j.connectors.kafka.sink.strategy.SinkAction
import org.neo4j.connectors.kafka.sink.strategy.cud.OperationType.CREATE
import org.neo4j.connectors.kafka.sink.strategy.cud.OperationType.DELETE
import org.neo4j.connectors.kafka.sink.strategy.cud.OperationType.MERGE
import org.neo4j.connectors.kafka.sink.strategy.cud.OperationType.UPDATE
import org.neo4j.connectors.kafka.sink.strategy.cud.Type.NODE
import org.neo4j.connectors.kafka.sink.strategy.cud.Type.RELATIONSHIP
import org.neo4j.connectors.kafka.utils.JSONUtils

interface Operation {

  fun toAction(): SinkAction

  companion object {
    private val SCHEMA: Schema

    init {
      val registryConfig = SchemaRegistryConfig.builder().pathType(PathType.LEGACY).build()
      val registry =
          SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12) {
              builder: SchemaRegistry.Builder? ->
            builder!!.schemaRegistryConfig(registryConfig)
          }
      try {
        Operation::class.java.getResourceAsStream("cud.schema.v1.json").use { schema ->
          checkNotNull(schema) { "Cannot CUD JSON schema" }
          SCHEMA = registry.getSchema(schema, InputFormat.JSON)
        }
      } catch (e: IOException) {
        throw IllegalStateException("Cannot load CUD JSON schema", e)
      }
    }

    fun from(values: Map<String, Any?>): Operation {
      val type =
          Type.fromString(
              when (val type = values[Keys.TYPE]) {
                is String -> type
                else ->
                    throw InvalidDataException("Unsupported data type ('$type') in CUD file type.")
              }
          ) ?: throw InvalidDataException("CUD file type must be specified.")
      val operation =
          OperationType.fromString(
              when (val operation = values[Keys.OPERATION]) {
                is String -> operation
                else ->
                    throw InvalidDataException(
                        "Unsupported data type ('$operation') for CUD file operation"
                    )
              }
          ) ?: throw InvalidDataException("CUD file operation must be specified.")

      val mapper = JSONUtils.getObjectMapper()
      val node = mapper.valueToTree<JsonNode>(values)
      val errors = SCHEMA.validate(node.toString(), InputFormat.JSON)
      if (errors.isNotEmpty()) {
        throw InvalidDataException(
            errors.joinToString(", ") { "${it.evaluationPath}: ${it.message}" }
        )
      }

      return when (type to operation) {
        NODE to CREATE -> CreateNode.from(values)
        NODE to UPDATE -> UpdateNode.from(values)
        NODE to MERGE -> MergeNode.from(values)
        NODE to DELETE -> DeleteNode.from(values)
        RELATIONSHIP to CREATE -> CreateRelationship.from(values)
        RELATIONSHIP to UPDATE -> UpdateRelationship.from(values)
        RELATIONSHIP to MERGE -> MergeRelationship.from(values)
        RELATIONSHIP to DELETE -> DeleteRelationship.from(values)
        else ->
            throw InvalidDataException(
                "Unknown type ('$type') and operation ('$operation') for CUD file"
            )
      }
    }
  }
}
