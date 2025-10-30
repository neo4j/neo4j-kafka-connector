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
import com.networknt.schema.Schema
import com.networknt.schema.SchemaRegistry
import com.networknt.schema.SpecificationVersion
import org.neo4j.connectors.kafka.exceptions.InvalidDataException
import org.neo4j.connectors.kafka.sink.strategy.cud.OperationType.CREATE
import org.neo4j.connectors.kafka.sink.strategy.cud.OperationType.DELETE
import org.neo4j.connectors.kafka.sink.strategy.cud.OperationType.MERGE
import org.neo4j.connectors.kafka.sink.strategy.cud.OperationType.UPDATE
import org.neo4j.connectors.kafka.sink.strategy.cud.Type.NODE
import org.neo4j.connectors.kafka.sink.strategy.cud.Type.RELATIONSHIP
import org.neo4j.connectors.kafka.utils.JSONUtils
import org.neo4j.cypherdsl.core.renderer.Renderer
import org.neo4j.driver.Query

interface Operation {

  fun toQuery(renderer: Renderer = Renderer.getDefaultRenderer()): Query

  companion object {
    private val SCHEMA: Schema =
        SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12)
            .getSchema(Operation::class.java.getResourceAsStream("cud.schema.v1.json"))

    fun from(values: Map<String, Any?>): Operation {
      val type =
          Type.fromString(
              when (val type = values[Keys.TYPE]) {
                is String -> type
                else ->
                    throw IllegalArgumentException(
                        "Unsupported data type ('$type') in CUD file type."
                    )
              }
          ) ?: throw IllegalArgumentException("CUD file type must be specified.")
      val operation =
          OperationType.fromString(
              when (val operation = values[Keys.OPERATION]) {
                is String -> operation
                else ->
                    throw IllegalArgumentException(
                        "Unsupported data type ('$operation') for CUD file operation"
                    )
              }
          ) ?: throw IllegalArgumentException("CUD file operation must be specified.")

      val mapper = JSONUtils.getObjectMapper()
      val node = mapper.valueToTree<JsonNode>(values)
      val errors = SCHEMA.validate(node)
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
            throw IllegalArgumentException(
                "Unknown type ('$type') and operation ('$operation') for CUD file"
            )
      }
    }
  }
}
