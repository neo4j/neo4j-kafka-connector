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
import com.fasterxml.jackson.module.kotlin.treeToValue
import com.networknt.schema.JsonSchema
import com.networknt.schema.JsonSchemaFactory
import com.networknt.schema.SpecVersion.VersionFlag
import org.neo4j.connectors.kafka.sink.strategy.InvalidDataException
import org.neo4j.connectors.kafka.sink.strategy.cud.Values.CREATE
import org.neo4j.connectors.kafka.sink.strategy.cud.Values.DELETE
import org.neo4j.connectors.kafka.sink.strategy.cud.Values.MERGE
import org.neo4j.connectors.kafka.sink.strategy.cud.Values.NODE
import org.neo4j.connectors.kafka.sink.strategy.cud.Values.RELATIONSHIP
import org.neo4j.connectors.kafka.sink.strategy.cud.Values.UPDATE
import org.neo4j.connectors.kafka.utils.JSONUtils
import org.neo4j.cypherdsl.core.renderer.Renderer
import org.neo4j.driver.Query

interface Operation {

  fun toQuery(renderer: Renderer = Renderer.getDefaultRenderer()): Query

  companion object {
    private val SCHEMA: JsonSchema =
        JsonSchemaFactory.getInstance(VersionFlag.V202012)
            .getSchema(Operation::class.java.getResourceAsStream("cud.schema.v1.json"))

    fun from(values: Map<String, Any?>): Operation {
      val type =
          when (val type = values[Keys.TYPE]) {
            is String -> type
            else -> throw IllegalArgumentException("Unknown type ('$type') in CUD file")
          }
      val operation =
          when (val operation = values[Keys.OPERATION]) {
            is String -> operation
            else -> throw IllegalArgumentException("Unknown operation ('$operation') for CUD file")
          }

      val mapper = JSONUtils.getObjectMapper()
      val node = mapper.valueToTree<JsonNode>(values)
      val errors = SCHEMA.validate(node)
      if (errors.isNotEmpty()) {
        throw InvalidDataException(
            errors.joinToString(", ") { "${it.evaluationPath}: ${it.message}" })
      }

      return when (type to operation) {
        NODE to CREATE -> mapper.treeToValue<CreateNode>(node)
        NODE to UPDATE -> mapper.treeToValue<UpdateNode>(node)
        NODE to MERGE -> mapper.treeToValue<MergeNode>(node)
        NODE to DELETE -> mapper.treeToValue<DeleteNode>(node)
        RELATIONSHIP to CREATE -> mapper.treeToValue<CreateRelationship>(node)
        RELATIONSHIP to UPDATE -> mapper.treeToValue<UpdateRelationship>(node)
        RELATIONSHIP to MERGE -> mapper.treeToValue<MergeRelationship>(node)
        RELATIONSHIP to DELETE -> mapper.treeToValue<DeleteRelationship>(node)
        else ->
            throw IllegalArgumentException(
                "Unknown type ('$type') and operation ('$operation') for CUD file",
            )
      }
    }
  }
}
