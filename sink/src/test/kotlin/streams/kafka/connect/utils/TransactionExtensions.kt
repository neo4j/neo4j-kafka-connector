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
package streams.kafka.connect.utils

import org.neo4j.driver.Result
import org.neo4j.driver.Transaction
import org.neo4j.driver.exceptions.NoSuchRecordException
import org.neo4j.driver.types.Node

fun Transaction.findNodes(label: String): Result =
    this.run(
        """
        MATCH (n:`${label}`)
        RETURN n
    """
            .trimIndent())

fun Transaction.findNode(label: String, key: String, value: Any): Node? =
    try {
      this.run(
              """
        MATCH (n:`${label}`{`$key`: ${'$'}value})
        RETURN n
    """
                  .trimIndent(),
              mapOf("value" to value))
          .single()[0]
          .asNode()
    } catch (e: NoSuchRecordException) {
      null
    }

fun Transaction.allRelationships(): Result =
    this.run(
        """
        MATCH ()-[r]->()
        RETURN r
    """
            .trimIndent())

fun Transaction.allNodes(): Result =
    this.run(
        """
        MATCH (n)
        RETURN n
    """
            .trimIndent())

fun Transaction.allLabels(): List<String> =
    this.run(
            """
        CALL db.labels() YIELD label
        RETURN label
    """
                .trimIndent())
        .list()
        .map { it["label"].asString() }
