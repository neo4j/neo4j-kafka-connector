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
package org.neo4j.connectors.kafka.testing

import org.neo4j.caniuse.CanIUse.canIUse
import org.neo4j.caniuse.Neo4j
import org.neo4j.caniuse.Schema
import org.neo4j.driver.Session

fun neo4jImage(): String =
    System.getenv("NEO4J_TEST_IMAGE").ifBlank {
      throw IllegalArgumentException("NEO4J_TEST_IMAGE environment variable is not defined!")
    }

fun Session.createNodeKeyConstraint(
    neo4j: Neo4j,
    name: String,
    label: String,
    vararg properties: String
) {
  if (canIUse(Schema.nodeKeyConstraints()).withNeo4j(neo4j)) {
    this.run(
            "CREATE CONSTRAINT $name FOR (n:$label) REQUIRE ${properties.joinToString(", ", "(", ")") { "n.$it"}} IS NODE KEY")
        .consume()
  } else {
    if (canIUse(Schema.nodePropertyUniquenessConstraints()).withNeo4j(neo4j)) {
      this.run(
              "CREATE CONSTRAINT $name FOR (n:$label) REQUIRE ${
            properties.joinToString(
                ", ",
                "(",
                ")"
            ) { "n.$it" }
          } IS UNIQUE")
          .consume()
    }

    if (canIUse(Schema.nodePropertyExistenceConstraints()).withNeo4j(neo4j)) {
      properties.forEach { prop ->
        this.run("CREATE CONSTRAINT ${name}_exists FOR (n:$label) REQUIRE n.$prop IS NOT NULL")
            .consume()
      }
    }
  }
}

fun Session.createRelationshipKeyConstraint(
    neo4j: Neo4j,
    name: String,
    type: String,
    vararg properties: String
) {
  if (canIUse(Schema.relationshipKeyConstraints()).withNeo4j(neo4j)) {
    this.run(
            "CREATE CONSTRAINT $name FOR ()-[r:$type]->() REQUIRE ${properties.joinToString(", ", "(", ")") { "r.$it" } } IS RELATIONSHIP KEY")
        .consume()
  } else {
    if (canIUse(Schema.relationshipPropertyUniquenessConstraints()).withNeo4j(neo4j)) {
      this.run(
              "CREATE CONSTRAINT $name FOR ()-[r:$type]->() REQUIRE ${
            properties.joinToString(
                ", ",
                "(",
                ")"
            ) { "r.$it"} 
          } IS UNIQUE")
          .consume()
    }

    if (canIUse(Schema.relationshipPropertyExistenceConstraints()).withNeo4j(neo4j)) {
      properties.forEach { prop ->
        this.run("CREATE CONSTRAINT $name FOR ()-[r:$type]->() REQUIRE r.$prop IS NOT NULL")
            .consume()
      }
    }
  }
}
