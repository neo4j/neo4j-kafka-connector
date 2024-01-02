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

package org.neo4j.connectors.kafka.testing

import org.neo4j.driver.Session

object DatabaseSupport {

  internal const val DEFAULT_DATABASE = "neo4j"

  internal fun Session.createDatabase(database: String): Session {
    if (database == DEFAULT_DATABASE) {
      return this
    }
    this.run("CREATE DATABASE \$db_name", mapOf("db_name" to database))
    return this
  }

  internal fun Session.dropDatabase(database: String): Session {
    if (database == DEFAULT_DATABASE) {
      return this
    }
    this.run("DROP DATABASE \$db_name", mapOf("db_name" to database))
    return this
  }

  internal fun Session.enableCdc(database: String = "neo4j"): Session {
    this.run(
        "ALTER DATABASE \$db_name SET OPTION txLogEnrichment \"FULL\"",
        mapOf("db_name" to database))
    return this
  }
}
