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

import java.time.Duration
import java.util.Locale
import org.neo4j.driver.Driver
import org.neo4j.driver.Session
import org.neo4j.driver.SessionConfig
import org.neo4j.driver.exceptions.Neo4jException
import org.slf4j.LoggerFactory

object DatabaseSupport {
  private val logger = LoggerFactory.getLogger(DatabaseSupport::class.java)
  private val DEFAULT_TIMEOUT = Duration.ofMinutes(1)
  internal const val DEFAULT_DATABASE = "neo4j"

  fun Driver.createDatabase(
      db: String,
      withCdc: Boolean = false,
      timeout: Duration = DEFAULT_TIMEOUT,
  ) {
    this.session(SessionConfig.forDatabase("system")).use { session ->
      session.createDatabase(db, withCdc)
    }

    this.waitForDatabaseToBeOnline(db, timeout)
  }

  fun Driver.dropDatabase(db: String) {
    this.session(SessionConfig.forDatabase("system")).use { session -> session.dropDatabase(db) }
  }

  private fun Session.createDatabase(database: String, withCdc: Boolean = false): Session {
    if (withCdc) {
      this.run(
              "CREATE OR REPLACE DATABASE \$db_name OPTIONS { txLogEnrichment: 'FULL' } WAIT 30 SECONDS",
              mapOf("db_name" to database),
          )
          .consume()
    } else {
      this.run("CREATE OR REPLACE DATABASE \$db_name WAIT 30 SECONDS", mapOf("db_name" to database))
          .consume()
    }

    return this
  }

  private fun Driver.waitForDatabaseToBeOnline(
      database: String,
      timeout: Duration = DEFAULT_TIMEOUT,
  ) {
    val deadline = System.currentTimeMillis() + timeout.toMillis()

    tailrec fun poll(): Boolean {
      if (System.currentTimeMillis() >= deadline) return false

      try {
        val status =
            this.session(SessionConfig.forDatabase("system")).use { session ->
              session
                  .run(
                      "SHOW DATABASES YIELD name, currentStatus WHERE name = \$db_name RETURN currentStatus",
                      mapOf("db_name" to database),
                  )
                  .single()
                  ?.get("currentStatus")
                  ?.asString()
                  ?.lowercase(Locale.ROOT)
            }

        if (status == "online") return true
      } catch (e: Neo4jException) {
        // There is a known concurrency issue that sometimes causes this statement to fail. Let's
        // assume that the database is not online yet and try again after a short delay.
        logger.warn("got an error while checking database status, will retry...", e)
      }

      Thread.sleep(1000)
      return poll()
    }

    require(poll()) { "Database $database did not become online within ${timeout.toMillis()} ms" }
  }

  private fun Session.dropDatabase(database: String): Session {
    if (database == DEFAULT_DATABASE) {
      return this
    }
    this.run("DROP DATABASE \$db_name WAIT 30 SECONDS", mapOf("db_name" to database)).consume()
    return this
  }
}
