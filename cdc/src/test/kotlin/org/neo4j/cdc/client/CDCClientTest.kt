package org.neo4j.cdc.client

import org.junit.jupiter.api.Test

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Disabled
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.GraphDatabase
import reactor.test.StepVerifier

/**
 * Just some tests against a running instance with cdc enabled.
 * Since I consider moving this into some Java classes, this will be testcontainers-based.
 *
 * @author Gerrit Meier
 */
class CDCClientTest {

    val driver = GraphDatabase.driver("neo4j://localhost:7687", AuthTokens.basic("neo4j", "verysecret"))
    val client = CDCClient(driver)

    @Test
    fun earliest() {
      StepVerifier.create(client.earliest())
          .assertNext { cv ->
            assertNotNull(cv.id)
          }.verifyComplete()

    }

  @Disabled // because it needs to have exactly one valid change :)
  @Test
  fun query() {
    StepVerifier.create(client.query(ChangeIdentifier("AwAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAA")))
        .assertNext { ce ->
          println(ce)
        }.verifyComplete()
  }

  @Test
  fun someChanges() {
    StepVerifier.create(client.current())
        .assertNext { cv ->
         assertNotNull(cv.id)
        }.verifyComplete()
  }
}
