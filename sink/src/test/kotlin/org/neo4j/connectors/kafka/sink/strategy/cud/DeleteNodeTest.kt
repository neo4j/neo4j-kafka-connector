package org.neo4j.connectors.kafka.sink.strategy.cud

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import io.kotest.matchers.throwable.shouldHaveMessage
import org.junit.jupiter.api.Test
import org.neo4j.connectors.kafka.sink.strategy.InvalidDataException
import org.neo4j.driver.Query

class DeleteNodeTest {

  @Test
  fun `should create correct statement`() {
    val operation = DeleteNode(setOf("Person"), mapOf("name" to "john", "surname" to "doe"), false)

    operation.toQuery() shouldBe
        Query(
            "MATCH (n:`Person` {name: ${'$'}keys.name, surname: ${'$'}keys.surname}) DELETE n",
            mapOf("keys" to mapOf("name" to "john", "surname" to "doe")))
  }

  @Test
  fun `should create correct statement with detach delete`() {
    val operation = DeleteNode(setOf("Person"), mapOf("name" to "john", "surname" to "doe"), true)

    operation.toQuery() shouldBe
        Query(
            "MATCH (n:`Person` {name: ${'$'}keys.name, surname: ${'$'}keys.surname}) DETACH DELETE n",
            mapOf("keys" to mapOf("name" to "john", "surname" to "doe")))
  }

  @Test
  fun `should create correct statement with multiple labels`() {
    val operation =
        DeleteNode(setOf("Person", "Employee"), mapOf("name" to "john", "surname" to "doe"), true)

    operation.toQuery() shouldBe
        Query(
            "MATCH (n:`Person`:`Employee` {name: ${'$'}keys.name, surname: ${'$'}keys.surname}) DETACH DELETE n",
            mapOf("keys" to mapOf("name" to "john", "surname" to "doe")))
  }

  @Test
  fun `should throw if no keys specified`() {
    val operation = DeleteNode(setOf("Person"), emptyMap())

    shouldThrow<InvalidDataException> { operation.toQuery() } shouldHaveMessage
        "Node must contain at least one ID property."
  }
}
