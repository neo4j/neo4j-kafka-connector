package org.neo4j.connectors.kafka.sink.strategy.cud

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import io.kotest.matchers.throwable.shouldHaveMessage
import org.junit.jupiter.api.Test
import org.neo4j.connectors.kafka.sink.strategy.InvalidDataException
import org.neo4j.driver.Query

class UpdateNodeTest {
  @Test
  fun `should create correct statement`() {
    val operation =
        UpdateNode(setOf("Person"), mapOf("name" to "john", "surname" to "doe"), mapOf("age" to 18))

    operation.toQuery() shouldBe
        Query(
            "MATCH (n:`Person` {name: ${'$'}keys.name, surname: ${'$'}keys.surname}) SET n += ${'$'}properties",
            mapOf(
                "keys" to mapOf("name" to "john", "surname" to "doe"),
                "properties" to mapOf("age" to 18)))
  }

  @Test
  fun `should create correct statement with multiple labels`() {
    val operation =
        UpdateNode(
            setOf("Person", "Employee"),
            mapOf("name" to "john", "surname" to "doe"),
            mapOf("age" to 18))

    operation.toQuery() shouldBe
        Query(
            "MATCH (n:`Person`:`Employee` {name: ${'$'}keys.name, surname: ${'$'}keys.surname}) SET n += ${'$'}properties",
            mapOf(
                "keys" to mapOf("name" to "john", "surname" to "doe"),
                "properties" to mapOf("age" to 18)))
  }

  @Test
  fun `should throw if no keys specified`() {
    val operation = UpdateNode(setOf("Person"), emptyMap(), mapOf("age" to 18))

    shouldThrow<InvalidDataException> { operation.toQuery() } shouldHaveMessage
        "Node must contain at least one ID property."
  }
}
