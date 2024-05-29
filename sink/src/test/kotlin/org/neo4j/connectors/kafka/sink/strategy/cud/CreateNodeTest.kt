package org.neo4j.connectors.kafka.sink.strategy.cud

import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test
import org.neo4j.driver.Query

class CreateNodeTest {

  @Test
  fun `should create correct statement`() {
    val operation =
        CreateNode(setOf("Person"), mapOf("id" to 1, "name" to "john", "surname" to "doe"))

    operation.toQuery() shouldBe
        Query(
            "CREATE (n:`Person` {}) SET n = ${'$'}properties",
            mapOf("properties" to mapOf("id" to 1, "name" to "john", "surname" to "doe")))
  }

  @Test
  fun `should create correct statement with multiple labels`() {
    val operation =
        CreateNode(
            setOf("Person", "Employee"), mapOf("id" to 1, "name" to "john", "surname" to "doe"))

    operation.toQuery() shouldBe
        Query(
            "CREATE (n:`Person`:`Employee` {}) SET n = ${'$'}properties",
            mapOf("properties" to mapOf("id" to 1, "name" to "john", "surname" to "doe")))
  }

  @Test
  fun `should create correct statement without labels`() {
    val operation = CreateNode(emptySet(), mapOf("id" to 1, "name" to "john", "surname" to "doe"))

    operation.toQuery() shouldBe
        Query(
            "CREATE (n {}) SET n = ${'$'}properties",
            mapOf("properties" to mapOf("id" to 1, "name" to "john", "surname" to "doe")))
  }
}
