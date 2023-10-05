package org.neo4j.connectors.kafka.source.testing

import org.junit.jupiter.api.extension.ExtendWith
import streams.kafka.connect.source.StreamingFrom

@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
@ExtendWith(Neo4jSourceExtension::class)
annotation class Neo4jSource(
  val schemaControlRegistryUri: String,
  val kafkaConnectUri: String,
  val topic: String,
  val neo4jUri: String,
  val neo4jUser: String = "neo4j",
  val neo4jPassword: String,
  val streamingProperty: String,
  val streamingFrom: StreamingFrom,
  val streamingQuery: String,
)
