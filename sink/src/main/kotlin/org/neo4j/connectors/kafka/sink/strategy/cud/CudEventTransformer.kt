package org.neo4j.connectors.kafka.sink.strategy.cud

import org.apache.kafka.connect.errors.ConnectException
import org.neo4j.connectors.kafka.sink.SinkMessage
import org.neo4j.connectors.kafka.sink.strategy.SinkAction
import org.neo4j.connectors.kafka.sink.strategy.SinkEventTransformer

@Suppress("UNCHECKED_CAST")
class CudEventTransformer : SinkEventTransformer {
  override fun transform(message: SinkMessage): SinkAction {
    val value =
        when (val value = message.valueFromConnectValue()) {
          is Map<*, *> -> value as Map<String, Any?>
          else -> throw ConnectException("Message value must be convertible to a Map.")
        }
    val cud = Operation.from(value)
    return cud.toAction()
  }
}
