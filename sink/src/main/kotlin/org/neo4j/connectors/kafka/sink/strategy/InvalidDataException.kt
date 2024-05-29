package org.neo4j.connectors.kafka.sink.strategy

import org.apache.kafka.connect.errors.ConnectException

class InvalidDataException : ConnectException {
  constructor(message: String?) : super(message)

  constructor(message: String?, cause: Throwable?) : super(message, cause)
}
