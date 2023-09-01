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
package org.neo4j.cdc.client

import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonMapperBuilder
import org.neo4j.cdc.client.model.ChangeEvent
import org.neo4j.cdc.client.model.ChangeIdentifier
import org.neo4j.cdc.client.model.Event
import org.neo4j.cdc.client.model.Metadata

/** @author Gerrit Meier */
class ResultMapper {

  companion object {
    private const val ID_FIELD = "id"
    private const val TX_ID_FIELD = "txId"
    private const val SEQ_FIELD = "seq"
    private const val METADATA_FIELD = "metadata"
    private const val EVENT_FIELD = "event"

    fun parseChangeIdentifier(message: Map<String, Any>): ChangeIdentifier {
      val changeIdentifierValue = message[ID_FIELD] as String
      return ChangeIdentifier(changeIdentifierValue)
    }

    fun parseChangeEvent(message: Map<String, Any>): ChangeEvent {
      val changeIdentifier = parseChangeIdentifier(message)
      val txId = message.get(TX_ID_FIELD) as Long
      val seq = (message.get(SEQ_FIELD) as Long).toInt()
      val objectMapper = jacksonMapperBuilder().addModules(JavaTimeModule()).build()
      val metadata = objectMapper.convertValue(message.get(METADATA_FIELD), Metadata::class.java)

      val event = objectMapper.convertValue(message.get(EVENT_FIELD), Event::class.java)

      return ChangeEvent(changeIdentifier, txId, seq, metadata, event)
    }
  }
}
