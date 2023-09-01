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

import org.neo4j.driver.Driver
import org.neo4j.driver.reactive.RxResult
import org.neo4j.driver.reactive.RxSession
import org.reactivestreams.Publisher
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

class CDCClient(private val driver: Driver, vararg selectors: Selector) : CDCService {
  override fun earliest(): Mono<ChangeIdentifier> {
    val query = "call cdc.earliest();"
    return queryForChangeIdentifier(query)
  }

  override fun current(): Mono<ChangeIdentifier> {
    val query = "call cdc.current();"
    return queryForChangeIdentifier(query)
  }

  private val closingFn: (RxSession) -> Publisher<RxSession>
    get() {
      val closingFn: (RxSession) -> Publisher<RxSession> = { session: RxSession -> session.close() }
      return closingFn
    }

  override fun query(from: ChangeIdentifier): Flux<ChangeEvent> {
    val query = "call cdc.query('${from.id}');"
    return Flux.usingWhen(
        Mono.fromSupplier { driver.rxSession() },
        { s: RxSession ->
          Flux.from(
              s.readTransaction { tx ->
                val result: RxResult = tx.run(query)
                Flux.from(result.records())
                    .map { records -> records.asMap() }
                    .map { map -> ResultMapper.parseChangeEvent(map) }
              },
          )
        },
        closingFn,
    )
  }

  private fun queryForChangeIdentifier(query: String): Mono<ChangeIdentifier> {
    return Mono.usingWhen(
        Mono.fromSupplier { driver.rxSession() },
        { s: RxSession ->
          Mono.from(
              s.readTransaction { tx ->
                val result: RxResult = tx.run(query)
                Mono.from(result.records())
                    .map { records -> records.asMap() }
                    .map { map -> ResultMapper.parseChangeIdentifier(map) }
              },
          )
        },
        closingFn,
    )
  }
}
