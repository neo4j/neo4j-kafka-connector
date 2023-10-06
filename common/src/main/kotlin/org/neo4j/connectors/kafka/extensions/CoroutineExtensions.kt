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
package org.neo4j.connectors.kafka.extensions

import java.util.concurrent.CopyOnWriteArraySet
import java.util.concurrent.TimeoutException
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.channels.ticker
import kotlinx.coroutines.selects.whileSelect

// taken from
// https://stackoverflow.com/questions/52192752/kotlin-how-to-run-n-coroutines-and-wait-for-first-m-results-or-timeout
@ObsoleteCoroutinesApi
@ExperimentalCoroutinesApi
suspend fun <T> List<Deferred<T>>.awaitAll(timeoutMs: Long): List<T> {
  val jobs = CopyOnWriteArraySet<Deferred<T>>(this)
  val result = ArrayList<T>(size)
  val timeout = ticker(timeoutMs)

  whileSelect {
    jobs.forEach { deferred ->
      deferred.onAwait {
        jobs.remove(deferred)
        result.add(it)
        result.size != size
      }
    }

    timeout.onReceive {
      jobs.forEach { it.cancel() }
      throw TimeoutException("Tasks $size cancelled after timeout of $timeoutMs ms.")
    }
  }

  return result
}

@ExperimentalCoroutinesApi
fun <T> Deferred<T>.errors() =
    when {
      isCompleted -> getCompletionExceptionOrNull()
      isCancelled -> getCompletionExceptionOrNull() // was getCancellationException()
      isActive -> RuntimeException("Job $this still active")
      else -> null
    }
