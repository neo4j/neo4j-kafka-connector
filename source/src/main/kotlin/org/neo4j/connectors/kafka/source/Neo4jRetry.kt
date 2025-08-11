/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.connectors.kafka.source

import kotlin.random.Random
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.toJavaDuration
import org.neo4j.driver.exceptions.DatabaseException
import org.neo4j.driver.internal.retry.ExponentialBackoffRetryLogic
import reactor.util.retry.RetrySpec

internal val Neo4jRetrySpec
  get() =
      RetrySpec.backoff(5, 100.milliseconds.toJavaDuration()).jitter(Random.nextDouble()).filter {
        // DatabaseException is added to handle a specific case where the database throws an error
        // while scanning logs for CDC events.
        ExponentialBackoffRetryLogic.isRetryable(it) || it is DatabaseException
      }
