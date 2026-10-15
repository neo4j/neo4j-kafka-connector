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
package org.neo4j.connectors.kafka.sink

import org.apache.kafka.connect.errors.ConnectException

/**
 * Raised in CDC strict mode when events in a batch found nothing to act on. It is deliberately not
 * a data exception, so the batch is never routed to the dead letter queue: the task fails and the
 * batch's transaction, including its exactly-once offset update, rolls back.
 */
class StrictModeViolationException(val topic: String, val partition: Int, val offsets: List<Long>) :
    ConnectException(
        "strict mode violation: events at offsets $offsets of topic '$topic' partition $partition " +
            "did not find the entities they target"
    )
