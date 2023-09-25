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
package org.neo4j.cdc.client;

import org.neo4j.cdc.client.model.ChangeEvent;
import org.neo4j.cdc.client.model.ChangeIdentifier;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author Gerrit Meier
 */
public interface CDCService {

    Mono<ChangeIdentifier> earliest();

    Mono<ChangeIdentifier> current();

    Flux<ChangeEvent> query(ChangeIdentifier from);

    Flux<ChangeEvent> stream(ChangeIdentifier from);
}
