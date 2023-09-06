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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.neo4j.cdc.client.model.ChangeEvent;
import org.neo4j.cdc.client.model.ChangeIdentifier;
import org.neo4j.cdc.client.selector.Selector;
import org.neo4j.driver.Driver;
import org.neo4j.driver.reactive.RxResult;
import org.neo4j.driver.reactive.RxSession;
import org.neo4j.driver.types.MapAccessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author Gerrit Meier
 */
public class CDCClient implements CDCService {

    private static final String CDC_EARLIEST_STATEMENT = "call cdc.earliest()";
    private static final String CDC_CURRENT_STATEMENT = "call cdc.current()";
    private static final String CDC_QUERY_STATEMENT = "call cdc.query($from, $selectors)";
    private final Driver driver;
    private final List<Selector> selectors;

    public CDCClient(Driver driver, Selector... selectors) {
        this.driver = driver;
        this.selectors = selectors == null ? List.of() : Arrays.asList(selectors);
    }

    @Override
    public Mono<ChangeIdentifier> earliest() {
        return queryForChangeIdentifier(CDC_EARLIEST_STATEMENT);
    }

    @Override
    public Mono<ChangeIdentifier> current() {
        return queryForChangeIdentifier(CDC_CURRENT_STATEMENT);
    }

    @Override
    public Flux<ChangeEvent> query(ChangeIdentifier from) {
        return Flux.usingWhen(
                Mono.fromSupplier(driver::rxSession),
                (RxSession session) -> Flux.from(session.readTransaction(tx -> {
                    RxResult result = tx.run(
                            CDC_QUERY_STATEMENT,
                            Map.of(
                                    "from",
                                    from.getId(),
                                    "selectors",
                                    selectors.stream().map(Selector::asMap).collect(Collectors.toList())));

                    return Flux.from(result.records()).map(MapAccessor::asMap).map(ResultMapper::parseChangeEvent);
                })),
                RxSession::close);
    }

    private Mono<ChangeIdentifier> queryForChangeIdentifier(String query) {
        return Mono.usingWhen(
                Mono.fromSupplier(driver::rxSession),
                (RxSession session) -> Mono.from(session.readTransaction(tx -> {
                    RxResult result = tx.run(query);
                    return Mono.from(result.records()).map(MapAccessor::asMap).map(ResultMapper::parseChangeIdentifier);
                })),
                RxSession::close);
    }
}
