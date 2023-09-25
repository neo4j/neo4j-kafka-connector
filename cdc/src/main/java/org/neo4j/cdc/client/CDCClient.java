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

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.neo4j.cdc.client.model.ChangeEvent;
import org.neo4j.cdc.client.model.ChangeIdentifier;
import org.neo4j.cdc.client.selector.Selector;
import org.neo4j.driver.Driver;
import org.neo4j.driver.reactive.RxResult;
import org.neo4j.driver.reactive.RxSession;
import org.neo4j.driver.types.MapAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author Gerrit Meier
 */
public class CDCClient implements CDCService {
    private final Logger log = LoggerFactory.getLogger(CDCClient.class);

    private static final String CDC_EARLIEST_STATEMENT = "call cdc.earliest()";
    private static final String CDC_CURRENT_STATEMENT = "call cdc.current()";
    private static final String CDC_QUERY_STATEMENT = "call cdc.query($from, $selectors)";
    private final Driver driver;
    private final List<Selector> selectors;
    private final Duration streamingPollInterval;

    public CDCClient(Driver driver, Selector... selectors) {
        this(driver, Duration.ofSeconds(1), selectors);
    }

    public CDCClient(Driver driver, Duration streamingPollInterval, Selector... selectors) {
        this.driver = Objects.requireNonNull(driver);
        this.streamingPollInterval = Objects.requireNonNull(streamingPollInterval);
        this.selectors = selectors == null ? List.of() : Arrays.asList(selectors);
    }

    @Override
    public Mono<ChangeIdentifier> earliest() {
        return queryForChangeIdentifier(CDC_EARLIEST_STATEMENT, "cdc.earliest");
    }

    @Override
    public Mono<ChangeIdentifier> current() {
        return queryForChangeIdentifier(CDC_CURRENT_STATEMENT, "cdc.current");
    }

    @Override
    public Flux<ChangeEvent> query(ChangeIdentifier from) {
        return Flux.usingWhen(
                        Mono.fromSupplier(driver::rxSession),
                        (RxSession session) -> Flux.from(session.readTransaction(tx -> {
                            var params = Map.of(
                                    "from",
                                    from.getId(),
                                    "selectors",
                                    selectors.stream().map(Selector::asMap).collect(Collectors.toList()));

                            log.trace("running cdc.query using parameters {}", params);
                            RxResult result = tx.run(CDC_QUERY_STATEMENT, params);

                            return Flux.from(result.records())
                                    .map(MapAccessor::asMap)
                                    .map(ResultMapper::parseChangeEvent);
                        })),
                        RxSession::close)
                .map(this::applyPropertyFilters)
                .doOnSubscribe(s -> log.trace("subscribed to cdc query"))
                .doOnComplete(() -> log.trace("subscription to cdc query completed"));
    }

    @Override
    public Flux<ChangeEvent> stream(ChangeIdentifier from) {
        var cursor = new AtomicReference<>(from);

        var query = Flux.usingWhen(
                Mono.fromSupplier(driver::rxSession),
                (RxSession session) -> Flux.from(session.readTransaction(tx -> {
                    var params = Map.of(
                            "from",
                            cursor.get().getId(),
                            "selectors",
                            selectors.stream().map(Selector::asMap).collect(Collectors.toList()));

                    log.trace("running cdc.query using parameters {}", params);
                    RxResult result = tx.run(CDC_QUERY_STATEMENT, params);

                    return Flux.from(result.records()).map(MapAccessor::asMap).map(ResultMapper::parseChangeEvent);
                })),
                RxSession::close);

        return Flux.concat(query, Mono.delay(streamingPollInterval).mapNotNull(x -> null))
                .map(this::applyPropertyFilters)
                .doOnNext(e -> cursor.set(e.getId()))
                .repeat()
                .doOnSubscribe(s -> log.trace("subscribed to cdc stream"))
                .doOnComplete(() -> log.trace("subscription to cdc stream completed"));
    }

    private ChangeEvent applyPropertyFilters(ChangeEvent original) {
        if (selectors.isEmpty()) {
            return original;
        }

        for (var selector : selectors) {
            if (selector.matches(original)) {
                return selector.applyProperties(original);
            }
        }

        return original;
    }

    private Mono<ChangeIdentifier> queryForChangeIdentifier(String query, String description) {
        return Mono.usingWhen(
                        Mono.fromSupplier(driver::rxSession),
                        (RxSession session) -> Mono.from(session.readTransaction(tx -> {
                            RxResult result = tx.run(query);
                            return Mono.from(result.records())
                                    .map(MapAccessor::asMap)
                                    .map(ResultMapper::parseChangeIdentifier);
                        })),
                        RxSession::close)
                .doOnSubscribe(s -> log.trace("subscribed to {}", description))
                .doOnSuccess(c -> log.trace("subscription to {} completed with '{}'", description, c))
                .doOnError(t -> log.atError().setCause(t).log("subscription to {} failed", description));
    }
}
