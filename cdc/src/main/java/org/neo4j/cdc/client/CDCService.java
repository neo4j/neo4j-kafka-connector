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
}
