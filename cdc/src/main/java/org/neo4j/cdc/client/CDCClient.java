package org.neo4j.cdc.client;

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

	private static final String CDC_EARLIEST_STATEMENT = "call cdc.earliest();";
	private static final String CDC_CURRENT_STATEMENT = "call cdc.current();";
	private static final String CDC_QUERY_STATEMENT = "call cdc.query('%s');"; // todo check injections concerns
	private final Driver driver;
	private final Selector selector;

	public CDCClient(Driver driver, Selector selector) {
		this.driver = driver;
		this.selector = selector;
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
		String query = String.format(CDC_QUERY_STATEMENT, from.getId());

		return Flux.usingWhen(
			Mono.fromSupplier(driver::rxSession),
			(RxSession session) ->
				Flux.from(
					session.readTransaction(tx -> {
						RxResult result = tx.run(query);
						return Flux.from(result.records())
							.map(MapAccessor::asMap)
							.map(ResultMapper::parseChangeEvent);
					})),
			RxSession::close
		);
	}

	private Mono<ChangeIdentifier> queryForChangeIdentifier(String query) {
		return Mono.usingWhen(
			Mono.fromSupplier(driver::rxSession),
			(RxSession session) ->
				Mono.from(
					session.readTransaction(tx -> {
						RxResult result = tx.run(query);
						return Mono.from(result.records())
							.map(MapAccessor::asMap)
							.map(ResultMapper::parseChangeIdentifier);
					})),
			RxSession::close
		);
	}
}
