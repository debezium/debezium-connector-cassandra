/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.tracing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.cassandra.Emitter;
import io.debezium.connector.cassandra.Record;

/**
 * An {@link Emitter} decorator that wraps change event emission in OpenTelemetry tracing spans.
 * Creates two nested spans per record:
 * <ul>
 *   <li>"db-log-write" — represents the original Cassandra write, backdated to the mutation timestamp</li>
 *   <li>"debezium-read" — represents Debezium's processing, timestamped at emit time</li>
 * </ul>
 * The span gap between the two timestamps represents the CDC lag.
 *
 * <p>This decorator is only wired in when {@code tracing.enabled=true} is set in the connector config.
 * If the OpenTelemetry API is not on the classpath, tracing is silently skipped and the delegate
 * emitter is called directly.
 */
public class TracingEmitter implements Emitter {

    private static final Logger LOGGER = LoggerFactory.getLogger(TracingEmitter.class);

    private final Emitter delegate;

    public TracingEmitter(Emitter delegate) {
        this.delegate = delegate;
        if (!TracingUtils.isOpenTelemetryAvailable()) {
            LOGGER.warn("tracing.enabled=true but OpenTelemetry API is not on the classpath. Tracing will be skipped.");
        }
    }

    @Override
    public void emit(Record record) {
        if (TracingUtils.isOpenTelemetryAvailable()) {
            TracingUtils.traceEmit(record, () -> delegate.emit(record));
        }
        else {
            delegate.emit(record);
        }
    }

    @Override
    public void close() throws Exception {
        delegate.close();
    }
}
