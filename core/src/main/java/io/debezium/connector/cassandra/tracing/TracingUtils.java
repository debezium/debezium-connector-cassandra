/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.tracing;

import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.header.Headers;

import io.debezium.connector.cassandra.Record;
import io.debezium.connector.cassandra.SourceInfo;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;

public final class TracingUtils {

    private static final String TRACING_COMPONENT = "debezium";
    private static final String DB_LOG_WRITE = "db-log-write";
    private static final String DEBEZIUM_READ = "debezium-read";
    private static final String DB_PREFIX = "db.";

    private static final boolean OPEN_TELEMETRY_AVAILABLE = resolveOpenTelemetryApiAvailable();
    // Only initialized when OTEL is available; null otherwise — avoids per-call GlobalOpenTelemetry.get()
    private static final OpenTelemetry OPEN_TELEMETRY = OPEN_TELEMETRY_AVAILABLE ? GlobalOpenTelemetry.get() : null;
    private static final Tracer TRACER = OPEN_TELEMETRY_AVAILABLE ? OPEN_TELEMETRY.getTracer(TRACING_COMPONENT) : null;

    private TracingUtils() {
    }

    public static boolean isOpenTelemetryAvailable() {
        return OPEN_TELEMETRY_AVAILABLE;
    }

    /**
     * Creates two nested spans around the emit action:
     * - "db-log-write" (backdated to the Cassandra mutation timestamp)
     * - "debezium-read" as a child (timestamped at Debezium processing time)
     * After the emit action, the current OTEL context is injected into the Kafka ProducerRecord
     * headers via {@link KafkaProducerHeadersSetter} so downstream consumers can continue the trace.
     */
    public static void traceEmit(Record record, Runnable emitAction) {
        SourceInfo source = record.getSource();
        long tsMillis = source.tsMicro != null ? source.tsMicro.toEpochMilli() : 0L;

        SpanBuilder dbLogSpanBuilder = TRACER.spanBuilder(DB_LOG_WRITE)
                .setSpanKind(SpanKind.INTERNAL)
                .setStartTimestamp(tsMillis, TimeUnit.MILLISECONDS);

        Span dbLogSpan = dbLogSpanBuilder.startSpan();
        try (Scope ignored = dbLogSpan.makeCurrent()) {
            dbLogSpan.setAttribute(DB_PREFIX + "instance", source.keyspaceTable.keyspace);
            dbLogSpan.setAttribute(DB_PREFIX + "type", source.connector);
            dbLogSpan.setAttribute(DB_PREFIX + "cdc-name", source.cluster);
            dbLogSpan.setAttribute(DB_PREFIX + "table", source.keyspaceTable.table);
            dbLogSpan.setAttribute(DB_PREFIX + "snapshot", String.valueOf(source.snapshot));
            dbLogSpan.setAttribute(DB_PREFIX + "file", source.offsetPosition.fileName);
            dbLogSpan.setAttribute(DB_PREFIX + "pos", String.valueOf(source.offsetPosition.filePosition));
            dbLogSpan.setAttribute(DB_PREFIX + "version", source.version);

            Span debeziumSpan = TRACER.spanBuilder(DEBEZIUM_READ)
                    .setStartTimestamp(record.getTs(), TimeUnit.MILLISECONDS)
                    .startSpan();
            try (Scope ignored2 = debeziumSpan.makeCurrent()) {
                debeziumSpan.setAttribute("op", record.getOp().getValue());
                debeziumSpan.setAttribute("ts_ms", String.valueOf(record.getTs()));

                // KafkaRecordEmitter.toProducerRecord() calls TracingUtils.injectProducerHeaders()
                // while this span is current, so the active context is propagated into record headers.
                emitAction.run();
            }
            finally {
                debeziumSpan.end();
            }
        }
        finally {
            dbLogSpan.end();
        }
    }

    /**
     * Injects the current OTEL context into Kafka producer record headers.
     * Must be called from within a span scope (e.g. inside traceEmit).
     * No-op if OTEL is not available.
     */
    public static void injectProducerHeaders(Headers headers) {
        if (!OPEN_TELEMETRY_AVAILABLE) {
            return;
        }
        OPEN_TELEMETRY.getPropagators().getTextMapPropagator()
                .inject(Context.current(), headers, KafkaProducerHeadersSetter.INSTANCE);
    }

    private static boolean resolveOpenTelemetryApiAvailable() {
        try {
            GlobalOpenTelemetry.get();
            return true;
        }
        catch (NoClassDefFoundError e) {
            // ignored
        }
        return false;
    }
}
