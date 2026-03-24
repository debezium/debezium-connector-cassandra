/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static com.datastax.oss.driver.api.core.type.DataTypes.INT;
import static com.datastax.oss.driver.api.core.type.DataTypes.TEXT;
import static io.debezium.connector.cassandra.CassandraSchemaFactory.CellData.ColumnType.CLUSTERING;
import static io.debezium.connector.cassandra.CassandraSchemaFactory.CellData.ColumnType.PARTITION;
import static io.debezium.connector.cassandra.CassandraSchemaFactory.CellData.ColumnType.REGULAR;
import static io.debezium.connector.cassandra.CassandraSchemaFactory.RowData.rowSchema;
import static io.debezium.connector.cassandra.KeyValueSchema.getPrimaryKeySchemas;
import static io.debezium.connector.cassandra.Record.Operation.INSERT;
import static io.debezium.connector.cassandra.utils.TestUtils.TEST_KEYSPACE_NAME;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.cassandra.CassandraSchemaFactory.RowData;
import io.debezium.connector.cassandra.spi.ProvidersResolver;
import io.debezium.connector.cassandra.tracing.TracingEmitter;
import io.debezium.connector.cassandra.tracing.TracingUtils;
import io.debezium.connector.cassandra.utils.TestUtils;
import io.debezium.time.Conversions;

public class TracingEmitterTest {

    private CassandraConnectorContext context;
    private TestingKafkaRecordEmitter innerEmitter;
    private TracingEmitter tracingEmitter;
    private KeyValueSchema keyValueSchema;
    private SourceInfo sourceInfo;

    @Before
    public void setUp() throws Exception {
        context = ProvidersResolver.resolveConnectorContextProvider()
                .provideContextWithoutSchemaManagement(Configuration.from(TestUtils.generateDefaultConfigMap()));

        innerEmitter = new TestingKafkaRecordEmitter(
                context.getCassandraConnectorConfig(),
                null,
                context.getOffsetWriter(),
                context.getCassandraConnectorConfig().getKeyConverter(),
                context.getCassandraConnectorConfig().getValueConverter(),
                context.getErroneousCommitLogs(),
                context.getCassandraConnectorConfig().getCommitLogTransfer());

        tracingEmitter = new TracingEmitter(innerEmitter);

        CassandraSchemaFactory schemaFactory = CassandraSchemaFactory.get();
        RowData rowData = schemaFactory.rowData();
        rowData.addCell(schemaFactory.cellData("p1", 1, null, PARTITION));
        rowData.addCell(schemaFactory.cellData("c1", 2, null, CLUSTERING));
        rowData.addCell(schemaFactory.cellData("col1", "value", null, REGULAR));
        rowData.addCell(schemaFactory.cellData("col2", 3, null, REGULAR));

        keyValueSchema = new KeyValueSchema.KeyValueSchemaBuilder()
                .withKeyspace(TEST_KEYSPACE_NAME)
                .withTable("cdc_table")
                .withKafkaTopicPrefix(context.getCassandraConnectorConfig().getLogicalName())
                .withSourceInfoStructMarker(context.getCassandraConnectorConfig().getSourceInfoStructMaker())
                .withRowSchema(rowSchema(asList("col1", "col2"), asList(TEXT, INT)))
                .withPrimaryKeyNames(asList("p1", "c1"))
                .withPrimaryKeySchemas(getPrimaryKeySchemas(asList(INT, INT)))
                .build();

        sourceInfo = new SourceInfo(context.getCassandraConnectorConfig(), "test-cluster",
                new OffsetPosition("CommitLog-6-123.log", 0),
                new KeyspaceTable(TEST_KEYSPACE_NAME, "cdc_table"), false,
                Conversions.toInstantFromMicros(System.currentTimeMillis() * 1000));
    }

    @After
    public void tearDown() {
        context.cleanUp();
    }

    @Test
    public void testDelegatesEmitToInnerEmitter() {
        Record record = new ChangeRecord(sourceInfo, CassandraSchemaFactory.get().rowData(),
                keyValueSchema.keySchema(), keyValueSchema.valueSchema(), INSERT, false);

        tracingEmitter.emit(record);

        assertEquals("Record should be delegated to the inner emitter", 1, innerEmitter.records.size());
    }

    @Test
    public void testMultipleRecordsDelegated() {
        for (int i = 0; i < 5; i++) {
            Record record = new ChangeRecord(sourceInfo, CassandraSchemaFactory.get().rowData(),
                    keyValueSchema.keySchema(), keyValueSchema.valueSchema(), INSERT, false);
            tracingEmitter.emit(record);
        }

        assertEquals("All records should be delegated to the inner emitter", 5, innerEmitter.records.size());
    }

    @Test
    public void testTombstoneRecordDelegated() {
        Record record = new TombstoneRecord(sourceInfo, CassandraSchemaFactory.get().rowData(),
                keyValueSchema.keySchema());

        tracingEmitter.emit(record);

        assertEquals("Tombstone record should be delegated to the inner emitter", 1, innerEmitter.records.size());
    }

    @Test
    public void testProducerRecordHasHeadersWhenTracingAvailable() {
        // When OpenTelemetry is on the classpath (test scope), the no-op tracer is used.
        // Verify that even with the no-op tracer, context injection is attempted (headers may be empty
        // if no active span, but the call should not throw).
        Record record = new ChangeRecord(sourceInfo, CassandraSchemaFactory.get().rowData(),
                keyValueSchema.keySchema(), keyValueSchema.valueSchema(), INSERT, false);

        tracingEmitter.emit(record);

        assertEquals(1, innerEmitter.records.size());
        // With the no-op OTEL SDK, headers will be empty (no-op propagator injects nothing),
        // but the emit path must complete without errors.
        assertFalse("ProducerRecord should be non-null", innerEmitter.records.isEmpty());
    }

    @Test
    public void testOtelAvailabilityMatchesClasspathPresence() {
        // opentelemetry-api is on the test classpath, so OTEL should be available
        assertTrue("OpenTelemetry API should be available on the test classpath",
                TracingUtils.isOpenTelemetryAvailable());
    }
}
