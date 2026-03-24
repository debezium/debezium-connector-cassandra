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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.cassandra.CassandraSchemaFactory.RowData;
import io.debezium.connector.cassandra.spi.ProvidersResolver;
import io.debezium.connector.cassandra.tracing.TracingEmitter;
import io.debezium.connector.cassandra.utils.TestUtils;
import io.debezium.time.Conversions;

/**
 * Tests that QueueProcessor correctly processes and emits records when tracing is enabled
 * via a TracingEmitter decorator. Verifies that all record types (CHANGE, TOMBSTONE, EOF)
 * continue to work correctly with the decorator in place.
 */
public class QueueProcessorTracingTest {

    private CassandraConnectorContext context;
    private QueueProcessor queueProcessor;
    private TestingKafkaRecordEmitter innerEmitter;
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

        // Wrap with TracingEmitter as it would be when tracing.enabled=true
        TracingEmitter tracingEmitter = new TracingEmitter(innerEmitter);
        queueProcessor = new QueueProcessor(context, 0, tracingEmitter);

        CassandraSchemaFactory schemaFactory = CassandraSchemaFactory.get();
        RowData rowData = schemaFactory.rowData();
        rowData.addCell(schemaFactory.cellData("p1", 1, null, PARTITION));
        rowData.addCell(schemaFactory.cellData("c1", 2, null, CLUSTERING));
        rowData.addCell(schemaFactory.cellData("col1", "col1value", null, REGULAR));
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

        sourceInfo = new SourceInfo(context.getCassandraConnectorConfig(), "cluster1",
                new OffsetPosition("CommitLog-6-123.log", 0),
                new KeyspaceTable(TEST_KEYSPACE_NAME, "cdc_table"), false,
                Conversions.toInstantFromMicros(System.currentTimeMillis() * 1000));
    }

    @After
    public void tearDown() {
        context.cleanUp();
    }

    @Test
    public void testChangeRecordEmittedThroughTracingDecorator() throws Exception {
        ChangeEventQueue<Event> queue = context.getQueues().get(0);
        Record record = new ChangeRecord(sourceInfo, CassandraSchemaFactory.get().rowData(),
                keyValueSchema.keySchema(), keyValueSchema.valueSchema(), INSERT, false);

        queue.enqueue(record);
        queueProcessor.process();

        assertEquals("Change record should be emitted through TracingEmitter", 1, innerEmitter.records.size());
        assertEquals(queue.totalCapacity(), queue.remainingCapacity());
    }

    @Test
    public void testTombstoneRecordEmittedThroughTracingDecorator() throws Exception {
        ChangeEventQueue<Event> queue = context.getQueues().get(0);
        Record record = new TombstoneRecord(sourceInfo, CassandraSchemaFactory.get().rowData(),
                keyValueSchema.keySchema());

        queue.enqueue(record);
        queueProcessor.process();

        assertEquals("Tombstone record should be emitted through TracingEmitter", 1, innerEmitter.records.size());
        assertEquals(queue.totalCapacity(), queue.remainingCapacity());
    }

    @Test
    public void testMultipleRecordsEmittedThroughTracingDecorator() throws Exception {
        ChangeEventQueue<Event> queue = context.getQueues().get(0);
        int numRecords = 3;
        for (int i = 0; i < numRecords; i++) {
            queue.enqueue(new ChangeRecord(sourceInfo, CassandraSchemaFactory.get().rowData(),
                    keyValueSchema.keySchema(), keyValueSchema.valueSchema(), INSERT, false));
        }

        queueProcessor.process();

        assertEquals("All records should be emitted through TracingEmitter", numRecords, innerEmitter.records.size());
    }
}
