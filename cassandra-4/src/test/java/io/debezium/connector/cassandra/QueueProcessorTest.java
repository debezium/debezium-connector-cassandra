/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static com.datastax.oss.driver.api.core.type.DataTypes.INT;
import static com.datastax.oss.driver.api.core.type.DataTypes.TEXT;
import static io.debezium.connector.cassandra.CellData.ColumnType.CLUSTERING;
import static io.debezium.connector.cassandra.CellData.ColumnType.PARTITION;
import static io.debezium.connector.cassandra.CellData.ColumnType.REGULAR;
import static io.debezium.connector.cassandra.KeyValueSchema.getPrimaryKeySchemas;
import static io.debezium.connector.cassandra.Record.Operation.INSERT;
import static io.debezium.connector.cassandra.Record.Operation.RANGE_TOMBSTONE;
import static io.debezium.connector.cassandra.RowData.rowSchema;
import static io.debezium.connector.cassandra.TestUtils.TEST_KEYSPACE_NAME;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.nio.ByteBuffer;

import org.apache.cassandra.db.marshal.AbstractType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.cassandra.transforms.CassandraTypeDeserializer;
import io.debezium.connector.cassandra.transforms.DebeziumTypeDeserializer;
import io.debezium.time.Conversions;

public class QueueProcessorTest {
    private CassandraConnectorContext context;
    private QueueProcessor queueProcessor;
    private TestingKafkaRecordEmitter emitter;
    private KeyValueSchema keyValueSchema;
    private RowData rowData;
    private SourceInfo sourceInfo;

    @Before
    public void setUp() throws Exception {
        context = generateTaskContext(Configuration.from(TestUtils.generateDefaultConfigMap()));
        emitter = new TestingKafkaRecordEmitter(
                context.getCassandraConnectorConfig().kafkaTopicPrefix(),
                context.getCassandraConnectorConfig().getHeartbeatTopicsPrefix(),
                context.getKafkaProducer(),
                context.getOffsetWriter(),
                context.getCassandraConnectorConfig().offsetFlushIntervalMs(),
                context.getCassandraConnectorConfig().maxOffsetFlushSize(),
                context.getCassandraConnectorConfig().getKeyConverter(),
                context.getCassandraConnectorConfig().getValueConverter(),
                context.getErroneousCommitLogs(),
                context.getCassandraConnectorConfig().getCommitLogTransfer());

        queueProcessor = new QueueProcessor(context, 0, emitter);

        keyValueSchema = new KeyValueSchema.KeyValueSchemaBuilder()
                .withKeyspace(TEST_KEYSPACE_NAME)
                .withTable("cdc_table")
                .withKafkaTopicPrefix(context.getCassandraConnectorConfig().kafkaTopicPrefix())
                .withSourceInfoStructMarker(context.getCassandraConnectorConfig().getSourceInfoStructMaker())
                .withRowSchema(rowSchema(asList("col1", "col2"), asList(TEXT, INT)))
                .withPrimaryKeyNames(asList("p1", "c1"))
                .withPrimaryKeySchemas(getPrimaryKeySchemas(asList(INT, INT)))
                .build();

        rowData = new RowData();
        rowData.addCell(new CellData("p1", 1, null, PARTITION));
        rowData.addCell(new CellData("c1", 2, null, CLUSTERING));
        rowData.addCell(new CellData("col1", "col1value", null, REGULAR));
        rowData.addCell(new CellData("col2", 3, null, REGULAR));

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
    public void testInsertChangeRecordProcessing() throws Exception {
        ChangeEventQueue<Event> queue = context.getQueues().get(0);
        Record record = new ChangeRecord(sourceInfo, rowData, keyValueSchema.keySchema(),
                keyValueSchema.valueSchema(), INSERT, false);

        queue.enqueue(record);

        assertEquals(1, queue.totalCapacity() - queue.remainingCapacity());

        queueProcessor.process();

        assertEquals(1, emitter.records.size());
        assertEquals(queue.totalCapacity(), queue.remainingCapacity());
    }

    @Test
    public void testRangeTombstoneChangeRecordProcessing() throws Exception {
        ChangeEventQueue<Event> queue = context.getQueues().get(0);

        rowData.addStart("1");
        rowData.addEnd("2");

        Record record = new ChangeRecord(sourceInfo, rowData, keyValueSchema.keySchema(),
                keyValueSchema.valueSchema(), RANGE_TOMBSTONE, false);

        queue.enqueue(record);

        assertEquals(1, queue.totalCapacity() - queue.remainingCapacity());

        queueProcessor.process();

        assertEquals(1, emitter.records.size());
        assertEquals(queue.totalCapacity(), queue.remainingCapacity());
    }

    @Test
    public void testProcessTombstoneRecords() throws Exception {
        ChangeEventQueue<Event> queue = context.getQueues().get(0);
        Record record = new TombstoneRecord(sourceInfo, rowData, keyValueSchema.keySchema());

        queue.enqueue(record);

        assertEquals(1, queue.totalCapacity() - queue.remainingCapacity());

        queueProcessor.process();

        assertEquals(1, emitter.records.size());
        assertEquals(queue.totalCapacity(), queue.remainingCapacity());
    }

    @Test
    public void testProcessEofEvent() throws Exception {
        ChangeEventQueue<Event> queue = context.getQueues().get(0);
        File commitLogFile = TestUtils.generateCommitLogFile();
        queue.enqueue(new EOFEvent(commitLogFile));

        assertEquals(1, queue.totalCapacity() - queue.remainingCapacity());
        queueProcessor.process();
        assertEquals(0, emitter.records.size());
        assertEquals(queue.totalCapacity(), queue.remainingCapacity());
    }

    private CassandraConnectorContext generateTaskContext(Configuration configuration) throws Exception {

        CassandraTypeDeserializer.init(new DebeziumTypeDeserializer() {
            @Override
            public Object deserialize(AbstractType abstractType, ByteBuffer bb) {
                return abstractType.getSerializer().deserialize(bb);
            }
        });

        return new CassandraConnectorContext(new CassandraConnectorConfig(configuration));
    }
}
