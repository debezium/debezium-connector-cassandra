/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static io.debezium.connector.cassandra.TestUtils.TEST_KEYSPACE_NAME;
import static io.debezium.connector.cassandra.TestUtils.TEST_TABLE_NAME;
import static io.debezium.connector.cassandra.TestUtils.TEST_TABLE_NAME_2;
import static io.debezium.connector.cassandra.TestUtils.deleteTestKeyspaceTables;
import static io.debezium.connector.cassandra.TestUtils.deleteTestOffsets;
import static io.debezium.connector.cassandra.TestUtils.runCql;
import static java.lang.String.format;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.File;
import java.time.Duration;
import java.util.List;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLogReader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.util.Testing;

public abstract class AbstractCommitLogProcessorTest extends EmbeddedCassandra4ConnectorTestBase {
    public ChangeEventQueue<Event> queue;
    public CassandraConnectorContext context;
    public Cassandra4CommitLogProcessor commitLogProcessor;

    @Before
    public void setUp() throws Exception {
        initialiseData();
        context = generateTaskContext();
        await().atMost(Duration.ofSeconds(60)).until(() -> context.getSchemaHolder()
                .getKeyValueSchema(new KeyspaceTable(TEST_KEYSPACE_NAME, TEST_TABLE_NAME)) != null);
        commitLogProcessor = new Cassandra4CommitLogProcessor(context);
        commitLogProcessor.initialize();
        queue = context.getQueues().get(0);
        readLogs();
    }

    @After
    public void tearDown() throws Exception {
        deleteTestOffsets(context);
        commitLogProcessor.destroy();
        deleteTestKeyspaceTables();
        context.cleanUp();
        Testing.Files.delete(DatabaseDescriptor.getCDCLogLocation());
    }

    @Test
    public void test() throws Exception {
        verifyEvents();
    }

    public abstract void initialiseData() throws Exception;

    public abstract void verifyEvents() throws Exception;

    public void createTable(String query) throws Exception {
        createTable(query, TEST_KEYSPACE_NAME, TEST_TABLE_NAME);
    }

    public void createTable2(String query) throws Exception {
        createTable(query, TEST_KEYSPACE_NAME, TEST_TABLE_NAME_2);
    }

    public void createTable(String query, String keyspace, String tableName) throws Exception {
        runCql(format(query, keyspace, tableName));
        Thread.sleep(5000);
    }

    public List<Event> getEvents() throws Exception {
        List<Event> events = queue.poll();
        assertFalse(events.isEmpty());
        return events;
    }

    public void readLogs() throws Exception {
        // check to make sure there are no records in the queue to begin with
        assertEquals(queue.totalCapacity(), queue.remainingCapacity());

        // process the logs in commit log directory
        File cdcLoc = new File(DatabaseDescriptor.getCommitLogLocation());
        File[] commitLogs = CommitLogUtil.getCommitLogs(cdcLoc);

        Cassandra4CommitLogReadHandlerImpl commitLogReadHandler = new Cassandra4CommitLogReadHandlerImpl(
                context.getSchemaHolder(),
                context.getQueues(),
                context.getOffsetWriter(),
                new RecordMaker(context.getCassandraConnectorConfig().tombstonesOnDelete(),
                        new Filters(context.getCassandraConnectorConfig().fieldExcludeList()),
                        context.getCassandraConnectorConfig()),
                new CommitLogProcessorMetrics());

        CommitLogReader reader = new CommitLogReader();

        for (File commitLog : commitLogs) {
            reader.readCommitLogSegment(commitLogReadHandler, commitLog, true);
        }
    }
}
