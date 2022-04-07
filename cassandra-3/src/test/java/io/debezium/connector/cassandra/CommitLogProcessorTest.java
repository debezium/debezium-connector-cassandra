/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static io.debezium.connector.cassandra.TestUtils.TEST_KEYSPACE_NAME;
import static io.debezium.connector.cassandra.TestUtils.deleteTestKeyspaceTables;
import static io.debezium.connector.cassandra.TestUtils.deleteTestOffsets;
import static io.debezium.connector.cassandra.TestUtils.keyspaceTable;
import static io.debezium.connector.cassandra.TestUtils.runCql;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.File;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.connector.base.ChangeEventQueue;

public class CommitLogProcessorTest extends EmbeddedCassandra3ConnectorTestBase {
    private CassandraConnectorContext context;
    private Cassandra3CommitLogProcessor commitLogProcessor;

    @Before
    public void setUp() throws Exception {
        context = generateTaskContext();
        commitLogProcessor = new Cassandra3CommitLogProcessor(context);
        commitLogProcessor.initialize();
    }

    @After
    public void tearDown() throws Exception {
        deleteTestOffsets(context);
        commitLogProcessor.destroy();
        deleteTestKeyspaceTables();
        context.cleanUp();
    }

    @Test
    public void testProcessCommitLogs() throws Exception {
        int commitLogRowSize = 10;
        Thread.sleep(10000);
        runCql("CREATE TABLE IF NOT EXISTS " + keyspaceTable("cdc_table") + " (a int, b int, PRIMARY KEY(a)) WITH cdc = true;");

        Awaitility.await().forever().until(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return context.getSchemaHolder().getKeyValueSchema(new KeyspaceTable(TEST_KEYSPACE_NAME, "cdc_table")) != null;
            }
        });

        for (int i = 0; i < commitLogRowSize; i++) {
            runCql("INSERT INTO " + keyspaceTable("cdc_table") + String.format("(a,b) VALUES (%s,%s);", i, i));
        }

        // check to make sure there are no records in the queue to begin with
        ChangeEventQueue<Event> queue = context.getQueues().get(0);
        assertEquals(queue.totalCapacity(), queue.remainingCapacity());

        // process the logs in commit log directory
        File cdcLoc = new File(DatabaseDescriptor.getCommitLogLocation());
        File[] commitLogs = CommitLogUtil.getCommitLogs(cdcLoc);
        for (File commitLog : commitLogs) {
            commitLogProcessor.processCommitLog(commitLog);
        }

        // verify the commit log has been processed and records have been enqueued
        List<Event> events = queue.poll();
        int eofEventSize = commitLogs.length;
        assertEquals(commitLogRowSize + eofEventSize, events.size());
        for (int i = 0; i < events.size(); i++) {
            Event event = events.get(i);
            if (event instanceof Record) {
                Record record = (Record) events.get(i);
                assertEquals(record.getEventType(), Event.EventType.CHANGE_EVENT);
                assertEquals(record.getSource().cluster, DatabaseDescriptor.getClusterName());
                assertFalse(record.getSource().snapshot);
                assertEquals(record.getSource().keyspaceTable.name(), keyspaceTable("cdc_table"));
            }
            else if (event instanceof EOFEvent) {
                EOFEvent eofEvent = (EOFEvent) event;
                assertFalse(context.getErroneousCommitLogs().contains(eofEvent.file.getName()));
            }
            else {
                throw new Exception("unexpected event type");
            }
        }

        deleteTestKeyspaceTables();
        deleteTestOffsets(context);
    }
}
