/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static io.debezium.connector.cassandra.TestUtils.TEST_KEYSPACE_NAME;
import static io.debezium.connector.cassandra.TestUtils.TEST_TABLE_NAME;
import static io.debezium.connector.cassandra.TestUtils.keyspaceTable;
import static io.debezium.connector.cassandra.TestUtils.runCql;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.apache.cassandra.config.DatabaseDescriptor;

public class RowInsertionModificationCommitLogProcessingTest extends AbstractCommitLogProcessorTest {

    @Override
    public void initialiseData() throws Exception {
        createTable("CREATE TABLE IF NOT EXISTS %s.%s (a int, b int, PRIMARY KEY(a)) WITH cdc = true;");
        for (int i = 0; i < 10; i++) {
            runCql(insertInto(TEST_KEYSPACE_NAME, TEST_TABLE_NAME)
                    .value("a", literal(i))
                    .value("b", literal(i))
                    .build());
        }
    }

    @Override
    public void verifyEvents() throws Exception {
        for (Event event : getEvents()) {
            if (event instanceof Record) {
                Record record = (Record) event;
                assertEquals(record.getEventType(), Event.EventType.CHANGE_EVENT);
                assertEquals(record.getSource().cluster, DatabaseDescriptor.getClusterName());
                assertFalse(record.getSource().snapshot);
                assertEquals(record.getSource().keyspaceTable.name(), keyspaceTable(TEST_TABLE_NAME));
            }
            else if (event instanceof EOFEvent) {
                EOFEvent eofEvent = (EOFEvent) event;
                assertFalse(context.getErroneousCommitLogs().contains(eofEvent.file.getName()));
            }
            else {
                throw new Exception("unexpected event type");
            }
        }
    }
}
