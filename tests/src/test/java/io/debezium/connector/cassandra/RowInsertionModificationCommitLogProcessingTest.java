/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static io.debezium.connector.cassandra.utils.TestUtils.TEST_KEYSPACE_NAME;
import static io.debezium.connector.cassandra.utils.TestUtils.TEST_TABLE_NAME;
import static io.debezium.connector.cassandra.utils.TestUtils.keyspaceTable;
import static io.debezium.connector.cassandra.utils.TestUtils.runCql;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

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
    public void verifyEvents() throws Throwable {
        for (Event event : getEvents(10)) {
            if (event instanceof Record) {
                Record record = (Record) event;
                assertEquals(record.getEventType(), Event.EventType.CHANGE_EVENT);
                assertEquals(record.getSource().cluster, context.getCassandraConnectorConfig().clusterName());
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
