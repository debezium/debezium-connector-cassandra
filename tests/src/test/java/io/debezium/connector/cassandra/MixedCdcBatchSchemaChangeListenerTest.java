/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static io.debezium.connector.cassandra.Event.EventType.CHANGE_EVENT;
import static io.debezium.connector.cassandra.Record.Operation.INSERT;
import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import io.debezium.connector.cassandra.utils.TestUtils;

class MixedCdcBatchSchemaChangeListenerTest extends AbstractCommitLogProcessorTest {

    @Override
    public void initialiseData() throws Exception {
        createTable("CREATE TABLE %s.%s (a int, b int, PRIMARY KEY ((a), b)) WITH cdc = true;",
                TestUtils.TEST_KEYSPACE_NAME, TestUtils.TEST_TABLE_NAME);
        createTable("CREATE TABLE %s.%s (a int, b int, PRIMARY KEY ((a), b)) WITH cdc = false;",
                TestUtils.TEST_KEYSPACE_NAME, TestUtils.TEST_TABLE_NAME_2);

        TestUtils.runCql(format(
                "BEGIN UNLOGGED BATCH " +
                        "INSERT INTO %1$s.%2$s (a, b) VALUES (1, 2); " +
                        "INSERT INTO %1$s.%3$s (a, b) VALUES (1, 20); " +
                        "APPLY BATCH;",
                TestUtils.TEST_KEYSPACE_NAME, TestUtils.TEST_TABLE_NAME, TestUtils.TEST_TABLE_NAME_2));
    }

    @Override
    public void verifyEvents() throws Throwable {
        List<Event> events = getEvents(1);
        Record insert = (Record) events.get(0);
        assertEquals(CHANGE_EVENT, insert.getEventType());
        assertEquals(INSERT, insert.getOp());
        assertTrue(insert.getRowData().hasCell("a"));
        assertTrue(insert.getRowData().hasCell("b"));
    }

    @Override
    public void assumeTestRuns() {
        assumeNotDse();
    }

}
