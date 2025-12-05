/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static io.debezium.connector.cassandra.Event.EventType.CHANGE_EVENT;
import static io.debezium.connector.cassandra.Record.Operation.INSERT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import com.datastax.oss.driver.api.querybuilder.QueryBuilder;

import io.debezium.connector.cassandra.utils.TestUtils;

class SchemaChangeListenerTest extends AbstractCommitLogProcessorTest {

    @Override
    public void initialiseData() throws Exception {
        createTable("CREATE TABLE %s.%s (a int, b int, PRIMARY KEY ((a), b)) WITH cdc = true;",
                TestUtils.TEST_KEYSPACE_NAME, TestUtils.TEST_TABLE_NAME);
        TestUtils.runCql(QueryBuilder.insertInto(TestUtils.TEST_KEYSPACE_NAME, TestUtils.TEST_TABLE_NAME)
                .value("a", literal(1))
                .value("b", literal(2))
                .build());
    }

    @Override
    public void verifyEvents() throws Throwable {
        // We have to read the first event before altering the table.
        // That way we make sure that the initial schema is registered and the schema change code path is triggered.
        List<Event> events = getEvents(1);
        Record insert1 = (Record) events.get(0);
        assertEquals(CHANGE_EVENT, insert1.getEventType());
        assertEquals(INSERT, insert1.getOp());
        assertTrue(insert1.getRowData().hasCell("a"));
        assertTrue(insert1.getRowData().hasCell("b"));
        assertFalse(insert1.getRowData().hasCell("c"));

        TestUtils.runCql(String.format("ALTER TABLE %s.%s ADD c int;", TestUtils.TEST_KEYSPACE_NAME, TestUtils.TEST_TABLE_NAME));

        TestUtils.runCql(QueryBuilder.insertInto(TestUtils.TEST_KEYSPACE_NAME, TestUtils.TEST_TABLE_NAME)
                .value("a", literal(3))
                .value("b", literal(4))
                .value("c", literal(5))
                .build());

        events = getEvents(2);
        Record insert2 = (Record) events.get(1);

        assertEquals(CHANGE_EVENT, insert2.getEventType());
        assertEquals(INSERT, insert2.getOp());
        assertTrue(insert2.getRowData().hasCell("a"));
        assertTrue(insert2.getRowData().hasCell("b"));
        assertTrue(insert2.getRowData().hasCell("c"));
    }

    @Override
    public void assumeTestRuns() {
        assumeNotDse();
    }

}
