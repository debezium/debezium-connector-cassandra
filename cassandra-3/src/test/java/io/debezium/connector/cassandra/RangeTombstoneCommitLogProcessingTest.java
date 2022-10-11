/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.deleteFrom;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static io.debezium.connector.cassandra.Event.EventType.CHANGE_EVENT;
import static io.debezium.connector.cassandra.Record.Operation.INSERT;
import static io.debezium.connector.cassandra.Record.Operation.RANGE_TOMBSTONE;
import static io.debezium.connector.cassandra.TestUtils.TEST_KEYSPACE_NAME;
import static io.debezium.connector.cassandra.TestUtils.TEST_TABLE_NAME;
import static io.debezium.connector.cassandra.TestUtils.runCql;
import static org.junit.Assert.assertEquals;

import java.util.List;

public class RangeTombstoneCommitLogProcessingTest extends AbstractCommitLogProcessorTest {

    @Override
    public void initialiseData() throws Exception {
        createTable("CREATE TABLE IF NOT EXISTS %s.%s (a int, b int, c int, d int, e int, PRIMARY KEY (a,b,c,d)) WITH cdc = true;");

        // INSERT INTO test_keyspace.table_name (a, b, c, d, e) VALUES (1, 1, 1, 1, 1);
        runCql(insertInto(TEST_KEYSPACE_NAME, TEST_TABLE_NAME)
                .value("a", literal(1))
                .value("b", literal(1))
                .value("c", literal(1))
                .value("d", literal(1))
                .value("e", literal(1))
                .build());

        // INSERT INTO test_keyspace.table_name (a, b, c, d, e) VALUES (1, 1, 2, 3, 2);
        runCql(insertInto(TEST_KEYSPACE_NAME, TEST_TABLE_NAME)
                .value("a", literal(1))
                .value("b", literal(1))
                .value("c", literal(2))
                .value("d", literal(3))
                .value("e", literal(2))
                .build());

        // "DELETE FROM ks.tb WHERE a = 1 AND b = 1 AND c <= 2";
        runCql(deleteFrom(TEST_KEYSPACE_NAME, TEST_TABLE_NAME)
                .whereColumn("a").isEqualTo(literal(1))
                .whereColumn("b").isEqualTo(literal(1))
                .whereColumn("c").isLessThanOrEqualTo(literal(2))
                .build());
    }

    @Override
    public void verifyEvents() throws Exception {
        List<Event> events = getEvents(3);

        assertEquals(3, events.size());

        Record insert = (Record) events.get(0);
        assertEquals(insert.getEventType(), CHANGE_EVENT);
        assertEquals(INSERT, insert.getOp());

        Record insert2 = (Record) events.get(1);
        assertEquals(insert2.getEventType(), CHANGE_EVENT);
        assertEquals(INSERT, insert2.getOp());

        Record range1 = (Record) events.get(2);
        assertEquals(range1.getEventType(), CHANGE_EVENT);
        assertEquals(RANGE_TOMBSTONE, range1.getOp());
        assertEquals("INCL_START_BOUND(1)", range1.getRowData().getStart());
        assertEquals("INCL_END_BOUND(1, 2)", range1.getRowData().getEnd());
    }
}
