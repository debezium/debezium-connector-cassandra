/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dse;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.deleteFrom;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static io.debezium.connector.cassandra.Event.EventType.CHANGE_EVENT;
import static io.debezium.connector.cassandra.Record.Operation.DELETE;
import static io.debezium.connector.cassandra.Record.Operation.INSERT;
import static io.debezium.connector.cassandra.TestUtils.TEST_KEYSPACE_NAME;
import static io.debezium.connector.cassandra.TestUtils.TEST_TABLE_NAME;
import static io.debezium.connector.cassandra.TestUtils.runCql;
import static org.junit.Assert.assertEquals;

import java.util.List;

import io.debezium.connector.cassandra.Event;
import io.debezium.connector.cassandra.Record;

public class PartitionKeyRowDeletionCommitLogProcessingTest extends AbstractCommitLogProcessorTest {

    @Override
    public void initialiseData() throws Exception {
        createTable("CREATE TABLE IF NOT EXISTS %s.%s (a int, b int, PRIMARY KEY(a)) WITH cdc = true;");

        runCql(insertInto(TEST_KEYSPACE_NAME, TEST_TABLE_NAME)
                .value("a", literal(1))
                .value("b", literal(1))
                .build());

        runCql(deleteFrom(TEST_KEYSPACE_NAME, TEST_TABLE_NAME).whereColumn("a").isEqualTo(literal(1)).build());
    }

    @Override
    public void verifyEvents() throws Exception {
        List<Event> events = getEvents(2);

        Record insert = (Record) events.get(0);
        assertEquals(insert.getEventType(), CHANGE_EVENT);
        assertEquals(INSERT, insert.getOp());

        Record delete = (Record) events.get(1);
        assertEquals(delete.getEventType(), CHANGE_EVENT);
        assertEquals(DELETE, delete.getOp());
    }
}
