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
import static io.debezium.connector.cassandra.Record.Operation.DELETE;
import static io.debezium.connector.cassandra.Record.Operation.INSERT;
import static io.debezium.connector.cassandra.TestUtils.TEST_KEYSPACE_NAME;
import static io.debezium.connector.cassandra.TestUtils.runCql;
import static org.junit.Assert.assertEquals;

import java.util.List;

public class ClusteringPartitionKeyCommitLogProcessingTest extends AbstractCommitLogProcessorTest {

    @Override
    public void initialiseData() {
        createTable("CREATE TABLE IF NOT EXISTS %s.%s (a int, b int, c int, PRIMARY KEY ((a), b)) WITH cdc = true;");

        runCql(insertInto(TEST_KEYSPACE_NAME, tableName)
                .value("a", literal(1))
                .value("b", literal(1))
                .value("c", literal(1))
                .build());

        runCql(insertInto(TEST_KEYSPACE_NAME, tableName)
                .value("a", literal(1))
                .value("b", literal(2))
                .value("c", literal(3))
                .build());

        runCql(deleteFrom(TEST_KEYSPACE_NAME, tableName).whereColumn("a").isEqualTo(literal(1)).build());
    }

    @Override
    public void verifyEvents() throws Exception {
        List<Event> events = getEvents();

        Record insert1 = (Record) events.get(0);
        assertEquals(insert1.getEventType(), CHANGE_EVENT);
        assertEquals(INSERT, insert1.getOp());

        Record insert2 = (Record) events.get(1);
        assertEquals(insert2.getEventType(), CHANGE_EVENT);
        assertEquals(INSERT, insert2.getOp());

        Record delete = (Record) events.get(2);
        assertEquals(delete.getEventType(), CHANGE_EVENT);
        assertEquals(DELETE, delete.getOp());
    }
}
