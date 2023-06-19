/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dse;

import static com.datastax.oss.driver.api.core.cql.BatchType.LOGGED;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.deleteFrom;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.relation.Relation.column;
import static io.debezium.connector.cassandra.Event.EventType.CHANGE_EVENT;
import static io.debezium.connector.cassandra.Record.Operation.DELETE;
import static io.debezium.connector.cassandra.Record.Operation.INSERT;
import static io.debezium.connector.cassandra.TestUtils.TEST_KEYSPACE_NAME;
import static io.debezium.connector.cassandra.TestUtils.TEST_TABLE_NAME;
import static io.debezium.connector.cassandra.TestUtils.runCql;

import java.util.HashMap;

import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;

public class BatchTest extends AbstractCommitLogProcessorTest {
    @Override
    public void initialiseData() throws Exception {
        createTable("CREATE TABLE %s.%s (\n" +
                "    p1 text,\n" +
                "    p2 text,\n" +
                "    p3 text,\n" +
                "    c1 text,\n" +
                "    col1 text,\n" +
                "    col2 text,\n" +
                "    amap map<text, text>,\n" +
                "    PRIMARY KEY ((p1, p2, p3), c1)\n" +
                ") WITH CLUSTERING ORDER BY (c1 ASC)\n" +
                "    AND cdc = true;");

        runCql(new BatchStatementBuilder(LOGGED)
                .addStatement(deleteFrom(TEST_KEYSPACE_NAME, TEST_TABLE_NAME)
                        .usingTimestamp(1683810323861L)
                        .where(column("p1").isEqualTo(literal("abc")))
                        .where(column("p2").isEqualTo(literal("p2value")))
                        .where(column("p3").isEqualTo(literal("p3value")))
                        .build())
                .addStatement(insertInto(TEST_KEYSPACE_NAME, TEST_TABLE_NAME)
                        .value("p1", literal("abc"))
                        .value("p2", literal("p2value"))
                        .value("p3", literal("p3value"))
                        .value("c1", literal(""))
                        .value("amap", literal(new HashMap<String, String>() {
                            {
                                put("key", "value");
                            }
                        }))
                        .value("col1", literal(""))
                        .usingTimestamp(1683810323862L)
                        .usingTtl(3600)
                        .build())
                .addStatement(insertInto(TEST_KEYSPACE_NAME, TEST_TABLE_NAME)
                        .value("p1", literal("abc"))
                        .value("p2", literal("p2value"))
                        .value("p3", literal("p3value"))
                        .value("c1", literal("c1value1"))
                        .value("col1", literal("col1value"))
                        .value("col2", literal("col2value"))
                        .value("amap", literal(new HashMap<String, String>() {
                            {
                                put("key", "value");
                            }
                        }))
                        .usingTimestamp(1683810323862L)
                        .usingTtl(3600)
                        .build())
                .addStatement(insertInto(TEST_KEYSPACE_NAME, TEST_TABLE_NAME)
                        .value("p1", literal("abc"))
                        .value("p2", literal("p2value"))
                        .value("p3", literal("p3value"))
                        .value("c1", literal("c1value2"))
                        .value("col1", literal("col1value"))
                        .value("col2", literal("col2value"))
                        .value("amap", literal(new HashMap<String, String>() {
                            {
                                put("key", "value");
                            }
                        }))
                        .usingTimestamp(1683810323862L)
                        .usingTtl(3600)
                        .build())
                .build());
    }

    @Override
    public void verifyEvents() throws Exception {
        assertEventTypes(getEvents(4), CHANGE_EVENT, DELETE, INSERT, INSERT, INSERT);
    }
}
