/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static io.debezium.connector.cassandra.utils.TestUtils.TEST_KEYSPACE_NAME;
import static io.debezium.connector.cassandra.utils.TestUtils.TEST_TABLE_NAME;
import static io.debezium.connector.cassandra.utils.TestUtils.deleteTestKeyspaceTables;
import static io.debezium.connector.cassandra.utils.TestUtils.deleteTestOffsets;
import static io.debezium.connector.cassandra.utils.TestUtils.runCql;
import static java.lang.String.format;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.cassandra.spi.CassandraTestProvider;
import io.debezium.connector.cassandra.spi.CommitLogProcessing;
import io.debezium.connector.cassandra.spi.ProvidersResolver;
import io.debezium.connector.cassandra.utils.TestUtils;

public abstract class AbstractCommitLogProcessorTest extends CassandraConnectorTestBase {
    protected CommitLogProcessorMetrics metrics = new CommitLogProcessorMetrics();

    protected CommitLogProcessing commitLogProcessing;

    public Configuration getContextConfiguration() throws Throwable {
        return Configuration.from(TestUtils.generateDefaultConfigMap());
    }

    @Before
    public void setUp() throws Throwable {
        initialiseData();

        provider = ProvidersResolver.resolveConnectorContextProvider();
        context = provider.provideContext(getContextConfiguration());

        commitLogProcessing = provider.provideCommitLogProcessing(context, metrics);

        await().atMost(Duration.ofSeconds(60)).until(() -> context.getSchemaHolder()
                .getKeyValueSchema(new KeyspaceTable(TEST_KEYSPACE_NAME, TEST_TABLE_NAME)) != null);

        metrics.registerMetrics();
    }

    @After
    public void tearDown() throws Exception {
        deleteTestOffsets(context);
        metrics.unregisterMetrics();
        deleteTestKeyspaceTables();
        context.cleanUp();
    }

    @Test
    public void test() throws Throwable {
        assumeTestRuns();
        verifyEvents();
    }

    public void assumeTestRuns() {
    }

    protected void assumeNotDse() {
        Assume.assumeFalse(ServiceLoader.load(CassandraTestProvider.class).findFirst().get().getClass().getName().contains("io.debezium.connector.dse"));
    }

    protected void assumeNotCassandra3() {
        Assume.assumeFalse(ServiceLoader.load(CassandraTestProvider.class).findFirst().get().getClass().getName().contains("Cassandra3TestProvider"));
    }

    public abstract void initialiseData() throws Throwable;

    public abstract void verifyEvents() throws Throwable;

    public void createTable(String query) {
        createTable(query, TEST_KEYSPACE_NAME, TEST_TABLE_NAME);
    }

    public void createTable(String query, String keyspace, String tableName) {
        runCql(format(query, keyspace, tableName));
    }

    public List<Event> getEvents(final int expectedSize) throws Throwable {
        ChangeEventQueue<Event> queue = context.getQueues().get(0);
        final List<Event> events = new ArrayList<>();

        AtomicReference<Throwable> throwable = new AtomicReference<>();

        await().atMost(60, TimeUnit.SECONDS).until(() -> {
            try {
                readLogs(queue);
            }
            catch (IOException t) {
                return false;
            }

            events.clear();
            events.addAll(queue.poll());
            return events.size() == expectedSize;
        });

        assertEquals(expectedSize, events.size());
        return events;
    }

    private void readLogs(ChangeEventQueue<Event> queue) throws IOException {
        // check to make sure there are no records in the queue to begin with
        assertEquals(queue.totalCapacity(), queue.remainingCapacity());

        // process the logs in commit log directory
        File cdcLoc = new File(context.getCassandraConnectorConfig().getCdcLogLocation());
        File[] commitLogs = CommitLogUtil.getCommitLogs(cdcLoc);

        commitLogProcessing.readAllCommitLogs(commitLogs);
    }

    public void assertEventTypes(List<Event> events, Event.EventType eventType, Record.Operation... operations) {
        assertEquals(events.size(), operations.length);
        for (int i = 0; i < events.size(); i++) {
            Record record = (Record) events.get(i);
            assertEquals(record.getEventType(), eventType);
            assertEquals(operations[i], record.getOp());
        }
    }
}
