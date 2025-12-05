/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.metrics;

import static io.debezium.connector.cassandra.utils.TestUtils.TEST_TABLE_NAME;
import static io.debezium.connector.cassandra.utils.TestUtils.TEST_TABLE_NAME_2;
import static io.debezium.connector.cassandra.utils.TestUtils.deleteTestKeyspaceTables;
import static io.debezium.connector.cassandra.utils.TestUtils.deleteTestOffsets;
import static io.debezium.connector.cassandra.utils.TestUtils.keyspaceTable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.management.ManagementFactory;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.cassandra.CassandraConnectorTestBase;
import io.debezium.connector.cassandra.utils.TestUtils;
import io.debezium.connector.common.CdcSourceTaskContext;

class CassandraSnapshotMetricsTest extends CassandraConnectorTestBase {

    private CassandraSnapshotMetrics snapshotMetrics;
    private MBeanServer mBeanServer;
    private ObjectName snapshotMetricsObjectName;

    @BeforeEach
    void setUp() throws Exception {
        // Setup context
        provider = io.debezium.connector.cassandra.spi.ProvidersResolver.resolveConnectorContextProvider();
        try {
            context = provider.provideContext(Configuration.from(TestUtils.generateDefaultConfigMap()));
        }
        catch (Throwable e) {
            throw new RuntimeException("Failed to create context", e);
        }

        // Setup metrics
        snapshotMetrics = new CassandraSnapshotMetrics((CdcSourceTaskContext) context);
        snapshotMetrics.registerMetrics();

        // Setup JMX
        mBeanServer = ManagementFactory.getPlatformMBeanServer();
        // Construct ObjectName based on Debezium pattern: debezium.cassandra:type=connector-metrics,context=snapshot,server=<logical-name>
        String serverName = context.getCassandraConnectorConfig().getLogicalName();
        snapshotMetricsObjectName = new ObjectName("debezium.cassandra:type=connector-metrics,context=snapshot,server=" + serverName);
    }

    @AfterEach
    void tearDown() throws Exception {
        deleteTestOffsets(context);
        snapshotMetrics.unregisterMetrics();
        deleteTestKeyspaceTables();
        context.cleanUp();
    }

    @Test
    void testSnapshotMetricsRegistration() throws Exception {
        // Verify that snapshot metrics MBean is registered
        assertTrue(mBeanServer.isRegistered(snapshotMetricsObjectName),
                "Snapshot metrics MBean should be registered");

        // Verify initial values
        assertEquals(0,
                mBeanServer.getAttribute(snapshotMetricsObjectName, "TotalTableCount"),
                "Initial total table count should be 0");

        assertEquals(0,
                mBeanServer.getAttribute(snapshotMetricsObjectName, "RemainingTableCount"),
                "Initial remaining table count should be 0");

        assertFalse((Boolean) mBeanServer.getAttribute(snapshotMetricsObjectName, "SnapshotRunning"),
                "Snapshot should not be running initially");

        assertFalse((Boolean) mBeanServer.getAttribute(snapshotMetricsObjectName, "SnapshotCompleted"),
                "Snapshot should not be completed initially");

        assertFalse((Boolean) mBeanServer.getAttribute(snapshotMetricsObjectName, "SnapshotAborted"),
                "Snapshot should not be aborted initially");

        assertEquals(0L,
                mBeanServer.getAttribute(snapshotMetricsObjectName, "SnapshotDurationInSeconds"),
                "Initial snapshot duration should be 0");
    }

    @Test
    void testSnapshotMetricsLifecycle() throws Exception {
        // Start snapshot
        int totalTables = 2;
        snapshotMetrics.setTableCount(totalTables);
        snapshotMetrics.startSnapshot();

        // Verify snapshot started
        assertTrue((Boolean) mBeanServer.getAttribute(snapshotMetricsObjectName, "SnapshotRunning"),
                "Snapshot should be running");

        assertFalse((Boolean) mBeanServer.getAttribute(snapshotMetricsObjectName, "SnapshotCompleted"),
                "Snapshot should not be completed when running");

        assertEquals(totalTables,
                mBeanServer.getAttribute(snapshotMetricsObjectName, "TotalTableCount"),
                "Total table count should be set");

        assertEquals(totalTables,
                mBeanServer.getAttribute(snapshotMetricsObjectName, "RemainingTableCount"),
                "Remaining table count should equal total initially");

        // Simulate table completion
        String tableName1 = keyspaceTable(TEST_TABLE_NAME);
        snapshotMetrics.setRowsScanned(tableName1, 100L);
        snapshotMetrics.completeTable();

        assertEquals(totalTables - 1,
                mBeanServer.getAttribute(snapshotMetricsObjectName, "RemainingTableCount"),
                "Remaining table count should decrease");

        // Complete second table
        String tableName2 = keyspaceTable(TEST_TABLE_NAME_2);
        snapshotMetrics.setRowsScanned(tableName2, 50L);
        snapshotMetrics.completeTable();

        assertEquals(0,
                mBeanServer.getAttribute(snapshotMetricsObjectName, "RemainingTableCount"),
                "Remaining table count should be 0");

        // Stop snapshot
        snapshotMetrics.stopSnapshot();

        // Verify snapshot completed
        assertFalse((Boolean) mBeanServer.getAttribute(snapshotMetricsObjectName, "SnapshotRunning"),
                "Snapshot should not be running after stop");

        assertTrue((Boolean) mBeanServer.getAttribute(snapshotMetricsObjectName, "SnapshotCompleted"),
                "Snapshot should be completed");

        assertFalse((Boolean) mBeanServer.getAttribute(snapshotMetricsObjectName, "SnapshotAborted"),
                "Snapshot should not be aborted");

        // Verify duration is calculated
        assertTrue((Long) mBeanServer.getAttribute(snapshotMetricsObjectName, "SnapshotDurationInSeconds") >= 0,
                "Snapshot duration should be greater than 0");
    }

    @Test
    void testSnapshotMetricsRowsScanned() throws Exception {
        // Set rows scanned for different tables
        String table1 = keyspaceTable(TEST_TABLE_NAME);
        String table2 = keyspaceTable(TEST_TABLE_NAME_2);

        snapshotMetrics.setRowsScanned(table1, 100L);
        snapshotMetrics.setRowsScanned(table2, 200L);

        // Get rows scanned map
        TabularData rowsScannedAttr = (TabularData) mBeanServer.getAttribute(snapshotMetricsObjectName, "RowsScanned");
        assertNotNull(rowsScannedAttr, "RowsScanned should not be null");

        CompositeData r1 = rowsScannedAttr.get(new Object[]{ table1 });
        CompositeData r2 = rowsScannedAttr.get(new Object[]{ table2 });
        assertEquals(100L, r1.get("value"));
        assertEquals(200L, r2.get("value"));

        // Update rows scanned for existing table
        snapshotMetrics.setRowsScanned(table1, 150L);

        // Verify update
        rowsScannedAttr = (TabularData) mBeanServer.getAttribute(snapshotMetricsObjectName, "RowsScanned");
        r1 = rowsScannedAttr.get(new Object[]{ table1 });
        assertEquals(150L, r1.get("value"));
    }

    @Test
    void testSnapshotMetricsAbort() throws Exception {
        // Start snapshot
        snapshotMetrics.setTableCount(2);
        snapshotMetrics.startSnapshot();

        assertTrue((Boolean) mBeanServer.getAttribute(snapshotMetricsObjectName, "SnapshotRunning"),
                "Snapshot should be running");

        // Abort snapshot
        snapshotMetrics.abortSnapshot();

        // Verify snapshot aborted
        assertFalse((Boolean) mBeanServer.getAttribute(snapshotMetricsObjectName, "SnapshotRunning"),
                "Snapshot should not be running after abort");

        assertFalse((Boolean) mBeanServer.getAttribute(snapshotMetricsObjectName, "SnapshotCompleted"),
                "Snapshot should not be completed after abort");

        assertTrue((Boolean) mBeanServer.getAttribute(snapshotMetricsObjectName, "SnapshotAborted"),
                "Snapshot should be aborted");
    }

    @Test
    void testSnapshotMetricsReset() throws Exception {
        // Set up some metrics
        snapshotMetrics.setTableCount(3);
        snapshotMetrics.startSnapshot();
        snapshotMetrics.setRowsScanned(keyspaceTable(TEST_TABLE_NAME), 50L);
        snapshotMetrics.completeTable();
        snapshotMetrics.stopSnapshot();

        // Verify metrics are set
        assertTrue((Integer) mBeanServer.getAttribute(snapshotMetricsObjectName, "TotalTableCount") > 0,
                "Total table count should be greater than 0");

        assertTrue((Boolean) mBeanServer.getAttribute(snapshotMetricsObjectName, "SnapshotCompleted"),
                "Snapshot should be completed");

        // Reset metrics
        snapshotMetrics.reset();

        // Verify metrics are reset
        assertEquals(0,
                mBeanServer.getAttribute(snapshotMetricsObjectName, "TotalTableCount"),
                "Total table count should be 0 after reset");

        assertEquals(0,
                mBeanServer.getAttribute(snapshotMetricsObjectName, "RemainingTableCount"),
                "Remaining table count should be 0 after reset");

        assertFalse((Boolean) mBeanServer.getAttribute(snapshotMetricsObjectName, "SnapshotRunning"),
                "Snapshot should not be running after reset");

        assertFalse((Boolean) mBeanServer.getAttribute(snapshotMetricsObjectName, "SnapshotCompleted"),
                "Snapshot should not be completed after reset");

        assertFalse((Boolean) mBeanServer.getAttribute(snapshotMetricsObjectName, "SnapshotAborted"),
                "Snapshot should not be aborted after reset");

        assertEquals(0L,
                mBeanServer.getAttribute(snapshotMetricsObjectName, "SnapshotDurationInSeconds"),
                "Snapshot duration should be 0 after reset");
    }

    @Test
    void testSnapshotMetricsUnregistration() throws Exception {
        // Verify MBean is registered
        assertTrue(mBeanServer.isRegistered(snapshotMetricsObjectName),
                "Snapshot metrics MBean should be registered");

        // Unregister metrics
        snapshotMetrics.unregisterMetrics();

        // Verify MBean is unregistered
        try {
            mBeanServer.getAttribute(snapshotMetricsObjectName, "TotalTableCount");
            throw new AssertionError("Expected InstanceNotFoundException after unregistering metrics");
        }
        catch (InstanceNotFoundException e) {
            // Expected - MBean should not be found after unregistration
        }
    }
}
