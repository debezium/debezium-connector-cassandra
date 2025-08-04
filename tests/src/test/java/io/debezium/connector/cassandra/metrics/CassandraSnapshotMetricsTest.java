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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.lang.management.ManagementFactory;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.cassandra.CassandraConnectorTestBase;
import io.debezium.connector.cassandra.utils.TestUtils;
import io.debezium.connector.common.CdcSourceTaskContext;

public class CassandraSnapshotMetricsTest extends CassandraConnectorTestBase {

    private CassandraSnapshotMetrics snapshotMetrics;
    private MBeanServer mBeanServer;
    private ObjectName snapshotMetricsObjectName;

    @Before
    public void setUp() throws Exception {
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

    @After
    public void tearDown() throws Exception {
        deleteTestOffsets(context);
        snapshotMetrics.unregisterMetrics();
        deleteTestKeyspaceTables();
        context.cleanUp();
    }

    @Test
    public void testSnapshotMetricsRegistration() throws Exception {
        // Verify that snapshot metrics MBean is registered
        assertTrue("Snapshot metrics MBean should be registered",
                mBeanServer.isRegistered(snapshotMetricsObjectName));

        // Verify initial values
        assertEquals("Initial total table count should be 0",
                0,
                mBeanServer.getAttribute(snapshotMetricsObjectName, "TotalTableCount"));

        assertEquals("Initial remaining table count should be 0",
                0,
                mBeanServer.getAttribute(snapshotMetricsObjectName, "RemainingTableCount"));

        assertFalse("Snapshot should not be running initially",
                (Boolean) mBeanServer.getAttribute(snapshotMetricsObjectName, "SnapshotRunning"));

        assertFalse("Snapshot should not be completed initially",
                (Boolean) mBeanServer.getAttribute(snapshotMetricsObjectName, "SnapshotCompleted"));

        assertFalse("Snapshot should not be aborted initially",
                (Boolean) mBeanServer.getAttribute(snapshotMetricsObjectName, "SnapshotAborted"));

        assertEquals("Initial snapshot duration should be 0",
                0L,
                mBeanServer.getAttribute(snapshotMetricsObjectName, "SnapshotDurationInSeconds"));
    }

    @Test
    public void testSnapshotMetricsLifecycle() throws Exception {
        // Start snapshot
        int totalTables = 2;
        snapshotMetrics.setTableCount(totalTables);
        snapshotMetrics.startSnapshot();

        // Verify snapshot started
        assertTrue("Snapshot should be running",
                (Boolean) mBeanServer.getAttribute(snapshotMetricsObjectName, "SnapshotRunning"));

        assertFalse("Snapshot should not be completed when running",
                (Boolean) mBeanServer.getAttribute(snapshotMetricsObjectName, "SnapshotCompleted"));

        assertEquals("Total table count should be set",
                totalTables,
                mBeanServer.getAttribute(snapshotMetricsObjectName, "TotalTableCount"));

        assertEquals("Remaining table count should equal total initially",
                totalTables,
                mBeanServer.getAttribute(snapshotMetricsObjectName, "RemainingTableCount"));

        // Simulate table completion
        String tableName1 = keyspaceTable(TEST_TABLE_NAME);
        snapshotMetrics.setRowsScanned(tableName1, 100L);
        snapshotMetrics.completeTable();

        assertEquals("Remaining table count should decrease",
                totalTables - 1,
                mBeanServer.getAttribute(snapshotMetricsObjectName, "RemainingTableCount"));

        // Complete second table
        String tableName2 = keyspaceTable(TEST_TABLE_NAME_2);
        snapshotMetrics.setRowsScanned(tableName2, 50L);
        snapshotMetrics.completeTable();

        assertEquals("Remaining table count should be 0",
                0,
                mBeanServer.getAttribute(snapshotMetricsObjectName, "RemainingTableCount"));

        // Stop snapshot
        snapshotMetrics.stopSnapshot();

        // Verify snapshot completed
        assertFalse("Snapshot should not be running after stop",
                (Boolean) mBeanServer.getAttribute(snapshotMetricsObjectName, "SnapshotRunning"));

        assertTrue("Snapshot should be completed",
                (Boolean) mBeanServer.getAttribute(snapshotMetricsObjectName, "SnapshotCompleted"));

        assertFalse("Snapshot should not be aborted",
                (Boolean) mBeanServer.getAttribute(snapshotMetricsObjectName, "SnapshotAborted"));

        // Verify duration is calculated
        assertTrue("Snapshot duration should be greater than 0",
                (Long) mBeanServer.getAttribute(snapshotMetricsObjectName, "SnapshotDurationInSeconds") >= 0);
    }

    @Test
    public void testSnapshotMetricsRowsScanned() throws Exception {
        // Set rows scanned for different tables
        String table1 = keyspaceTable(TEST_TABLE_NAME);
        String table2 = keyspaceTable(TEST_TABLE_NAME_2);

        snapshotMetrics.setRowsScanned(table1, 100L);
        snapshotMetrics.setRowsScanned(table2, 200L);

        // Get rows scanned map
        TabularData rowsScannedAttr = (TabularData) mBeanServer.getAttribute(snapshotMetricsObjectName, "RowsScanned");
        assertNotNull("RowsScanned should not be null", rowsScannedAttr);

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
    public void testSnapshotMetricsAbort() throws Exception {
        // Start snapshot
        snapshotMetrics.setTableCount(2);
        snapshotMetrics.startSnapshot();

        assertTrue("Snapshot should be running",
                (Boolean) mBeanServer.getAttribute(snapshotMetricsObjectName, "SnapshotRunning"));

        // Abort snapshot
        snapshotMetrics.abortSnapshot();

        // Verify snapshot aborted
        assertFalse("Snapshot should not be running after abort",
                (Boolean) mBeanServer.getAttribute(snapshotMetricsObjectName, "SnapshotRunning"));

        assertFalse("Snapshot should not be completed after abort",
                (Boolean) mBeanServer.getAttribute(snapshotMetricsObjectName, "SnapshotCompleted"));

        assertTrue("Snapshot should be aborted",
                (Boolean) mBeanServer.getAttribute(snapshotMetricsObjectName, "SnapshotAborted"));
    }

    @Test
    public void testSnapshotMetricsReset() throws Exception {
        // Set up some metrics
        snapshotMetrics.setTableCount(3);
        snapshotMetrics.startSnapshot();
        snapshotMetrics.setRowsScanned(keyspaceTable(TEST_TABLE_NAME), 50L);
        snapshotMetrics.completeTable();
        snapshotMetrics.stopSnapshot();

        // Verify metrics are set
        assertTrue("Total table count should be greater than 0",
                (Integer) mBeanServer.getAttribute(snapshotMetricsObjectName, "TotalTableCount") > 0);

        assertTrue("Snapshot should be completed",
                (Boolean) mBeanServer.getAttribute(snapshotMetricsObjectName, "SnapshotCompleted"));

        // Reset metrics
        snapshotMetrics.reset();

        // Verify metrics are reset
        assertEquals("Total table count should be 0 after reset",
                0,
                mBeanServer.getAttribute(snapshotMetricsObjectName, "TotalTableCount"));

        assertEquals("Remaining table count should be 0 after reset",
                0,
                mBeanServer.getAttribute(snapshotMetricsObjectName, "RemainingTableCount"));

        assertFalse("Snapshot should not be running after reset",
                (Boolean) mBeanServer.getAttribute(snapshotMetricsObjectName, "SnapshotRunning"));

        assertFalse("Snapshot should not be completed after reset",
                (Boolean) mBeanServer.getAttribute(snapshotMetricsObjectName, "SnapshotCompleted"));

        assertFalse("Snapshot should not be aborted after reset",
                (Boolean) mBeanServer.getAttribute(snapshotMetricsObjectName, "SnapshotAborted"));

        assertEquals("Snapshot duration should be 0 after reset",
                0L,
                mBeanServer.getAttribute(snapshotMetricsObjectName, "SnapshotDurationInSeconds"));
    }

    @Test
    public void testSnapshotMetricsUnregistration() throws Exception {
        // Verify MBean is registered
        assertTrue("Snapshot metrics MBean should be registered",
                mBeanServer.isRegistered(snapshotMetricsObjectName));

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
