/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.metrics;

import static io.debezium.connector.cassandra.utils.TestUtils.deleteTestKeyspaceTables;
import static io.debezium.connector.cassandra.utils.TestUtils.deleteTestOffsets;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.lang.management.ManagementFactory;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.cassandra.CassandraConnectorTestBase;
import io.debezium.connector.cassandra.spi.ProvidersResolver;
import io.debezium.connector.cassandra.utils.TestUtils;
import io.debezium.connector.common.CdcSourceTaskContext;

public class CassandraStreamingMetricsTest extends CassandraConnectorTestBase {

    private CassandraStreamingMetrics streamingMetrics;
    private MBeanServer mBeanServer;
    private ObjectName streamingMetricsObjectName;

    @Before
    public void setUp() throws Exception {
        // Setup context and processing
        provider = ProvidersResolver.resolveConnectorContextProvider();
        try {
            context = provider.provideContext(Configuration.from(TestUtils.generateDefaultConfigMap()));
        }
        catch (Throwable e) {
            throw new RuntimeException("Failed to create context", e);
        }

        // Setup metrics
        streamingMetrics = new CassandraStreamingMetrics((CdcSourceTaskContext) context);
        streamingMetrics.registerMetrics();

        // Setup JMX
        mBeanServer = ManagementFactory.getPlatformMBeanServer();
        // Construct ObjectName based on Debezium pattern: debezium.cassandra:type=connector-metrics,context=streaming,server=<logical-name>
        String serverName = context.getCassandraConnectorConfig().getLogicalName();
        streamingMetricsObjectName = new ObjectName("debezium.cassandra:type=connector-metrics,context=streaming,server=" + serverName);
    }

    @After
    public void tearDown() throws Exception {
        deleteTestOffsets(context);
        streamingMetrics.unregisterMetrics();
        deleteTestKeyspaceTables();
        context.cleanUp();
    }

    @Test
    public void testStreamingMetricsRegistration() throws Exception {
        // Verify that streaming metrics MBean is registered
        assertTrue("Streaming metrics MBean should be registered",
                mBeanServer.isRegistered(streamingMetricsObjectName));

        // Verify initial values
        assertEquals("Initial commit log position should be -1",
                -1L,
                mBeanServer.getAttribute(streamingMetricsObjectName, "CommitLogPosition"));

        assertEquals("Initial processed mutations should be 0",
                0L,
                mBeanServer.getAttribute(streamingMetricsObjectName, "NumberOfProcessedMutations"));

        assertEquals("Initial unrecoverable errors should be 0",
                0L,
                mBeanServer.getAttribute(streamingMetricsObjectName, "NumberOfUnrecoverableErrors"));

        // Commit log filename should be null initially
        assertNull("Initial commit log filename should be null", mBeanServer.getAttribute(streamingMetricsObjectName, "CommitLogFilename"));
    }

    @Test
    public void testStreamingMetricsUpdates() throws Exception {
        // Set commit log filename
        String testFilename = "CommitLog-1234567890.log";
        streamingMetrics.setCommitLogFilename(testFilename);

        assertEquals("Commit log filename should be updated",
                testFilename,
                mBeanServer.getAttribute(streamingMetricsObjectName, "CommitLogFilename"));

        // Set commit log position
        long testPosition = 12345L;
        streamingMetrics.setCommitLogPosition(testPosition);

        assertEquals("Commit log position should be updated",
                testPosition,
                mBeanServer.getAttribute(streamingMetricsObjectName, "CommitLogPosition"));

        // Simulate successful mutation processing
        streamingMetrics.onSuccess();
        streamingMetrics.onSuccess();

        assertEquals("Processed mutations should be incremented",
                2L,
                mBeanServer.getAttribute(streamingMetricsObjectName, "NumberOfProcessedMutations"));

        // Simulate unrecoverable error
        streamingMetrics.onUnrecoverableError();

        assertEquals("Unrecoverable errors should be incremented",
                1L,
                mBeanServer.getAttribute(streamingMetricsObjectName, "NumberOfUnrecoverableErrors"));
    }

    @Test
    public void testStreamingMetricsReset() throws Exception {
        // Update some metrics
        streamingMetrics.setCommitLogFilename("test.log");
        streamingMetrics.setCommitLogPosition(100L);
        streamingMetrics.onSuccess();
        streamingMetrics.onUnrecoverableError();

        // Verify metrics are set
        assertNotNull("Commit log filename should be set",
                mBeanServer.getAttribute(streamingMetricsObjectName, "CommitLogFilename"));
        assertTrue("Processed mutations should be greater than 0",
                (Long) mBeanServer.getAttribute(streamingMetricsObjectName, "NumberOfProcessedMutations") > 0);

        // Reset metrics
        streamingMetrics.reset();

        // Verify metrics are reset
        assertNull("Commit log filename should be null after reset", mBeanServer.getAttribute(streamingMetricsObjectName, "CommitLogFilename"));

        assertEquals("Commit log position should be -1 after reset",
                -1L,
                mBeanServer.getAttribute(streamingMetricsObjectName, "CommitLogPosition"));

        assertEquals("Processed mutations should be 0 after reset",
                0L,
                mBeanServer.getAttribute(streamingMetricsObjectName, "NumberOfProcessedMutations"));

        assertEquals("Unrecoverable errors should be 0 after reset",
                0L,
                mBeanServer.getAttribute(streamingMetricsObjectName, "NumberOfUnrecoverableErrors"));
    }

    @Test
    public void testStreamingMetricsUnregistration() throws Exception {
        // Verify MBean is registered
        assertTrue("Streaming metrics MBean should be registered",
                mBeanServer.isRegistered(streamingMetricsObjectName));

        // Unregister metrics
        streamingMetrics.unregisterMetrics();

        // Verify MBean is unregistered
        try {
            mBeanServer.getAttribute(streamingMetricsObjectName, "CommitLogPosition");
            throw new AssertionError("Expected InstanceNotFoundException after unregistering metrics");
        }
        catch (InstanceNotFoundException e) {
            // Expected - MBean should not be found after unregistration
        }
    }

}
