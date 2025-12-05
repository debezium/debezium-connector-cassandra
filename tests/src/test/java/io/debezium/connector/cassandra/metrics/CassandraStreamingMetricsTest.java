/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.metrics;

import static io.debezium.connector.cassandra.utils.TestUtils.deleteTestKeyspaceTables;
import static io.debezium.connector.cassandra.utils.TestUtils.deleteTestOffsets;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.management.ManagementFactory;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.cassandra.CassandraConnectorTestBase;
import io.debezium.connector.cassandra.spi.ProvidersResolver;
import io.debezium.connector.cassandra.utils.TestUtils;
import io.debezium.connector.common.CdcSourceTaskContext;

class CassandraStreamingMetricsTest extends CassandraConnectorTestBase {

    private CassandraStreamingMetrics streamingMetrics;
    private MBeanServer mBeanServer;
    private ObjectName streamingMetricsObjectName;

    @BeforeEach
    void setUp() throws Exception {
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

    @AfterEach
    void tearDown() throws Exception {
        deleteTestOffsets(context);
        streamingMetrics.unregisterMetrics();
        deleteTestKeyspaceTables();
        context.cleanUp();
    }

    @Test
    void testStreamingMetricsRegistration() throws Exception {
        // Verify that streaming metrics MBean is registered
        assertTrue(mBeanServer.isRegistered(streamingMetricsObjectName),
                "Streaming metrics MBean should be registered");

        // Verify initial values
        assertEquals(-1L,
                mBeanServer.getAttribute(streamingMetricsObjectName, "CommitLogPosition"),
                "Initial commit log position should be -1");

        assertEquals(0L,
                mBeanServer.getAttribute(streamingMetricsObjectName, "NumberOfProcessedMutations"),
                "Initial processed mutations should be 0");

        assertEquals(0L,
                mBeanServer.getAttribute(streamingMetricsObjectName, "NumberOfUnrecoverableErrors"),
                "Initial unrecoverable errors should be 0");

        // Commit log filename should be null initially
        assertNull(mBeanServer.getAttribute(streamingMetricsObjectName, "CommitLogFilename"), "Initial commit log filename should be null");
    }

    @Test
    void testStreamingMetricsUpdates() throws Exception {
        // Set commit log filename
        String testFilename = "CommitLog-1234567890.log";
        streamingMetrics.setCommitLogFilename(testFilename);

        assertEquals(testFilename,
                mBeanServer.getAttribute(streamingMetricsObjectName, "CommitLogFilename"),
                "Commit log filename should be updated");

        // Set commit log position
        long testPosition = 12345L;
        streamingMetrics.setCommitLogPosition(testPosition);

        assertEquals(testPosition,
                mBeanServer.getAttribute(streamingMetricsObjectName, "CommitLogPosition"),
                "Commit log position should be updated");

        // Simulate successful mutation processing
        streamingMetrics.onSuccess();
        streamingMetrics.onSuccess();

        assertEquals(2L,
                mBeanServer.getAttribute(streamingMetricsObjectName, "NumberOfProcessedMutations"),
                "Processed mutations should be incremented");

        // Simulate unrecoverable error
        streamingMetrics.onUnrecoverableError();

        assertEquals(1L,
                mBeanServer.getAttribute(streamingMetricsObjectName, "NumberOfUnrecoverableErrors"),
                "Unrecoverable errors should be incremented");
    }

    @Test
    void testStreamingMetricsReset() throws Exception {
        // Update some metrics
        streamingMetrics.setCommitLogFilename("test.log");
        streamingMetrics.setCommitLogPosition(100L);
        streamingMetrics.onSuccess();
        streamingMetrics.onUnrecoverableError();

        // Verify metrics are set
        assertNotNull(mBeanServer.getAttribute(streamingMetricsObjectName, "CommitLogFilename"),
                "Commit log filename should be set");
        assertTrue((Long) mBeanServer.getAttribute(streamingMetricsObjectName, "NumberOfProcessedMutations") > 0,
                "Processed mutations should be greater than 0");

        // Reset metrics
        streamingMetrics.reset();

        // Verify metrics are reset
        assertNull(mBeanServer.getAttribute(streamingMetricsObjectName, "CommitLogFilename"), "Commit log filename should be null after reset");

        assertEquals(-1L,
                mBeanServer.getAttribute(streamingMetricsObjectName, "CommitLogPosition"),
                "Commit log position should be -1 after reset");

        assertEquals(0L,
                mBeanServer.getAttribute(streamingMetricsObjectName, "NumberOfProcessedMutations"),
                "Processed mutations should be 0 after reset");

        assertEquals(0L,
                mBeanServer.getAttribute(streamingMetricsObjectName, "NumberOfUnrecoverableErrors"),
                "Unrecoverable errors should be 0 after reset");
    }

    @Test
    void testStreamingMetricsUnregistration() throws Exception {
        // Verify MBean is registered
        assertTrue(mBeanServer.isRegistered(streamingMetricsObjectName),
                "Streaming metrics MBean should be registered");

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
