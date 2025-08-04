/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.metrics;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import io.debezium.annotation.ThreadSafe;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.metrics.Metrics;
import io.debezium.pipeline.metrics.traits.SnapshotMetricsMXBean;

/**
 * Standard snapshot metrics for Cassandra connector that follows Debezium naming conventions.
 * JMX ObjectName pattern: debezium.cassandra:type=connector-metrics,context=snapshot,server=<logical-name>
 */
@ThreadSafe
public class CassandraSnapshotMetrics extends Metrics implements SnapshotMetricsMXBean {

    private final AtomicInteger totalTableCount = new AtomicInteger();
    private final AtomicInteger remainingTableCount = new AtomicInteger();
    private final AtomicBoolean snapshotRunning = new AtomicBoolean();
    private final AtomicBoolean snapshotCompleted = new AtomicBoolean();
    private final AtomicBoolean snapshotAborted = new AtomicBoolean();
    private final AtomicLong snapshotStartTime = new AtomicLong();
    private final AtomicLong snapshotCompletedTime = new AtomicLong();
    private final AtomicLong snapshotAbortedTime = new AtomicLong();
    private final ConcurrentMap<String, Long> rowsScanned = new ConcurrentHashMap<>();

    public CassandraSnapshotMetrics(CdcSourceTaskContext taskContext) {
        super(taskContext, "snapshot");
    }

    public void reset() {
        totalTableCount.set(0);
        remainingTableCount.set(0);
        snapshotRunning.set(false);
        snapshotCompleted.set(false);
        snapshotAborted.set(false);
        snapshotStartTime.set(0L);
        snapshotCompletedTime.set(0L);
        snapshotAbortedTime.set(0L);
        rowsScanned.clear();
    }

    public void registerMetrics() {
        register();
    }

    public void unregisterMetrics() {
        unregister();
    }

    @Override
    public int getTotalTableCount() {
        return totalTableCount.get();
    }

    @Override
    public int getRemainingTableCount() {
        return remainingTableCount.get();
    }

    @Override
    public boolean getSnapshotCompleted() {
        return snapshotCompleted.get();
    }

    @Override
    public boolean getSnapshotRunning() {
        return snapshotRunning.get();
    }

    @Override
    public boolean getSnapshotAborted() {
        return snapshotAborted.get();
    }

    @Override
    public Map<String, Long> getRowsScanned() {
        return rowsScanned;
    }

    @Override
    public long getSnapshotDurationInSeconds() {
        return snapshotDurationInSeconds();
    }

    public long snapshotDurationInSeconds() {
        long startMillis = snapshotStartTime.get();
        if (startMillis == 0L) {
            return 0;
        }
        long stopMillis = snapshotCompletedTime.get();
        if (snapshotAbortedTime.get() > 0L) {
            stopMillis = snapshotAbortedTime.get();
        }
        if (stopMillis <= 0L) {
            stopMillis = System.currentTimeMillis();
        }
        return (stopMillis - startMillis) / 1000L;
    }

    public void setTableCount(int count) {
        totalTableCount.set(count);
        remainingTableCount.set(count);
    }

    public void completeTable() {
        remainingTableCount.decrementAndGet();
    }

    public void startSnapshot() {
        snapshotRunning.set(true);
        snapshotCompleted.set(false);
        snapshotAborted.set(false);
        snapshotStartTime.set(System.currentTimeMillis());
        snapshotCompletedTime.set(0L);
        snapshotAbortedTime.set(0L);
    }

    public void stopSnapshot() {
        snapshotCompleted.set(true);
        snapshotAborted.set(false);
        snapshotRunning.set(false);
        snapshotCompletedTime.set(System.currentTimeMillis());
    }

    public void abortSnapshot() {
        snapshotCompleted.set(false);
        snapshotAborted.set(true);
        snapshotRunning.set(false);
        snapshotAbortedTime.set(System.currentTimeMillis());
    }

    public void setRowsScanned(String key, Long value) {
        rowsScanned.put(key, value);
    }

    /**
     * These metrics are not implemented for the Cassandra connector.
     * They return default or empty values to satisfy the SnapshotMetricsMXBean interface.
     */
    @Override
    public boolean getSnapshotPaused() {
        return false;
    }

    @Override
    public boolean getSnapshotSkipped() {
        return false;
    }

    @Override
    public String getChunkFrom() {
        return "";
    }

    @Override
    public String getChunkTo() {
        return "";
    }

    @Override
    public String getTableFrom() {
        return "";
    }

    @Override
    public String getTableTo() {
        return "";
    }

    @Override
    public long getSnapshotPausedDurationInSeconds() {
        return 0;
    }

    @Override
    public String[] getCapturedTables() {
        return new String[0];
    }

    @Override
    public String getChunkId() {
        return "";
    }
}
