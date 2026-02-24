/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.metrics;

import java.util.concurrent.atomic.AtomicLong;

import io.debezium.annotation.ThreadSafe;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.metrics.Metrics;

/**
 * Standard streaming metrics for Cassandra connector that follows Debezium naming conventions.
 * JMX ObjectName pattern: debezium.cassandra:type=connector-metrics,context=streaming,server=<logical-name>
 */
@ThreadSafe
public class CassandraStreamingMetrics extends Metrics implements CassandraStreamingMetricsMXBean {

    private volatile String commitLogFilename;
    private final AtomicLong commitLogPosition = new AtomicLong(-1L);
    private final AtomicLong numberOfProcessedMutations = new AtomicLong();
    private final AtomicLong numberOfUnrecoverableErrors = new AtomicLong();
    private final AtomicLong cdcDirectoryTotalBytes = new AtomicLong();

    public CassandraStreamingMetrics(CdcSourceTaskContext taskContext) {
        super(taskContext, "streaming");
    }

    public void reset() {
        numberOfProcessedMutations.set(0);
        numberOfUnrecoverableErrors.set(0);
        commitLogFilename = null;
        commitLogPosition.set(-1L);
        cdcDirectoryTotalBytes.set(0);
    }

    public void registerMetrics() {
        register();
    }

    public void unregisterMetrics() {
        unregister();
    }

    @Override
    public String getCommitLogFilename() {
        return commitLogFilename;
    }

    public void setCommitLogFilename(String commitLogFilename) {
        this.commitLogFilename = commitLogFilename;
        setCommitLogPosition(-1L);
    }

    @Override
    public long getCommitLogPosition() {
        return commitLogPosition.get();
    }

    public void setCommitLogPosition(long position) {
        this.commitLogPosition.set(position);
    }

    @Override
    public long getNumberOfProcessedMutations() {
        return numberOfProcessedMutations.get();
    }

    public void onSuccess() {
        numberOfProcessedMutations.incrementAndGet();
    }

    @Override
    public long getNumberOfUnrecoverableErrors() {
        return numberOfUnrecoverableErrors.get();
    }

    public void onUnrecoverableError() {
        numberOfUnrecoverableErrors.incrementAndGet();
    }

    @Override
    public long getCdcDirectoryTotalBytes() {
        return cdcDirectoryTotalBytes.get();
    }

    public void setCdcDirectoryTotalBytes(long bytes) {
        this.cdcDirectoryTotalBytes.set(bytes);
    }
}