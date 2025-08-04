/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.metrics;

/**
 * MXBean interface for Cassandra streaming metrics to ensure JMX compliance.
 */
public interface CassandraStreamingMetricsMXBean {

    String getCommitLogFilename();

    long getCommitLogPosition();

    long getNumberOfProcessedMutations();

    long getNumberOfUnrecoverableErrors();
}