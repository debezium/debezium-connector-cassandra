/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dse;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLogReadHandler;

import io.debezium.connector.cassandra.AbstractProcessor;
import io.debezium.connector.cassandra.CassandraConnectorContext;
import io.debezium.connector.cassandra.CommitLogIdxProcessor;
import io.debezium.connector.cassandra.metrics.CassandraStreamingMetrics;

public class Dse6816ConnectorTask extends Dse680ConnectorTask {

    @Override
    protected AbstractProcessor getCommitLogProcessor(CassandraConnectorContext context, CassandraStreamingMetrics metrics, CommitLogReadHandler handler) {
        return new CommitLogIdxProcessor(context, metrics, new DseCommitLogSegmentReader(context, metrics), DatabaseDescriptor.getCDCLogLocation());
    }

}
