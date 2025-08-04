/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.io.File;
import java.io.IOException;

import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.CommitLogReadHandler;
import org.apache.cassandra.db.commitlog.CommitLogReader;

import io.debezium.connector.cassandra.metrics.CassandraStreamingMetrics;

public class Cassandra5CommitLogSegmentReader implements CommitLogSegmentReader {

    private final CommitLogReader commitLogReader;

    private final CommitLogReadHandler commitLogReadHandler;

    public Cassandra5CommitLogSegmentReader(CassandraConnectorContext context, CassandraStreamingMetrics metrics) {
        this.commitLogReader = new CommitLogReader();
        this.commitLogReadHandler = new Cassandra5CommitLogReadHandlerImpl(context, metrics);
    }

    @Override
    public void readCommitLogSegment(File file, long segmentId, int position) throws IOException {
        commitLogReader.readCommitLogSegment(commitLogReadHandler, new org.apache.cassandra.io.util.File(file), new CommitLogPosition(segmentId, position),
                CommitLogReader.ALL_MUTATIONS, false);
    }

}
