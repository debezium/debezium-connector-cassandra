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

public class Cassandra4CommitLogSegmentReader implements CommitLogSegmentReader {

    private final CommitLogReader commitLogReader;

    private final CommitLogReadHandler commitLogReadHandler;

    public Cassandra4CommitLogSegmentReader(CassandraConnectorContext context, CommitLogProcessorMetrics metrics) {
        this.commitLogReader = new CommitLogReader();
        this.commitLogReadHandler = new Cassandra4CommitLogReadHandlerImpl(context, metrics);
    }

    @Override
    public void readCommitLogSegment(File file, long segmentId, int position) throws IOException {
        commitLogReader.readCommitLogSegment(commitLogReadHandler, file, new CommitLogPosition(segmentId, position),
                CommitLogReader.ALL_MUTATIONS, false);
    }

}
