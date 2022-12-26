/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.io.File;
import java.io.IOException;

public interface CommitLogSegmentReader {

    void readCommitLogSegment(File file, long segmentId, int position) throws IOException;

}
