/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

public class CommitLogProcessingResult {
    public enum Result {
        OK,
        ERROR,
        DOES_NOT_EXIST,
        COMPLETED_PREMATURELY;
    }

    public final LogicalCommitLog commitLog;
    public final Result result;
    public final Exception ex;

    public CommitLogProcessingResult(LogicalCommitLog commitLog) {
        this(commitLog, Result.OK, null);
    }

    public CommitLogProcessingResult(LogicalCommitLog commitLog, Result result) {
        this(commitLog, result, null);
    }

    public CommitLogProcessingResult(LogicalCommitLog commitLog, Result result, Exception ex) {
        this.commitLog = commitLog;
        this.result = result;
        this.ex = ex;
    }

    @Override
    public String toString() {
        return "ProcessingResult{" +
                "commitLog=" + commitLog +
                ", result=" + result +
                ", ex=" + (ex != null ? ex.getMessage() : "none") + '}';
    }

}
