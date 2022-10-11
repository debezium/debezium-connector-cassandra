/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static io.debezium.connector.cassandra.CommitLogProcessingResult.Result.COMPLETED_PREMATURELY;
import static io.debezium.connector.cassandra.CommitLogProcessingResult.Result.ERROR;

import java.util.List;

import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.base.ChangeEventQueue;

public class Cassandra4CommitLogRealTimeParser extends AbstractCassandra4CommitLogParser {
    private static final Logger LOGGER = LoggerFactory.getLogger(Cassandra4CommitLogRealTimeParser.class);
    private Integer offset = null;

    public Cassandra4CommitLogRealTimeParser(LogicalCommitLog commitLog, List<ChangeEventQueue<Event>> queues,
                                             CommitLogProcessorMetrics metrics, CassandraConnectorContext context) {
        super(commitLog, queues, metrics, context);
    }

    @Override
    public CommitLogProcessingResult parse() {
        try {
            parseIndexFile(commitLog);
            while (!commitLog.completed) {
                LOGGER.debug("Polling for completeness of idx file for: {}", commitLog);
                if (completePrematurely) {
                    LOGGER.warn("{} completed prematurely", commitLog);
                    return new CommitLogProcessingResult(commitLog, COMPLETED_PREMATURELY);
                }

                CommitLogPosition commitLogPosition = null;
                if (offset == null) {
                    LOGGER.debug("Start to read the partial file : {}", commitLog);
                    commitLogPosition = new CommitLogPosition(commitLog.commitLogSegmentId, 0);
                }
                else if (offset < commitLog.offsetOfEndOfLastWrittenCDCMutation) {
                    LOGGER.debug("Resume to read the partial file: {}", commitLog);
                    commitLogPosition = new CommitLogPosition(commitLog.commitLogSegmentId, offset);
                }
                else {
                    LOGGER.debug("No movement in offset in idx file: {}", commitLog);
                }

                if (commitLogPosition != null) {
                    processCommitLog(commitLog, commitLogPosition);
                    offset = commitLog.offsetOfEndOfLastWrittenCDCMutation;
                    metrics.setCommitLogPosition(commitLogPosition.position);
                }

                LOGGER.debug("Sleep for idx file to be complete");
                Thread.sleep(pollingInterval);
                parseIndexFile(commitLog);
            }

            LOGGER.info("Completed idx file for: {}", commitLog);
            CommitLogPosition commitLogPosition;
            if (offset != null) {
                commitLogPosition = new CommitLogPosition(commitLog.commitLogSegmentId, offset);
            }
            else {
                commitLogPosition = new CommitLogPosition(commitLog.commitLogSegmentId, 0);
            }
            metrics.setCommitLogPosition(commitLogPosition.position);
            processCommitLog(commitLog, commitLogPosition);
            return new CommitLogProcessingResult(commitLog);
        }
        catch (final Exception ex) {
            LOGGER.error("Processing of {} errored out", commitLog, ex);
            return new CommitLogProcessingResult(commitLog, ERROR, ex);
        }
    }
}
