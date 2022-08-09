/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static io.debezium.connector.cassandra.Cassandra4CommitLogProcessor.ProcessingResult.Result.ERROR;

import java.util.List;

import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.base.ChangeEventQueue;

public class Cassandra4CommitLogRealTimeParser extends AbstractCassandra4CommitLogParser {
    private static final Logger LOGGER = LoggerFactory.getLogger(Cassandra4CommitLogRealTimeParser.class);
    private Integer offset = null;

    public Cassandra4CommitLogRealTimeParser(Cassandra4CommitLogProcessor.LogicalCommitLog commitLog, List<ChangeEventQueue<Event>> queues,
                                             CommitLogProcessorMetrics metrics, CassandraConnectorContext context) {
        super(commitLog, queues, metrics, context);
    }

    @Override
    public Cassandra4CommitLogProcessor.ProcessingResult parse() {
        try {
            parseIndexFile(commitLog);
            while (!commitLog.completed) {
                LOGGER.info("Polling for completeness of idx file for: {}", commitLog.toString());
                if (completePrematurely) {
                    LOGGER.info("{} completed prematurely", commitLog.toString());
                    return new Cassandra4CommitLogProcessor.ProcessingResult(commitLog, Cassandra4CommitLogProcessor.ProcessingResult.Result.COMPLETED_PREMATURELY);
                }

                CommitLogPosition commitLogPosition = null;
                if (offset == null) {
                    LOGGER.info("Start to read the partial file : {}", commitLog.toString());
                    commitLogPosition = new CommitLogPosition(commitLog.commitLogSegmentId, 0);
                }
                else if (offset < commitLog.offsetOfEndOfLastWrittenCDCMutation) {
                    LOGGER.info("Resume to read the partial file : {}", commitLog.toString());
                    commitLogPosition = new CommitLogPosition(commitLog.commitLogSegmentId, offset);
                }
                else {
                    LOGGER.info("No movement in offset in IDX file: {}", commitLog.toString());
                }

                if (commitLogPosition != null) {
                    processCommitLog(commitLog, commitLogPosition);
                    offset = commitLog.offsetOfEndOfLastWrittenCDCMutation;
                    metrics.setCommitLogPosition(commitLogPosition.position);
                }

                LOGGER.info("Sleep for idx file to be complete");
                Thread.sleep(pollingInterval);
                parseIndexFile(commitLog);
            }

            LOGGER.info("IDX file is completed for: {}", commitLog.toString());
            CommitLogPosition commitLogPosition;
            if (offset != null) {
                commitLogPosition = new CommitLogPosition(commitLog.commitLogSegmentId, offset);
            }
            else {
                commitLogPosition = new CommitLogPosition(commitLog.commitLogSegmentId, 0);
            }
            metrics.setCommitLogPosition(commitLogPosition.position);
            processCommitLog(commitLog, commitLogPosition);
            return new Cassandra4CommitLogProcessor.ProcessingResult(commitLog);
        }
        catch (final Exception ex) {
            LOGGER.error("Processing of {} errored out", commitLog.toString(), ex);
            return new Cassandra4CommitLogProcessor.ProcessingResult(commitLog, ERROR, ex);
        }
    }
}
