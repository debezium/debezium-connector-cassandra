/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.cassandra;

import static io.debezium.connector.cassandra.CassandraConnectorConfig.DEFAULT_COMMIT_LOG_TRANSFER_CLASS;
import static io.debezium.connector.cassandra.CommitLogProcessingResult.Result.COMPLETED_PREMATURELY;
import static io.debezium.connector.cassandra.CommitLogProcessingResult.Result.DOES_NOT_EXIST;
import static io.debezium.connector.cassandra.CommitLogProcessingResult.Result.ERROR;
import static io.debezium.connector.cassandra.CommitLogProcessingResult.Result.OK;

import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.cassandra.exceptions.CassandraConnectorTaskException;

public class CommitLogIdxParser {
    private static final Logger LOGGER = LoggerFactory.getLogger(CommitLogIdxParser.class);

    private final CommitLogSegmentReader commitLogReader;
    private final List<ChangeEventQueue<Event>> queues;
    private final CommitLogProcessorMetrics metrics;

    private final CommitLogTransfer commitLogTransfer;
    private final Set<String> erroneousCommitLogs;
    private boolean completePrematurely = false;
    private final LogicalCommitLog commitLog;
    private final int pollingInterval;
    private final boolean realTimeProcessingEnabled;
    private Integer offset;

    public CommitLogIdxParser(LogicalCommitLog commitLog, final CommitLogProcessorMetrics metrics,
                              final CassandraConnectorContext context,
                              CommitLogSegmentReader commitLogReader) {
        this.queues = context.getQueues();
        this.metrics = metrics;
        this.commitLog = commitLog;
        this.commitLogReader = commitLogReader;
        this.commitLogTransfer = context.getCassandraConnectorConfig().getCommitLogTransfer();
        this.erroneousCommitLogs = context.getErroneousCommitLogs();
        this.pollingInterval = context.getCassandraConnectorConfig().getCommitLogMarkedCompletePollInterval();
        this.realTimeProcessingEnabled = context.getCassandraConnectorConfig().isCommitLogRealTimeProcessingEnabled();
    }

    public void complete() {
        completePrematurely = true;
    }

    private CommitLogProcessingResult parse() {
        try {
            parseIndexFile(commitLog);
            while (!commitLog.completed) {
                LOGGER.debug("Polling for completeness of idx file for: {}", commitLog);
                if (completePrematurely) {
                    LOGGER.warn("{} completed prematurely", commitLog);
                    return new CommitLogProcessingResult(commitLog, COMPLETED_PREMATURELY);
                }

                if (realTimeProcessingEnabled) {
                    Integer commitLogPosition;
                    if (offset == null) {
                        LOGGER.debug("Start to read the partial file : {}", commitLog);
                        commitLogPosition = 0;
                    }
                    else if (offset < commitLog.offsetOfEndOfLastWrittenCDCMutation) {
                        LOGGER.debug("Resume to read the partial file: {}", commitLog);
                        commitLogPosition = offset;
                    }
                    else {
                        LOGGER.debug("No movement in offset in idx file: {}", commitLog);
                        commitLogPosition = null;
                    }

                    if (commitLogPosition != null) {
                        metrics.setCommitLogPosition(commitLogPosition);
                        processCommitLog(commitLog, commitLogPosition);
                        offset = commitLog.offsetOfEndOfLastWrittenCDCMutation;
                    }
                }

                LOGGER.debug("Sleep for idx file to be complete");
                Thread.sleep(pollingInterval);
                parseIndexFile(commitLog);
            }

            LOGGER.info("Completed idx file for: {}", commitLog);
            int commitLogPosition = offset == null ? 0 : offset;
            metrics.setCommitLogPosition(commitLogPosition);
            processCommitLog(commitLog, commitLogPosition);
            return new CommitLogProcessingResult(commitLog);
        }
        catch (final Exception ex) {
            LOGGER.error("Processing of {} errored out", commitLog, ex);
            return new CommitLogProcessingResult(commitLog, ERROR, ex);
        }
    }

    public CommitLogProcessingResult process() {
        if (!commitLog.exists()) {
            LOGGER.warn("Commit log " + commitLog + " does not exist!");
            return new CommitLogProcessingResult(commitLog, DOES_NOT_EXIST);
        }

        LOGGER.info("Processing commit log {}", commitLog.log.toString());
        metrics.setCommitLogFilename(commitLog.log.toString());
        metrics.setCommitLogPosition(0);

        CommitLogProcessingResult result = parse();
        if (result.result == OK || result.result == ERROR) {
            enqueueEOFEvent();
        }
        CommitLogIdxProcessor.removeProcessing(this);
        LOGGER.debug("Processing {} callables.", CommitLogIdxProcessor.submittedProcessings.size());
        return result;
    }

    private void enqueueEOFEvent() {
        try {
            queues.get(Math.abs(commitLog.log.getName().hashCode() % queues.size())).enqueue(new EOFEvent(commitLog.log));
        }
        catch (InterruptedException e) {
            throw new CassandraConnectorTaskException(String.format(
                    "Enqueuing has been interrupted while enqueuing EOF Event for file %s", commitLog.log.getName()), e);
        }
    }

    private void processCommitLog(LogicalCommitLog logicalCommitLog, int position) {
        try {
            LOGGER.debug("Starting to read commit log segments {} on position {}", logicalCommitLog, position);
            commitLogReader.readCommitLogSegment(logicalCommitLog.log, logicalCommitLog.commitLogSegmentId,
                    position);
            LOGGER.debug("Finished reading commit log segments {} on position {}", logicalCommitLog, position);
        }
        catch (Exception e) {
            if (commitLogTransfer.getClass().getName().equals(DEFAULT_COMMIT_LOG_TRANSFER_CLASS)) {
                throw new DebeziumException(String.format("Error occurred while processing commit log %s",
                        logicalCommitLog.log), e);
            }
            LOGGER.error("Error occurred while processing commit log " + logicalCommitLog.log, e);
            erroneousCommitLogs.add(logicalCommitLog.log.getName());
            enqueueEOFEvent();
        }
    }

    private void parseIndexFile(LogicalCommitLog commitLog) throws DebeziumException {
        try {
            commitLog.parseCommitLogIndex();
        }
        catch (final DebeziumException ex) {
            erroneousCommitLogs.add(commitLog.log.getName());
            throw ex;
        }
    }
}
