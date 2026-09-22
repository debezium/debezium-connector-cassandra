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

import java.io.File;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.cassandra.exceptions.CassandraConnectorTaskException;
import io.debezium.connector.cassandra.metrics.CassandraStreamingMetrics;

public class CommitLogIdxParser {
    private static final Logger LOGGER = LoggerFactory.getLogger(CommitLogIdxParser.class);

    private final CommitLogSegmentReader commitLogReader;
    private final List<ChangeEventQueue<Event>> queues;
    private final CassandraStreamingMetrics metrics;

    private final CommitLogTransfer commitLogTransfer;
    private final Set<String> erroneousCommitLogs;
    private boolean completePrematurely = false;
    private final LogicalCommitLog commitLog;
    private final int pollingInterval;
    private final boolean realTimeProcessingEnabled;
    private Integer offset;

    public CommitLogIdxParser(LogicalCommitLog commitLog, final CassandraStreamingMetrics metrics,
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

    LogicalCommitLog getCommitLog() {
        return commitLog;
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

                int offsetBeforeSleep = commitLog.offsetOfEndOfLastWrittenCDCMutation;

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

                if (!commitLog.completed && commitLog.offsetOfEndOfLastWrittenCDCMutation == offsetBeforeSleep && isAbandoned()) {
                    // A newer segment exists, so this one is abandoned rather than merely idle.
                    LOGGER.warn("Idx offset for {} has not advanced and a newer commit log segment already exists - " +
                            "treating segment as abandoned, completing at offset {}",
                            commitLog, commitLog.offsetOfEndOfLastWrittenCDCMutation);
                    commitLog.completed = true;
                }
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
            // always the cdc_raw/ location, even if exists() fell back to commitlog/ for reading -
            // that is where the sibling .idx lives and where cleanup looks for both files
            File canonicalLog = new File(commitLog.index.getParentFile(), commitLog.log.getName());
            queues.get(Math.abs(commitLog.log.getName().hashCode() % queues.size())).enqueue(new EOFEvent(canonicalLog));
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

    private boolean isAbandoned() {
        File cdcRawDir = commitLog.index.getParentFile();
        if (cdcRawDir == null || !cdcRawDir.isDirectory()) {
            return false;
        }
        File[] siblingIndexes = CommitLogUtil.getIndexes(cdcRawDir);
        if (siblingIndexes != null) {
            for (File siblingIndex : siblingIndexes) {
                if (CommitLogUtil.compareCommitLogsIndexes(siblingIndex, commitLog.index) > 0) {
                    return true;
                }
            }
        }
        // the .log is hard-linked into cdc_raw/ at allocation, before its .idx is ever written,
        // so a newer .log sibling can prove abandonment before any newer .idx exists
        File[] siblingLogs = CommitLogUtil.getCommitLogs(cdcRawDir);
        if (siblingLogs != null) {
            for (File siblingLog : siblingLogs) {
                if (CommitLogUtil.compareCommitLogs(siblingLog, commitLog.log) > 0) {
                    return true;
                }
            }
        }
        return false;
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
