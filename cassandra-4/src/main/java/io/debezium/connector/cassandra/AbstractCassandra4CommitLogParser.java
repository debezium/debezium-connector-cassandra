/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.cassandra;

import static io.debezium.connector.cassandra.CassandraConnectorConfig.DEFAULT_COMMIT_LOG_TRANSFER_CLASS;
import static io.debezium.connector.cassandra.CommitLogProcessingResult.Result.DOES_NOT_EXIST;

import java.util.List;
import java.util.Set;

import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.CommitLogReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.cassandra.exceptions.CassandraConnectorTaskException;

public abstract class AbstractCassandra4CommitLogParser {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractCassandra4CommitLogParser.class);

    private final CommitLogReader commitLogReader;
    protected final List<ChangeEventQueue<Event>> queues;
    protected final CommitLogProcessorMetrics metrics;
    private final Cassandra4CommitLogReadHandlerImpl commitLogReadHandler;

    private final CommitLogTransfer commitLogTransfer;
    private final Set<String> erroneousCommitLogs;
    protected boolean completePrematurely = false;
    protected LogicalCommitLog commitLog;
    protected int pollingInterval;

    public AbstractCassandra4CommitLogParser(LogicalCommitLog commitLog, final List<ChangeEventQueue<Event>> queues,
                                             final CommitLogProcessorMetrics metrics,
                                             final CassandraConnectorContext context) {
        this.commitLogReader = new CommitLogReader();
        this.queues = queues;
        this.metrics = metrics;
        this.commitLog = commitLog;

        this.commitLogReadHandler = new Cassandra4CommitLogReadHandlerImpl(
                context.getSchemaHolder(),
                context.getQueues(),
                context.getOffsetWriter(),
                new RecordMaker(context.getCassandraConnectorConfig().tombstonesOnDelete(),
                        new Filters(context.getCassandraConnectorConfig().fieldExcludeList()),
                        context.getCassandraConnectorConfig()),
                metrics);

        this.commitLogTransfer = context.getCassandraConnectorConfig().getCommitLogTransfer();
        this.erroneousCommitLogs = context.getErroneousCommitLogs();
        this.pollingInterval = context.getCassandraConnectorConfig().getCommitLogMarkedCompletePollInterval();
    }

    public void complete() {
        completePrematurely = true;
    }

    public abstract CommitLogProcessingResult parse();

    public CommitLogProcessingResult process() {
        if (!commitLog.exists()) {
            LOGGER.warn("Commit log " + commitLog + " does not exist!");
            return new CommitLogProcessingResult(commitLog, DOES_NOT_EXIST);
        }

        LOGGER.info("Processing commit log {}", commitLog.log.toString());
        metrics.setCommitLogFilename(commitLog.log.toString());
        metrics.setCommitLogPosition(0);

        CommitLogProcessingResult result = parse();
        enqueueEOFEvent();
        Cassandra4CommitLogProcessor.removeProcessing(this);
        LOGGER.debug("Processing {} callables.", Cassandra4CommitLogProcessor.submittedProcessings.size());
        return result;
    }

    protected void enqueueEOFEvent() {
        try {
            queues.get(Math.abs(commitLog.log.getName().hashCode() % queues.size())).enqueue(new EOFEvent(commitLog.log));
        }
        catch (InterruptedException e) {
            throw new CassandraConnectorTaskException(String.format(
                    "Enqueuing has been interrupted while enqueuing EOF Event for file %s", commitLog.log.getName()), e);
        }
    }

    protected void processCommitLog(LogicalCommitLog logicalCommitLog, CommitLogPosition position) {
        try {
            LOGGER.debug("Starting to read commit log segments {} on position {}", logicalCommitLog, position);
            commitLogReader.readCommitLogSegment(commitLogReadHandler, logicalCommitLog.log, position, false);
            LOGGER.debug("Finished reading commit log segments {} on position {}", logicalCommitLog, position);
        }
        catch (Exception e) {
            if (commitLogTransfer.getClass().getName().equals(DEFAULT_COMMIT_LOG_TRANSFER_CLASS)) {
                throw new DebeziumException(String.format("Error occurred while processing commit log %s",
                        logicalCommitLog.log), e);
            }
            LOGGER.error("Error occurred while processing commit log " + logicalCommitLog.log, e);
            enqueueEOFEvent();
            erroneousCommitLogs.add(logicalCommitLog.log.getName());
        }
    }

    protected void parseIndexFile(LogicalCommitLog commitLog) throws DebeziumException {
        try {
            commitLog.parseCommitLogIndex();
        }
        catch (final DebeziumException ex) {
            erroneousCommitLogs.add(commitLog.log.getName());
            throw ex;
        }
    }
}
