/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.cassandra;

import java.util.List;
import java.util.Set;

import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.CommitLogReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.cassandra.exceptions.CassandraConnectorTaskException;

public abstract class Cassandra4CommitLogParserBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(Cassandra4CommitLogParserBase.class);

    private final CommitLogReader commitLogReader;
    protected final List<ChangeEventQueue<Event>> queues;
    protected final CommitLogProcessorMetrics metrics;
    private final Cassandra4CommitLogReadHandlerImpl commitLogReadHandler;

    private final CommitLogTransfer commitLogTransfer;
    private final Set<String> erroneousCommitLogs;
    protected boolean completePrematurely = false;
    protected Cassandra4CommitLogProcessor.LogicalCommitLog commitLog;

    public Cassandra4CommitLogParserBase(Cassandra4CommitLogProcessor.LogicalCommitLog commitLog, final List<ChangeEventQueue<Event>> queues,
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
    }

    public void complete() {
        completePrematurely = true;
    }

    public abstract Cassandra4CommitLogProcessor.ProcessingResult parse();

    public Cassandra4CommitLogProcessor.ProcessingResult process() {
        if (!commitLog.exists()) {
            LOGGER.warn("Commit log " + commitLog + " does not exist!");
            return new Cassandra4CommitLogProcessor.ProcessingResult(commitLog, Cassandra4CommitLogProcessor.ProcessingResult.Result.DOES_NOT_EXIST);
        }

        LOGGER.info("Processing commit log {}", commitLog.log.toString());
        metrics.setCommitLogFilename(commitLog.log.toString());
        metrics.setCommitLogPosition(0);

        Cassandra4CommitLogProcessor.ProcessingResult result = parse();
        enqueueEOFEvent();
        Cassandra4CommitLogProcessor.submittedProcessings.remove(this);
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

    protected void processCommitLog(Cassandra4CommitLogProcessor.LogicalCommitLog logicalCommitLog, CommitLogPosition position) {
        try {
            commitLogReader.readCommitLogSegment(commitLogReadHandler, logicalCommitLog.log, position, false);
        }
        catch (Exception e) {
            if (commitLogTransfer.getClass().getName().equals(CassandraConnectorConfig.DEFAULT_COMMIT_LOG_TRANSFER_CLASS)) {
                throw new DebeziumException(String.format("Error occurred while processing commit log %s",
                        logicalCommitLog.log), e);
            }
            LOGGER.error("Error occurred while processing commit log " + logicalCommitLog.log, e);
            enqueueEOFEvent();
            erroneousCommitLogs.add(logicalCommitLog.log.getName());
        }
    }

    protected void parseIndexFile(Cassandra4CommitLogProcessor.LogicalCommitLog commitLog) throws DebeziumException {
        try {
            commitLog.parseCommitLogIndex();
        }
        catch (final DebeziumException ex) {
            erroneousCommitLogs.add(commitLog.log.getName());
            throw ex;
        }
    }
}
