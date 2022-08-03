/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.cassandra;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.CommitLogReader;
import org.apache.commons.math3.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.cassandra.exceptions.CassandraConnectorTaskException;

public abstract class Cassandra4CommitLogParserBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(Cassandra4CommitLogParserBase.class);
    private static final String ARCHIVE_FOLDER = "archive";
    private static final String ERROR_FOLDER = "error";

    private final CommitLogReader commitLogReader;
    private final List<ChangeEventQueue<Event>> queues;
    protected final CommitLogProcessorMetrics metrics;
    private final Cassandra4CommitLogReadHandlerImpl commitLogReadHandler;

    private final CommitLogTransfer commitLogTransfer;
    private final Set<String> erroneousCommitLogs;
    protected boolean completePrematurely = false;

    private final String commitLogRelocationDirectory;
    private final ExecutorService executorService;
    private final Set<Pair<Cassandra4CommitLogProcessor.LogicalCommitLog, Future<Cassandra4CommitLogProcessor.ProcessingResult>>> submittedProcessings = ConcurrentHashMap
            .newKeySet();

    public Cassandra4CommitLogParserBase(final List<ChangeEventQueue<Event>> queues,
                                         final CommitLogProcessorMetrics metrics,
                                         final CassandraConnectorContext context) {
        this.commitLogReader = new CommitLogReader();
        this.queues = queues;
        this.metrics = metrics;

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
        this.commitLogRelocationDirectory = context.getCassandraConnectorConfig().commitLogRelocationDir();
        this.executorService = Executors.newSingleThreadExecutor();

    }

    public void complete() {
        completePrematurely = true;
    }

    public abstract Cassandra4CommitLogProcessor.ProcessingResult parse(Cassandra4CommitLogProcessor.LogicalCommitLog commitLog);

    public Future<Cassandra4CommitLogProcessor.ProcessingResult> process(Cassandra4CommitLogProcessor.LogicalCommitLog commitLog) {
        Future<Cassandra4CommitLogProcessor.ProcessingResult> result = executorService.submit(() -> parse(commitLog));
        submittedProcessings.add(new Pair<>(commitLog, result));
        return result;
    }

    protected void processCommitLog(Cassandra4CommitLogProcessor.LogicalCommitLog logicalCommitLog, CommitLogPosition position) {
        try {
            try {
                commitLogReader.readCommitLogSegment(commitLogReadHandler, logicalCommitLog.log, position, false);
            }
            catch (Exception e) {
                if (commitLogTransfer.getClass().getName().equals(CassandraConnectorConfig.DEFAULT_COMMIT_LOG_TRANSFER_CLASS)) {
                    throw new DebeziumException(String.format("Error occurred while processing commit log %s",
                            logicalCommitLog.log), e);
                }
                LOGGER.error("Error occurred while processing commit log " + logicalCommitLog.log, e);
                queues.get(Math.abs(logicalCommitLog.log.getName().hashCode() % queues.size())).enqueue(new EOFEvent(logicalCommitLog.log));
                erroneousCommitLogs.add(logicalCommitLog.log.getName());
            }
        }
        catch (InterruptedException e) {
            throw new CassandraConnectorTaskException(String.format(
                    "Enqueuing has been interrupted while enqueuing EOF Event for file %s", logicalCommitLog.log.getName()), e);
        }
    }

    private void moveCommitLog(Cassandra4CommitLogProcessor.LogicalCommitLog commitLog) {
        if (erroneousCommitLogs.contains(commitLog)) {
            Path relocationDir = Paths.get(commitLogRelocationDirectory, ERROR_FOLDER);
            CommitLogUtil.moveCommitLog(commitLog.log.toPath(), relocationDir);
        }
        else {
            LOGGER.info("Successfully processed: {}, will move to archive folder", commitLog.toString());
            Path relocationDir = Paths.get(commitLogRelocationDirectory, ARCHIVE_FOLDER);
            CommitLogUtil.moveCommitLog(commitLog.log.toPath(), relocationDir);
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

    public boolean isRunning() {
        return !executorService.isShutdown() && !executorService.isTerminated();
    }

    public void stop() {
        try {
            for (final Pair<Cassandra4CommitLogProcessor.LogicalCommitLog, Future<Cassandra4CommitLogProcessor.ProcessingResult>> submittedProcessing : submittedProcessings) {
                try {
                    complete();
                    submittedProcessing.getSecond().get();
                }
                catch (final Exception ex) {
                    LOGGER.warn("Waiting for submitted task for commit log: {}.", submittedProcessing.getFirst(), ex);
                }
            }
            executorService.shutdown();
        }
        catch (final Exception ex) {
            throw new RuntimeException("Unable to close executor service in CommitLogProcessor in a timely manner");
        }
    }

    protected void markAndMoveCommitLog(Cassandra4CommitLogProcessor.LogicalCommitLog commitLog) {
        submittedProcessings.remove(commitLog);
        moveCommitLog(commitLog);
    }
}