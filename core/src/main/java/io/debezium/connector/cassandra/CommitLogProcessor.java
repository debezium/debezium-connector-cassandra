/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.cassandra.exceptions.CassandraConnectorTaskException;

/**
 * The {@link CommitLogProcessor} is used to process CommitLog in CDC directory.
 * Upon readCommitLog, it processes the entire CommitLog specified in the {@link CassandraConnectorConfig}
 * and converts each row change in the commit log into a {@link Record},
 * and then emit the log via a {@link KafkaRecordEmitter}.
 */
public class CommitLogProcessor extends AbstractProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(CommitLogProcessor.class);

    private static final String NAME = "Commit Log Processor";

    private final CassandraConnectorContext context;
    private final CommitLogSegmentReader commitLogReader;
    private final File cdcDir;
    private AbstractDirectoryWatcher watcher;
    private final List<ChangeEventQueue<Event>> queues;
    private final boolean latestOnly;
    private final CommitLogProcessorMetrics metrics;
    private boolean initial = true;
    private final boolean errorCommitLogReprocessEnabled;
    private final CommitLogTransfer commitLogTransfer;
    private final Set<String> erroneousCommitLogs;
    private final File commitLogDir;

    public CommitLogProcessor(CassandraConnectorContext context, CommitLogProcessorMetrics metrics,
                              CommitLogSegmentReader commitLogReader, File cdcDir,
                              File commitLogDir) {
        super(NAME, Duration.ZERO);
        this.commitLogReader = commitLogReader;
        this.queues = context.getQueues();
        this.context = context;
        this.metrics = metrics;
        this.cdcDir = cdcDir;
        latestOnly = this.context.getCassandraConnectorConfig().latestCommitLogOnly();
        errorCommitLogReprocessEnabled = this.context.getCassandraConnectorConfig().errorCommitLogReprocessEnabled();
        commitLogTransfer = this.context.getCassandraConnectorConfig().getCommitLogTransfer();
        erroneousCommitLogs = this.context.getErroneousCommitLogs();
        this.commitLogDir = commitLogDir;
    }

    @Override
    public void initialize() {
        metrics.registerMetrics();
    }

    @Override
    public void destroy() {
        metrics.unregisterMetrics();
    }

    @Override
    public void process() throws IOException, InterruptedException {
        LOGGER.debug("Processing commitLogFiles while initial is {}", initial);

        if (watcher == null) {
            watcher = new AbstractDirectoryWatcher(cdcDir.toPath(),
                    this.context.getCassandraConnectorConfig().cdcDirPollInterval(),
                    Collections.singleton(ENTRY_CREATE)) {
                @Override
                void handleEvent(WatchEvent<?> event, Path path) {
                    if (isRunning()) {
                        processCommitLog(path.toFile());
                    }
                }
            };
        }

        if (latestOnly) {
            processLastModifiedCommitLog();
            throw new InterruptedException();
        }
        if (initial) {
            LOGGER.info("Reading existing commit logs in {}", cdcDir);
            File[] commitLogFiles = CommitLogUtil.getCommitLogs(cdcDir);
            Arrays.sort(commitLogFiles, CommitLogUtil::compareCommitLogs);
            for (File commitLogFile : commitLogFiles) {
                if (isRunning()) {
                    processCommitLog(commitLogFile);
                }
            }
            // If commit.log.error.reprocessing.enabled is set to true, download all error commitLog files upon starting for re-processing.
            if (errorCommitLogReprocessEnabled) {
                LOGGER.info("CommitLog Error Processing is enabled. Attempting to get all error commitLog files for re-processing.");
                commitLogTransfer.getErrorCommitLogFiles();
            }
            initial = false;
        }
        watcher.poll();
    }

    void processCommitLog(File file) {
        if (file == null) {
            LOGGER.warn("Commit log is null");
            return;
        }
        if (!file.exists()) {
            LOGGER.warn("Commit log " + file.getName() + " does not exist");
            return;
        }
        try {
            try {
                LOGGER.info("Processing commit log {}", file.getName());
                metrics.setCommitLogFilename(file.getName());
                commitLogReader.readCommitLogSegment(file, -1L, 0);
                if (!latestOnly) {
                    queues.get(Math.abs(file.getName().hashCode() % queues.size())).enqueue(new EOFEvent(file));
                }
                LOGGER.info("Successfully processed commit log {}", file.getName());
            }
            catch (Exception e) {
                if (commitLogTransfer.getClass().getName().equals(CassandraConnectorConfig.DEFAULT_COMMIT_LOG_TRANSFER_CLASS)) {
                    throw new DebeziumException(String.format("Error occurred while processing commit log %s",
                            file.getName()), e);
                }
                LOGGER.error("Error occurred while processing commit log " + file.getName(), e);
                if (!latestOnly) {
                    queues.get(Math.abs(file.getName().hashCode() % queues.size())).enqueue(new EOFEvent(file));
                    erroneousCommitLogs.add(file.getName());
                }
            }
        }
        catch (InterruptedException e) {
            throw new CassandraConnectorTaskException(String.format(
                    "Enqueuing has been interrupted while enqueuing EOF Event for file %s", file.getName()), e);
        }
    }

    void processLastModifiedCommitLog() {
        LOGGER.warn("CommitLogProcessor will read the last modified commit log from the COMMIT LOG "
                + "DIRECTORY based on modified timestamp, NOT FROM THE CDC_RAW DIRECTORY. This method "
                + "should not be used in PRODUCTION!");
        File[] files = CommitLogUtil.getCommitLogs(commitLogDir);

        File lastModified = null;
        for (File file : files) {
            if (lastModified == null || lastModified.lastModified() < file.lastModified()) {
                lastModified = file;
            }
        }

        if (lastModified != null) {
            processCommitLog(lastModified);
        }
        else {
            LOGGER.debug("No commit logs found in {}", commitLogDir);
        }
    }
}
