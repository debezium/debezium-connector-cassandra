/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.base.ChangeEventQueue;

/**
 * The {@link Cassandra4CommitLogProcessor} is used to process CommitLog in CDC directory.
 * Upon readCommitLog, it processes the entire CommitLog specified in the {@link CassandraConnectorConfig}
 * and converts each row change in the commit log into a {@link Record},
 * and then emit the log via a {@link KafkaRecordEmitter}.
 */
public class Cassandra4CommitLogProcessor extends AbstractProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(Cassandra4CommitLogProcessor.class);

    private static final String NAME = "Commit Log Processor";

    private final CassandraConnectorContext context;
    private final File cdcDir;
    private AbstractDirectoryWatcher watcher;
    private final List<ChangeEventQueue<Event>> queues;
    private final CommitLogProcessorMetrics metrics = new CommitLogProcessorMetrics();
    private boolean initial = true;
    private final boolean errorCommitLogReprocessEnabled;
    private final CommitLogTransfer commitLogTransfer;
    private final Cassandra4CommitLogParserBase cassandra4CommitLogParserBase;

    public Cassandra4CommitLogProcessor(CassandraConnectorContext context) {
        super(NAME, Duration.ZERO);
        this.context = context;
        queues = this.context.getQueues();
        commitLogTransfer = this.context.getCassandraConnectorConfig().getCommitLogTransfer();
        errorCommitLogReprocessEnabled = this.context.getCassandraConnectorConfig().errorCommitLogReprocessEnabled();
        cdcDir = new File(DatabaseDescriptor.getCDCLogLocation());

        if (context.getCassandraConnectorConfig().isCommitLogRealTimeProcessingEnabled()) {
            cassandra4CommitLogParserBase = new Cassandra4CommitLogNearRealTimeParser(queues, metrics, this.context);
        }
        else {
            cassandra4CommitLogParserBase = new Cassandra4CommitLogBatchParser(queues, metrics, this.context);
        }
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
    public void stop() {
        cassandra4CommitLogParserBase.stop();
        super.stop();
    }

    private void submit(Path index) {
        cassandra4CommitLogParserBase.process(new LogicalCommitLog(index.toFile()));
    }

    @Override
    public boolean isRunning() {
        return super.isRunning() && cassandra4CommitLogParserBase.isRunning();
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
                        // react only on _cdc.idx files, run a thread which will basically wait until it is COMPLETED
                        // and then read it all at once.
                        // if another commit log is created in while this just submitted is being processed,
                        // since executor service is single-threaded, it will block until the previous log is processed,
                        // basically achieving sequential log processing
                        if (path.getFileName().toString().endsWith("_cdc.idx")) {
                            submit(path);
                        }
                    }
                }
            };
        }

        if (initial) {
            LOGGER.info("Reading existing commit logs in {}", cdcDir);
            File[] indexes = CommitLogUtil.getIndexes(cdcDir);
            Arrays.sort(indexes, CommitLogUtil::compareCommitLogsIndexes);
            for (File index : indexes) {
                if (isRunning()) {
                    submit(index.toPath());
                }
            }
            // If commit.log.error.reprocessing.enabled is set to true, download all error commitLog files upon starting for re-processing.
            if (errorCommitLogReprocessEnabled) {
                LOGGER.info("CommitLog Error Processing is enabled. Attempting to get all error commitLog files for re-processing.");
                // this will place it back to cdc dir so watched detects it hence it will be processed again
                commitLogTransfer.getErrorCommitLogFiles();
            }
            initial = false;
        }
        watcher.poll();
    }

    public static class ProcessingResult {
        public enum Result {
            OK,
            ERROR,
            DOES_NOT_EXIST,
            COMPLETED_PREMATURELY;
        }

        public final LogicalCommitLog commitLog;
        public final Result result;
        public final Exception ex;

        public ProcessingResult(LogicalCommitLog commitLog) {
            this(commitLog, Result.OK, null);
        }

        public ProcessingResult(LogicalCommitLog commitLog, Result result) {
            this(commitLog, result, null);
        }

        public ProcessingResult(LogicalCommitLog commitLog, Result result, Exception ex) {
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

    public static class LogicalCommitLog {
        CommitLogPosition commitLogPosition;
        File log;
        File index;
        long commitLogSegmentId;
        int offsetOfEndOfLastWrittenCDCMutation = 0;
        boolean completed = false;

        public LogicalCommitLog(File index) {
            this.index = index;
            this.log = parseCommitLogName(index);
            this.commitLogSegmentId = parseSegmentId(log);
            this.commitLogPosition = new CommitLogPosition(commitLogSegmentId, 0);
        }

        public static File parseCommitLogName(File index) {
            String newFileName = index.toPath().getFileName().toString().replace("_cdc.idx", ".log");
            return index.toPath().getParent().resolve(newFileName).toFile();
        }

        public static long parseSegmentId(File logName) {
            return Long.parseLong(logName.getName().split("-")[2].split("\\.")[0]);
        }

        public boolean exists() {
            return log.exists();
        }

        public void parseCommitLogIndex() throws DebeziumException {
            if (!index.exists()) {
                return;
            }
            try {
                List<String> lines = Files.readAllLines(index.toPath(), StandardCharsets.UTF_8);
                if (lines.isEmpty()) {
                    return;
                }

                offsetOfEndOfLastWrittenCDCMutation = Integer.parseInt(lines.get(0));

                if (lines.size() == 2) {
                    completed = "COMPLETED".equals(lines.get(1));
                }
            }
            catch (final Exception ex) {
                throw new DebeziumException(String.format("Unable to parse commit log index file %s", index.toPath()),
                        ex);
            }
        }

        @Override
        public String toString() {
            return "LogicalCommitLog{" +
                    "commitLogPosition=" + commitLogPosition +
                    ", synced=" + offsetOfEndOfLastWrittenCDCMutation +
                    ", completed=" + completed +
                    ", log=" + log +
                    ", index=" + index +
                    ", commitLogSegmentId=" + commitLogSegmentId +
                    '}';
        }
    }
}