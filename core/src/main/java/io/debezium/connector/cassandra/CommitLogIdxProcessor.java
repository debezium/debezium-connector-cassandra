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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.math3.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link CommitLogIdxProcessor} is used to process CommitLog in CDC directory.
 * Upon readCommitLog, it processes the entire CommitLog specified in the {@link CassandraConnectorConfig}
 * and converts each row change in the commit log into a {@link Record},
 * and then emit the log via a {@link KafkaRecordEmitter}.
 */
public class CommitLogIdxProcessor extends AbstractProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(CommitLogIdxProcessor.class);

    private static final String NAME = "Commit Log Processor";

    private final CassandraConnectorContext context;
    private final File cdcDir;
    private AbstractDirectoryWatcher watcher;
    private final CommitLogProcessorMetrics metrics;
    private boolean initial = true;
    private final boolean errorCommitLogReprocessEnabled;
    private final CommitLogTransfer commitLogTransfer;
    private final ExecutorService executorService;
    final static Set<Pair<CommitLogIdxParser, Future<CommitLogProcessingResult>>> submittedProcessings = ConcurrentHashMap.newKeySet();
    private final CommitLogSegmentReader commitLogReader;

    public CommitLogIdxProcessor(CassandraConnectorContext context, CommitLogProcessorMetrics metrics,
                                 CommitLogSegmentReader commitLogReader, File cdcDir) {
        super(NAME, Duration.ZERO);
        this.context = context;
        commitLogTransfer = this.context.getCassandraConnectorConfig().getCommitLogTransfer();
        errorCommitLogReprocessEnabled = this.context.getCassandraConnectorConfig().errorCommitLogReprocessEnabled();
        this.cdcDir = cdcDir;
        executorService = Executors.newSingleThreadExecutor();
        this.metrics = metrics;
        this.commitLogReader = commitLogReader;
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
        try {
            executorService.shutdown();
            for (final Pair<CommitLogIdxParser, Future<CommitLogProcessingResult>> submittedProcessing : submittedProcessings) {
                try {
                    submittedProcessing.getFirst().complete();
                    submittedProcessing.getSecond().get();
                }
                catch (final Exception ex) {
                    LOGGER.warn("Waiting for submitted task to finish has failed.");
                }
            }
        }
        catch (final Exception ex) {
            throw new RuntimeException("Unable to close executor service in CommitLogProcessor in a timely manner");
        }
        super.stop();
    }

    protected synchronized static void removeProcessing(CommitLogIdxParser parser) {
        submittedProcessings.stream()
                .filter(p -> p.getFirst() == parser)
                .findFirst()
                .map(submittedProcessings::remove);
    }

    public void submit(Path index) {
        final CommitLogIdxParser parser = new CommitLogIdxParser(new LogicalCommitLog(index.toFile()), metrics,
                this.context, commitLogReader);
        Future<CommitLogProcessingResult> future = executorService.submit(parser::process);
        submittedProcessings.add(new Pair<>(parser, future));
        LOGGER.debug("Processing {} callables.", submittedProcessings.size());
    }

    @Override
    public boolean isRunning() {
        return super.isRunning() && !executorService.isShutdown() && !executorService.isTerminated();
    }

    @Override
    public void process() throws IOException, InterruptedException {
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
}
