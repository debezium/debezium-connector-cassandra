/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.cassandra.metrics.CassandraStreamingMetrics;

class CommitLogIdxProcessorTest {

    @TempDir
    Path cdcDir;

    @TempDir
    Path sourceDir;

    private CassandraConnectorContext context;
    private CassandraConnectorConfig config;
    private CassandraStreamingMetrics metrics;
    private CommitLogSegmentReader reader;
    private CommitLogIdxProcessor processor;

    @AfterEach
    void tearDown() {
        if (processor != null) {
            processor.stop();
        }
        CommitLogIdxProcessor.submittedProcessings.clear();
    }

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        context = mock(CassandraConnectorContext.class);
        config = mock(CassandraConnectorConfig.class);
        CommitLogTransfer transfer = mock(CommitLogTransfer.class);
        metrics = mock(CassandraStreamingMetrics.class);
        reader = mock(CommitLogSegmentReader.class);

        ChangeEventQueue<Event> queue = mock(ChangeEventQueue.class);

        when(context.getCassandraConnectorConfig()).thenReturn(config);
        when(context.getReprocessingCommitLogs()).thenReturn(ConcurrentHashMap.newKeySet());
        when(context.getErroneousCommitLogs()).thenReturn(ConcurrentHashMap.newKeySet());
        when(context.getQueues()).thenReturn(List.of(queue));
        when(config.errorCommitLogReprocessEnabled()).thenReturn(false);
        when(config.getCommitLogTransfer()).thenReturn(transfer);
        when(config.cdcDirPollInterval()).thenReturn(Duration.ofMillis(50));
        when(config.getCommitLogProcessorShutdownTimeoutSeconds()).thenReturn(10);
        when(config.getCommitLogMarkedCompletePollInterval()).thenReturn(50);
        when(config.isCommitLogRealTimeProcessingEnabled()).thenReturn(false);
    }

    private void startProcessor() throws Exception {
        processor = new CommitLogIdxProcessor(context, metrics, reader, cdcDir.toFile());
        processor.initialize();
        Thread t = new Thread(() -> {
            try {
                processor.start();
            }
            catch (Exception e) {
                // expected on stop()
            }
        });
        t.setDaemon(true);
        t.start();
        Thread.sleep(200);
    }

    private CountDownLatch[] hookReader() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        doAnswer(inv -> {
            entered.countDown();
            release.await(5, TimeUnit.SECONDS);
            return null;
        }).when(reader).readCommitLogSegment(any(), anyLong(), anyInt());
        return new CountDownLatch[]{ entered, release };
    }

    @Test
    void shouldPopulateReprocessingCommitLogsFromErrorCommitLogFiles() throws Exception {
        CommitLogTransfer transfer = mock(CommitLogTransfer.class);
        Set<String> reprocessingCommitLogs = ConcurrentHashMap.newKeySet();

        when(context.getReprocessingCommitLogs()).thenReturn(reprocessingCommitLogs);
        when(config.errorCommitLogReprocessEnabled()).thenReturn(true);
        when(config.getCommitLogTransfer()).thenReturn(transfer);
        when(transfer.getErrorCommitLogFiles()).thenReturn(List.of("CommitLog-6-100.log", "CommitLog-6-101.log"));

        CommitLogIdxProcessor p = new CommitLogIdxProcessor(context, metrics, reader, cdcDir.toFile());
        p.initialize();
        Thread t = new Thread(() -> {
            try {
                p.start();
            }
            catch (Exception ignored) {
            }
        });
        t.setDaemon(true);
        t.start();
        Thread.sleep(300);
        p.stop();

        assertTrue(reprocessingCommitLogs.contains("CommitLog-6-100.log"));
        assertTrue(reprocessingCommitLogs.contains("CommitLog-6-101.log"));
    }

    @Test
    @EnabledOnOs(OS.LINUX)
    void shouldSubmitSegmentWhenIdxWrittenAfterHardLinkedLog() throws Exception {
        String segmentName = "CommitLog-8-11111";
        Path logSource = sourceDir.resolve(segmentName + ".log");
        Files.writeString(logSource, "commitlog data");

        CountDownLatch[] latches = hookReader();
        CountDownLatch readerEntered = latches[0];
        CountDownLatch readerRelease = latches[1];

        Files.createLink(cdcDir.resolve(segmentName + ".log"), logSource);

        startProcessor();

        Files.writeString(cdcDir.resolve(segmentName + "_cdc.idx"), "4096\nCOMPLETED");

        assertTrue(readerEntered.await(5, TimeUnit.SECONDS), "segment was not submitted");

        try {
            assertTrue(CommitLogIdxProcessor.submittedProcessings.stream()
                    .anyMatch(p -> p.getFirst().getCommitLog().index.getName().equals(segmentName + "_cdc.idx")));
        }
        finally {
            readerRelease.countDown();
        }
    }

    @Test
    void shouldSubmitSegmentViaPeriodicRescanWhenIdxAlreadyPresent() throws Exception {
        String segmentName = "CommitLog-8-22222";
        Path logSource = sourceDir.resolve(segmentName + ".log");
        Files.writeString(logSource, "commitlog data");

        CountDownLatch[] latches = hookReader();
        CountDownLatch readerEntered = latches[0];
        CountDownLatch readerRelease = latches[1];

        Files.createLink(cdcDir.resolve(segmentName + ".log"), logSource);
        Files.writeString(cdcDir.resolve(segmentName + "_cdc.idx"), "4096\nCOMPLETED");

        startProcessor();

        assertTrue(readerEntered.await(5, TimeUnit.SECONDS), "segment was not submitted");

        try {
            assertTrue(CommitLogIdxProcessor.submittedProcessings.stream()
                    .anyMatch(p -> p.getFirst().getCommitLog().index.getName().equals(segmentName + "_cdc.idx")));
        }
        finally {
            readerRelease.countDown();
        }
    }

    @Test
    @EnabledOnOs(OS.LINUX)
    void shouldNotSubmitSameSegmentTwiceWhenWatcherAndRescanBothFire() throws Exception {
        String segmentName = "CommitLog-8-33333";
        Path logSource = sourceDir.resolve(segmentName + ".log");
        Files.writeString(logSource, "commitlog data");

        CountDownLatch[] latches = hookReader();
        CountDownLatch readerEntered = latches[0];
        CountDownLatch readerRelease = latches[1];

        Files.createLink(cdcDir.resolve(segmentName + ".log"), logSource);

        startProcessor();

        Files.writeString(cdcDir.resolve(segmentName + "_cdc.idx"), "4096\nCOMPLETED");

        assertTrue(readerEntered.await(5, TimeUnit.SECONDS), "segment was not submitted");

        try {
            long submissionCount = CommitLogIdxProcessor.submittedProcessings.stream()
                    .filter(p -> p.getFirst().getCommitLog().index.getName().equals(segmentName + "_cdc.idx"))
                    .count();
            assertTrue(submissionCount == 1, "expected exactly one submission, got " + submissionCount);
        }
        finally {
            readerRelease.countDown();
        }
    }

    @Test
    @Timeout(5)
    void stalledParserBlocksExecutorUntilNewerSegmentProvesAbandonment() throws Exception {
        String seg1 = "CommitLog-8-44441";
        Path log1 = sourceDir.resolve(seg1 + ".log");
        Files.writeString(log1, "commitlog data");
        Files.createLink(cdcDir.resolve(seg1 + ".log"), log1);
        Files.writeString(cdcDir.resolve(seg1 + "_cdc.idx"), "4194304\n"); // no COMPLETED

        String seg2 = "CommitLog-8-44442";
        Path log2 = sourceDir.resolve(seg2 + ".log");
        Files.writeString(log2, "commitlog data");

        CountDownLatch seg2Processed = new CountDownLatch(1);
        doAnswer(inv -> {
            File f = (File) inv.getArgument(0);
            if (f.getName().equals(seg2 + ".log")) {
                seg2Processed.countDown();
            }
            return null;
        }).when(reader).readCommitLogSegment(any(), anyLong(), anyInt());

        startProcessor(); // picks up seg1 via initial scan; executor is now blocked on it

        Files.createLink(cdcDir.resolve(seg2 + ".log"), log2);
        Files.writeString(cdcDir.resolve(seg2 + "_cdc.idx"), "4096\nCOMPLETED");

        // seg2 is a newer segment, so seg1 is abandoned and the executor unblocks
        assertTrue(seg2Processed.await(4, TimeUnit.SECONDS));
    }

    @Test
    @Timeout(5)
    void soleActiveSegmentIsNotForceCompletedWithoutNewerSegment() throws Exception {
        String seg = "CommitLog-8-66661";
        Path log = sourceDir.resolve(seg + ".log");
        Files.writeString(log, "commitlog data");
        Files.createLink(cdcDir.resolve(seg + ".log"), log);
        Files.writeString(cdcDir.resolve(seg + "_cdc.idx"), "4194304\n"); // no COMPLETED

        startProcessor(); // picks up seg via initial scan

        Thread.sleep(400); // several poll intervals pass with no newer segment present

        assertTrue(CommitLogIdxProcessor.submittedProcessings.stream()
                .anyMatch(p -> p.getFirst().getCommitLog().index.getName().equals(seg + "_cdc.idx")));
        verify(reader, never()).readCommitLogSegment(any(), anyLong(), anyInt());
    }

    @Test
    void shouldFindLogViaCommitlogFallbackWhenNotHardLinkedIntoCdcRaw() throws Exception {
        String segmentName = "CommitLog-8-55555";
        Path commitlogDir = cdcDir.getParent().resolve("commitlog");
        Files.createDirectories(commitlogDir);
        Files.writeString(commitlogDir.resolve(segmentName + ".log"), "commitlog data");
        Files.writeString(cdcDir.resolve(segmentName + "_cdc.idx"), "4096\nCOMPLETED");

        CountDownLatch[] latches = hookReader();
        CountDownLatch readerEntered = latches[0];
        CountDownLatch readerRelease = latches[1];

        startProcessor();

        assertTrue(readerEntered.await(5, TimeUnit.SECONDS));
        readerRelease.countDown();
    }
}
