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

    /**
     * Starts the processor in a background thread (as the connector task does) and
     * returns a latch that is counted down once the processor's start() loop is running.
     *
     * The processor loops calling process() until stop() is called. Tests must call
     * processor.stop() (done in tearDown) to terminate the loop.
     */
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
        // Give the processor loop time to initialise the watcher on the first process() call
        Thread.sleep(200);
    }

    /**
     * Wires the mock reader to signal when it is entered and block until released.
     * Returns [entered, release] latches.
     */
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

    // -------------------------------------------------------------------------
    // Original test — kept intact
    // -------------------------------------------------------------------------

    @Test
    void shouldPopulateReprocessingCommitLogsFromErrorCommitLogFiles() throws Exception {
        CommitLogTransfer transfer = mock(CommitLogTransfer.class);
        Set<String> reprocessingCommitLogs = ConcurrentHashMap.newKeySet();

        when(context.getReprocessingCommitLogs()).thenReturn(reprocessingCommitLogs);
        when(config.errorCommitLogReprocessEnabled()).thenReturn(true);
        when(config.getCommitLogTransfer()).thenReturn(transfer);
        when(transfer.getErrorCommitLogFiles()).thenReturn(List.of("CommitLog-6-100.log", "CommitLog-6-101.log"));

        // Use direct process() call for this test — it only checks reprocessingCommitLogs population
        CommitLogIdxProcessor p = new CommitLogIdxProcessor(context, metrics, reader, cdcDir.toFile());
        p.initialize();
        // Manually set running via start() in a thread, then stop immediately after one cycle
        CountDownLatch processed = new CountDownLatch(1);
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

    // -------------------------------------------------------------------------
    // Bug regression: hard-linked .log + directly-written .idx
    // -------------------------------------------------------------------------

    /**
     * Proves that the processor submits a segment when the _cdc.idx is written
     * directly into cdc_raw/ (as Cassandra does), even though the .log arrived
     * via hard link and fired no ENTRY_CREATE event.
     *
     * This is the core Cassandra 5 CDC bug scenario: without ENTRY_MODIFY in the
     * watch kinds, the idx write is invisible and the segment is never processed.
     */
    @Test
    @EnabledOnOs(OS.LINUX)
    void shouldSubmitSegmentWhenIdxWrittenAfterHardLinkedLog() throws Exception {
        String segmentName = "CommitLog-8-11111";
        Path logSource = sourceDir.resolve(segmentName + ".log");
        Files.writeString(logSource, "commitlog data");

        CountDownLatch[] latches = hookReader();
        CountDownLatch readerEntered = latches[0];
        CountDownLatch readerRelease = latches[1];

        // Simulate Cassandra 5: hard-link .log into cdc_raw/ at allocation time
        Files.createLink(cdcDir.resolve(segmentName + ".log"), logSource);

        startProcessor(); // watcher initialised, no idx yet

        // Simulate Cassandra sealing the segment: write _cdc.idx directly into cdc_raw/
        Files.writeString(cdcDir.resolve(segmentName + "_cdc.idx"), "4096\nCOMPLETED");

        // Wait until the executor thread is inside readCommitLogSegment() — parser is alive
        assertTrue(readerEntered.await(5, TimeUnit.SECONDS),
                "Reader was never entered — segment was not submitted. " +
                        "This proves the unpatched ENTRY_CREATE-only watcher is blind to Cassandra 5 segments.");

        try {
            assertTrue(CommitLogIdxProcessor.submittedProcessings.stream()
                    .anyMatch(p -> p.getFirst().getCommitLog().index.getName().equals(segmentName + "_cdc.idx")),
                    "Processor must have the segment in submittedProcessings while the reader is blocked");
        }
        finally {
            readerRelease.countDown();
        }
    }

    /**
     * Proves that the periodic rescan independently catches idx files that were
     * present before the watcher was initialised — simulating segments written
     * while Debezium was down (e.g. during an upgrade).
     */
    @Test
    void shouldSubmitSegmentViaPeriodicRescanWhenIdxAlreadyPresent() throws Exception {
        String segmentName = "CommitLog-8-22222";
        Path logSource = sourceDir.resolve(segmentName + ".log");
        Files.writeString(logSource, "commitlog data");

        CountDownLatch[] latches = hookReader();
        CountDownLatch readerEntered = latches[0];
        CountDownLatch readerRelease = latches[1];

        // Both files present before the processor starts — simulates post-upgrade scenario
        Files.createLink(cdcDir.resolve(segmentName + ".log"), logSource);
        Files.writeString(cdcDir.resolve(segmentName + "_cdc.idx"), "4096\nCOMPLETED");

        startProcessor();

        assertTrue(readerEntered.await(5, TimeUnit.SECONDS),
                "Reader was never entered — segment was not submitted via initial scan / periodic rescan");

        try {
            assertTrue(CommitLogIdxProcessor.submittedProcessings.stream()
                    .anyMatch(p -> p.getFirst().getCommitLog().index.getName().equals(segmentName + "_cdc.idx")),
                    "Processor must submit pre-existing segments via the initial scan / periodic rescan");
        }
        finally {
            readerRelease.countDown();
        }
    }

    /**
     * Proves that the same idx file is never submitted twice even when both the
     * watcher event and the periodic rescan fire for it in the same process() cycle.
     *
     * Without the submittedIndexes deduplication set, the segment would be processed
     * twice, producing duplicate CDC events on the Kafka topic.
     */
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

        startProcessor(); // watcher initialised, no idx yet

        // Write idx — both the watcher (ENTRY_MODIFY) and the periodic rescan will see it
        Files.writeString(cdcDir.resolve(segmentName + "_cdc.idx"), "4096\nCOMPLETED");

        assertTrue(readerEntered.await(5, TimeUnit.SECONDS),
                "Reader was never entered — segment was not submitted");

        try {
            long submissionCount = CommitLogIdxProcessor.submittedProcessings.stream()
                    .filter(p -> p.getFirst().getCommitLog().index.getName().equals(segmentName + "_cdc.idx"))
                    .count();
            assertTrue(submissionCount == 1,
                    "Segment must be submitted exactly once even when both watcher and rescan fire — " +
                            "got " + submissionCount + " submissions");
        }
        finally {
            readerRelease.countDown();
        }
    }

    /**
     * Proves the executor stall: a segment whose idx has no COMPLETED line blocks the
     * single-threaded executor, preventing a second (fully-completed) segment from ever
     * being processed.
     *
     * This is the root cause of the Cassandra 5 CDC bug in the upgrade scenario:
     * - Segment 1 was active at upgrade time — idx has offset but no COMPLETED.
     * - Segment 2 (a strictly newer segment id) arrives after Debezium starts — idx has
     *   COMPLETED.
     * - Without abandonment detection, the executor is permanently blocked on segment 1
     *   and segment 2 is never processed.
     * - With abandonment detection (the fix), segment 1's parser notices segment 2 is a
     *   newer segment already on disk — proving segment 1 will never be written to again
     *   — unblocks, and segment 2 is processed next.
     *
     * Note this relies on segment 2 having a numerically newer segment id than segment 1;
     * that is what makes segment 1 provably abandoned rather than merely idle. See
     * {@link CommitLogIdxParserTest#parserDoesNotForceCompleteActiveSegmentWithoutNewerSegment()}
     * for the corresponding negative case (no newer segment, must not force-complete).
     *
     * The @Timeout(5) proves the test would hang forever on the unpatched code.
     */
    @Test
    @org.junit.jupiter.api.Timeout(5)
    void stalledParserBlocksExecutorUntilNewerSegmentProvesAbandonment() throws Exception {
        // Segment 1: active at upgrade time — idx has offset but no COMPLETED
        String seg1 = "CommitLog-8-44441";
        Path log1 = sourceDir.resolve(seg1 + ".log");
        Files.writeString(log1, "commitlog data");
        Files.createLink(cdcDir.resolve(seg1 + ".log"), log1);
        Files.writeString(cdcDir.resolve(seg1 + "_cdc.idx"), "4194304\n"); // no COMPLETED

        // Segment 2: fully sealed — arrives after Debezium starts
        String seg2 = "CommitLog-8-44442";
        Path log2 = sourceDir.resolve(seg2 + ".log");
        Files.writeString(log2, "commitlog data");

        // Track which segments the reader processes
        Set<String> processed = ConcurrentHashMap.newKeySet();
        CountDownLatch seg2Processed = new CountDownLatch(1);
        doAnswer(inv -> {
            File f = (File) inv.getArgument(0);
            processed.add(f.getName());
            if (f.getName().equals(seg2 + ".log")) {
                seg2Processed.countDown();
            }
            return null;
        }).when(reader).readCommitLogSegment(any(), anyLong(), anyInt());

        startProcessor(); // picks up seg1 immediately via initial scan; executor enters stall loop

        // Now place seg2 — executor is blocked on seg1, so seg2 queues up
        Files.createLink(cdcDir.resolve(seg2 + ".log"), log2);
        Files.writeString(cdcDir.resolve(seg2 + "_cdc.idx"), "4096\nCOMPLETED");

        // Seg1's parser notices seg2 is a newer segment already on disk, proving seg1 is
        // abandoned, and unblocks after one poll interval (50ms). Seg1 completes, seg2 runs.
        assertTrue(seg2Processed.await(4, TimeUnit.SECONDS),
                "Segment 2 was never processed — abandonment detection did not unblock the executor. " +
                        "Without the fix this test would hang until @Timeout kills it.");
    }

    /**
     * Companion negative case at the processor/executor level: a single active segment
     * with no COMPLETED marker and no newer segment anywhere on disk must keep the
     * executor busy rather than being force-completed by elapsed time alone. This proves
     * the executor-level behavior matches the parser-level guarantee verified in
     * {@link CommitLogIdxParserTest#parserDoesNotForceCompleteActiveSegmentWithoutNewerSegment()}.
     */
    @Test
    @org.junit.jupiter.api.Timeout(5)
    void soleActiveSegmentIsNotForceCompletedWithoutNewerSegment() throws Exception {
        String seg = "CommitLog-8-66661";
        Path log = sourceDir.resolve(seg + ".log");
        Files.writeString(log, "commitlog data");
        Files.createLink(cdcDir.resolve(seg + ".log"), log);
        Files.writeString(cdcDir.resolve(seg + "_cdc.idx"), "4194304\n"); // no COMPLETED

        // Real-time processing is disabled in this test suite (see setUp()), so the reader
        // is only invoked once the parser's loop exits — i.e. once the segment is (wrongly
        // or rightly) treated as complete. It must NOT be invoked here at all, since with no
        // newer segment on disk the segment must stay pending indefinitely.
        startProcessor(); // picks up seg immediately via initial scan

        // Several poll intervals (50ms each) pass with no newer segment present. The
        // parser must still be the one and only submission in flight — completion would
        // remove it from submittedProcessings and invoke the reader.
        Thread.sleep(400);

        assertTrue(CommitLogIdxProcessor.submittedProcessings.stream()
                .anyMatch(p -> p.getFirst().getCommitLog().index.getName().equals(seg + "_cdc.idx")),
                "The sole active segment must not be force-completed just because time passed " +
                        "with no newer segment on disk — doing so would silently drop any data " +
                        "written to it afterward");
        verify(reader, never()).readCommitLogSegment(any(), anyLong(), anyInt());
    }

    /**
     * Proves that a segment whose .log is absent from cdc_raw/ but present in the
     * commitlog/ sibling directory is still found via LogicalCommitLog's fallback
     * path lookup — no exception thrown.
     *
     * This covers the Cassandra 5 low-write-volume case where the .log stays in
     * commitlog/ with Links=1 and only the .idx appears in cdc_raw/.
     */
    @Test
    void shouldFindLogViaCommitlogFallbackWhenNotHardLinkedIntoCdcRaw() throws Exception {
        String segmentName = "CommitLog-8-55555";
        // Create a commitlog/ sibling directory next to cdcDir
        Path commitlogDir = cdcDir.getParent().resolve("commitlog");
        Files.createDirectories(commitlogDir);
        Files.writeString(commitlogDir.resolve(segmentName + ".log"), "commitlog data");
        Files.writeString(cdcDir.resolve(segmentName + "_cdc.idx"), "4096\nCOMPLETED");

        CountDownLatch[] latches = hookReader();
        CountDownLatch readerEntered = latches[0];
        CountDownLatch readerRelease = latches[1];

        startProcessor();

        assertTrue(readerEntered.await(5, TimeUnit.SECONDS),
                "Reader was never entered — fallback .log lookup in commitlog/ did not work");
        readerRelease.countDown();
    }
}
