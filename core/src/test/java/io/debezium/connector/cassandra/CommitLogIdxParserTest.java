/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.cassandra.metrics.CassandraStreamingMetrics;

/**
 * Tests for {@link CommitLogIdxParser}.
 *
 * <p>Verifies that the parser does not block indefinitely when the _cdc.idx file
 * never receives a COMPLETED marker, without falsely treating a merely-idle active
 * segment as abandoned. A segment is only treated as abandoned once a strictly
 * newer commit log segment already exists alongside it - Cassandra only ever
 * writes to one active segment at a time, so a newer segment proves the older one
 * will never be written to again. This can happen for:
 * <ul>
 *   <li>A stale pre-upgrade segment whose idx was written by a previous node and
 *       will never be updated, once the (restarted/upgraded) node rolls to a new
 *       segment.</li>
 *   <li>A segment abandoned mid-write once Cassandra rolls over to the next one.</li>
 * </ul>
 * Without any such newer segment present, the parser must keep waiting - a
 * temporarily quiet active segment must never be treated as complete, since that
 * would silently drop any data written to it afterward.
 */
class CommitLogIdxParserTest {

    @TempDir
    Path cdcRawDir;

    private CommitLogIdxParser buildParser(File idx) {
        CassandraConnectorContext context = mock(CassandraConnectorContext.class);
        CassandraConnectorConfig config = mock(CassandraConnectorConfig.class);
        CommitLogSegmentReader reader = mock(CommitLogSegmentReader.class);
        CassandraStreamingMetrics metrics = mock(CassandraStreamingMetrics.class);

        ChangeEventQueue<Event> queue = new ChangeEventQueue.Builder<Event>()
                .pollInterval(java.time.Duration.ofMillis(100))
                .maxBatchSize(100)
                .maxQueueSize(1000)
                .loggingContextSupplier(() -> null)
                .build();

        Set<String> erroneousCommitLogs = ConcurrentHashMap.newKeySet();
        Set<String> reprocessingCommitLogs = ConcurrentHashMap.newKeySet();

        when(context.getCassandraConnectorConfig()).thenReturn(config);
        when(context.getErroneousCommitLogs()).thenReturn(erroneousCommitLogs);
        when(context.getReprocessingCommitLogs()).thenReturn(reprocessingCommitLogs);
        when(context.getQueues()).thenReturn(List.of(queue));
        when(config.getCommitLogTransfer()).thenReturn(new BlackHoleCommitLogTransfer());
        when(config.getCommitLogMarkedCompletePollInterval()).thenReturn(100);
        when(config.isCommitLogRealTimeProcessingEnabled()).thenReturn(false);

        return new CommitLogIdxParser(new LogicalCommitLog(idx), metrics, context, reader);
    }

    /**
     * Stale segment: idx has only an offset line (no COMPLETED), .log exists, and a
     * strictly newer segment's idx is already present alongside it - simulating a
     * pre-upgrade segment that a restarted/upgraded node has since moved past.
     * The parser must return OK without blocking.
     */
    @Test
    @Timeout(5)
    void parserDoesNotBlockOnAbandonedSegmentWithNoCompletedLine() throws Exception {
        File log = cdcRawDir.resolve("CommitLog-8-1700000100000.log").toFile();
        File idx = cdcRawDir.resolve("CommitLog-8-1700000100000_cdc.idx").toFile();

        assertTrue(log.createNewFile());
        Files.writeString(idx.toPath(), "4194304\n");
        // A newer segment already exists alongside it, proving Cassandra has moved on
        // and this one will never receive a COMPLETED marker.
        Files.writeString(cdcRawDir.resolve("CommitLog-8-1700000150000_cdc.idx"), "0\n");

        CommitLogIdxParser parser = buildParser(idx);
        CommitLogProcessingResult result = parser.process();

        assertEquals(CommitLogProcessingResult.Result.OK, result.result,
                "parser must return OK for an abandoned segment once a newer segment exists");
    }

    /**
     * Segment abandoned mid-write: idx has only an offset line and is never updated
     * again, but Cassandra has already rolled over to a newer segment. The parser
     * must return without blocking once that newer segment is visible.
     */
    @Test
    @Timeout(5)
    void parserDoesNotBlockIndefinitelyOnceSegmentIsAbandoned() throws Exception {
        File log = cdcRawDir.resolve("CommitLog-8-1700000200000.log").toFile();
        File idx = cdcRawDir.resolve("CommitLog-8-1700000200000_cdc.idx").toFile();

        assertTrue(log.createNewFile());
        Files.writeString(idx.toPath(), "1048576\n");
        Files.writeString(cdcRawDir.resolve("CommitLog-8-1700000250000_cdc.idx"), "0\n");

        CommitLogIdxParser parser = buildParser(idx);
        CommitLogProcessingResult result = parser.process();

        assertEquals(CommitLogProcessingResult.Result.OK, result.result,
                "parser must return without blocking once a newer segment proves this one is abandoned");
    }

    /**
     * Regression test for the unsafe "one quiet polling interval means done" heuristic:
     * an active segment that is still the newest segment on disk must NOT be force
     * completed merely because no new mutation arrived during a polling interval - doing
     * so would silently drop any data Cassandra later writes to it. The parser must keep
     * waiting until either more data arrives or a newer segment proves it was abandoned.
     */
    @Test
    @Timeout(5)
    void parserDoesNotForceCompleteActiveSegmentWithoutNewerSegment() throws Exception {
        File log = cdcRawDir.resolve("CommitLog-8-1700000600000.log").toFile();
        File idx = cdcRawDir.resolve("CommitLog-8-1700000600000_cdc.idx").toFile();

        assertTrue(log.createNewFile());
        Files.writeString(idx.toPath(), "2048\n");

        CommitLogIdxParser parser = buildParser(idx);

        CountDownLatch finished = new CountDownLatch(1);
        AtomicReference<CommitLogProcessingResult> resultRef = new AtomicReference<>();
        Thread t = new Thread(() -> {
            resultRef.set(parser.process());
            finished.countDown();
        });
        t.setDaemon(true);
        t.start();

        // pollingInterval is 100ms (see buildParser). Wait several intervals with no
        // newer segment present - the parser must still be looping, not force-completed.
        assertFalse(finished.await(400, TimeUnit.MILLISECONDS),
                "parser must not force-complete an active segment just because no newer segment exists yet");

        // A newer segment now appears, simulating Cassandra rolling over. This proves
        // the original segment is abandoned and unblocks the parser.
        Files.writeString(cdcRawDir.resolve("CommitLog-8-1700000700000_cdc.idx"), "0\n");

        assertTrue(finished.await(4, TimeUnit.SECONDS),
                "parser must complete once a newer commit log segment proves this one is abandoned");
        assertEquals(CommitLogProcessingResult.Result.OK, resultRef.get().result);
    }

    /**
     * Baseline: idx has both offset and COMPLETED lines — the normal case.
     * The parser must return OK both before and after any fix.
     */
    @Test
    @Timeout(5)
    void parserReturnsOkForNormalCompletedSegment() throws Exception {
        File log = cdcRawDir.resolve("CommitLog-8-1700000300000.log").toFile();
        File idx = cdcRawDir.resolve("CommitLog-8-1700000300000_cdc.idx").toFile();

        assertTrue(log.createNewFile());
        Files.writeString(idx.toPath(), "4194304\nCOMPLETED\n");

        CommitLogIdxParser parser = buildParser(idx);
        CommitLogProcessingResult result = parser.process();

        assertEquals(CommitLogProcessingResult.Result.OK, result.result,
                "parser must return OK for a normally completed segment");
    }
}
