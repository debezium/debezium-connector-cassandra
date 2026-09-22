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

    @Test
    @Timeout(5)
    void parserDoesNotBlockOnAbandonedSegmentWithNoCompletedLine() throws Exception {
        File log = cdcRawDir.resolve("CommitLog-8-1700000100000.log").toFile();
        File idx = cdcRawDir.resolve("CommitLog-8-1700000100000_cdc.idx").toFile();

        assertTrue(log.createNewFile());
        Files.writeString(idx.toPath(), "4194304\n");
        // a newer segment already exists, so this one is abandoned
        Files.writeString(cdcRawDir.resolve("CommitLog-8-1700000150000_cdc.idx"), "0\n");

        CommitLogIdxParser parser = buildParser(idx);
        CommitLogProcessingResult result = parser.process();

        assertEquals(CommitLogProcessingResult.Result.OK, result.result);
    }

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

        assertEquals(CommitLogProcessingResult.Result.OK, result.result);
    }

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

        // pollingInterval is 100ms; several intervals pass with no newer segment present
        assertFalse(finished.await(400, TimeUnit.MILLISECONDS));

        Files.writeString(cdcRawDir.resolve("CommitLog-8-1700000700000_cdc.idx"), "0\n");

        assertTrue(finished.await(4, TimeUnit.SECONDS));
        assertEquals(CommitLogProcessingResult.Result.OK, resultRef.get().result);
    }

    @Test
    @Timeout(5)
    void parserReturnsOkForNormalCompletedSegment() throws Exception {
        File log = cdcRawDir.resolve("CommitLog-8-1700000300000.log").toFile();
        File idx = cdcRawDir.resolve("CommitLog-8-1700000300000_cdc.idx").toFile();

        assertTrue(log.createNewFile());
        Files.writeString(idx.toPath(), "4194304\nCOMPLETED\n");

        CommitLogIdxParser parser = buildParser(idx);
        CommitLogProcessingResult result = parser.process();

        assertEquals(CommitLogProcessingResult.Result.OK, result.result);
    }
}
