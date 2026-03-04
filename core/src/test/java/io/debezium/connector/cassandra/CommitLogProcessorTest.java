/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.jupiter.api.Test;

import io.debezium.connector.cassandra.metrics.CassandraStreamingMetrics;

class CommitLogProcessorTest {

    @Test
    void shouldPopulateReprocessingCommitLogsFromErrorCommitLogFiles() throws Exception {
        CassandraConnectorContext context = mock(CassandraConnectorContext.class);
        CassandraConnectorConfig config = mock(CassandraConnectorConfig.class);
        CommitLogTransfer transfer = mock(CommitLogTransfer.class);
        CassandraStreamingMetrics metrics = mock(CassandraStreamingMetrics.class);
        CommitLogSegmentReader reader = mock(CommitLogSegmentReader.class);

        Set<String> reprocessingCommitLogs = ConcurrentHashMap.newKeySet();

        when(context.getCassandraConnectorConfig()).thenReturn(config);
        when(context.getReprocessingCommitLogs()).thenReturn(reprocessingCommitLogs);
        when(context.getErroneousCommitLogs()).thenReturn(ConcurrentHashMap.newKeySet());
        when(context.getQueues()).thenReturn(new ArrayList<>());
        when(config.latestCommitLogOnly()).thenReturn(false);
        when(config.errorCommitLogReprocessEnabled()).thenReturn(true);
        when(config.getCommitLogTransfer()).thenReturn(transfer);
        when(config.cdcDirPollInterval()).thenReturn(Duration.ofMillis(1));
        when(transfer.getErrorCommitLogFiles()).thenReturn(List.of("CommitLog-6-100.log", "CommitLog-6-101.log"));

        Path cdcDir = Files.createTempDirectory("dbz-1647-cdc");
        Path commitLogDir = Files.createTempDirectory("dbz-1647-cl");

        try {
            CommitLogProcessor processor = new CommitLogProcessor(context, metrics, reader, cdcDir.toFile(), commitLogDir.toFile());
            processor.initialize();
            processor.process();

            assertTrue(reprocessingCommitLogs.contains("CommitLog-6-100.log"),
                    "CommitLog-6-100.log should be in reprocessingCommitLogs after getErrorCommitLogFiles()");
            assertTrue(reprocessingCommitLogs.contains("CommitLog-6-101.log"),
                    "CommitLog-6-101.log should be in reprocessingCommitLogs after getErrorCommitLogFiles()");
        }
        finally {
            Files.deleteIfExists(cdcDir);
            Files.deleteIfExists(commitLogDir);
        }
    }
}
