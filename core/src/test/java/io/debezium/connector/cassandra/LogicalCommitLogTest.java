/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LogicalCommitLogTest {

    @TempDir
    Path cdcRawDir;

    @TempDir
    Path commitLogDir;

    @Test
    void existsReturnsTrueWhenLogPresentDirectlyInCdcRaw() throws Exception {
        File idx = cdcRawDir.resolve("CommitLog-8-1_cdc.idx").toFile();
        Files.createFile(cdcRawDir.resolve("CommitLog-8-1.log"));

        assertTrue(new LogicalCommitLog(idx, commitLogDir.toFile()).exists());
    }

    @Test
    void existsReturnsTrueViaExplicitCommitLogDirFallback() throws Exception {
        File idx = cdcRawDir.resolve("CommitLog-8-2_cdc.idx").toFile();
        Files.createFile(commitLogDir.resolve("CommitLog-8-2.log"));

        assertTrue(new LogicalCommitLog(idx, commitLogDir.toFile()).exists());
    }

    @Test
    void existsReturnsFalseWhenLogMissingFromBothLocations() throws Exception {
        File idx = cdcRawDir.resolve("CommitLog-8-3_cdc.idx").toFile();

        assertFalse(new LogicalCommitLog(idx, commitLogDir.toFile()).exists());
    }

    @Test
    void existsReturnsFalseWithoutGuessingWhenNoCommitLogDirProvided() throws Exception {
        File idx = cdcRawDir.resolve("CommitLog-8-4_cdc.idx").toFile();
        Files.createFile(commitLogDir.resolve("CommitLog-8-4.log"));

        assertFalse(new LogicalCommitLog(idx, null).exists());
    }
}
