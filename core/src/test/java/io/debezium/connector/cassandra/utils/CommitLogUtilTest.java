/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

import org.junit.jupiter.api.Test;

import io.debezium.connector.cassandra.CommitLogUtil;

class CommitLogUtilTest {

    @Test
    void testMoveCommitLog() throws IOException {
        Path fromDr = Files.createTempDirectory("from");
        Path toDir = Files.createTempDirectory("to");
        assertTrue(new File(fromDr.toString(), "CommitLog-6-1.log").createNewFile());
        assertTrue(new File(fromDr.toString(), "Not-CommitLog-6-1.log").createNewFile());
        for (File file : Objects.requireNonNull(fromDr.toFile().listFiles())) {
            CommitLogUtil.moveCommitLog(file.toPath(), toDir);
        }

        assertEquals(1, Objects.requireNonNull(toDir.toFile().listFiles()).length);
        assertEquals(1, Objects.requireNonNull(fromDr.toFile().listFiles()).length);
        assertEquals(new File(toDir.toFile(), "CommitLog-6-1.log"), Objects.requireNonNull(toDir.toFile().listFiles())[0]);
        assertEquals(new File(fromDr.toFile(), "Not-CommitLog-6-1.log"), Objects.requireNonNull(fromDr.toFile().listFiles())[0]);
    }

    @Test
    void testDeleteCommitLog() throws IOException {
        Path dir = Files.createTempDirectory("temp");
        File commitLog = new File(dir.toString(), "CommitLog-6-1.log");
        File notCommitLog = new File(dir.toString(), "Not-CommitLog-6-1.log");
        assertTrue(commitLog.createNewFile());
        assertTrue(notCommitLog.createNewFile());
        CommitLogUtil.deleteCommitLog(commitLog);
        try {
            CommitLogUtil.deleteCommitLog(notCommitLog);
        }
        catch (Exception ex) {
            // it exists, because it was not deleted, because it is not a commit log
            assertTrue(notCommitLog.exists());
        }
        assertFalse(commitLog.exists());
    }

    @Test
    void testGetCommitLogs() throws IOException {
        Path dir = Files.createTempDirectory("temp");
        for (int i = 0; i < 10; i++) {
            if (i % 2 == 0) {
                assertTrue(new File(dir.toString(), "CommitLog-6-" + i + ".log").createNewFile());
            }
            else {
                assertTrue(new File(dir.toString(), "Not-CommitLog-6-" + i + ".log").createNewFile());
            }
        }
        assertEquals(5, CommitLogUtil.getCommitLogs(dir.toFile()).length);
    }

    @Test
    void testCompareCommitLogs() {
        String commit1 = "CommitLog-6-1.log";
        String commit2 = "CommitLog-6-2.log";
        String commit3 = "CommitLog-6-0.log";
        String commit4 = "CommitLog-6-1.log";
        assertEquals(-1, CommitLogUtil.compareCommitLogs(commit1, commit2));
        assertEquals(1, CommitLogUtil.compareCommitLogs(commit1, commit3));
        assertEquals(0, CommitLogUtil.compareCommitLogs(commit1, commit4));
    }
}
