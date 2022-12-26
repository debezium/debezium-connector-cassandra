/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;

import io.debezium.DebeziumException;

public class LogicalCommitLog {
    File log;
    File index;
    long commitLogSegmentId;
    int offsetOfEndOfLastWrittenCDCMutation = 0;
    boolean completed = false;

    public LogicalCommitLog(File index) {
        this.index = index;
        this.log = parseCommitLogName(index);
        this.commitLogSegmentId = parseSegmentId(log);
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
                "synced=" + offsetOfEndOfLastWrittenCDCMutation +
                ", completed=" + completed +
                ", log=" + log +
                ", index=" + index +
                ", commitLogSegmentId=" + commitLogSegmentId +
                '}';
    }

}
