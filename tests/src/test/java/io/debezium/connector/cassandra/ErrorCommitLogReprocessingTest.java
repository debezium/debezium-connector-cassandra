/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static io.debezium.connector.cassandra.utils.TestUtils.TEST_KEYSPACE_NAME;
import static io.debezium.connector.cassandra.utils.TestUtils.TEST_TABLE_NAME;
import static io.debezium.connector.cassandra.utils.TestUtils.runCql;
import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.List;

/**
 * Integration test verifying that mutations from error commit logs are not silently skipped
 * when those logs are moved back to the CDC directory for reprocessing, even after newer
 * segments have already advanced the per-table offset past the error file's segment ID.
 */
class ErrorCommitLogReprocessingTest extends AbstractCommitLogProcessorTest {

    private static final int ROW_COUNT = 5;

    @Override
    public void initialiseData() throws Throwable {
        runCql(format("CREATE TABLE IF NOT EXISTS %s.%s (id int PRIMARY KEY, val int) WITH cdc = true",
                TEST_KEYSPACE_NAME, TEST_TABLE_NAME));
        for (int i = 0; i < ROW_COUNT; i++) {
            runCql(insertInto(TEST_KEYSPACE_NAME, TEST_TABLE_NAME)
                    .value("id", literal(i))
                    .value("val", literal(i))
                    .build());
        }
    }

    @Override
    public void verifyEvents() throws Throwable {
        // First pass: read commit logs normally, then simulate KafkaRecordEmitter marking offsets
        List<Event> firstPassEvents = getEvents(ROW_COUNT);
        assertEquals(ROW_COUNT, firstPassEvents.size(), "First pass should emit all " + ROW_COUNT + " mutations");
        markOffsets(firstPassEvents);

        // Second pass: re-read without marking for reprocessing — all mutations skipped by offset check
        List<Event> skippedEvents = getEvents(0);
        assertEquals(0, skippedEvents.size(), "Mutations should be skipped when not marked for reprocessing");

        // Simulate what CommitLogProcessor does on startup with errorCommitLogReprocessEnabled=true:
        // getErrorCommitLogFiles() moves files back to CDC dir and returns their names,
        // which are added to reprocessingCommitLogs so the offset check is bypassed.
        File[] commitLogs = CommitLogUtil.getCommitLogs(CDC_RAW_DIR);
        assertTrue(commitLogs.length > 0, "Expected at least one commit log in CDC dir");
        for (File commitLog : commitLogs) {
            context.getReprocessingCommitLogs().add(commitLog.getName());
        }

        // Third pass: re-read with reprocessingCommitLogs populated — offset check bypassed
        List<Event> reprocessedEvents = getEvents(ROW_COUNT);
        assertEquals(ROW_COUNT, reprocessedEvents.size(),
                "Mutations from reprocessed commit logs must not be skipped even when offsets are already marked");
    }

    // Simulates what KafkaRecordEmitter does after emitting each record to Kafka.
    // Without this, the offset is never written and the skip check never fires.
    private void markOffsets(List<Event> events) {
        events.stream()
                .filter(e -> e instanceof ChangeRecord)
                .map(e -> (ChangeRecord) e)
                .filter(Record::shouldMarkOffset)
                .forEach(record -> {
                    SourceInfo source = record.getSource();
                    context.getOffsetWriter().markOffset(
                            source.keyspaceTable.name(),
                            source.offsetPosition.serialize(),
                            source.snapshot);
                });
    }
}
