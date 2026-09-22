/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for {@link AbstractDirectoryWatcher} covering the inotify event types
 * relevant to Cassandra 5 CDC segment discovery.
 *
 * <p>Cassandra 5 writes the _cdc.idx file directly into cdc_raw/ when a segment
 * is sealed. If the idx file was pre-created at segment allocation time, the seal
 * write fires {@code ENTRY_MODIFY}, not {@code ENTRY_CREATE}. A watcher that only
 * registers {@code ENTRY_CREATE} therefore misses the seal write.
 *
 * <p>The fix registers {@code ENTRY_MODIFY} as well, and adds a periodic rescan
 * as a safety net for any events that may be missed.
 */
@EnabledOnOs(OS.LINUX)
class AbstractDirectoryWatcherTest {

    @TempDir
    Path watchedDir;

    @TempDir
    Path sourceDir;

    // -------------------------------------------------------------------------
    // Helper: collect all events fired into watchedDir within a short window
    // -------------------------------------------------------------------------
    private List<WatchEvent.Kind<?>> collectEvents(Set<WatchEvent.Kind<?>> kinds, Runnable action)
            throws IOException, InterruptedException {
        List<WatchEvent.Kind<?>> fired = new ArrayList<>();
        AbstractDirectoryWatcher watcher = new AbstractDirectoryWatcher(watchedDir, Duration.ofMillis(500), kinds) {
            @Override
            void handleEvent(WatchEvent<?> event, Path path) {
                fired.add(event.kind());
            }
        };
        action.run();
        // Give the kernel a moment to deliver the event, then poll once.
        Thread.sleep(100);
        watcher.poll();
        return fired;
    }

    /**
     * Proves the bug: when Cassandra seals a segment it writes the _cdc.idx into
     * cdc_raw/ as a modify (the file was pre-created at allocation time), not a
     * create.  An ENTRY_CREATE-only watcher therefore never fires for the idx seal
     * write — the segment is invisible.
     */
    @Test
    void entryCreateOnlyWatcherMissesIdxSealWrite() throws Exception {
        Path idx = watchedDir.resolve("CommitLog-8-12345_cdc.idx");
        // Pre-create the idx as Cassandra does at segment allocation time
        Files.writeString(idx, "0");

        List<Path> seen = new ArrayList<>();
        AbstractDirectoryWatcher watcher = new AbstractDirectoryWatcher(
                watchedDir, Duration.ofMillis(500), Set.of(ENTRY_CREATE)) {
            @Override
            void handleEvent(WatchEvent<?> event, Path path) {
                if (path.getFileName().toString().endsWith("_cdc.idx")) {
                    seen.add(path);
                }
            }
        };

        // Simulate Cassandra sealing the segment: overwrite idx with offset + COMPLETED
        Thread.sleep(50); // ensure watcher is registered before the write
        Files.writeString(idx, "4096\nCOMPLETED");
        Thread.sleep(100);
        watcher.poll();

        assertTrue(seen.isEmpty(),
                "ENTRY_CREATE-only watcher must NOT see the _cdc.idx seal write (it is a MODIFY) — " +
                        "this is the exact condition that made the unpatched processor blind to Cassandra 5 segments");
    }

    /**
     * Proves the fix: ENTRY_MODIFY fires when the _cdc.idx file is written directly
     * into the watched directory (as Cassandra does — the idx is never hard-linked).
     * This is the event the fixed CommitLogIdxProcessor relies on to discover new segments.
     */
    @Test
    void entryModifyIsFiredForIdxWrite() throws Exception {
        Path idx = watchedDir.resolve("CommitLog-8-12345_cdc.idx");
        // Create the file first so a subsequent write fires MODIFY, not CREATE.
        Files.writeString(idx, "0");

        List<WatchEvent.Kind<?>> events = collectEvents(
                Set.of(ENTRY_CREATE, ENTRY_MODIFY),
                () -> {
                    try {
                        Files.writeString(idx, "4096\nCOMPLETED");
                    }
                    catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });

        assertTrue(events.contains(ENTRY_MODIFY),
                "ENTRY_MODIFY must fire when the _cdc.idx file is written — this is the event " +
                        "the fixed CommitLogIdxProcessor uses to discover sealed Cassandra 5 segments");
    }

    /**
     * End-to-end: simulates the full Cassandra 5 segment lifecycle.
     *
     * 1. Hard-link .log into cdc_raw/ (segment allocated) — idx pre-created as empty
     * 2. Write _cdc.idx with offset+COMPLETED (segment sealed) — fires ENTRY_MODIFY
     *
     * ENTRY_CREATE-only: misses the seal write because the idx already exists.
     * ENTRY_CREATE + ENTRY_MODIFY: catches the seal write via ENTRY_MODIFY.
     */
    @Test
    void cassandra5SegmentLifecycleRequiresEntryModify() throws Exception {
        // --- ENTRY_CREATE-only watcher: misses the seal ---
        Path idx1 = watchedDir.resolve("CommitLog-8-99999_cdc.idx");
        Files.writeString(idx1, "0"); // pre-created at allocation

        List<Path> createOnlySeen = new ArrayList<>();
        AbstractDirectoryWatcher createOnlyWatcher = new AbstractDirectoryWatcher(
                watchedDir, Duration.ofMillis(500), Set.of(ENTRY_CREATE)) {
            @Override
            void handleEvent(WatchEvent<?> event, Path path) {
                if (path.getFileName().toString().endsWith("_cdc.idx")) {
                    createOnlySeen.add(path);
                }
            }
        };
        Thread.sleep(50);
        Files.writeString(idx1, "4096\nCOMPLETED"); // seal — MODIFY, not CREATE
        Thread.sleep(100);
        createOnlyWatcher.poll();

        assertTrue(createOnlySeen.isEmpty(),
                "ENTRY_CREATE-only watcher must NOT see the seal write — proves the unpatched code is blind");

        // --- ENTRY_CREATE + ENTRY_MODIFY watcher: catches the seal ---
        Path idx2 = watchedDir.resolve("CommitLog-8-88888_cdc.idx");
        Files.writeString(idx2, "0"); // pre-created at allocation

        List<Path> fixedSeen = new ArrayList<>();
        AbstractDirectoryWatcher fixedWatcher = new AbstractDirectoryWatcher(
                watchedDir, Duration.ofMillis(500), Set.of(ENTRY_CREATE, ENTRY_MODIFY)) {
            @Override
            void handleEvent(WatchEvent<?> event, Path path) {
                if (path.getFileName().toString().endsWith("_cdc.idx")) {
                    fixedSeen.add(path);
                }
            }
        };
        Thread.sleep(50);
        Files.writeString(idx2, "4096\nCOMPLETED"); // seal — fires ENTRY_MODIFY
        Thread.sleep(100);
        fixedWatcher.poll();

        assertFalse(fixedSeen.isEmpty(),
                "Fixed watcher (ENTRY_CREATE + ENTRY_MODIFY) must see the idx seal write — proves the fix works");
    }
}
