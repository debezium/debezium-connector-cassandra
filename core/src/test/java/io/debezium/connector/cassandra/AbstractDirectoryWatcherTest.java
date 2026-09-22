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

@EnabledOnOs(OS.LINUX)
class AbstractDirectoryWatcherTest {

    @TempDir
    Path watchedDir;

    @TempDir
    Path sourceDir;

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
        Thread.sleep(100);
        watcher.poll();
        return fired;
    }

    @Test
    void entryCreateOnlyWatcherMissesIdxSealWrite() throws Exception {
        Path idx = watchedDir.resolve("CommitLog-8-12345_cdc.idx");
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

        Thread.sleep(50);
        Files.writeString(idx, "4096\nCOMPLETED");
        Thread.sleep(100);
        watcher.poll();

        assertTrue(seen.isEmpty(), "ENTRY_CREATE-only watcher must not see the idx seal write");
    }

    @Test
    void entryModifyIsFiredForIdxWrite() throws Exception {
        Path idx = watchedDir.resolve("CommitLog-8-12345_cdc.idx");
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

        assertTrue(events.contains(ENTRY_MODIFY), "ENTRY_MODIFY must fire when the idx file is written");
    }

    @Test
    void cassandra5SegmentLifecycleRequiresEntryModify() throws Exception {
        Path idx1 = watchedDir.resolve("CommitLog-8-99999_cdc.idx");
        Files.writeString(idx1, "0");

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
        Files.writeString(idx1, "4096\nCOMPLETED");
        Thread.sleep(100);
        createOnlyWatcher.poll();

        assertTrue(createOnlySeen.isEmpty(), "ENTRY_CREATE-only watcher must not see the seal write");

        Path idx2 = watchedDir.resolve("CommitLog-8-88888_cdc.idx");
        Files.writeString(idx2, "0");

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
        Files.writeString(idx2, "4096\nCOMPLETED");
        Thread.sleep(100);
        fixedWatcher.poll();

        assertFalse(fixedSeen.isEmpty(), "ENTRY_CREATE + ENTRY_MODIFY watcher must see the idx seal write");
    }
}
