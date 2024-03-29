/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.cassandra.exceptions.CassandraConnectorConfigException;
import io.debezium.connector.cassandra.exceptions.CassandraConnectorTaskException;

/**
 * A concrete implementation of {@link OffsetWriter} which tracks the progress of events
 * being processed by the {@link SnapshotProcessor} and respective commit log processor to
 * property files, snapshot_offset.properties and commitlog_offset.properties, respectively.
 *
 * The property key is the table for the offset, and is serialized in the format of <keyspace>.<table>
 * The property value is the offset position, and is serialized in the format of <file_name>:<file_position>.
 *
 * For snapshots, a table is either fully processed or not processed at all,
 * so offset is given a default value of ":-1" , where the filename is an empty
 * string, and file position is -1.
 *
 * For commit logs, the file_name represents the commit log file name and
 * file position represents bytes read in the commit log.
 */
public class FileOffsetWriter implements OffsetWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileOffsetWriter.class);

    public static final String SNAPSHOT_OFFSET_FILE = "snapshot_offset.properties";
    public static final String COMMITLOG_OFFSET_FILE = "commitlog_offset.properties";

    private final Properties snapshotProps = new Properties();
    private final Properties commitLogProps = new Properties();

    private final File snapshotOffsetFile;
    private final File commitLogOffsetFile;

    private final FileLock snapshotOffsetFileLock;
    private final FileLock commitLogOffsetFileLock;

    private final OffsetFlushPolicy offsetFlushPolicy;
    private final ExecutorService executorService;

    private long timeOfLastFlush;
    private long unflushedRecordCount;

    public FileOffsetWriter(String offsetDir, OffsetFlushPolicy offsetFlushPolicy) throws IOException {
        if (offsetDir == null) {
            throw new CassandraConnectorConfigException("Offset file directory must be configured at the start");
        }

        this.offsetFlushPolicy = offsetFlushPolicy;
        this.timeOfLastFlush = System.currentTimeMillis();

        File offsetDirectory = new File(offsetDir);
        if (!offsetDirectory.exists()) {
            Files.createDirectories(offsetDirectory.toPath());
        }
        this.snapshotOffsetFile = Paths.get(offsetDirectory.getAbsolutePath(), SNAPSHOT_OFFSET_FILE).toFile();
        this.commitLogOffsetFile = Paths.get(offsetDirectory.getAbsolutePath(), COMMITLOG_OFFSET_FILE).toFile();

        snapshotOffsetFileLock = init(this.snapshotOffsetFile);
        commitLogOffsetFileLock = init(this.commitLogOffsetFile);

        loadOffset(this.snapshotOffsetFile, snapshotProps);
        loadOffset(this.commitLogOffsetFile, commitLogProps);
        this.executorService = Executors.newFixedThreadPool(1);
    }

    public FileOffsetWriter(String offsetDir, Duration offsetFlushIntervalMs, long maxOffsetFlushSize) throws IOException {
        this(offsetDir, offsetFlushIntervalMs.isZero() ? OffsetFlushPolicy.always() : OffsetFlushPolicy.periodic(offsetFlushIntervalMs, maxOffsetFlushSize));
    }

    public FileOffsetWriter(String offsetDir) throws IOException {
        this(offsetDir, OffsetFlushPolicy.never());
    }

    @Override
    public void markOffset(String sourceTable, String sourceOffset, boolean isSnapshot) {
        executorService.submit(() -> performMarkOffset(sourceTable, sourceOffset, isSnapshot));
    }

    private void performMarkOffset(String sourceTable, String sourceOffset, boolean isSnapshot) {
        if (isSnapshot) {
            if (!isOffsetProcessed(sourceTable, sourceOffset, isSnapshot)) {
                snapshotProps.setProperty(sourceTable, sourceOffset);
            }
        }
        else {
            if (!isOffsetProcessed(sourceTable, sourceOffset, isSnapshot)) {
                commitLogProps.setProperty(sourceTable, sourceOffset);
            }
        }
        unflushedRecordCount += 1;
        maybeFlushOffset();
    }

    @Override
    public boolean isOffsetProcessed(String sourceTable, String sourceOffset, boolean isSnapshot) {
        if (isSnapshot) {
            return snapshotProps.containsKey(sourceTable);
        }
        else {
            OffsetPosition currentOffset = OffsetPosition.parse(sourceOffset);
            OffsetPosition recordedOffset = commitLogProps.containsKey(sourceTable) ? OffsetPosition.parse((String) commitLogProps.get(sourceTable)) : null;
            return recordedOffset != null && currentOffset.compareTo(recordedOffset) <= 0;
        }
    }

    private void maybeFlushOffset() {
        long now = System.currentTimeMillis();
        long timeSinceLastFlush = now - timeOfLastFlush;
        if (offsetFlushPolicy.shouldFlush(Duration.ofMillis(timeSinceLastFlush), unflushedRecordCount)) {
            this.performFlush();
            timeOfLastFlush = now;
            unflushedRecordCount = 0;
        }
    }

    @Override
    public void flush() {
        executorService.submit(this::performFlush);
    }

    private void performFlush() {
        try {
            saveOffset(snapshotOffsetFile, snapshotProps);
            saveOffset(commitLogOffsetFile, commitLogProps);
        }
        catch (IOException e) {
            LOGGER.warn("Ignoring flush failure", e);
        }
    }

    @Override
    public void close() {
        try {
            if (!executorService.awaitTermination(1, TimeUnit.SECONDS)) {
                executorService.shutdown();
            }
        }
        catch (InterruptedException ignored) {
        }

        try {
            snapshotOffsetFileLock.release();
        }
        catch (IOException e) {
            LOGGER.warn("Failed to release snapshot offset file lock");
        }

        try {
            commitLogOffsetFileLock.release();
        }
        catch (IOException e) {
            LOGGER.warn("Failed to release commit log offset file lock");
        }
    }

    private static void saveOffset(File offsetFile, Properties props) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(offsetFile)) {
            props.store(fos, null);
        }
        catch (IOException e) {
            throw new IOException("Failed to save offset for file " + offsetFile.getAbsolutePath(), e);
        }
    }

    private void loadOffset(File offsetFile, Properties props) throws IOException {
        try (FileInputStream fis = new FileInputStream(offsetFile)) {
            props.load(fis);
        }
        catch (IOException e) {
            throw new IOException("Failed to load offset for file " + offsetFile.getAbsolutePath(), e);
        }
    }

    private FileLock init(File offsetFile) throws IOException {
        Path lockPath = initLockPath(offsetFile);

        try {
            FileChannel channel = FileChannel.open(lockPath, StandardOpenOption.READ, StandardOpenOption.WRITE);
            FileLock lock = channel.tryLock();
            if (lock == null) {
                throw new CassandraConnectorTaskException(
                        "Failed to acquire file lock on " + lockPath + ". There might be another Cassandra Connector Task running");
            }
            return lock;
        }
        catch (OverlappingFileLockException e) {
            throw new CassandraConnectorTaskException("Failed to acquire file lock on " + lockPath + ". There might be another thread running", e);
        }
    }

    private Path initLockPath(File offsetFile) throws IOException {
        if (!offsetFile.exists()) {
            Files.createFile(offsetFile.toPath());
        }
        if (System.getProperty("os.name").toLowerCase().contains("windows")) {
            offsetFile = new File(offsetFile.getAbsolutePath() + ".lock");
            if (!offsetFile.exists()) {
                Files.createFile(offsetFile.toPath());
            }
        }
        return offsetFile.toPath();
    }
}
