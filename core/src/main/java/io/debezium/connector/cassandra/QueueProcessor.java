/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import io.debezium.connector.base.ChangeEventQueue;

/**
 * A thread that constantly polls records from the queue and emit them to Kafka via the KafkaRecordEmitter.
 * The processor is also responsible for marking the offset to file and deleting the commit log files.
 */
public class QueueProcessor extends AbstractProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueueProcessor.class);

    private final ChangeEventQueue<Event> queue;
    private final KafkaRecordEmitter kafkaRecordEmitter;
    private final String commitLogRelocationDir;
    private final Set<String> erroneousCommitLogs;

    private static final String NAME_PREFIX = "Queue Processor ";
    public static final String ARCHIVE_FOLDER = "archive";
    public static final String ERROR_FOLDER = "error";

    public QueueProcessor(CassandraConnectorContext context, int index) {
        super(NAME_PREFIX + "[" + index + "]", Duration.ZERO);
        this.queue = context.getQueues().get(index);
        this.erroneousCommitLogs = context.getErroneousCommitLogs();
        this.commitLogRelocationDir = context.getCassandraConnectorConfig().commitLogRelocationDir();
        this.kafkaRecordEmitter = new KafkaRecordEmitter(
                context.getCassandraConnectorConfig().kafkaTopicPrefix(),
                context.getCassandraConnectorConfig().getHeartbeatTopicsPrefix(),
                context.getKafkaProducer(),
                context.getOffsetWriter(),
                context.getCassandraConnectorConfig().offsetFlushIntervalMs(),
                context.getCassandraConnectorConfig().maxOffsetFlushSize(),
                context.getCassandraConnectorConfig().getKeyConverter(),
                context.getCassandraConnectorConfig().getValueConverter(),
                context.getErroneousCommitLogs(),
                context.getCassandraConnectorConfig().getCommitLogTransfer());
    }

    @VisibleForTesting
    public QueueProcessor(CassandraConnectorContext context, int index, KafkaRecordEmitter emitter) {
        super(NAME_PREFIX + "[" + index + "]", Duration.ZERO);
        this.queue = context.getQueues().get(index);
        this.kafkaRecordEmitter = emitter;
        this.erroneousCommitLogs = context.getErroneousCommitLogs();
        this.commitLogRelocationDir = context.getCassandraConnectorConfig().commitLogRelocationDir();
    }

    @Override
    public void process() throws InterruptedException {
        List<Event> events = queue.poll();
        for (Event event : events) {
            processEvent(event);
        }
    }

    @Override
    public void initialize() throws Exception {
        File dir = new File(commitLogRelocationDir);
        if (!dir.exists()) {
            if (!dir.mkdir()) {
                throw new IOException("Failed to create " + commitLogRelocationDir);
            }
        }
        File archiveDir = new File(dir, ARCHIVE_FOLDER);
        if (!archiveDir.exists()) {
            if (!archiveDir.mkdir()) {
                throw new IOException("Failed to create " + archiveDir);
            }
        }
        File errorDir = new File(dir, ERROR_FOLDER);
        if (!errorDir.exists()) {
            if (!errorDir.mkdir()) {
                throw new IOException("Failed to create " + errorDir);
            }
        }
    }

    @Override
    public void destroy() {
        kafkaRecordEmitter.close();
    }

    private void processEvent(Event event) {
        if (event == null) {
            return;
        }
        switch (event.getEventType()) {
            case CHANGE_EVENT:
                ChangeRecord changeRecord = (ChangeRecord) event;
                kafkaRecordEmitter.emit(changeRecord);
                break;
            case TOMBSTONE_EVENT:
                TombstoneRecord tombstoneRecord = (TombstoneRecord) event;
                kafkaRecordEmitter.emit(tombstoneRecord);
                break;
            case EOF_EVENT:
                EOFEvent eofEvent = (EOFEvent) event;
                String commitLogFileName = eofEvent.file.getName();
                LOGGER.info("Encountered EOF event for {} ...", commitLogFileName);
                String folder = erroneousCommitLogs.contains(commitLogFileName) ? ERROR_FOLDER : ARCHIVE_FOLDER;
                if (CommitLogUtil.moveCommitLog(eofEvent.file, Paths.get(commitLogRelocationDir, folder))) {
                    LOGGER.info("Moved {} into {} folder.", commitLogFileName, folder);
                }
                break;
            default:
                LOGGER.warn("Encountered unexpected record with type: {}", event.getEventType());
        }
    }
}
