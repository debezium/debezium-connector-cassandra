/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.spi.topic.TopicNamingStrategy;

/**
 * This emitter is responsible for emitting records to Kafka broker and managing offsets post send.
 */
public class KafkaRecordEmitter implements Emitter {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaRecordEmitter.class);

    private final KafkaProducer<byte[], byte[]> producer;
    private final TopicNamingStrategy<KeyspaceTable> topicNamingStrategy;
    private final OffsetWriter offsetWriter;
    private final OffsetFlushPolicy offsetFlushPolicy;
    private final Set<String> erroneousCommitLogs;
    private final CommitLogTransfer commitLogTransfer;
    private final Map<Record, Future<RecordMetadata>> futures = new LinkedHashMap<>();
    private final Object lock = new Object();
    private final Converter keyConverter;
    private final Converter valueConverter;
    private long timeOfLastFlush;
    private long emitCount = 0;

    @SuppressWarnings("unchecked")
    public KafkaRecordEmitter(CassandraConnectorConfig connectorConfig, KafkaProducer<byte[], byte[]> kafkaProducer,
                              OffsetWriter offsetWriter, Duration offsetFlushIntervalMs, long maxOffsetFlushSize,
                              Converter keyConverter, Converter valueConverter, Set<String> erroneousCommitLogs,
                              CommitLogTransfer commitLogTransfer) {
        this.producer = kafkaProducer;
        this.topicNamingStrategy = connectorConfig.getTopicNamingStrategy(CommonConnectorConfig.TOPIC_NAMING_STRATEGY);
        this.offsetWriter = offsetWriter;
        this.offsetFlushPolicy = offsetFlushIntervalMs.isZero() ? OffsetFlushPolicy.always() : OffsetFlushPolicy.periodic(offsetFlushIntervalMs, maxOffsetFlushSize);
        this.erroneousCommitLogs = erroneousCommitLogs;
        this.commitLogTransfer = commitLogTransfer;
        this.keyConverter = keyConverter;
        this.valueConverter = valueConverter;
    }

    @Override
    public void emit(Record record) {
        try {
            synchronized (lock) {
                ProducerRecord<byte[], byte[]> producerRecord = toProducerRecord(record);
                Future<RecordMetadata> future = producer.send(producerRecord);
                LOGGER.trace("Sent to topic {}: {}", producerRecord.topic(), record);
                futures.put(record, future);
                maybeFlushAndMarkOffset();
            }
        }
        catch (Exception e) {
            if (record.getSource().snapshot || commitLogTransfer.getClass().getName().equals(CassandraConnectorConfig.DEFAULT_COMMIT_LOG_TRANSFER_CLASS)) {
                throw new DebeziumException(String.format("Failed to send record %s", record), e);
            }
            LOGGER.error("Failed to send the record {}. Error: ", record, e);
            erroneousCommitLogs.add(record.getSource().offsetPosition.fileName);
        }
    }

    protected ProducerRecord<byte[], byte[]> toProducerRecord(Record record) {
        String topic = topicNamingStrategy.dataChangeTopic(record.getSource().keyspaceTable);
        byte[] serializedKey = keyConverter.fromConnectData(topic, record.getKeySchema(), record.buildKey());
        byte[] serializedValue = valueConverter.fromConnectData(topic, record.getValueSchema(), record.buildValue());
        return new ProducerRecord<>(topic, serializedKey, serializedValue);
    }

    private void maybeFlushAndMarkOffset() {
        long now = System.currentTimeMillis();
        long timeSinceLastFlush = now - timeOfLastFlush;
        if (offsetFlushPolicy.shouldFlush(Duration.ofMillis(timeSinceLastFlush), futures.size())) {
            flushAndMarkOffset();
            timeOfLastFlush = now;
        }
    }

    private void flushAndMarkOffset() {
        futures.entrySet().stream().filter(this::flush).filter(this::hasOffset).forEach(this::markOffset);
        offsetWriter.flush();
        futures.clear();
    }

    private boolean flush(Map.Entry<Record, Future<RecordMetadata>> recordEntry) {
        try {
            recordEntry.getValue().get(); // wait
            if (++emitCount % 10_000 == 0) {
                LOGGER.debug("Emitted {} records to Kafka Broker", emitCount);
                emitCount = 0;
            }
            return true;
        }
        catch (ExecutionException | InterruptedException e) {
            LOGGER.error("Failed to emit record {}", recordEntry.getKey(), e);
            return false;
        }
    }

    private boolean hasOffset(Map.Entry<Record, Future<RecordMetadata>> recordEntry) {
        Record record = recordEntry.getKey();
        if (record.getSource().snapshot || commitLogTransfer.getClass().getName().equals(CassandraConnectorConfig.DEFAULT_COMMIT_LOG_TRANSFER_CLASS)) {
            return record.shouldMarkOffset();
        }
        return record.shouldMarkOffset() && !erroneousCommitLogs.contains(record.getSource().offsetPosition.fileName);
    }

    private void markOffset(Map.Entry<Record, Future<RecordMetadata>> recordEntry) {
        SourceInfo source = recordEntry.getKey().getSource();
        String sourceTable = source.keyspaceTable.name();
        String sourceOffset = source.offsetPosition.serialize();
        boolean isSnapshot = source.snapshot;
        offsetWriter.markOffset(sourceTable, sourceOffset, isSnapshot);
        if (isSnapshot) {
            LOGGER.debug("Mark snapshot offset for table '{}'", sourceTable);
        }
    }

    public void close() throws Exception {
        producer.close();
    }
}
