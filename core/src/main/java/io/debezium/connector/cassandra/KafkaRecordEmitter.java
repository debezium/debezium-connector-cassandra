/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
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
    private static final int RECORD_LOG_COUNT = 10_000;

    private final KafkaProducer<byte[], byte[]> producer;
    private final TopicNamingStrategy<KeyspaceTable> topicNamingStrategy;
    private final OffsetWriter offsetWriter;
    private final Set<String> erroneousCommitLogs;
    private final CommitLogTransfer commitLogTransfer;
    private final Converter keyConverter;
    private final Converter valueConverter;
    private final AtomicLong emitCount = new AtomicLong();

    @SuppressWarnings("unchecked")
    public KafkaRecordEmitter(CassandraConnectorConfig connectorConfig, KafkaProducer<byte[], byte[]> kafkaProducer,
                              OffsetWriter offsetWriter, Converter keyConverter, Converter valueConverter,
                              Set<String> erroneousCommitLogs, CommitLogTransfer commitLogTransfer) {
        this.producer = kafkaProducer;
        this.topicNamingStrategy = connectorConfig.getTopicNamingStrategy(CommonConnectorConfig.TOPIC_NAMING_STRATEGY);
        this.offsetWriter = offsetWriter;
        this.erroneousCommitLogs = erroneousCommitLogs;
        this.commitLogTransfer = commitLogTransfer;
        this.keyConverter = keyConverter;
        this.valueConverter = valueConverter;
    }

    @Override
    public void emit(Record record) {
        try {
            ProducerRecord<byte[], byte[]> producerRecord = toProducerRecord(record);
            producer.send(producerRecord, (metadata, exception) -> callback(record, exception));
            LOGGER.trace("Sent to topic {}: {}", producerRecord.topic(), record);
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

    private void callback(Record record, Exception exception) {
        if (exception != null) {
            LOGGER.error("Failed to emit record {}", record, exception);
            return;
        }
        long emitted = emitCount.incrementAndGet();
        if (emitted % RECORD_LOG_COUNT == 0) {
            LOGGER.debug("Emitted {} records to Kafka Broker", emitted);
            emitCount.addAndGet(-emitted);
        }
        if (hasOffset(record)) {
            markOffset(record);
        }
    }

    private boolean hasOffset(Record record) {
        if (record.getSource().snapshot || commitLogTransfer.getClass().getName().equals(CassandraConnectorConfig.DEFAULT_COMMIT_LOG_TRANSFER_CLASS)) {
            return record.shouldMarkOffset();
        }
        return record.shouldMarkOffset() && !erroneousCommitLogs.contains(record.getSource().offsetPosition.fileName);
    }

    private void markOffset(Record record) {
        SourceInfo source = record.getSource();
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
