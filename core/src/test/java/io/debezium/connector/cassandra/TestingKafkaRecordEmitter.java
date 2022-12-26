/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.storage.Converter;

public class TestingKafkaRecordEmitter extends KafkaRecordEmitter {

    public List<ProducerRecord<byte[], byte[]>> records = new ArrayList<>();

    public TestingKafkaRecordEmitter(CassandraConnectorConfig connectorConfig, KafkaProducer<byte[], byte[]> kafkaProducer,
                                     OffsetWriter offsetWriter, Duration offsetFlushIntervalMs, long maxOffsetFlushSize,
                                     Converter keyConverter, Converter valueConverter, Set<String> erroneousCommitLogs,
                                     CommitLogTransfer commitLogTransfer) {
        super(connectorConfig, kafkaProducer, offsetWriter, offsetFlushIntervalMs, maxOffsetFlushSize, keyConverter, valueConverter,
                erroneousCommitLogs, commitLogTransfer);
    }

    @Override
    public void emit(Record record) {
        toProducerRecord(record);
    }

    @Override
    protected ProducerRecord<byte[], byte[]> toProducerRecord(Record record) {
        ProducerRecord<byte[], byte[]> producerRecord = super.toProducerRecord(record);
        records.add(producerRecord);
        return producerRecord;
    }
}
