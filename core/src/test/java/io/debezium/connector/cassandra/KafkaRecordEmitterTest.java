/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static io.debezium.connector.cassandra.utils.TestUtils.generateDefaultConfigMap;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;

class KafkaRecordEmitterTest {

    @SuppressWarnings("unchecked")
    @Test
    void shouldMarkCommitLogErroneousWhenKafkaCallbackFails() throws Exception {
        CassandraConnectorConfig config = new CassandraConnectorConfig(Configuration.from(generateDefaultConfigMap()));
        KafkaProducer<byte[], byte[]> producer = mock(KafkaProducer.class);
        OffsetWriter offsetWriter = mock(OffsetWriter.class);
        Set<String> erroneousCommitLogs = ConcurrentHashMap.newKeySet();

        KafkaRecordEmitter emitter = new KafkaRecordEmitter(
                config,
                producer,
                offsetWriter,
                config.getKeyConverter(),
                config.getValueConverter(),
                erroneousCommitLogs,
                config.getCommitLogTransfer()) {
            @Override
            protected ProducerRecord<byte[], byte[]> toProducerRecord(Record record) {
                return new ProducerRecord<>("topic", new byte[]{ 1 }, new byte[]{ 2 });
            }
        };

        SourceInfo source = new SourceInfo(
                config,
                "cluster1",
                new OffsetPosition("CommitLog-6-123.log", 0),
                new KeyspaceTable("ks", "tbl"),
                false,
                Instant.now());

        Record record = mock(Record.class);
        when(record.getSource()).thenReturn(source);

        doAnswer(invocation -> {
            Callback callback = invocation.getArgument(1);
            callback.onCompletion(null, new RuntimeException("simulated send failure"));
            return mock(Future.class);
        }).when(producer).send(any(ProducerRecord.class), any(Callback.class));

        emitter.emit(record);

        assertTrue(erroneousCommitLogs.contains("CommitLog-6-123.log"));
        verify(offsetWriter, never()).markOffset(any(), any(), anyBoolean());
    }
}
