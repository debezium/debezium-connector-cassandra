/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.cassandra.exceptions.CassandraConnectorTaskException;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.spi.topic.TopicNamingStrategy;

public class SourceRecordEmitter implements Emitter {

    private final ChangeEventQueue<DataChangeEvent> queue;

    private final TopicNamingStrategy<KeyspaceTable> topicNamingStrategy;

    private final CassandraPartition partition;

    private final CassandraOffsetContext offset;

    public SourceRecordEmitter(ChangeEventQueue<DataChangeEvent> queue, TopicNamingStrategy<KeyspaceTable> topicNamingStrategy,
                               CassandraPartition partition, CassandraOffsetContext offset) {
        this.queue = queue;
        this.topicNamingStrategy = topicNamingStrategy;
        this.partition = partition;
        this.offset = offset;
    }

    @Override
    public void close() throws Exception {
    }

    @Override
    public void emit(Record record) {
        SourceInfo source = record.getSource();
        offset.putOffset(source.keyspaceTable.name(), source.snapshot, source.offsetPosition.serialize());
        SourceRecord sourceRecord = new SourceRecord(
                partition.getSourcePartition(),
                offset.getOffset(),
                topicNamingStrategy.dataChangeTopic(source.keyspaceTable),
                null,
                record.getKeySchema(),
                record.buildKey(),
                record.getValueSchema(),
                record.buildValue(),
                null,
                null);
        DataChangeEvent event = new DataChangeEvent(sourceRecord);
        try {
            this.queue.enqueue(event);
        }
        catch (InterruptedException e) {
            throw new CassandraConnectorTaskException(e);
        }
    }

}
