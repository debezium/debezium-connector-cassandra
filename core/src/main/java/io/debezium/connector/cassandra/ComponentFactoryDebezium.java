/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;

public class ComponentFactoryDebezium implements ComponentFactory {

    private final ChangeEventQueue<DataChangeEvent> queue;

    private final CassandraPartition partition;

    private final CassandraOffsetContext offset;

    public ComponentFactoryDebezium(ChangeEventQueue<DataChangeEvent> queue, CassandraPartition partition,
                                    CassandraOffsetContext offset) {
        this.queue = queue;
        this.partition = partition;
        this.offset = offset;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Emitter recordEmitter(CassandraConnectorContext context) {
        return new SourceRecordEmitter(queue, context.getCassandraConnectorConfig()
                .getTopicNamingStrategy(CommonConnectorConfig.TOPIC_NAMING_STRATEGY), partition, offset);
    }

    @Override
    public OffsetWriter offsetWriter(CassandraConnectorConfig config) {
        return new OffsetWriter() {

            @Override
            public Future<?> markOffset(String sourceTable, String sourceOffset, boolean isSnapshot) {
                offset.putOffset(sourceTable, isSnapshot, sourceOffset);
                return CompletableFuture.completedFuture(new Object());
            }

            @Override
            public boolean isOffsetProcessed(String sourceTable, String sourceOffset, boolean isSnapshot) {
                String previousOffset = offset.getOffset(sourceTable, isSnapshot);
                OffsetPosition previousOffsetPosition = previousOffset == null ? null
                        : OffsetPosition.parse(previousOffset);
                OffsetPosition currentOffsetPosition = OffsetPosition.parse(sourceOffset);
                return previousOffsetPosition != null && currentOffsetPosition.compareTo(previousOffsetPosition) <= 0;
            }

            @Override
            public Future<?> flush() {
                return CompletableFuture.completedFuture(new Object());
            }

            @Override
            public void close() {

            }
        };
    }
}
