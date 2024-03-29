/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.io.IOException;

import org.apache.kafka.clients.producer.KafkaProducer;

import io.debezium.connector.cassandra.exceptions.CassandraConnectorConfigException;

public class ComponentFactoryStandalone implements ComponentFactory {

    @Override
    public OffsetWriter offsetWriter(CassandraConnectorConfig config) {
        try {
            return new FileOffsetWriter(config.offsetBackingStoreDir(), config.offsetFlushIntervalMs(), config.maxOffsetFlushSize());
        }
        catch (IOException e) {
            throw new CassandraConnectorConfigException(String.format("cannot create file offset writer into %s", config.offsetBackingStoreDir()), e);
        }
    }

    @Override
    public Emitter recordEmitter(CassandraConnectorContext context) {
        CassandraConnectorConfig config = context.getCassandraConnectorConfig();
        return new KafkaRecordEmitter(
                config,
                new KafkaProducer<>(config.getKafkaConfigs()),
                context.getOffsetWriter(),
                config.getKeyConverter(),
                config.getValueConverter(),
                context.getErroneousCommitLogs(),
                config.getCommitLogTransfer());
    }

}
