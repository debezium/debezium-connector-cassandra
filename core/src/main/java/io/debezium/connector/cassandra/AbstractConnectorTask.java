/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static io.debezium.connector.cassandra.CassandraConnectorConfig.CASSANDRA_NODE_ID;
import static io.debezium.connector.cassandra.CassandraConnectorConfig.COMMIT_LOG_RELOCATION_DIR;
import static io.debezium.connector.cassandra.CassandraConnectorConfig.SCHEMA_POLL_INTERVAL_MS;
import static io.debezium.connector.cassandra.CassandraConnectorConfig.SNAPSHOT_POLL_INTERVAL_MS;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.cassandra.exceptions.CassandraConnectorConfigException;
import io.debezium.connector.cassandra.exceptions.CassandraConnectorTaskException;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.spi.Offsets;

public abstract class AbstractConnectorTask extends BaseSourceTask<CassandraPartition, CassandraOffsetContext> {

    private volatile ChangeEventQueue<DataChangeEvent> queue;

    private CassandraConnectorTaskTemplate template;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    protected ChangeEventSourceCoordinator<CassandraPartition, CassandraOffsetContext> start(Configuration config) {
        CassandraConnectorConfig connectorConfig = new CassandraConnectorConfig(config);

        if (connectorConfig.numOfChangeEventQueues() != 1) {
            throw new CassandraConnectorConfigException(String.format("configuration property %s must be equal to 1",
                    CassandraConnectorConfig.NUM_OF_CHANGE_EVENT_QUEUES.name()));
        }
        connectorConfig.setValidationFieldList(Arrays.asList(CASSANDRA_NODE_ID, COMMIT_LOG_RELOCATION_DIR,
                SCHEMA_POLL_INTERVAL_MS, SNAPSHOT_POLL_INTERVAL_MS));

        this.queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(connectorConfig.getPollInterval())
                .maxBatchSize(connectorConfig.getMaxBatchSize())
                .maxQueueSize(connectorConfig.getMaxQueueSize())
                .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
                .loggingContextSupplier(() -> template.getTaskContext().configureLoggingContext(connectorConfig.getContextName()))
                .build();

        CassandraOffsetContext.Loader offsetLoader = new CassandraOffsetContext.Loader();
        Offsets<CassandraPartition, CassandraOffsetContext> previousOffsets = getPreviousOffsets(
                new CassandraPartition.Provider(connectorConfig), offsetLoader);
        CassandraOffsetContext previousOffset = previousOffsets.getTheOnlyOffset();
        if (previousOffset == null) {
            previousOffset = offsetLoader.load(new HashMap<>());
        }

        template = init(connectorConfig,
                new ComponentFactoryDebezium(queue, previousOffsets.getTheOnlyPartition(), previousOffset));
        try {
            template.start();
        }
        catch (Exception e) {
            throw new CassandraConnectorTaskException(e);
        }
        return null;
    }

    protected abstract CassandraConnectorTaskTemplate init(CassandraConnectorConfig conf, ComponentFactory factory);

    @Override
    protected List<SourceRecord> doPoll() throws InterruptedException {
        List<DataChangeEvent> records = queue.poll();
        return records.stream().map(DataChangeEvent::getRecord).collect(Collectors.toList());
    }

    @Override
    protected void doStop() {
        try {
            template.stopAll();
        }
        catch (Exception e) {
            throw new CassandraConnectorTaskException(e);
        }
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return Collections.emptyList();
    }
}
