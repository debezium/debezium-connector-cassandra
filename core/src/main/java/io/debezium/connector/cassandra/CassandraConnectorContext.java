/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.clients.producer.KafkaProducer;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.cassandra.exceptions.CassandraConnectorTaskException;
import io.debezium.connector.common.CdcSourceTaskContext;

/**
 * Contains contextual information and objects scoped to the lifecycle
 * of CassandraConnectorTask implementation.
 */
public class CassandraConnectorContext extends CdcSourceTaskContext {
    private final CassandraConnectorConfig config;
    private final CassandraClient cassandraClient;
    private final List<ChangeEventQueue<Event>> queues = new ArrayList<>();
    private final KafkaProducer kafkaProducer;
    private final SchemaHolder schemaHolder;
    private final OffsetWriter offsetWriter;
    private final Set<String> erroneousCommitLogs;
    private final AbstractSchemaChangeListener schemaChangeListener;

    public CassandraConnectorContext(CassandraConnectorConfig config,
                                     SchemaLoader schemaLoader,
                                     SchemaChangeListenerProvider schemaChangeListenerProvider)
            throws Exception {
        super(config.getContextName(), config.getLogicalName(), Collections::emptySet);
        this.config = config;

        try {
            // Create a HashSet to record names of CommitLog Files which are not successfully read or streamed.
            this.erroneousCommitLogs = ConcurrentHashMap.newKeySet();

            // Loading up DDL schemas from disk
            schemaLoader.load(this.config.cassandraConfig());

            this.schemaChangeListener = schemaChangeListenerProvider.provide(this.config);

            // Setting up Cassandra driver
            this.cassandraClient = new CassandraClient(config.cassandraDriverConfig(), schemaChangeListener);

            prepareQueues();

            // Creating Kafka Producer
            this.kafkaProducer = new KafkaProducer(this.config.getKafkaConfigs());

            // Setting up schema holder ...
            this.schemaHolder = schemaChangeListener.getSchemaHolder();

            // Setting up a file-based offset manager ...
            this.offsetWriter = new FileOffsetWriter(this.config.offsetBackingStoreDir());
        }
        catch (Exception e) {
            // Clean up CassandraClient and FileOffsetWrite if connector context fails to be completely initialized.
            cleanUp();
            throw new CassandraConnectorTaskException("Failed to initialize Cassandra Connector Context.", e);
        }

    }

    private void prepareQueues() {
        int numOfChangeEventQueues = this.config.numOfChangeEventQueues();
        for (int i = 0; i < numOfChangeEventQueues; i++) {
            ChangeEventQueue<Event> queue = new ChangeEventQueue.Builder<Event>()
                    .pollInterval(this.config.pollInterval())
                    .maxBatchSize(this.config.maxBatchSize())
                    .maxQueueSize(this.config.maxQueueSize())
                    .loggingContextSupplier(() -> this.configureLoggingContext(this.config.getContextName()))
                    .build();
            queues.add(queue);
        }
    }

    public void cleanUp() {
        if (this.cassandraClient != null) {
            this.cassandraClient.close();
        }
        if (this.offsetWriter != null) {
            this.offsetWriter.close();
        }
    }

    public CassandraConnectorConfig getCassandraConnectorConfig() {
        return config;
    }

    public CassandraClient getCassandraClient() {
        return cassandraClient;
    }

    public List<ChangeEventQueue<Event>> getQueues() {
        return queues;
    }

    public KafkaProducer getKafkaProducer() {
        return kafkaProducer;
    }

    public OffsetWriter getOffsetWriter() {
        return offsetWriter;
    }

    public SchemaHolder getSchemaHolder() {
        return schemaHolder;
    }

    public Set<String> getErroneousCommitLogs() {
        return erroneousCommitLogs;
    }
}
