/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.io.File;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.schema.Schema;

import io.debezium.connector.cassandra.metrics.CassandraStreamingMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;

/**
 * A task that reads Cassandra commit log in CDC directory and generate corresponding data
 * change events which will be emitted to Kafka. If the table has not been bootstrapped,
 * this task will also take a snapshot of existing data in the database and convert each row
 * into a change event as well.
 */
public class CassandraConnectorTask {
    public static class Cassandra5SchemaLoader implements SchemaLoader {
        @Override
        public void load(String cassandraYaml) {
            cassandraYaml = cassandraYaml.startsWith("/") ? cassandraYaml : "/" + cassandraYaml;
            System.setProperty("cassandra.config", "file://" + cassandraYaml);
            if (!DatabaseDescriptor.isDaemonInitialized() && !DatabaseDescriptor.isToolInitialized()) {
                DatabaseDescriptor.toolInitialization();
                Schema.instance.updateHandler.reset(true);
            }
        }
    }

    public static class Cassandra5SchemaChangeListenerProvider implements SchemaChangeListenerProvider {
        @Override
        public AbstractSchemaChangeListener provide(CassandraConnectorConfig config) {
            return new Cassandra5SchemaChangeListener(config.getLogicalName(),
                    config.getSourceInfoStructMaker(),
                    new SchemaHolder());
        }
    }

    public static void main(String[] args) throws Exception {
        CassandraConnectorTaskTemplate.main(args, config -> init(config, new ComponentFactoryStandalone()));
    }

    static CassandraConnectorTaskTemplate init(CassandraConnectorConfig config, ComponentFactory factory) {
        return new CassandraConnectorTaskTemplate(config,
                new Cassandra5TypeProvider(),
                new Cassandra5SchemaLoader(),
                new Cassandra5SchemaChangeListenerProvider(),
                context -> {
                    CassandraStreamingMetrics metrics = new CassandraStreamingMetrics((CdcSourceTaskContext) context);
                    return new AbstractProcessor[]{ new CommitLogIdxProcessor(context, metrics,
                            new Cassandra5CommitLogSegmentReader(context, metrics),
                            new File(DatabaseDescriptor.getCDCLogLocation())) };
                },
                factory);
    }
}
