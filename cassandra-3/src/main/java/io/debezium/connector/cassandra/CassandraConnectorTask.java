/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.io.File;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;

/**
 * A task that reads Cassandra commit log in CDC directory and generate corresponding data
 * change events which will be emitted to Kafka. If the table has not been bootstrapped,
 * this task will also take a snapshot of existing data in the database and convert each row
 * into a change event as well.
 */
public class CassandraConnectorTask {

    public static class Cassandra3SchemaLoader implements SchemaLoader {

        @Override
        public void load(String cassandraYaml) {
            cassandraYaml = cassandraYaml.startsWith("/") ? cassandraYaml : "/" + cassandraYaml;
            System.setProperty("cassandra.config", "file://" + cassandraYaml);
            if (!DatabaseDescriptor.isDaemonInitialized() && !DatabaseDescriptor.isToolInitialized()) {
                DatabaseDescriptor.toolInitialization();
                Schema.instance.loadFromDisk(false);
            }
        }
    }

    public static class Cassandra3SchemaChangeListenerProvider implements SchemaChangeListenerProvider {

        @Override
        public AbstractSchemaChangeListener provide(CassandraConnectorConfig config) {
            return new Cassandra3SchemaChangeListener(config.getLogicalName(),
                    config.getSourceInfoStructMaker(),
                    new SchemaHolder());
        }
    }

    public static void main(String[] args) throws Exception {
        CassandraConnectorTaskTemplate.main(args, config -> init(config, new ComponentFactoryStandalone()));
    }

    static CassandraConnectorTaskTemplate init(CassandraConnectorConfig config, ComponentFactory factory) {
        CommitLogProcessorMetrics metrics = new CommitLogProcessorMetrics();
        return new CassandraConnectorTaskTemplate(config,
                new Cassandra3TypeProvider(),
                new Cassandra3SchemaLoader(),
                new Cassandra3SchemaChangeListenerProvider(),
                context -> new AbstractProcessor[]{ new CommitLogProcessor(context, metrics,
                        new Cassandra3CommitLogSegmentReader(context, metrics),
                        new File(DatabaseDescriptor.getCDCLogLocation()),
                        new File(DatabaseDescriptor.getCommitLogLocation())) },
                factory);
    }
}
