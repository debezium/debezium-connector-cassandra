/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.schema.Schema;

import io.debezium.connector.cassandra.transforms.CassandraTypeDeserializer;

/**
 * A task that reads Cassandra commit log in CDC directory and generate corresponding data
 * change events which will be emitted to Kafka. If the table has not been bootstrapped,
 * this task will also take a snapshot of existing data in the database and convert each row
 * into a change event as well.
 */
public class CassandraConnectorTask {
    public static class Cassandra4SchemaLoader implements SchemaLoader {
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

    public static class Cassandra4SchemaChangeListenerProvider implements SchemaChangeListenerProvider {
        @Override
        public AbstractSchemaChangeListener provide(CassandraConnectorConfig config) {
            return new Cassandra4SchemaChangeListener(config.getLogicalName(),
                    config.getSourceInfoStructMaker(),
                    new SchemaHolder());
        }
    }

    public static void main(String[] args) throws Exception {
        CassandraTypeDeserializer.init((abstractType, bb) -> abstractType.getSerializer().deserialize(bb));
        CassandraConnectorTaskTemplate.main(args, config -> new CassandraConnectorTaskTemplate(config,
                new Cassandra4SchemaLoader(),
                new Cassandra4SchemaChangeListenerProvider(),
                context -> new AbstractProcessor[]{ new Cassandra4CommitLogProcessor(context) }));
    }
}
