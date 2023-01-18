/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dse;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.schema.SchemaManager;

import io.debezium.connector.cassandra.SchemaLoader;

public class DseSchemaLoader implements SchemaLoader {

    @Override
    public void load(String cassandraYaml) {
        cassandraYaml = cassandraYaml.startsWith("/") ? cassandraYaml : "/" + cassandraYaml;
        System.setProperty("cassandra.config", "file://" + cassandraYaml);
        if (!DatabaseDescriptor.isDaemonInitialized() && !DatabaseDescriptor.isToolInitialized()) {
            DatabaseDescriptor.toolInitialization();
            SchemaManager.instance.loadFromDisk(false);
        }
    }
}