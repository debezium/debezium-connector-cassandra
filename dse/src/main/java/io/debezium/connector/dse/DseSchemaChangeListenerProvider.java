/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dse;

import io.debezium.connector.cassandra.AbstractSchemaChangeListener;
import io.debezium.connector.cassandra.CassandraConnectorConfig;
import io.debezium.connector.cassandra.SchemaChangeListenerProvider;
import io.debezium.connector.cassandra.SchemaHolder;

public class DseSchemaChangeListenerProvider implements SchemaChangeListenerProvider {

    @Override
    public AbstractSchemaChangeListener provide(CassandraConnectorConfig config) {
        return new DseSchemaChangeListener(config.getLogicalName(),
                config.getSourceInfoStructMaker(),
                new SchemaHolder());
    }
}