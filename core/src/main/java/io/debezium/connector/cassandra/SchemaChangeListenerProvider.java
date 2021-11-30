/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

/**
 * Implement this interface in order to provide a Cassandra specific schema change listener reacting on DDL changes.
 */
public interface SchemaChangeListenerProvider {
    AbstractSchemaChangeListener provide(CassandraConnectorConfig config);
}
