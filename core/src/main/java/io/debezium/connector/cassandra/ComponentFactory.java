/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

public interface ComponentFactory {

    OffsetWriter offsetWriter(CassandraConnectorConfig config);

    Emitter recordEmitter(CassandraConnectorContext context);

}
