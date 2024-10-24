/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.spi;

import io.debezium.config.Configuration;
import io.debezium.connector.cassandra.CassandraConnectorContext;
import io.debezium.connector.cassandra.CommitLogProcessorMetrics;

public interface CassandraTestProvider {

    CassandraConnectorContext provideContext(Configuration configuration) throws Throwable;

    CassandraConnectorContext provideContextWithoutSchemaManagement(Configuration configuration);

    CommitLogProcessing provideCommitLogProcessing(CassandraConnectorContext context, CommitLogProcessorMetrics metrics);
}
