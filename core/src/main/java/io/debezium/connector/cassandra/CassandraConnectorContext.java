/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.util.List;
import java.util.Set;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.util.LoggingContext;

/**
 * Contains contextual information and objects scoped to the lifecycle
 * of CassandraConnectorTask implementation.
 */
public interface CassandraConnectorContext {

    void cleanUp();

    CassandraConnectorConfig getCassandraConnectorConfig();

    LoggingContext.PreviousContext configureLoggingContext(String contextName);

    CassandraClient getCassandraClient();

    List<ChangeEventQueue<Event>> getQueues();

    OffsetWriter getOffsetWriter();

    SchemaHolder getSchemaHolder();

    Set<String> getErroneousCommitLogs();

    String getClusterName();
}
