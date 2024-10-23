/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import org.apache.kafka.connect.connector.Task;

public class Cassandra5Connector extends AbstractSourceConnector {

    @Override
    public Class<? extends Task> taskClass() {
        return Cassandra5ConnectorTask.class;
    }

}
