/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dse;

import org.apache.kafka.connect.connector.Task;

import io.debezium.connector.cassandra.AbstractSourceConnector;

public class Dse680Connector extends AbstractSourceConnector {

    @Override
    public Class<? extends Task> taskClass() {
        return Dse680ConnectorTask.class;
    }

}
