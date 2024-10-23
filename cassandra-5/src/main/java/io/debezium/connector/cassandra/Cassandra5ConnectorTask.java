/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

public class Cassandra5ConnectorTask extends AbstractConnectorTask {

    @Override
    protected CassandraConnectorTaskTemplate init(CassandraConnectorConfig config, ComponentFactory factory) {
        return CassandraConnectorTask.init(config, factory);
    }

}
