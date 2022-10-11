/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

public class Cassandra3ConnectorTask extends AbstractConnectorTask {

    @Override
    protected CassandraConnectorTaskTemplate init(CassandraConnectorConfig conf, ComponentFactory factory) {
        return CassandraConnectorTask.init(conf, factory);
    }

}
