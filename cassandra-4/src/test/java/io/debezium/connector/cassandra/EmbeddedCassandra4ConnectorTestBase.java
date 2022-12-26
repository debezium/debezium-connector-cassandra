/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import io.debezium.config.Configuration;
import io.debezium.connector.cassandra.CassandraConnectorTask.Cassandra4SchemaChangeListenerProvider;
import io.debezium.connector.cassandra.CassandraConnectorTask.Cassandra4SchemaLoader;
import io.debezium.connector.cassandra.transforms.CassandraTypeDeserializer;

public abstract class EmbeddedCassandra4ConnectorTestBase extends CassandraConnectorTestBase {

    @Override
    protected CassandraConnectorContext generateTaskContext(Configuration configuration) throws Exception {

        CassandraConnectorConfig config = new CassandraConnectorConfig(configuration);
        Cassandra4TypeProvider provider = new Cassandra4TypeProvider();
        CassandraTypeDeserializer.init(provider.deserializers(), config.getDecimalMode(), config.getVarIntMode(),
                provider.baseTypeForReversedType());

        return new CassandraConnectorContext(config,
                new Cassandra4SchemaLoader(),
                new Cassandra4SchemaChangeListenerProvider(),
                new FileOffsetWriter(config.offsetBackingStoreDir()));
    }
}
