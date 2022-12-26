/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import io.debezium.config.Configuration;
import io.debezium.connector.cassandra.CassandraConnectorTask.Cassandra3SchemaChangeListenerProvider;
import io.debezium.connector.cassandra.CassandraConnectorTask.Cassandra3SchemaLoader;
import io.debezium.connector.cassandra.transforms.CassandraTypeDeserializer;

public class EmbeddedCassandra3ConnectorTestBase extends CassandraConnectorTestBase {

    @Override
    protected CassandraConnectorContext generateTaskContext(Configuration configuration) throws Exception {

        CassandraConnectorConfig config = new CassandraConnectorConfig(configuration);
        Cassandra3TypeProvider provider = new Cassandra3TypeProvider();
        CassandraTypeDeserializer.init(provider.deserializers(), config.getDecimalMode(), config.getVarIntMode(),
                provider.baseTypeForReversedType());

        return new CassandraConnectorContext(config,
                new Cassandra3SchemaLoader(),
                new Cassandra3SchemaChangeListenerProvider(),
                new FileOffsetWriter(config.offsetBackingStoreDir()));
    }
}
