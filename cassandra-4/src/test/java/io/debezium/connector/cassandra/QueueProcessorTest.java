/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import io.debezium.config.Configuration;
import io.debezium.connector.cassandra.transforms.CassandraTypeDeserializer;

public class QueueProcessorTest extends AbstractQueueProcessorTest {
    @Override
    public CassandraConnectorContext generateTaskContext(Configuration configuration) {

        CassandraConnectorConfig config = new CassandraConnectorConfig(configuration);
        Cassandra4TypeProvider provider = new Cassandra4TypeProvider();
        CassandraTypeDeserializer.init(provider.deserializers(), config.getDecimalMode(), config.getVarIntMode(),
                provider.baseTypeForReversedType());

        return new CassandraConnectorContext(new CassandraConnectorConfig(configuration));
    }
}
