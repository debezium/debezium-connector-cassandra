/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dse;

import io.debezium.config.Configuration;
import io.debezium.connector.cassandra.CassandraConnectorConfig;
import io.debezium.connector.cassandra.CassandraConnectorContext;
import io.debezium.connector.cassandra.CassandraConnectorTestBase;
import io.debezium.connector.cassandra.FileOffsetWriter;
import io.debezium.connector.cassandra.transforms.CassandraTypeDeserializer;

public abstract class DseConnectorTestBase extends CassandraConnectorTestBase {

    @Override
    protected CassandraConnectorContext generateTaskContext(Configuration configuration) throws Exception {

        CassandraConnectorConfig config = new CassandraConnectorConfig(configuration);
        DseTypeProvider provider = new DseTypeProvider();
        CassandraTypeDeserializer.init(provider.deserializers(), config.getDecimalMode(), config.getVarIntMode(),
                provider.baseTypeForReversedType());

        return new CassandraConnectorContext(config,
                new DseSchemaLoader(),
                new DseSchemaChangeListenerProvider(),
                new FileOffsetWriter(config));
    }
}
