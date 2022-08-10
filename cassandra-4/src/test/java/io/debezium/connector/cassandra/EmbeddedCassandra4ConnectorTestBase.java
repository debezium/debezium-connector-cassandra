/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.marshal.AbstractType;

import io.debezium.config.Configuration;
import io.debezium.connector.cassandra.CassandraConnectorTask.Cassandra4SchemaChangeListenerProvider;
import io.debezium.connector.cassandra.CassandraConnectorTask.Cassandra4SchemaLoader;
import io.debezium.connector.cassandra.transforms.CassandraTypeDeserializer;
import io.debezium.connector.cassandra.transforms.DebeziumTypeDeserializer;

public abstract class EmbeddedCassandra4ConnectorTestBase extends CassandraConnectorTestBase {

    @Override
    protected CassandraConnectorContext generateTaskContext(Configuration configuration) throws Exception {

        CassandraTypeDeserializer.init(new DebeziumTypeDeserializer() {
            @Override
            public Object deserialize(AbstractType abstractType, ByteBuffer bb) {
                return abstractType.getSerializer().deserialize(bb);
            }
        });

        return new CassandraConnectorContext(new CassandraConnectorConfig(configuration),
                new Cassandra4SchemaLoader(),
                new Cassandra4SchemaChangeListenerProvider());
    }
}
