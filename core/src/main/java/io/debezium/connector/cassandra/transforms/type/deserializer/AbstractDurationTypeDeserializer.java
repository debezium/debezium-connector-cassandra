/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms.type.deserializer;

import java.nio.ByteBuffer;

import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.connector.cassandra.transforms.CassandraTypeKafkaSchemaBuilders;
import io.debezium.connector.cassandra.transforms.DebeziumTypeDeserializer;

public abstract class AbstractDurationTypeDeserializer extends LogicalTypeDeserializer {

    public AbstractDurationTypeDeserializer(DebeziumTypeDeserializer deserializer, Integer dataType, Object abstractType) {
        super(deserializer, dataType, abstractType);
    }

    /*
     * Cassandra Duration type is serialized into micro seconds in double.
     */
    @Override
    public Object deserialize(Object abstractType, ByteBuffer bb) {
        Object value = super.deserialize(abstractType, bb);
        return formatDeserializedValue(abstractType, value);
    }

    @Override
    public SchemaBuilder getSchemaBuilder(Object abstractType) {
        return CassandraTypeKafkaSchemaBuilders.DURATION_TYPE;
    }

}
