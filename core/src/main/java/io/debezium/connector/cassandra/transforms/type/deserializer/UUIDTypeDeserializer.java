/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms.type.deserializer;

import static io.debezium.connector.cassandra.transforms.CassandraTypeKafkaSchemaBuilders.UUID_TYPE;

import java.nio.ByteBuffer;

import org.apache.kafka.connect.data.SchemaBuilder;

import com.datastax.oss.protocol.internal.ProtocolConstants;

import io.debezium.connector.cassandra.transforms.DebeziumTypeDeserializer;

public class UUIDTypeDeserializer extends LogicalTypeDeserializer {

    public UUIDTypeDeserializer(DebeziumTypeDeserializer deserializer, Object abstractType) {
        super(deserializer, ProtocolConstants.DataType.UUID, abstractType);
    }

    @Override
    public Object deserialize(Object abstractType, ByteBuffer bb) {
        Object value = super.deserialize(abstractType, bb);
        return formatDeserializedValue(abstractType, value);
    }

    @Override
    public SchemaBuilder getSchemaBuilder(Object abstractType) {
        return UUID_TYPE;
    }

    @Override
    public Object formatDeserializedValue(Object abstractType, Object value) {
        return value.toString();
    }
}
