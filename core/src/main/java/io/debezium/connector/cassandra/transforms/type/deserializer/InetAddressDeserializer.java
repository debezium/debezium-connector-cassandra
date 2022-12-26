/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms.type.deserializer;

import java.net.InetAddress;
import java.nio.ByteBuffer;

import org.apache.kafka.connect.data.SchemaBuilder;

import com.datastax.oss.protocol.internal.ProtocolConstants;

import io.debezium.connector.cassandra.transforms.CassandraTypeKafkaSchemaBuilders;
import io.debezium.connector.cassandra.transforms.DebeziumTypeDeserializer;

public class InetAddressDeserializer extends LogicalTypeDeserializer {

    public InetAddressDeserializer(DebeziumTypeDeserializer deserializer, Object abstractType) {
        super(deserializer, ProtocolConstants.DataType.INET, abstractType);
    }

    @Override
    public Object deserialize(Object abstractType, ByteBuffer bb) {
        Object value = super.deserialize(abstractType, bb);
        return formatDeserializedValue(abstractType, value);
    }

    @Override
    public SchemaBuilder getSchemaBuilder(Object abstractType) {
        return CassandraTypeKafkaSchemaBuilders.STRING_TYPE;
    }

    @Override
    public Object formatDeserializedValue(Object abstractType, Object value) {
        InetAddress inetAddress = (InetAddress) value;
        return inetAddress.toString();
    }
}
